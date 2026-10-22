"""API de níveis de rios (telemetria da ANA, sem chave) para o front-rios.

Fonte: https://telemetriaws1.ana.gov.br/ServiceANA.asmx (leituras a cada 15 min, nível em cm).
A ANA é lenta (~1,5 min por mês de telemetria), então tudo é lido de um cache em disco:
`cache_rios/` vem versionado no repositório e é atualizado pelo `aquecer_cache.py`
(rodado todo dia pelo GitHub Actions). Na Vercel o disco do projeto é somente leitura:
o que a API baixar na hora vai para /tmp e vale enquanto a instância estiver viva.

O histórico longo vem da série convencional (HidroSerieHistorica, desde ~1900, mas
só até onde a ANA já consolidou, ~2014) completada pela telemetria mês a mês.
"""
import asyncio
import json
import logging
import os
import statistics
import time
import xml.etree.ElementTree as ET
from contextlib import nullcontext
from datetime import date, datetime, timedelta
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

RAIZ = Path(__file__).resolve().parent
SEMENTE = RAIZ / "cache_rios"  # cache versionado no repositório
NA_VERCEL = bool(os.environ.get("VERCEL"))
CACHE = Path("/tmp/cache_rios") if NA_VERCEL else SEMENTE  # na Vercel só /tmp aceita escrita
INDICE = "indice.json"  # arquivo do cache -> quando foi baixado (o git troca o mtime pela hora do clone)

log = logging.getLogger("rios")

URL_ANA = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
URL_HISTORICO = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroSerieHistorica"
CACHE_MES_ATUAL_S = 3600
CACHE_HISTORICO_S = 30 * 86400  # a série consolidada muda raramente
# Quanto uma requisição espera a ANA antes de responder com o que tem (o resto vai em
# `meses_ainda_baixando`). Precisa caber no maxDuration da função (vercel.json).
PRAZO_RESPOSTA_S = float(os.environ.get("RIOS_PRAZO_S", 40 if NA_VERCEL else 240))
MAX_SIMULTANEAS = 3
MAX_SIMULTANEAS_FUNDO = 1
TENTATIVAS = 4
_SEM: asyncio.Semaphore | None = None
_SEM_FUNDO: asyncio.Semaphore | None = None
_CLIENTE: httpx.AsyncClient | None = None
_TAREFAS: dict[str, asyncio.Task] = {}
_MEMORIA: dict[str, tuple[float, dict]] = {}  # arquivo -> (baixado_em, conteúdo), evita reler o disco

ESTACOES = {
    "manaus": {"rio": "Rio Negro", "cidade": "Manaus", "uf": "AM", "codigo": "14990000"},
    "itacoatiara": {"rio": "Rio Amazonas", "cidade": "Itacoatiara", "uf": "AM", "codigo": "16030000"},
    "tabatinga": {"rio": "Rio Solimões", "cidade": "Tabatinga", "uf": "AM", "codigo": "10100000"},
    "obidos": {"rio": "Rio Amazonas", "cidade": "Óbidos", "uf": "PA", "codigo": "17050001"},
    "santarem": {"rio": "Rio Tapajós", "cidade": "Santarém", "uf": "PA", "codigo": "17900000"},
    "maraba": {"rio": "Rio Tocantins", "cidade": "Marabá", "uf": "PA", "codigo": "29050000"},
    "porto_velho": {"rio": "Rio Madeira", "cidade": "Porto Velho", "uf": "RO", "codigo": "15400000"},
    "rio_branco": {"rio": "Rio Acre", "cidade": "Rio Branco", "uf": "AC", "codigo": "13600002"},
    "boa_vista": {"rio": "Rio Branco", "cidade": "Boa Vista", "uf": "RR", "codigo": "14620000"},
    "ladario": {"rio": "Rio Paraguai", "cidade": "Ladário", "uf": "MS", "codigo": "66825000"},
}


def _carregar_indice() -> dict[str, float]:
    try:
        return json.loads((SEMENTE / INDICE).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}


_INDICE = _carregar_indice()


def _sem_acento(texto: str) -> str:
    trocas = str.maketrans("áàâãéêíóôõúç", "aaaaeeiooouc")
    return texto.lower().strip().translate(trocas).replace(" ", "_").replace("-", "_")


def _resolver(estacao: str) -> tuple[str, dict]:
    chave = _sem_acento(estacao)
    if chave in ESTACOES:
        return chave, ESTACOES[chave]
    for k, e in ESTACOES.items():
        if chave in (e["codigo"], _sem_acento(e["cidade"])) or chave == _sem_acento(e["rio"]):
            return k, e
    validas = ", ".join(f"{k} ({e['rio']}/{e['uf']})" for k, e in ESTACOES.items())
    raise ValueError(f"Estação '{estacao}' não encontrada. Use uma destas: {validas}.")


def _parse_xml(conteudo: str) -> list[tuple[str, float]]:
    """Leituras válidas (data-hora ISO, nível em cm). Nível 0 ou vazio é descartado."""
    leituras = []
    for item in ET.fromstring(conteudo).iter("DadosHidrometereologicos"):
        nivel = (item.findtext("Nivel") or "").strip()
        quando = (item.findtext("DataHora") or "").strip()
        try:
            valor = float(nivel)
        except ValueError:
            continue
        if valor > 0 and quando:
            leituras.append((quando, valor))
    return leituras


def _semaforo() -> asyncio.Semaphore:
    """Limite global de requisições simultâneas à ANA (ela responde 429 se passar disso)."""
    global _SEM
    if _SEM is None:
        _SEM = asyncio.Semaphore(MAX_SIMULTANEAS)
    return _SEM


def _semaforo_fundo() -> asyncio.Semaphore:
    """Fila dos downloads de segundo plano, dentro do limite global."""
    global _SEM_FUNDO
    if _SEM_FUNDO is None:
        _SEM_FUNDO = asyncio.Semaphore(MAX_SIMULTANEAS_FUNDO)
    return _SEM_FUNDO


def _cliente() -> httpx.AsyncClient:
    """Cliente compartilhado: downloads em segundo plano sobrevivem ao fim da requisição."""
    global _CLIENTE
    if _CLIENTE is None or _CLIENTE.is_closed:
        _CLIENTE = httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=30.0))
    return _CLIENTE


def _tarefa(chave: str, fabrica) -> asyncio.Task:
    """Um download por chave: requisições simultâneas esperam a mesma tarefa em vez de repeti-la."""
    tarefa = _TAREFAS.get(chave)
    if tarefa is None or tarefa.done():
        tarefa = asyncio.create_task(fabrica())
        _TAREFAS[chave] = tarefa

        def _fim(t: asyncio.Task) -> None:
            if _TAREFAS.get(chave) is t:
                del _TAREFAS[chave]
            if not t.cancelled() and t.exception():
                log.warning("Download %s falhou: %s", chave, t.exception())

        tarefa.add_done_callback(_fim)
    return tarefa


async def _aguardar(tarefas: list[asyncio.Task], ate: float) -> None:
    """Espera as tarefas até o instante `ate` (monotonic); o que não terminar segue rodando."""
    pendentes = {t for t in tarefas if not t.done()}
    if pendentes and time.monotonic() < ate:
        await asyncio.wait(pendentes, timeout=ate - time.monotonic())


async def _baixar(
    cliente: httpx.AsyncClient, codigo: str, inicio: date, fim: date, fundo: bool = False
) -> list[tuple[str, float]]:
    params = {
        "codEstacao": codigo,
        "dataInicio": inicio.strftime("%d/%m/%Y"),
        "dataFim": fim.strftime("%d/%m/%Y"),
    }
    for tentativa in range(TENTATIVAS):
        async with _semaforo_fundo() if fundo else nullcontext(), _semaforo():
            r = await cliente.get(URL_ANA, params=params)
        if r.status_code != 429 and r.status_code < 500:
            break
        if tentativa < TENTATIVAS - 1:
            espera = r.headers.get("Retry-After", "")
            espera_s = float(espera) if espera.isdigit() else 3 * 2**tentativa
            log.info("ANA respondeu %s; nova tentativa em %.0f s", r.status_code, espera_s)
            await asyncio.sleep(min(espera_s, 30))
    r.raise_for_status()
    return _parse_xml(r.text)


def _ler_json(nome: str) -> tuple[dict | None, float]:
    """Conteúdo de um arquivo do cache (None se não existe) e quando foi baixado.

    Procura primeiro no cache gravável (/tmp na Vercel) e depois na semente do repositório.
    """
    arquivo = CACHE / nome
    if not arquivo.exists():
        arquivo = SEMENTE / nome
        if not arquivo.exists():
            return None, 0.0
    baixado_em = _INDICE.get(nome) or arquivo.stat().st_mtime
    em_memoria = _MEMORIA.get(nome)
    if em_memoria and em_memoria[0] == baixado_em:
        return em_memoria[1], baixado_em
    dados = json.loads(arquivo.read_text(encoding="utf-8"))
    _MEMORIA[nome] = (baixado_em, dados)
    return dados, baixado_em


def _gravar_json(nome: str, dados: dict) -> None:
    CACHE.mkdir(parents=True, exist_ok=True)
    (CACHE / nome).write_text(json.dumps(dados), encoding="utf-8")
    _INDICE[nome] = time.time()
    _MEMORIA[nome] = (_INDICE[nome], dados)
    if CACHE == SEMENTE:  # fora da Vercel o índice vai junto para o git
        (SEMENTE / INDICE).write_text(json.dumps(_INDICE, indent=0, sort_keys=True), encoding="utf-8")


def _ler_cache_mes(codigo: str, ano: int, mes: int) -> tuple[dict | None, bool]:
    """Conteúdo do mês em cache (None se nunca baixado) e se ele está vencido.

    Um mês só é definitivo se foi baixado depois de o mês acabar; antes disso
    (mês corrente, ou mês que fechou depois do último download) vence em CACHE_MES_ATUAL_S.
    """
    dados, baixado_em = _ler_json(f"{codigo}_{ano:04d}-{mes:02d}.json")
    if dados is None:
        return None, True
    fim_do_mes = datetime(ano + (mes == 12), mes % 12 + 1, 1).timestamp()
    vencido = baixado_em < fim_do_mes and time.time() - baixado_em >= CACHE_MES_ATUAL_S
    return dados, vencido


async def _buscar_mes(cliente: httpx.AsyncClient, codigo: str, ano: int, mes: int, fundo: bool = False) -> dict:
    """Agregado diário {AAAA-MM-DD: [mediana, mín, máx]} em cm, de um mês, com cache.

    Se já há um arquivo vencido, baixa só a partir do penúltimo dia em cache e mescla.
    """
    dados, vencido = _ler_cache_mes(codigo, ano, mes)
    if dados is not None and not vencido:
        return dados

    hoje = date.today()
    inicio = date(ano, mes, 1)
    proximo = date(ano + (mes == 12), mes % 12 + 1, 1)
    if dados:
        inicio = max(inicio, date.fromisoformat(max(dados)) - timedelta(days=1))
    leituras = await _baixar(cliente, codigo, inicio, min(proximo, hoje + timedelta(days=1)), fundo)
    prefixo = f"{ano:04d}-{mes:02d}"
    por_dia: dict[str, list[float]] = {}
    for quando, valor in leituras:
        if quando.startswith(prefixo):
            por_dia.setdefault(quando[:10], []).append(valor)
    # Mediana: leituras travadas ou picos isolados do sensor não puxam o valor do dia.
    novos = {d: [round(statistics.median(v), 1), min(v), max(v)] for d, v in por_dia.items()}
    diario = dict(sorted({**(dados or {}), **novos}.items()))
    _gravar_json(f"{codigo}_{prefixo}.json", diario)
    return diario


def _meses(inicio: date, fim: date) -> list[tuple[int, int]]:
    meses = []
    a, m = inicio.year, inicio.month
    while (a, m) <= (fim.year, fim.month):
        meses.append((a, m))
        a, m = (a + 1, 1) if m == 12 else (a, m + 1)
    return meses


def _tarefa_mes(codigo: str, ano: int, mes: int, fundo: bool = False) -> asyncio.Task:
    return _tarefa(f"{codigo}_{ano:04d}-{mes:02d}", lambda: _buscar_mes(_cliente(), codigo, ano, mes, fundo))


async def _serie_diaria(codigo: str, inicio: date, fim: date) -> tuple[dict, list[str], list[str], list[str]]:
    """Série diária do período, meses que a ANA não entregou, meses ainda baixando e meses
    devolvidos do cache vencido enquanto atualizam em segundo plano.

    Só espera a ANA pelos meses que não têm nada em cache.
    """
    serie: dict[str, list[float]] = {}
    falhas, pendentes, atualizando = [], [], []
    esperar: list[tuple[str, asyncio.Task]] = []
    for a, m in _meses(inicio, fim):
        rotulo = f"{a:04d}-{m:02d}"
        dados, vencido = _ler_cache_mes(codigo, a, m)
        tarefa = _tarefa_mes(codigo, a, m) if vencido else None
        if dados is None:
            esperar.append((rotulo, tarefa))
            continue
        serie.update(dados)
        if vencido:
            atualizando.append(rotulo)
    await _aguardar([t for _, t in esperar], time.monotonic() + PRAZO_RESPOSTA_S)
    for rotulo, tarefa in esperar:
        if not tarefa.done():
            pendentes.append(rotulo)
        elif tarefa.cancelled() or tarefa.exception():
            falhas.append(rotulo)
        else:
            serie.update(tarefa.result())
    inicio_s, fim_s = inicio.isoformat(), fim.isoformat()
    serie = {d: v for d, v in sorted(serie.items()) if inicio_s <= d <= fim_s}
    return serie, falhas, pendentes, atualizando


def _parse_historico(conteudo: str) -> dict[str, float]:
    """Cota diária {AAAA-MM-DD: cm} da série convencional; mês consistido (nível 2) vence o bruto (1)."""
    meses: dict[str, tuple[int, dict[str, float]]] = {}
    for item in ET.fromstring(conteudo).iter("SerieHistorica"):
        quando = (item.findtext("DataHora") or "")[:10]
        nivel = int(item.findtext("NivelConsistencia") or 1)
        if not quando or nivel < meses.get(quando, (0, {}))[0]:
            continue
        ano, mes = int(quando[:4]), int(quando[5:7])
        dias = {}
        for dia in range(1, 32):
            texto = (item.findtext(f"Cota{dia:02d}") or "").strip()
            try:
                valor = float(texto)
                dias[date(ano, mes, dia).isoformat()] = valor
            except ValueError:  # vazio ou dia que não existe no mês
                continue
        meses[quando] = (nivel, dias)
    return {d: v for _, dias in meses.values() for d, v in dias.items() if v > 0}


async def _buscar_historico(codigo: str, fundo: bool = False) -> dict[str, float]:
    """Série convencional inteira da estação, com cache de 30 dias (o download leva minutos)."""
    nome = f"{codigo}_historico.json"
    dados, baixado_em = _ler_json(nome)
    if dados is not None and time.time() - baixado_em < CACHE_HISTORICO_S:
        return dados
    params = {"codEstacao": codigo, "dataInicio": "", "dataFim": "", "tipoDados": "1", "nivelConsistencia": ""}
    async with _semaforo_fundo() if fundo else nullcontext(), _semaforo():
        r = await _cliente().get(URL_HISTORICO, params=params)
    r.raise_for_status()
    diario = dict(sorted(_parse_historico(r.text).items()))
    _gravar_json(nome, diario)
    return diario


def _tarefa_historico(codigo: str, fundo: bool = False) -> asyncio.Task:
    return _tarefa(f"{codigo}_historico", lambda: _buscar_historico(codigo, fundo))


def _inicio_telemetria(historico: dict[str, float], hoje: date) -> date:
    """Onde a telemetria precisa completar: mês seguinte ao fim da série convencional."""
    if not historico:
        return date(hoje.year - 10, 1, 1)
    ultimo = date.fromisoformat(max(historico))
    return date(ultimo.year + (ultimo.month == 12), ultimo.month % 12 + 1, 1)


def _anos_atras(d: date, anos: int) -> date:
    try:
        return d.replace(year=d.year - anos)
    except ValueError:  # 29/fev
        return d.replace(year=d.year - anos, day=28)


def _m(cm: float) -> float:
    return round(cm / 100, 2)


def _valor_proximo(serie: dict, alvo: date, janela: int = 5) -> float | None:
    for delta in range(janela + 1):
        for d in (alvo - timedelta(days=delta), alvo + timedelta(days=delta)):
            if d.isoformat() in serie:
                return _m(serie[d.isoformat()][0])
    return None


def _resumo(chave: str, est: dict, serie: dict, hoje: date) -> dict:
    if not serie:
        raise ValueError(f"A ANA não devolveu leituras para {est['rio']} em {est['cidade']} no período.")
    dias = list(serie)
    medias = [v[0] for v in serie.values()]
    d_max = max(dias, key=lambda d: serie[d][2])
    d_min = min(dias, key=lambda d: serie[d][1])
    ultimo = dias[-1]

    recentes = [serie[d][0] for d in dias[-15:]]
    variacao_15d = _m(recentes[-1] - recentes[0]) if len(recentes) > 1 else 0.0
    tendencia = "estável" if abs(variacao_15d) < 0.1 else ("subindo" if variacao_15d > 0 else "descendo")

    mensal: dict[str, list[list[float]]] = {}
    for d, v in serie.items():
        mensal.setdefault(d[:7], []).append(v)
    resumo_mensal = [
        {
            "mes": mes,
            "media_m": _m(statistics.fmean(x[0] for x in vs)),
            "min_m": _m(min(x[1] for x in vs)),
            "max_m": _m(max(x[2] for x in vs)),
        }
        for mes, vs in mensal.items()
    ]

    atual = _m(serie[ultimo][0])
    ha_1_ano = _valor_proximo(serie, _anos_atras(hoje, 1))
    ha_2_anos = _valor_proximo(serie, _anos_atras(hoje, 2))
    return {
        "estacao": chave,
        "rio": est["rio"],
        "cidade": est["cidade"],
        "uf": est["uf"],
        "codigo_ana": est["codigo"],
        "periodo": {"inicio": dias[0], "fim": ultimo, "dias_com_dados": len(dias)},
        "atual_m": atual,
        "data_atual": ultimo,
        "maximo_m": _m(serie[d_max][2]),
        "data_maximo": d_max,
        "minimo_m": _m(serie[d_min][1]),
        "data_minimo": d_min,
        "media_m": _m(statistics.fmean(medias)),
        "ha_1_ano_m": ha_1_ano,
        "diferenca_1_ano_m": round(atual - ha_1_ano, 2) if ha_1_ano is not None else None,
        "ha_2_anos_m": ha_2_anos,
        "diferenca_2_anos_m": round(atual - ha_2_anos, 2) if ha_2_anos is not None else None,
        "tendencia_15d": tendencia,
        "variacao_15d_m": variacao_15d,
        "resumo_mensal": resumo_mensal,
        "observacao": "Cota da régua da ANA (referência local da estação), não profundidade do rio.",
    }


app = FastAPI(title="Rios (ANA)")
# O front chama a API pelo rewrite /api da Vercel (mesma origem); o CORS libera chamadas diretas.
app.add_middleware(
    CORSMiddleware,
    allow_origins=os.environ.get("RIOS_ORIGENS", "*").split(","),
    allow_methods=["GET"],
)


def _resolver_http(estacao: str) -> tuple[str, dict]:
    try:
        return _resolver(estacao)
    except ValueError as erro:
        raise HTTPException(404, str(erro)) from erro


@app.get("/")
@app.get("/api")
def raiz() -> dict:
    return {"api": "rios", "rotas": ["/api/estacoes", "/api/serie?estacao=manaus&anos=3", "/api/referencia?estacao=manaus"]}


@app.get("/api/estacoes")
def api_estacoes(uf: str = "") -> list[dict]:
    """Estações disponíveis (chave, rio, cidade, UF e código ANA). Filtre por UF, ex.: 'AM'."""
    uf = uf.strip().upper()
    return [{"estacao": k, **e} for k, e in ESTACOES.items() if not uf or e["uf"] == uf]


@app.get("/api/serie")
async def api_serie(estacao: str = "manaus", anos: int = 2) -> dict:
    """Resumo de N anos atrás até hoje mais a série diária em metros:
    [[dia, mediana, mín, máx], ...] para o front desenhar o gráfico."""
    anos = max(1, min(int(anos), 5))
    chave, est = _resolver_http(estacao)
    hoje = date.today()
    serie, falhas, pendentes, atualizando = await _serie_diaria(est["codigo"], _anos_atras(hoje, anos), hoje)
    try:
        resumo = _resumo(chave, est, serie, hoje)
    except ValueError as erro:
        raise HTTPException(503, str(erro)) from erro
    resumo.update(meses_sem_dados=falhas, meses_ainda_baixando=pendentes, atualizando=atualizando)
    resumo["serie"] = [[d, _m(v[0]), _m(v[1]), _m(v[2])] for d, v in serie.items()]
    return resumo


def _diario_em_cache(codigo: str) -> tuple[dict[str, float], bool]:
    """Cota diária (cm) de toda a história que já está em cache, sem esperar a ANA.

    O que falta é pedido em segundo plano; o segundo valor diz se a série veio completa.
    """
    historico, _ = _ler_json(f"{codigo}_historico.json")
    completo = historico is not None
    if historico is None:
        historico = {}
        _tarefa_historico(codigo)
    diario: dict[str, float] = {}
    for a, m in _meses(_inicio_telemetria(historico, date.today()), date.today()):
        dados, vencido = _ler_cache_mes(codigo, a, m)
        if vencido:
            _tarefa_mes(codigo, a, m, fundo=True)
        if dados is None:
            completo = False
            continue
        diario.update({d: v[0] for d, v in dados.items()})
    diario.update(historico)  # consistido vence telemetria
    return diario, completo


@app.get("/api/referencia")
async def api_referencia(estacao: str = "manaus") -> dict:
    """Régua de comparação de toda a história da estação: recordes, mediana, percentis
    (de 5 em 5 %) e a mediana de cada mês do ano (o "normal para a época")."""
    chave, est = _resolver_http(estacao)
    diario, completo = _diario_em_cache(est["codigo"])
    if not diario:
        raise HTTPException(503, "Histórico ainda baixando da ANA; tente de novo em alguns minutos.")
    valores = sorted(diario.values())
    d_max = max(diario, key=diario.get)
    d_min = min(diario, key=diario.get)
    por_mes: dict[int, list[float]] = {}
    for d, v in diario.items():
        por_mes.setdefault(int(d[5:7]), []).append(v)
    return {
        "estacao": chave,
        "rio": est["rio"],
        "cidade": est["cidade"],
        "periodo": {"inicio": min(diario), "fim": max(diario), "dias_com_dados": len(diario)},
        "completo": completo,
        "recorde_maximo_m": _m(diario[d_max]),
        "data_recorde_maximo": d_max,
        "recorde_minimo_m": _m(diario[d_min]),
        "data_recorde_minimo": d_min,
        "mediana_m": _m(statistics.median(valores)),
        "percentis_m": [_m(valores[round(p / 100 * (len(valores) - 1))]) for p in range(0, 101, 5)],
        "mediana_por_mes_m": [_m(statistics.median(por_mes[m])) if m in por_mes else None for m in range(1, 13)],
    }
