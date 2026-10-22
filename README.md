# back-rios

API (FastAPI) com o nível dos rios da Amazônia e do Pantanal, a partir da telemetria da ANA.
É o back-end do [front-end-rios](https://github.com/JosiasAz/front-end-rios).

| Rota | O que devolve |
|---|---|
| `GET /api/estacoes?uf=AM` | Estações disponíveis |
| `GET /api/serie?estacao=manaus&anos=3` | Resumo de 1 a 5 anos + série diária em metros |
| `GET /api/referencia?estacao=manaus` | Recordes, mediana, percentis e "normal para a época" de toda a história |

## Como funciona o cache

A ANA demora ~1,5 min por mês de dados, então a API lê tudo de `cache_rios/`, que fica no repositório.
O workflow `.github/workflows/atualizar-cache.yml` roda `aquecer_cache.py` todo dia, baixa o que falta
e faz commit — e cada commit gera um deploy novo na Vercel. `cache_rios/indice.json` guarda quando cada
arquivo foi baixado (o git não preserva a data dos arquivos).

Na Vercel o disco é somente leitura: se faltar algum mês, a API espera a ANA por até 40 s
(`RIOS_PRAZO_S`), grava em `/tmp` e devolve o que não deu tempo em `meses_ainda_baixando`.

## Rodar localmente

```bash
python -m venv .venv
.venv\Scripts\activate            # Linux/Mac: source .venv/bin/activate
pip install -r requirements.txt uvicorn
uvicorn rios:app --port 8000      # o proxy do front (ng serve) aponta para cá
```

Para completar o cache na sua máquina: `python aquecer_cache.py` (`RIOS_LIMITE_MIN=30` limita o tempo).

## Deploy na Vercel

1. Crie o repositório no GitHub e faça o push deste diretório.
2. Na Vercel: **Add New → Project**, importe o repositório. A Vercel detecta a FastAPI pelo `pyproject.toml` (`entrypoint = "rios:app"`). Não precisa de build command nem variáveis.
3. A função roda em São Paulo (`gru1`) com até 60 s por requisição (`vercel.json`).
4. No GitHub, rode o workflow **Atualizar cache da ANA** uma vez à mão (aba *Actions* → *Run workflow*) para completar o cache.

Variáveis opcionais: `RIOS_PRAZO_S` (espera máxima pela ANA por requisição) e `RIOS_ORIGENS`
(origens CORS separadas por vírgula; padrão `*`).

> Se o repositório for **privado** no plano Hobby, a Vercel pode bloquear o deploy dos commits feitos pelo
> `github-actions[bot]`. Nesse caso deixe o repositório público ou crie um *Deploy Hook* na Vercel e chame-o
> no fim do workflow.
