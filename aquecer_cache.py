"""Baixa da ANA tudo o que falta em cache_rios/ (rodado todo dia pelo GitHub Actions).

Primeiro os últimos 5 anos de cada estação (o que /api/serie usa), depois o histórico
longo (o que /api/referencia usa). Para no limite de tempo e guarda o que já baixou:
a próxima execução continua de onde parou.

    python aquecer_cache.py              # limite padrão: 300 min
    RIOS_LIMITE_MIN=20 python aquecer_cache.py
"""
import asyncio
import logging
import os
from datetime import date

import rios

log = logging.getLogger("aquecer")


async def _meses(codigo: str, meses: list[tuple[int, int]]) -> None:
    resultados = await asyncio.gather(*[rios._tarefa_mes(codigo, a, m) for a, m in meses], return_exceptions=True)
    falhas = sum(isinstance(r, Exception) for r in resultados)
    if falhas:
        log.warning("%s: %d de %d meses falharam", codigo, falhas, len(meses))


async def aquecer() -> None:
    hoje = date.today()
    recentes = rios._meses(rios._anos_atras(hoje, 5), hoje)
    for chave, est in rios.ESTACOES.items():
        await _meses(est["codigo"], recentes)
        log.info("%s: últimos 5 anos prontos", chave)
    for chave, est in rios.ESTACOES.items():
        try:
            historico = await rios._tarefa_historico(est["codigo"])
        except Exception as erro:  # noqa: BLE001 - segue para as outras estações
            log.warning("%s: histórico falhou: %s", chave, erro)
            historico = {}
        await _meses(est["codigo"], rios._meses(rios._inicio_telemetria(historico, hoje), hoje))
        log.info("%s: histórico completo", chave)


async def main() -> None:
    limite_s = float(os.environ.get("RIOS_LIMITE_MIN", 300)) * 60
    try:
        await asyncio.wait_for(aquecer(), timeout=limite_s)
        log.info("Cache completo.")
    except TimeoutError:
        log.info("Limite de tempo atingido; o resto fica para a próxima execução.")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    asyncio.run(main())
