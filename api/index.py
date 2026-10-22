"""Entrada da função Python na Vercel: todas as rotas caem aqui (ver rewrites em vercel.json)."""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from rios import app  # noqa: E402, F401
