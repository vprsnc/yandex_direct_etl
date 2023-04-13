from loguru import logger
from setup import e17154444_TOKEN
from direct import get_stats
from sender import send_stats

ACC = "e17154444"

logger.add(f"logs/{ACC}.log", level="DEBUG")

get_stats(ACC, e17154444_TOKEN)
send_stats(ACC)
