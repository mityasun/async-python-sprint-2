import logging

logging.basicConfig(
    level="INFO",
    format='%(asctime)s, %(levelname)s, %(funcName)s, %(message)s',
    handlers=[
        logging.FileHandler("report.log", mode='w', encoding='UTF-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)


def coroutine(f):
    def wrap(*args, **kwargs):
        gen = f(*args, **kwargs)
        gen.send(None)
        return gen
    return wrap
