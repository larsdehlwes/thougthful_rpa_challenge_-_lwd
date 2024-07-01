import sys
import logging

def get_and_configure_logger(logger_name: str = "formatted_logger", loglevel: int = logging.INFO):
    """ Returns a logger with a custom format and log level. """
    FORMAT = "{ %(levelname)s - %(lineno)d: - %(funcName).10s } %(message)s"
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter(FORMAT))
    logger = logging.getLogger(logger_name)
    logger.addHandler(h)
    logger.setLevel(loglevel)
    logger.propagate = False
    return logger
