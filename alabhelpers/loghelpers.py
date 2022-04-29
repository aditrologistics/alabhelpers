import logging
import sys
import os


_DEFAULT_LOGLEVEL = "INFO"


def setup_logger(
        *,
        name: str = None,
        level: int = None,
        formatstr: str = None) -> logging.Logger:
    logger = logging.getLogger(name)
    if level is None:
        level = logging.getLevelName(os.environ.get("LOGLEVEL", _DEFAULT_LOGLEVEL))
    if any([_.name == name for _ in logger.handlers]):
        logger.info(f"Handler {name} already initialized")
        return logger

    formatstr = formatstr or '%(asctime)s | %(levelname)-8s | %(message)s'
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stderr)
    handler.name = name
    handler.setLevel(level)
    formatter = logging.Formatter(formatstr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False  # Prevent duplicate loglines in cloud watch
    logger.info(f"Configuring a logger '{name}' for {level}")
    return logger
