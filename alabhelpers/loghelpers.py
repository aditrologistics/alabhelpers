import logging
import sys
import os
import time

_DEFAULT_LOGLEVEL = "INFO"

_default_formatstr = '%(asctime)s | %(levelname)-8s | %(message)s'
# No need to include time in cloudwatch logs
if os.environ.get("LAMBDA_TASK_ROOT"):
    _default_formatstr = '%(levelname)-8s | %(message)s'


def setup_logger(
        *,
        name: str = None,
        level: int = None,
        formatstr: str = None) -> logging.Logger:
    """
    Sets up a logger with some sensible defaults, and returns it.

    Args:
        name (str, optional): Name of logger. Defaults to None.
        level (int, optional): loglevel. Defaults to None.
            If None, loglevel is read from the environment variable LOGLEVEL.
            If no LOGLEVEL environment variable is found, the default loglevel (_DEFAULT_LOGLEVEL) is used.
        formatstr (str, optional): The logger's format string. Defaults to None.

    Returns:
        logging.Logger: Logger than can be used for logging.
    """
    logger = logging.getLogger(name)
    if level is None:
        level = logging.getLevelName(os.environ.get("LOGLEVEL", _DEFAULT_LOGLEVEL))
    if any([_.name == name for _ in logger.handlers]):
        logger.info(f"Handler {name} already initialized")
        return logger

    formatstr = formatstr or _default_formatstr
    logger.setLevel(level)
    handler = logging.StreamHandler(sys.stderr)
    handler.name = name
    handler.setLevel(level)
    formatter = logging.Formatter(formatstr)
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False  # Prevent duplicate loglines in cloud watch
    if isinstance(level, int):
        level = logging.getLevelName(level)
    logger.info(f"Configured logger '{name}' with level {level}.")
    return logger


def timeit(logger):
    def timeit_decorator(method):
        def timeit_wrapper(*args, **kw):
            ts = time.time()
            result = method(*args, **kw)
            te = time.time()
            if 'log_time' in kw:
                name = kw.get('log_name', method.__name__.upper())
                kw['log_time'][name] = int((te - ts) * 1000)
            else:
                logger.debug('%r  %2.2f ms' % \
                             (method.__name__, (te - ts) * 1000))
            return result

        return timeit_wrapper
    return timeit_decorator
