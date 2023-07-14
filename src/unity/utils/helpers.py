import logging
import sys

def get_logger():
  logger = logging.getLogger("unity-utils")
  if not logger.hasHandlers():
      loglevel = logging.INFO
      handler = logging.StreamHandler(sys.stdout)
      formatter = logging.Formatter("[%(asctime)s]-[%(name)s]-[%(levelname)s] %(message)s (%(filename)s:%(lineno)d)")
      handler.setFormatter(formatter)
      logger.addHandler(handler)
      logger.setLevel(loglevel)
      logger.handler_set = True
  return logger
