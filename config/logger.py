from loguru import logger
from typing import Optional
import logging


class Logger:
    """无需实例化即可使用的日志记录器"""

    _logger = None

    @classmethod
    def initialize(
        cls,
        name: str = __name__,
        log_file: Optional[str] = None,
        level: int = logging.INFO,
    ):
        """初始化日志记录器(只需调用一次)"""
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(level)

            # 避免重复添加handler
            if not cls._logger.handlers:
                formatter = logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
                    datefmt="%Y-%m-%d %H:%M:%S",
                )

                # 控制台处理器
                console_handler = logging.StreamHandler()
                console_handler.setLevel(level)
                console_handler.setFormatter(formatter)
                cls._logger.addHandler(console_handler)

                # 文件处理器(如果指定了日志文件)
                if log_file:
                    file_handler = logging.FileHandler(log_file)
                    file_handler.setLevel(level)
                    file_handler.setFormatter(formatter)
                    cls._logger.addHandler(file_handler)

    @classmethod
    def set_name(cls, new_name: str):
        """设置日志记录器的名称"""
        pass  # loguru自动处理

    @staticmethod
    def debug(msg: str, *args, **kwargs):
        """记录debug级别日志"""
        logger.debug(msg, *args, **kwargs)

    @staticmethod
    def info(msg: str, *args, **kwargs):
        """记录info级别日志"""
        logger.info(msg, *args, **kwargs)

    @staticmethod
    def warning(msg: str, *args, **kwargs):
        """记录warning级别日志"""
        logger.warning(msg, *args, **kwargs)

    @staticmethod
    def error(msg: str, *args, **kwargs):
        """记录error级别日志"""
        logger.error(msg, *args, **kwargs)

    @staticmethod
    def exception(msg: str, *args, **kwargs):
        """记录exception级别日志"""
        logger.exception(msg, *args, **kwargs)

    @staticmethod
    def critical(msg: str, *args, **kwargs):
        """记录critical级别日志"""
        logger.critical(msg, *args, **kwargs)
