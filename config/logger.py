import logging
from typing import Optional

class Logger:
    """无需实例化即可使用的日志记录器"""
    
    _logger = None
    
    @classmethod
    def initialize(cls, name: str = __name__, log_file: Optional[str] = None, level: int = logging.INFO):
        """初始化日志记录器(只需调用一次)"""
        if cls._logger is None:
            cls._logger = logging.getLogger(name)
            cls._logger.setLevel(level)
            
            # 避免重复添加handler
            if not cls._logger.handlers:
                formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S'
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
        if cls._logger is None:
            cls.initialize()
        cls._logger.name = new_name
    
    @classmethod
    def debug(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()  # 自动初始化默认配置
        cls._logger.debug(msg, *args, **kwargs)
    
    @classmethod
    def info(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()
        cls._logger.info(msg, *args, **kwargs)
    
    @classmethod
    def warning(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()
        cls._logger.warning(msg, *args, **kwargs)
    
    @classmethod
    def error(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()
        cls._logger.error(msg, *args, **kwargs)
    
    @classmethod
    def exception(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()
        cls._logger.exception(msg, *args, **kwargs)
    
    @classmethod
    def critical(cls, msg: str, *args, **kwargs):
        if cls._logger is None:
            cls.initialize()
        cls._logger.critical(msg, *args, **kwargs)

