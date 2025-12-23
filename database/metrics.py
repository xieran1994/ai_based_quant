"""股票技术指标"""

import numpy as np
import pandas as pd
from typing import List, Union, Tuple


class TechnicalIndicators:
    """股票常用技术指标计算类"""

    @staticmethod
    def sma(data: Union[List, np.ndarray, pd.Series], period: int = 20) -> np.ndarray:
        """
        简单移动平均线 (Simple Moving Average)

        Args:
            data: 价格数据
            period: 周期

        Returns:
            SMA值
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)
        return pd.Series(data).rolling(window=period).mean().values

    @staticmethod
    def ema(data: Union[List, np.ndarray, pd.Series], period: int = 20) -> np.ndarray:
        """
        指数移动平均线 (Exponential Moving Average)

        Args:
            data: 价格数据
            period: 周期

        Returns:
            EMA值
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)
        return pd.Series(data).ewm(span=period, adjust=False).mean().values

    @staticmethod
    def rsi(data: Union[List, np.ndarray, pd.Series], period: int = 14) -> np.ndarray:
        """
        相对强弱指数 (Relative Strength Index)

        Args:
            data: 价格数据
            period: 周期

        Returns:
            RSI值 (0-100)
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)

        delta = np.diff(data)
        gain = np.where(delta > 0, delta, 0)
        loss = np.where(delta < 0, -delta, 0)

        avg_gain = pd.Series(gain).rolling(window=period).mean().values
        avg_loss = pd.Series(loss).rolling(window=period).mean().values

        rs = avg_gain / (avg_loss + 1e-10)
        rsi = 100 - (100 / (1 + rs))

        return rsi

    @staticmethod
    def macd(
        data: Union[List, np.ndarray, pd.Series],
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        MACD指标 (Moving Average Convergence Divergence)

        Args:
            data: 价格数据
            fast: 快速EMA周期
            slow: 慢速EMA周期
            signal: 信号线EMA周期

        Returns:
            (MACD线, 信号线, 柱状图)
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)

        ema_fast = pd.Series(data).ewm(span=fast, adjust=False).mean().values
        ema_slow = pd.Series(data).ewm(span=slow, adjust=False).mean().values

        macd_line = ema_fast - ema_slow
        signal_line = pd.Series(macd_line).ewm(span=signal, adjust=False).mean().values
        histogram = macd_line - signal_line

        return macd_line, signal_line, histogram

    @staticmethod
    def bollinger_bands(
        data: Union[List, np.ndarray, pd.Series], period: int = 20, std_dev: float = 2.0
    ) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
        """
        布林带 (Bollinger Bands)

        Args:
            data: 价格数据
            period: 周期
            std_dev: 标准差倍数

        Returns:
            (上轨, 中线, 下轨)
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)

        series = pd.Series(data)
        middle_band = series.rolling(window=period).mean().values
        std = series.rolling(window=period).std().values

        upper_band = middle_band + (std * std_dev)
        lower_band = middle_band - (std * std_dev)

        return upper_band, middle_band, lower_band

    @staticmethod
    def stochastic(
        high: Union[List, np.ndarray, pd.Series],
        low: Union[List, np.ndarray, pd.Series],
        close: Union[List, np.ndarray, pd.Series],
        period: int = 14,
    ) -> Tuple[np.ndarray, np.ndarray]:
        """
        随机指标 (Stochastic Oscillator)

        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            period: 周期

        Returns:
            (K线, D线)
        """
        if isinstance(high, (list, pd.Series)):
            high = np.array(high)
        if isinstance(low, (list, pd.Series)):
            low = np.array(low)
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)

        high_series = pd.Series(high)
        low_series = pd.Series(low)

        highest_high = high_series.rolling(window=period).max().values
        lowest_low = low_series.rolling(window=period).min().values

        k_line = 100 * (close - lowest_low) / (highest_high - lowest_low + 1e-10)
        d_line = pd.Series(k_line).rolling(window=3).mean().values

        return k_line, d_line

    @staticmethod
    def atr(
        high: Union[List, np.ndarray, pd.Series],
        low: Union[List, np.ndarray, pd.Series],
        close: Union[List, np.ndarray, pd.Series],
        period: int = 14,
    ) -> np.ndarray:
        """
        平均真实波幅 (Average True Range)

        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            period: 周期

        Returns:
            ATR值
        """
        if isinstance(high, (list, pd.Series)):
            high = np.array(high)
        if isinstance(low, (list, pd.Series)):
            low = np.array(low)
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)

        tr1 = high - low
        tr2 = np.abs(high - np.roll(close, 1))
        tr3 = np.abs(low - np.roll(close, 1))

        tr = np.maximum(tr1, np.maximum(tr2, tr3))
        atr = pd.Series(tr).rolling(window=period).mean().values

        return atr

    @staticmethod
    def obv(
        close: Union[List, np.ndarray, pd.Series],
        volume: Union[List, np.ndarray, pd.Series],
    ) -> np.ndarray:
        """
        成交量指标 (On-Balance Volume)

        Args:
            close: 收盘价
            volume: 成交量

        Returns:
            OBV值
        """
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)
        if isinstance(volume, (list, pd.Series)):
            volume = np.array(volume)

        obv = np.zeros(len(close))
        obv[0] = volume[0]

        for i in range(1, len(close)):
            if close[i] > close[i - 1]:
                obv[i] = obv[i - 1] + volume[i]
            elif close[i] < close[i - 1]:
                obv[i] = obv[i - 1] - volume[i]
            else:
                obv[i] = obv[i - 1]

        return obv

    @staticmethod
    def roc(data: Union[List, np.ndarray, pd.Series], period: int = 12) -> np.ndarray:
        """
        变化率指标 (Rate of Change)

        Args:
            data: 价格数据
            period: 周期

        Returns:
            ROC值
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)

        roc = np.zeros(len(data))
        for i in range(period, len(data)):
            roc[i] = ((data[i] - data[i - period]) / data[i - period]) * 100

        return roc

    @staticmethod
    def apo(
        data: Union[List, np.ndarray, pd.Series], fast: int = 12, slow: int = 26
    ) -> np.ndarray:
        """
        绝对价格振荡指标 (Absolute Price Oscillator)

        Args:
            data: 价格数据
            fast: 快速EMA周期
            slow: 慢速EMA周期

        Returns:
            APO值
        """
        if isinstance(data, (list, pd.Series)):
            data = np.array(data)

        ema_fast = pd.Series(data).ewm(span=fast, adjust=False).mean().values
        ema_slow = pd.Series(data).ewm(span=slow, adjust=False).mean().values

        return ema_fast - ema_slow

    @staticmethod
    def vwap(
        high: Union[List, np.ndarray, pd.Series],
        low: Union[List, np.ndarray, pd.Series],
        close: Union[List, np.ndarray, pd.Series],
        volume: Union[List, np.ndarray, pd.Series],
    ) -> np.ndarray:
        """
        成交量加权平均价格 (Volume Weighted Average Price)

        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            volume: 成交量

        Returns:
            VWAP值
        """
        if isinstance(high, (list, pd.Series)):
            high = np.array(high)
        if isinstance(low, (list, pd.Series)):
            low = np.array(low)
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)
        if isinstance(volume, (list, pd.Series)):
            volume = np.array(volume)

        typical_price = (high + low + close) / 3
        vwap = np.cumsum(typical_price * volume) / np.cumsum(volume)

        return vwap

    @staticmethod
    def williams_r(
        high: Union[List, np.ndarray, pd.Series],
        low: Union[List, np.ndarray, pd.Series],
        close: Union[List, np.ndarray, pd.Series],
        period: int = 14,
    ) -> np.ndarray:
        """
        威廉指标 (Williams %R)

        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            period: 周期

        Returns:
            Williams %R值 (-100 到 0)
        """
        if isinstance(high, (list, pd.Series)):
            high = np.array(high)
        if isinstance(low, (list, pd.Series)):
            low = np.array(low)
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)

        high_series = pd.Series(high)
        low_series = pd.Series(low)

        highest_high = high_series.rolling(window=period).max().values
        lowest_low = low_series.rolling(window=period).min().values

        williams_r = -100 * (highest_high - close) / (highest_high - lowest_low + 1e-10)

        return williams_r

    @staticmethod
    def cci(
        high: Union[List, np.ndarray, pd.Series],
        low: Union[List, np.ndarray, pd.Series],
        close: Union[List, np.ndarray, pd.Series],
        period: int = 20,
    ) -> np.ndarray:
        """
        商品通道指数 (Commodity Channel Index)

        Args:
            high: 最高价
            low: 最低价
            close: 收盘价
            period: 周期

        Returns:
            CCI值
        """
        if isinstance(high, (list, pd.Series)):
            high = np.array(high)
        if isinstance(low, (list, pd.Series)):
            low = np.array(low)
        if isinstance(close, (list, pd.Series)):
            close = np.array(close)

        typical_price = (high + low + close) / 3
        tp_series = pd.Series(typical_price)

        sma_tp = tp_series.rolling(window=period).mean().values
        std_tp = tp_series.rolling(window=period).std().values

        cci = (typical_price - sma_tp) / (0.015 * std_tp + 1e-10)

        return cci
