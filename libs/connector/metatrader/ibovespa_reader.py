from libs.connector.metatrader.base_reader import MetaTraderReader
from datetime import datetime, timedelta
import MetaTrader5 as mt5
import pandas as pd


class BovespaReader(MetaTraderReader):
    """
    Class to get data from Bovespa stocks.
    """

    def __init__(self) -> None:
        """
        Initialize an object from class BovespaReader
        """
        super().__init__()
        self.get_bovespa_tickers()

    def get_bovespa_tickers(self) -> list:
        """
        Return a list with all ibovespa symbols that have data at least at
        the seven days ago
        """
        self.get_all_symbols()\
            ._filtering_market_type()\
            ._filtering_on_pn_stocks()\
            ._filtering_active_stocks()

    def get_stock_dataframe(
            self,
            stock_ticker: str,
            kpi_list: list = None,
            last_days: int = 90
    ) -> pd.DataFrame():
        """
        Get data about stocks in the specified time range.

        Args:
            stock_ticker: the stock's ticker to get information.
            last_days: default 90. Numbers of days to search information.
            kpi_list: default ['close']. List of data to get about ticker.
            It can be on of then ['open', 'high', 'low', 'close',
            tick_volume', 'spread', 'real_volume']

        Returns:
        time: It represents the timestamp or the date and time of the bar
        or candlestick. The time is typically recorded in Unix timestamp
        format, which represents the number of seconds elapsed since
        January 1, 1970.

        open: It represents the opening price of the bar or candlestick,
        which is the price at the beginning of the time period.

        high: It represents the highest price reached during the time period.

        low: It represents the lowest price reached during the time period.

        close: It represents the closing price of the bar or candlestick,
        which is the price at the end of the time period.

        tick_volume: It represents the number of ticks or price changes
        that occurred during the time period. Each tick represents a change
        in price, and the tick_volume indicates the total number of ticks
        recorded.

        spread: It represents the difference between the bid and ask prices
        at the close of the bar or candlestick. It indicates the
        transaction costs or the spread charged by the broker.

        real_volume: It represents the actual trading volume or the total
        number of contracts or lots traded during the time period.
        The real_volume provides information about the size of trading
        positions and the overall market participation.
        """

        self.last_days = last_days
        self.stock_ticker = stock_ticker
        self.kpi_list = kpi_list

        if self.kpi_list is None:
            self.kpi_list = [
                'open', 'high', 'low', 'close',
                'tick_volume', 'spread', 'real_volume'
            ]

        self._get_data()\
            ._parsing_get_data()

        return self.stocks_df

    def _filtering_market_type(self):
        """
        Filter the information about the specific exchange and type of market.
        """
        self.bovespa_tickers_df = self.symbols
        mask_bovespa = self.bovespa_tickers_df['stock_exchange'] == 'BOVESPA'
        mask_avista = self.bovespa_tickers_df['kpi_type'] == 'A VISTA'
        self.bovespa_tickers_df = self.bovespa_tickers_df[
            mask_bovespa | mask_avista
        ]
        self.bovespa_tickers_df.drop(
            ['stock_exchange', 'kpi_type'],
            axis=1,
            inplace=True)
        return self

    def _filtering_on_pn_stocks(self):
        """
        Filter the stock's type ON and PN,
        stocks that ticker ends with '3' or '4'.
        """
        mask_length = self.bovespa_tickers_df['name'].str.len() == 5
        mask_ends_with = self.bovespa_tickers_df['name'].str[-1].isin(
            ['3', '4']
        )
        self.bovespa_tickers_df = self.bovespa_tickers_df[
            mask_length & mask_ends_with
        ]
        self.list_bovespa_tickers = self.bovespa_tickers_df.name.to_list()
        return self

    def _filtering_active_stocks(self):
        """
        Filter the stock that have informantion at least at the 7 days ago.
        """
        today = datetime.now()
        seven_days_ago = today - timedelta(days=7)
        list_tickers_to_remove = []

        for ticker in self.list_bovespa_tickers:
            try:
                bovespa_parcial_df = self.get_stock_dataframe(
                    ticker,
                    kpi_list=['close'],
                    last_days=1
                )
                if seven_days_ago <= bovespa_parcial_df.time.max() <= today:
                    pass
                else:
                    list_tickers_to_remove.append(ticker)
            except KeyError as error:
                print(
                    f"{ticker} does not have recent data - KeyError: {error}"
                )
                list_tickers_to_remove.append(ticker)
        self.bovespa_tickers_df = self.bovespa_tickers_df[
            ~self.bovespa_tickers_df['name'].isin(list_tickers_to_remove)
        ]
        self.list_bovespa_tickers = [
            x for x in self.list_bovespa_tickers
            if x not in list_tickers_to_remove
        ]
        return self

    def _get_data(self):
        """
        Function to colect information about a stock.
        """
        self.stocks_df = mt5.copy_rates_from_pos(
            self.stock_ticker,
            mt5.TIMEFRAME_D1,
            0,
            self.last_days
        )

        return self

    def _parsing_get_data(self):
        """
        Function to parsing the into a dataframe.
        Ajust the datatime values and choosing
        specific columns.
        """
        # parsing data in pandas DataFrame
        self.stocks_df = pd.DataFrame(self.stocks_df)
        self.stocks_df['time'] = pd.to_datetime(
            self.stocks_df['time'],
            unit='s'
        )

        # filtering columns
        self.stocks_df = self.stocks_df[['time'] + self.kpi_list]

        return self

    def get_bovespa_dataframe(
            self,
            bovespa_tickers_list: str,
            kpi_list: list = None,
            last_days: int = 90
    ) -> pd.DataFrame():
        """
        Get data about a list of bovespa's stocks in the specified time range.

        Args:
            stock_ticker: the stock's ticker to get information.
            last_days: default 90. Numbers of days to search information.
            kpi_list: default ['close']. List of data to get about ticker.
            It can be on of then ['open', 'high', 'low', 'close',
            tick_volume', 'spread', 'real_volume']

        Returns:
        time: It represents the timestamp or the date and time of the bar
        or candlestick. The time is typically recorded in Unix timestamp
        format, which represents the number of seconds elapsed since
        January 1, 1970.

        open: It represents the opening price of the bar or candlestick,
        which is the price at the beginning of the time period.

        high: It represents the highest price reached during the time period.

        low: It represents the lowest price reached during the time period.

        close: It represents the closing price of the bar or candlestick,
        which is the price at the end of the time period.

        tick_volume: It represents the number of ticks or price changes
        that occurred during the time period. Each tick represents a change
        in price, and the tick_volume indicates the total number of ticks
        recorded.

        spread: It represents the difference between the bid and ask prices
        at the close of the bar or candlestick. It indicates the
        transaction costs or the spread charged by the broker.

        real_volume: It represents the actual trading volume or the total
        number of contracts or lots traded during the time period.
        The real_volume provides information about the size of trading
        positions and the overall market participation.
        """

        self.last_days = last_days
        self.kpi_list = kpi_list
        self.bovespa_tickers_list = bovespa_tickers_list
        self.bovespa_data_df = pd.DataFrame()

        for ticker in self.bovespa_tickers_list:
            bovespa_parcial_df = self.get_stock_dataframe(
                ticker,
                kpi_list=self.kpi_list,
                last_days=self.last_days
            )
            bovespa_parcial_df['stock'] = ticker
            self.bovespa_data_df = pd.concat([
                self.bovespa_data_df,
                bovespa_parcial_df
            ])
        return self.bovespa_data_df
