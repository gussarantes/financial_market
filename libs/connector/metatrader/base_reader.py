import abc
from libs.connector.metatrader.connector import MetaTraderConnector
import MetaTrader5 as mt5
import pandas as pd


class MetaTraderReader(abc.ABC):
    """
    Class to get data from stocks
    """

    def __init__(self) -> None:
        """
        Initialize a new instance of MetaTraderReader.
        """
        MetaTraderConnector()
        # return self

    @abc.abstractmethod
    def get_stock_dataframe(self):
        """
        Get data from a specific group ou unique stock
        """

    def get_all_symbols(self) -> pd.DataFrame():
        """
        Get all symbols in the plataform and format in a pandas dataframe
        """

        # get all symbols in MetaTrader5
        self.symbols = mt5.symbols_get()

        # data in pandas dataframe
        self.symbols = pd.DataFrame(
            self.symbols,
            columns=self.symbols[0]._asdict().keys()
        )
        self.symbols = self.symbols[['name', 'path', 'description']]
        self.symbols[
            ['stock_exchange', 'kpi_type']
        ] = self.symbols['path'].str.split("\\", expand=True).loc[:, 0:1]
        del self.symbols['path']
        return self
