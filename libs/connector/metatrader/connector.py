import MetaTrader5 as mt5


class MetaTraderConnector:
    """
    Class to create a connection with MetaTrader.
    """

    def __init__(self) -> None:
        """
        Initialize a new instance of connector.
        """
        if not mt5.initialize():
            print('Please, start MetaTrader5.', mt5.last_error())
            return None
        else:
            print('MetaTradar5 loaded!')
