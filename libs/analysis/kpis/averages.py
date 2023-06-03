import pandas as pd


class AverageAnalysis:
    """
    Class to analyze the trends of the stock.
    """
    def __init__(self, df_stocks: pd.DataFrame()) -> None:
        """
        Initialize a symbol object as a DataFrameReader.

        Args:
            df_stocks: dataframe with information to analyze.
        """
        self.dataframe = df_stocks

    def stock_analyser(
        self,
        stock_ticker: str,
        list_moving_average: list = [9],
        list_exponential_moving_average: list = [21, 50],
        kpi_reference: list = ['close']
    ) -> pd.DataFrame():
        """
        Function to create special average to a specific stock.
        Calcute moving and exponential moving average.

        Args:
            stock_ticker: ticker of stock.
            list_moving_average: default [9]. List with time windows to
            calculate moving average.

            list_exponential_moving_average: default [21, 50]. List with
            time windows to calculate moving average.

            kpi_reference: default ['close'].
            List with witch stock feature the averages will be
            calculated.

        Returns:
            A dataframe with all kpi calculated to the stock.
        """
        self.stock = stock_ticker
        self.df_stock = self.dataframe[
            self.dataframe['stock'] == self.stock
        ].copy()
        self.list_moving_average = list_moving_average
        self.list_exponential_moving_average = list_exponential_moving_average
        self.kpi_reference = kpi_reference

        self.calculate_moving_average()\
            .calculate_exponential_moving_average()

        return self.df_stock

    def calculate_moving_average(
        self
    ):
        """
        Function to calculate Moving Average.
        """
        self.last_average_applied = 'moving average'
        self.list_moving_average = self._check_data_and_params(
            self.list_moving_average
        )
        for type_average in self.list_moving_average:
            for reference in self.kpi_reference:
                kpi_name = f'ma{type_average}_{reference}'
                self.df_stock[kpi_name] = self.df_stock[reference].rolling(
                    window=type_average
                ).mean()

        return self

    def calculate_exponential_moving_average(
        self
    ):
        """
        Function to calculate Exponential Moving Average.
        """
        self.last_average_applied = 'exponential moving average'
        self.list_exponential_moving_average = self._check_data_and_params(
            self.list_exponential_moving_average
        )
        for type_average in self.list_exponential_moving_average:
            for reference in self.kpi_reference:
                kpi_name = f'ema{type_average}_{reference}'
                self.df_stock[kpi_name] = self.df_stock[reference].ewm(
                    span=type_average,
                    adjust=False
                ).mean()

        return self

    def _check_data_and_params(self, list_kpi: list):
        """
        Funtion to check if parameters or number of records are
        enough to calculated each type of average.
        """
        var_df_size = self.df_stock.shape[0]
        list_kpi_to_remove = []
        for kpi in list_kpi:
            try:
                kpi = int(kpi)
                var_number_minimum_value = kpi + 10
                self.df_stock.iloc[var_number_minimum_value]
            except ValueError as error:
                print(
                    f"""
                    Error in {self.stock} {self.last_average_applied}:
                    {kpi} is not a type of moving average - KeyError: {error}
                    """
                )
                list_kpi_to_remove.append(kpi)
                pass
            except IndexError as error:
                print(f"""
                    Error in {self.stock} {self.last_average_applied}:
                    Doesn't have enough data to calculate kpi: {kpi}.
                    Only {var_df_size} records.
                    {var_number_minimum_value} are necessery.
                    KeyError: {error}
                """)
                list_kpi_to_remove.append(kpi)
                pass
        list_kpi = [x for x in list_kpi if x not in list_kpi_to_remove]
        return list_kpi
