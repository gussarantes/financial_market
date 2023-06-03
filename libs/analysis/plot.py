import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt


class Ploter:
    """
    Class to plot information about the symbol.
    """

    def __init__(self, df: pd.DataFrame):
        """
        Initialize the class to plot data.
        """
        self.df = df

    def symbol_lineplot(
            self,
            x_axis: str,
            y_axis: str,
            figsize: tuple = (20, 8)
    ):
        """
        """
        # setup graph
        n_cols = 1
        n_rows = 1
        self.figsize = figsize
        plt.rcParams['axes.titlesize'] = 20
        plt.rcParams['axes.labelsize'] = 15
        plt.rcParams['xtick.labelsize'] = 10
        plt.rcParams['ytick.labelsize'] = 10
        fig, ax = plt.subplots(n_rows, n_cols, figsize=figsize)

        # plot
        ax = sns.lineplot(
            x=x_axis,
            y=y_axis,
            data=self.df,
            fillstyle='none'
        )

        ax.set(xlabel='Period', ylabel=y_axis)
        plt.title(
            f"""{y_axis.upper()}"""
        )
        plt.show()
