from etl.utils import (
    get_spark,
    get_task_parameters,
    clean_output_directory)
from etl.transactions_etl import get_transactions
import os


def main():
    clean_output_directory(
        os.path.join(os.getcwd(), "output", "TRANSACTIONS")
    )
    spark = get_spark()
    task_parameters = get_task_parameters()
    df = get_transactions(
        spark,
        task_parameters,
        b_write=True
    )
    print('Printing Transactions...')
    df.show()


if __name__ == "__main__":
    main()
