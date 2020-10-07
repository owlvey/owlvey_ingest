import pandas as pd
if __name__ == "__main__":

    users = pd.read_excel("./data/metadata.xlsx", sheet_name='Users')
    print(users.head())