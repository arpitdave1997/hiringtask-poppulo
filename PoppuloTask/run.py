import os
from datetime import date

DIRECTORY_PATH = f"{os.path.dirname(__file__)}"
SUBDIRECTORY_PATH = f"{os.path.dirname(__file__)}/{str(date.today())}"

def initializeSubDirectory():
        if not os.path.exists(SUBDIRECTORY_PATH):
                os.makedirs(SUBDIRECTORY_PATH)

        return

if __name__ == "__main__":
        initializeSubDirectory()

        data = pd.read_csv('data.csv')