import pandas as pd
from os import listdir
from modules.init_db.init_db import conn_db
from utils import utils
from modules.init_db.init_db import importSrcData

def load_csv_to_db():
    # Variables
    settings_path = 'settings/settings.json'
    file_to_csv = 'data/to_csv'
    csv_separator = ';'
    csv_encoding = 'UTF-8'

    # Lecture du nom de la base de données
    dbname = utils.read_settings(settings_path, 'db', 'name')
    all_csv_files = listdir(file_to_csv)
    conn = conn_db(dbname)
    
    for csv_file in all_csv_files:
        table_name = csv_file.split('/')[-1].split('.')[0]
        csv_file_path = f'{file_to_csv}/{csv_file}'
        
        # Chargement du CSV en DataFrame
        df = pd.read_csv(csv_file_path, sep=csv_separator, encoding=csv_encoding, low_memory=False)
        
        # Importation des données dans la table
        importSrcData(df, table_name, conn)
        print(f"Fichier {csv_file} ajouté à la base de données.")
    
    conn.close()
    return
