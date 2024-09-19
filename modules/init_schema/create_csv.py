from utils.utils import convertXlsxToCsv
import re
from os import listdir


def create_csv():
    # Définition des variables
    input_folder = 'data/input'
    output_folder = 'data/to_csv'
    demo_file_pattern = r'demo.csv|demo.xlsx'

    # Liste des dossiers dans le répertoire d'entrée
    all_folders = listdir(input_folder)

    for folder_name in all_folders:
        folder_path = f'{input_folder}/{folder_name}'
        all_files = listdir(folder_path)

        for input_file_name in all_files:
            input_file_path = f'{folder_path}/{input_file_name}'
            output_file_name = f'{input_file_name.split(".")[0]}.csv'
            output_file_path = f'{output_folder}/{output_file_name}'

            # Ignorer les fichiers de démonstration
            if re.search(demo_file_pattern, input_file_name):
                print(f'{input_file_name} not added (demo file)')
            
            # Conversion des fichiers Excel en CSV
            elif input_file_name.lower().endswith('.xlsx'):
                convertXlsxToCsv(input_file_path, output_file_path)
                print(f'Converted Excel file and added: {input_file_name}')
            
            # Fichiers CSV : pas de conversion nécessaire, juste un message
            elif input_file_name.lower().endswith('.csv'):
                print(f'CSV file already exists and was added: {input_file_name}')
