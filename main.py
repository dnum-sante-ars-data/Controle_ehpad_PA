import json
import argparse
import re
from os import listdir
import pandas as pd
from modules.init_db.init_db import initDb, importSrcData, connDb, createTablesWithTypes, tableExists
from utils import utils
from modules.transform.transform import executeTransform, inittable
from modules.export.export import localToSFTP
from modules.importsource.importSource import decryptFile

### il y a 1 fois la fonction read_settings, supprimer celle ci, on ne doit appeler que celle de utils
def read_settings():
    with open('settings/settings.json') as f:
        data = json.load(f)
    if "parametres" in data:
        return data["parametres"], data
    else:
        raise KeyError("La clé 'parametres' n'existe pas dans le fichier JSON")

def main(args):
    if args.commande == "import":
        importData()
    elif args.commande == "create_csv":
        createCsv()
    elif args.commande == "controle":
        execute_change_type()
    elif args.commande == "init_database":
        exeDbInit()
    elif args.commande == "load_csv":
        loadCsvToDb()
    elif args.commande == "transform":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                transform(r)
        else:
            transform(args.region)
    elif args.commande == "export":
        if args.region is None:
            print("MERCI DE RENSEIGNER LA REGION SOUHAITEE. Si VOUS VOULEZ TOUTES LES REGIONS VEUILLEZ METTRE 0")
        elif args.region == 0:
            list_region = utils.read_settings('settings/settings.json', "region", "code")
            for r in list_region:
                createExport(r)
        else:
            createExport(args.region)
    elif args.commande == "all":
        allFunctions(args.region)
    return

### en python les fonctions s'écrivent minuscule et _
def exeDbInit():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    conn = initDb(dbname)
    params, data = read_settings()
    createTablesWithTypes(conn, data)
    conn.close()
    return

### Créer un module à part
def createCsv():
    ### Toujours chercher à mettre en variable pour simplfifier la lecture du code, par ex "data/input" > input = "data/input", et mettre les variable au début de la fonction
    allFolders = listdir('data/input')
    allFolders.remove('sivss')
    ### ne plus utiliser cette fonction car le fichier sivss arrive bien un 1 fichier déjà concaténé contrairement à avant, supprimer la ligne au dessus aussi
    utils.concatSignalement()
    for folderName in allFolders:
        folderPath = 'data/input/{}'.format(folderName)
        allFiles = listdir(folderPath)
        for inputFileName in allFiles:
            inputFilePath = folderPath + '/' + inputFileName
            outputFilePath = 'data/to_csv/' + inputFileName.split('.')[0] + '.csv'
            if re.search('demo.csv|demo.xlsx', inputFileName):
                print('file demo not added')
            elif inputFileName.split('.')[-1].lower() == 'xlsx':
                utils.convertXlsxToCsv(inputFilePath, outputFilePath)
                print('converted excel file and added: {}'.format(inputFileName))
            elif inputFileName.split('.')[-1].lower() == 'csv':
                outputExcel = inputFilePath.split('.')[0] + '.xlsx'
                df = pd.read_csv(inputFilePath, sep=';', encoding='latin-1', low_memory=False)
                df.to_excel(outputExcel, index=None, header=True, encoding='UTF-8')
                df2 = pd.read_excel(outputExcel)
                df2.to_csv(outputFilePath, index=None, header=True, sep=';', encoding='UTF-8')
                print('added csv file: {}'.format(inputFileName))

### à déplacer dans un module spécifique, par ex load_to_db
def loadCsvToDb():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    allCsv = listdir('data/to_csv')
    conn = connDb(dbname)
    _, data = read_settings()
    for inputCsvFilePath in allCsv:
        table_name = inputCsvFilePath.split('/')[-1].split('.')[0]
        if tableExists(conn, table_name):
            print(f"La table {table_name} existe déjà. Ajout des données sans modification du type.")
            ### Re écrire la fonction pour enlever clean data et csv reader et faire une variable dataframe =
            importSrcData(
                utils.cleanSrcData(
                    utils.csvReader('data/to_csv/' + inputCsvFilePath)
                ),
                table_name,
                conn
            )
        else:
            print(f"La table {table_name} n'existe pas. Création de la table et ajout des données.")
            createTablesWithTypes(conn, data)
            importSrcData(
                utils.cleanSrcData(
                    utils.csvReader('data/to_csv/' + inputCsvFilePath)
                ),
                table_name,
                conn
            )
        print("file added to db: {}".format(inputCsvFilePath))
    conn.close()
    return

def transform(region):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = connDb(dbname)
    params, _ = read_settings()
    inittable(conn)
    print("Table initialisée avec succès.")
    executeTransform(region)
    print(f"Transformation exécutée pour la région {region}.")
    conn.close()

def allFunctions(region):
    exeDbInit()
    loadCsvToDb()
    
    if region == 0:
        list_region = utils.read_settings('settings/settings.json', "region", "code")
        print(f"Liste des régions lues : {list_region}")
        for r in list_region:
            print(f"Traitement de la région : {r}")
            transform(r)
            # createExport(r)
    else:
        print(f"Traitement de la région spécifiée : {region}")
        transform(region)
        # createExport(region)
    
    return

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code région pour filtrer")
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
