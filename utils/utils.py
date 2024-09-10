import os
import pandas as pd
from unidecode import unidecode
import re 
import json
import logging
from os import listdir

# Fonction pour vérifier et supprimer un fichier existant
def checkIfPathExists(file):
    if os.path.exists(file):
        os.remove(file)
        print('Ancien fichier écrasé')

# Conversion d'un fichier Excel en CSV
def convertXlsxToCsv(inputExcelFilePath, outputCsvFilePath):
    try:
        # Lecture du fichier Excel
        excelFile = pd.read_excel(inputExcelFilePath, header=0)
        checkIfPathExists(outputCsvFilePath)
        # Conversion du fichier Excel en fichier CSV
        excelFile.to_csv(outputCsvFilePath, index=None, header=True, sep=';', encoding='UTF-8')
        return outputCsvFilePath
    except ValueError as err:
        print(err)
        return str(err)

# Conversion d'un fichier CSV en Excel
def convertCsvToXlsx(inputCsvFilePath, outputExcelFilePath):
    try:
        # Lecture du fichier CSV
        csvFile = pd.read_csv(inputCsvFilePath, sep=';', encoding='UTF-8')
        checkIfPathExists(outputExcelFilePath)
        # Conversion du fichier CSV en fichier Excel
        csvFile.to_excel(outputExcelFilePath, index=None, header=True, encoding='UTF-8')
        return outputExcelFilePath
    except ValueError as err:
        print(err)
        return str(err)

# Lecture d'un fichier CSV
def csvReader(csvFilePath):
    df = pd.read_csv(csvFilePath, sep=';', encoding='UTF-8', low_memory=False)
    return df

# Nettoyage du texte
def cleanTxt(text):
    try:
        text = str(text).lower()
    except (TypeError, NameError): 
        pass
    text = unidecode(text)
    text = re.sub('[ ]+', '_', text)
    text = re.sub('[^0-9a-zA-Z_-]', '', text)
    return str(text)

# Nettoyage des données sources
def cleanSrcData(df):
    df.columns = [cleanTxt(i) for i in df.columns.values.tolist()]
    return df

# Lecture des paramètres de configuration
def read_settings(path_in, dict_key, elem):
    with open(path_in) as f:
        dict_ret = json.load(f)

    if dict_key in dict_ret:
        param_config = dict_ret[dict_key]
    else:
        raise KeyError(f"La clé '{dict_key}' n'existe pas dans le fichier JSON")

    if isinstance(param_config, list) and len(param_config) > 0:
        first_entry = param_config[0]
        if elem in first_entry:
            return first_entry[elem]
        else:
            raise KeyError(f"La clé '{elem}' n'existe pas dans le premier élément de la liste sous '{dict_key}'")
    else:
        raise TypeError(f"Le contenu sous '{dict_key}' doit être une liste contenant un dictionnaire")

    logging.info("Lecture param config " + path_in + ".")


# Fonction pour concaténer les différentes régions de SIVSS
def concatSignalement():
    folderPath = 'data/input/sivss'
    allSignalFiles = listdir(folderPath)
    checkIfPathExists('data/to_csv/all_sivss.csv')
    df = pd.DataFrame()
    for fileName in allSignalFiles:
        df = pd.concat([df, pd.read_excel('data/input/sivss/' + fileName)])
    print('signalement concaténés')
    df.to_csv('data/to_csv/all_sivss.csv', index=None, header=True, sep=';', encoding='UTF-8')
    print('all_sivss.csv créé')
    return
