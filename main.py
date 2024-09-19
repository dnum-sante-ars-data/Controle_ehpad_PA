import argparse
from modules.init_db.init_db import init_db, conn_db
from utils import utils
from modules.transform.transform import execute_transform, init_table
#from modules.export.export import create_export
from modules.importsource.import_source import get_data_with_SFTP
from modules.init_schema.create_csv import create_csv 
from modules.init_schema.load_csv_to_db import load_csv_to_db 

def main(args):
    if args.commande == "import":
        get_data_with_SFTP()
    elif args.commande == "create_csv":
        create_csv()  
    elif args.commande == "init_database":
        exe_db_init()
    elif args.commande == "load_csv":
        load_csv_to_db()  
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
                create_export(r)
        else:
            create_export(args.region)
    elif args.commande == "all":
        all_functions(args.region)

def exe_db_init():
    dbname = utils.read_settings('settings/settings.json', "db", "name")
    conn = init_db(dbname)
    conn.close()

def transform(region):
    dbname = utils.read_settings("settings/settings.json", "db", "name")
    conn = conn_db(dbname)
    params = utils.read_settings('settings/settings.json', 'parametres', 'param_N')
    init_table(conn)
    print("Table initialisée avec succès.")
    execute_transform(region)
    print(f"Transformation exécutée pour la région {region}.")
    conn.close()

def all_functions(region):
    #get_data_with_SFTP()
    exe_db_init()
    create_csv()
    load_csv_to_db() 
    
    if region == 0:
        list_region = utils.read_settings('settings/settings.json', "region", "code")
        print(f"Liste des régions lues : {list_region}")
        for r in list_region:
            print(f"Traitement de la région : {r}")
            transform(r)
            # create_export(r)
    else:
        print(f"Traitement de la région spécifiée : {region}")
        transform(region)
        # create_export(region)

parser = argparse.ArgumentParser()
parser.add_argument("commande", type=str, help="Commande à exécuter")
parser.add_argument("region", type=int, help="Code région pour filtrer")
args = parser.parse_args()

if __name__ == "__main__":
    main(args)
