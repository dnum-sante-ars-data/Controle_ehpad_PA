# -*- coding: utf-8 -*-
"""
Created on Wed Mar 22 10:38:45 2023

@author: mathieu.olivier
"""
import pandas as pd
from datetime import datetime
import json
import paramiko
from modules.Info.info import sftpInfo,outputName

def outputName(region):
    with open('settings/settings.json') as f:
        # Load the JSON data from the file
        data = json.load(f)
    # Find the index of the code equal to 2 in the "code" list
    index = data["region"][0]["code"].index(region)
    # Get the corresponding "nom" value from the "nom" list
    nom = data["region"][0]["nom"][index]
    return nom


def localToSFTP(region):
    # Informations de connexion SFTP
    hostname, username, passphrase = sftpInfo()
    paramiko.util.log_to_file("paramiko.log")
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print('création du client')
    ssh.connect(hostname, username=username, password=passphrase)
    # Create an SFTP client object
    #client = paramiko.SFTPClient.from_transport(
    #    paramiko.Transport((hostname, 22))
    #)
    print('Ouverture de la connexion SFTP')
    # Authenticate with the server
    #client.connect(username=username, password=passphrase)
    date_string = datetime.today().strftime('%d%m%Y')
    localpath = 'data/output/{}_{}.xlsx'.format(outputName(region),date_string)
    remotepath = '/SCN_BDD/SIREC/{}_{}.xlsx'.format(outputName(region),date_string)
    sftp = ssh.open_sftp()
    print('sftp open')
    sftp.put(localpath, remotepath)
    #client.put(local_path, remote_path)
    print('Fichier {}_{}.xlsx déposé en dans /SCN_BDD/SIREC'.format(outputName(region),date_string))
    # Close the SFTP client connection
    #client.close()
    print('Fermeture de la connexion SFTP')
    return
    