# -*- coding: utf-8 -*----------------------------------------------------------
# Name:        recuperer_donnees_aghyre_v1
# Purpose:     Récupération des données d'Aghyre V1 avec le webservice associé
#
# Author:      Alain Gauthier
#
# Created:     11/03/2025
# Licence:     GPL V3
#-------------------------------------------------------------------------------

import datetime as dt
import os
import sys
import json
import urllib3
import pandas as pd
import requests

# config .ini
import configparser # Permet de parser le fichier de paramètres

# analyse du flux xml sandre
from libhydro.conv.xml import Message

urllib3.disable_warnings()

#-------------------------------------------------------------------------------

def get_params(input_file):
    """Renvoie un dict contenant les paramètres lus dans input_file

    Args:
        input_file (str): chemin vers le fichier de paramètres .ini

    Returns:
        dict: paramètres par clé:valeur lus
    """
    config = configparser.RawConfigParser()
    lus = config.read(input_file)
    if len(lus) == 0 :
        raise IOError(f"échec de lecture du fichier de paramètres : \n {input_file}")

    params = {}
    # liste de fichiers contenant la liste des rubriques à récupérer et leurs noms
    # /!\ ATTENTION : par souci de cohérence regrouper les données par unité homogène dans les fichiers de rubriques
    # ce fichier est au format CSV avec en colonne 1 l'identifiant de la rubrique et en colonne 2 le nom
    params["FIC_RUBRIQUES"]=config.get('params','FIC_RUBRIQUES')
    params["FIC_RUBRIQUES"] = [v.strip() for v in params["FIC_RUBRIQUES"].split('\n')]
    # retour à la ligne en début de paramètre
    if params["FIC_RUBRIQUES"].count('') >0:
        params["FIC_RUBRIQUES"].remove('')
    # date de début de la requête
    params["DEBUT"] = config.get('params','DEBUT')
    params['DEBUT'] = dt.datetime.strptime(params['DEBUT'], "%d/%m/%Y")
    # date de fin de la requête optionnelle
    try:
        params["FIN"] = config.get('params','FIN')
        params['FIN'] = dt.datetime.strptime(params['FIN'], "%d/%m/%Y")
    except:
        params["FIN"] = dt.datetime.now()
    # pas de temps de la restitution
    params['DT'] = [v.strip() for v in config.get('params', 'DT').split('\n')]
    if params['DT'].count('') >0:
        params['DT'].remove('')
    # résultat
    params["RESULTATS"]=config.get('params','RESULTATS')
    return params

#-------------------------------------------------------------------------------

def lire_fichiers_rubriques(liste_fic):
    """Lecture de la liste des fichiers des rubriques à télécharger.
    Renvoie un dict contenant les listes de rubriques avec en clé le nom du fichier lu

    Args:
        liste_fic (list): liste de nom de fichiers à lire

    Returns:
        dict: données des listes de rubriques regroupées par nom de fichier à utiliser en sortie de programme
    """
    # données lues
    resultat = {}
    # boucle sur les fichiers
    for fic in liste_fic:
        df= pd.read_csv(fic, sep=';', index_col=0)
        # les idenfifiants doivent être des str
        df.index = df.index.astype('str')
        # nom final utile pour nommer le fichier des données résultant de la requête
        nomfic = os.path.basename(fic)
        resultat[nomfic] = df
    # fin
    return resultat

#-------------------------------------------------------------------------------

class ClientAghyre():
    """
    classe de gestion de la session de connexion au webservice
    """


    def __init__(self):
        """
        Constructeur
        """
        self.session = requests.Session()
        self.session.verify=False


    def request(self, method, url, **kwargs):
        """
        Requête
        """
        # paramètres de la requête (identifiants mis en dur, défini pour le bulletin de situation hydro)
        # TODO : voir si on doit paramétrer les identifiants si définis pour des usages différents.
        params = {
            'login'       :'identifiant_BSH',
            'password'    :'DGH_BSH*',
            'codification':'ID_AGHYRE',
        }
        # selon contexte
        if 'param_url' in kwargs:
            params['rubriques'] = kwargs['param_url']
        if 'debut' in kwargs:
            params['dateDebut'] = kwargs['debut'].strftime("%Y-%m-%dT%H:%M:%S")
        if 'fin' in kwargs:
            params['dateFin'] = kwargs['fin'].strftime("%Y-%m-%dT%H:%M:%S")

        response = self.session.request(method, url, json=params)
        # Vérification de la réponse
        if response.status_code != 200:
            raise requests.exceptions.RequestException(f"Erreur lors de la requête : {response.status_code}")
        # renvoi du contenu de la réponse
        return response.content

#-------------------------------------------------------------------------------

def recuperation_donnees(client, id_aghyre, debut, fin) :
    """
    Renvoie un  texte contenant les données correspondantes à la requête
    """
    # URL des requêtes
    param_url = [id_aghyre]
    flux_sandre = client.request('POST', 'https://www.vnf.fr/aghyre/api/diffusion/donnees', param_url=param_url, debut=debut, fin=fin)
    # utilisation de libhydo pour deserialiser le flux xml sandre
    message = Message.from_string(flux_sandre, strict=False)
    # désérialisation du message dans un dict avec format json
    dico = json.loads(message.to_json())

    # dataframe contenant la série temporelle des données récupérées
    try:
        df = pd.DataFrame.from_records(dico['Donnees']['SeriesObsHydro'][0]["ObssHydro"])
    except Exception as e:
        print(e)
        raise IOError(f" !! Echec de récupération des données pour la rubrique {id_aghyre} : pas de données !!") from e
    # fin
    return df

#-------------------------------------------------------------------------------

def recup_liste_donnees(client, liste_rubriques, debut, fin):
    """Lancement des requêtes de récupération des rubriques passées en paramètre sous la forme d'une liste.
    Toutes les données récupérées sont renvoyées dans une liste de dataframe

    Args:
        client (ClientAghyre): client exécutant la requête
        liste_rubriques (list): liste des identifiants des rubriques que l'on veut récupérer
        debut (datetime): date de début de la période de requête
        fin (datetime): date de fin de la période de requête

    Returns:
        list(dataframe): liste de dataframe contenant les données récupérées
    """
    donnees=dict()
    for id_aghyre in liste_rubriques:
        print("traitement de :",id_aghyre)
        # récupération des données
        df = recuperation_donnees(client, id_aghyre, debut, fin)
        # ajout au résultat si des données sont récupérées
        if not df.empty:
            print("--> données récupérées")
            donnees[id_aghyre] = df
    # fin
    return donnees

#-------------------------------------------------------------------------------

def formater_chroniques(dico_donnees, deltat):
    """création de chroniques avec la date en index et les valeurs des rubriques
    identifiées par l'identifiant de rubrique en colonne.
    les chroniques lues sont dans un dataframe contenant :
       DtObsHydro  : colonne des dates
       ResObsHydro : colonne des valeurs lues
       autres colonnes non utilisées...

    Args:
        dico_donnees (dict): données à traiter sous la forme d'un dataframe par
        identifiant de rubrique
        deltat (str) : pas de temps de la chronique produite, dans le format accepté par
        resample de pandas.Dataframe

    Returns:
        DataFrame: chronique comportant en index la date et en colonne les valeurs rangées
        par identifiant de rubrique
    """
    liste_df = []
    # boucle sur les id des rubriques lues
    for id_rubrique, df in dico_donnees.items():
        # mise en forme de l'index date
        chronique = df.set_index('DtObsHydro')
        chronique.index = pd.to_datetime(chronique.index)
        # on traite juste la colonne des valeurs de la rubrique
        chronique = chronique.rename(columns={'ResObsHydro': id_rubrique})
        # ajout du résultat
        liste_df.append(chronique[id_rubrique])
    # traitement sous forme de chronique
    resultat = pd.concat(liste_df, axis=1)
    # mise à jour du pas de temps :
    if deltat != '-1':
        resultat = resultat.resample(deltat).mean()
    # fin
    return resultat

#-------------------------------------------------------------------------------

def main():
    """fonction principale lancée en début de programme
    """
    # fichier .ini obligatoire
    if len(sys.argv) != 2:
        raise IOError("il manque le fichier de paramètres")
    else:
        inputfile = sys.argv[1]

    print(f'input file : {inputfile}')

    # lecture des paramètres
    params = get_params(inputfile)

    # lecture des fichiers des rubriques à récupérer
    dico_rubriques = lire_fichiers_rubriques(params['FIC_RUBRIQUES'])

    # client pour faire les requêtes
    client = ClientAghyre()

    # indice initial poru les pas de temps (params['DT'])
    i=0
    # pour chaque entrée de dico_rubriques
    for fic, df in dico_rubriques.items():
        # paramètres de la requête
        liste_rubriques = df.index.to_numpy()
        # requêtes
        dico_donnees = recup_liste_donnees(client, liste_rubriques, params['DEBUT'], params['FIN'])
        # formatage des données lues en chroniques
        deltat= params['DT'][i]
        chroniques_donnees = formater_chroniques(dico_donnees, deltat)
        # écriture des données dans un fichier
        fic_res = os.path.join(params['RESULTATS'], f'chronique_{fic}')
        print('écriture de : ', fic_res)
        chroniques_donnees.to_csv(fic_res, sep=';')
        # incrément de i pour le pas de temps suivant
        i+=1

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
