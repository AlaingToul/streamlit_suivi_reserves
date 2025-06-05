# -*- coding: utf-8 -*----------------------------------------------------------
# Name:        app.py
# Purpose:     Applicatioon Streamlit de suivi des réserves en eau de VNF
#              Les données sont issues de aghyre v1
#
# Author:      Alain Gauthier
#
# Created:     04/06/2025
# Licence:     GPL V3
#-------------------------------------------------------------------------------

import os
import io
import subprocess
import datetime as dt
import numpy as np
import pandas as pd

import matplotlib.pyplot as plt
import seaborn as sns

import streamlit as st

# dossier racine où se trouvent les données récupérées et à présenter
Racine = "./donnees"


def lire_caracteristiques_reservoirs():
    """Lecture des caractéristiques des réservoirs à partir du fichier excel

    Returns:
        pd.DataFrame: DataFrame contenant les caractéristiques des réservoirs
    """
    # fichier à lire
    fic_caracteristiques_reservoirs = "./donnees/Caractéristiques des réserves.xlsx"

    # lecture du fichier
    df_carac_reservoir = pd.read_excel(fic_caracteristiques_reservoirs,
                                    sheet_name="Réservoirs",
                                    skiprows=2,
                                    header=1,
                                    )

    # on conserve les données présentes dans aghyre
    df_carac_reservoir = df_carac_reservoir.dropna(subset='ID Aghyre - VMJ utile',
                                                axis=0,
                                                how='any')

    # index par identifiant de rubrlique sur Aghyre

    df_carac_reservoir = df_carac_reservoir.set_index('ID Aghyre - VMJ utile')
    df_carac_reservoir.index = df_carac_reservoir.index.map(lambda x: f'{int(x)}')

    # fin
    return df_carac_reservoir

#-------------------------------------------------------------------------------

@st.cache_data
def get_donnees_reservoirs():
    """Récupération des données de suivi des réserves sur aGHyre.
    Les résultats renvoyés sont les chroniques des volumes utiles au pas de temps mensuel
    et les identifiants des rubriques associées.

    Returns:
        (pd.DataFrame, pd.DataFrame): les dataframes contenant les chroniques et les identifiants des rubriques
    """
    # le script récupère les données de suivi des volumes utiles des réservoirs
    # et les enregistre dans le dossier ./donnees/chroniques

    inp="./donnees/recuperer_donnees_aghyre_v1.ini"
    with subprocess.Popen(["python", "./scripts/Aghyre/recuperer_donnees_aghyre_v1.py", inp],
                                stderr=subprocess.STDOUT,
                                stdout=subprocess.PIPE,
                                bufsize=0,
                                text=True) as process:
        # lecture des sorties de process
        for l in process.stdout:
            st.write(l.rstrip())
        # attente de la fin
        process.wait()

    # le fichier "inp" contient les informations pour lander la requête sur aGHyre
    # et les données sont enregistrées dans le dossier ./donnees/chroniques
    # la correspondance nom de réservoir - code rubrique
    fic_id_rub_m3 = "./donnees/rubriques_volume_utile_m3.csv"
    fic_id_rub_Mm3 = "./donnees/rubriques_volume_utile_Mm3.csv"
    # la chronique des volumes utiles des réservoirs
    fic_vol_utile_m3 = "./donnees/chroniques/chronique_rubriques_volume_utile_m3.csv"
    fic_vol_utile_Mm3 = "./donnees/chroniques/chronique_rubriques_volume_utile_Mm3.csv"

    # lecture des données

    # idenfifiants des rubriques
    df_id_rub_m3  = pd.read_csv(fic_id_rub_m3, sep=';')
    df_id_rub_Mm3 = pd.read_csv(fic_id_rub_Mm3, sep=';')
    df_id_rub = pd.concat([df_id_rub_Mm3, df_id_rub_m3], axis=0)
    # index par id de rubrique comme valeur numérique
    df_id_rub = df_id_rub.set_index('id_rubrique')

    # chroniques : conversion de tout en Mm3 et réunion des données
    df_vol_utile_m3 = pd.read_csv(fic_vol_utile_m3, sep=';', index_col=0, parse_dates=True) * 1.e-6
    df_vol_utile_Mm3 = pd.read_csv(fic_vol_utile_Mm3, sep=';', index_col=0, parse_dates=True)
    df_vol_utile = df_vol_utile_Mm3.join(df_vol_utile_m3, how='outer')

    # suppression des lignes vides
    df_vol_utile = df_vol_utile.dropna(axis=0, how='all')

    # construction des données au pas de temps journalier
    df_vol_utile = df_vol_utile.resample('D').mean()
    df_vol_utile = df_vol_utile.resample('MS').first()

    # fin
    return df_vol_utile, df_id_rub

#-------------------------------------------------------------------------------

def calculer_bilan_annuel(df_vol_utile):
    """Calcul du bilan annuel des volumes utiles globaux des réservoirs

    Args:
        df_vol_utile (pd.DataFrame): DataFrame contenant les chroniques des volumes utiles

    Returns:
        pd.DataFrame: DataFrame contenant le bilan annuel des volumes utiles
    """
    # somme de tous les volumes
    df_vol_total = pd.DataFrame(df_vol_utile.apply(np.nansum, axis=1),
                                index=df_vol_utile.index,
                                columns=['volume_Mm3'])

    # pour regrouper les graphes par année
    df_vol_annees = df_vol_total.pivot_table(index=df_vol_total.index.month,
                                             columns=df_vol_total.index.year,
                                             values='volume_Mm3')

    # fin
    return df_vol_annees

#-------------------------------------------------------------------------------

def afficher_volume_global(df_vol_annees, df_vol_utile):
    """Affichage du volume global des réservoirs

    Args:
        df_vol_annees (pd.DataFrame): DataFrame contenant le bilan annuel des volumes utiles
        df_vol_utile (pd.DataFrame): DataFrame contenant les chroniques des volumes utiles
    """
    # affichage du volume global
    st.subheader("Volume global des réserves en eau de VNF")
    st.write("Volume utile total des réservoirs (en $Mm^3$)")

    # affichage
    # figure
    fig, ax = plt.subplots(1,1)
    # fig.set_figwidth(largeur)
    # fig.set_figheight(hauteur)

    # depuis 2021
    df_vol_annees.columns.name = 'année'
    df_vol_annees.loc[:,2021:].plot(ax=ax, legend=True)

    # limites
    ax.set_ylim(0,160)
    ax.set_title("Evolution du volume global des réserves en eau VNF (2021-2025)")
    ax.set_xlabel('')
    ax.set_ylabel("Volume global VNF ($Mm^3$)")
    ax.set_xticks(df_vol_annees.index, df_vol_utile.index.map(lambda t:t.strftime('%B')).unique(), rotation=45, ha='right')

    ax.grid(axis='both', color='grey', linestyle='--', linewidth=0.5, alpha=0.5)

    fig.tight_layout()
    st.pyplot(fig)

#-------------------------------------------------------------------------------

def afficher_synthsese_par_reservoirs(df_vol_utile, df_carac_reservoir):
    """Affichage de la synthèse par réservoirs à la date choisie par l'utilisateur

    Args:
        df_vol_utile (pd.DataFrame): chroniques des volumes utiles des réservoirs
        df_carac_reservoir (pd.DataFrame): caractéristiques des réservoirs
    """

    # affichage de la synthèse par réservoirs à la date choisie par l'utilisateur
    st.subheader("Synthèse par réservoirs")
    # date de référence
    date_synthese = st.date_input("Choisir la date :",
                                value=dt.date.today(),
                                min_value=dt.date(2015, 1, 1),
                                max_value=dt.date.today())
    # date de référence
    date_synthese = pd.Timestamp(date_synthese.year, date_synthese.month, 1)

    # affichage de la synthèse des réservoirs
    st.write("Synthèse des réservoirs à la date de référence (1er du mois) : ", date_synthese.strftime('%d/%m/%Y'))

    date_prec = date_synthese - pd.DateOffset(months=1)
    # formatage dates
    date_synthese = date_synthese.strftime('%Y-%m')

    # remplissage à la date demandée
    df_remplissage = df_vol_utile.loc[date_synthese].T
    df_remplissage.columns = ['Volume utile']

    # noms des réservoirs
    df_remplissage['Barrages réservoirs'] = df_carac_reservoir['Barrages réservoirs']
    # capacité max utile
    df_remplissage['Capacité maximale utile'] = df_carac_reservoir['Capacité maximale utile (en Mm3)']

    # indicateur statistique regroupé par mois
    df_group_mois = df_vol_utile.groupby(df_vol_utile.index.month)
    # valeur de référence sur 10 ans avec autorisation de manque de 1 valeur manquante (10%)
    df_10_ans = df_group_mois.rolling(window=10, min_periods=9).mean()
    df_10_ans.index = df_10_ans.index.get_level_values(1)
    df_10_ans = df_10_ans.sort_index()

    # valeur de référence sur 10 ans (à la date de synthèse)
    df_remplissage["Valeur de référence sur 10 ans"] = df_10_ans.loc[date_synthese].T

    # taux de remplissage utile
    df_remplissage["Taux de remplissage utile"] = df_remplissage['Volume utile'] / df_remplissage['Capacité maximale utile']

    # arrangement des colonnes
    df_remplissage = df_remplissage[['Barrages réservoirs',
                                    'Capacité maximale utile',
                                    "Valeur de référence sur 10 ans",
                                    'Volume utile',
                                    "Taux de remplissage utile"]]

    # Tendance d'évolution par rapport au mois précédent

    # taux de remplissage mois précédent
    df_rempl_prec = df_vol_utile.loc[date_prec.strftime('%Y-%m')].T.apply(lambda x: x /
                                                                          df_remplissage.loc[x.index, 'Capacité maximale utile'])
    df_remplissage["tendance"] = df_remplissage['Taux de remplissage utile'] - df_rempl_prec[date_prec]

    # pour finalisation
    df_remplissage["Voies d'eau"] = df_carac_reservoir["Voies d'eau"]
    df_remplissage["Voies d'eau"] = df_remplissage["Voies d'eau"].ffill()
    # Attention au nom de la colonne : fragile /!\
    df_remplissage['DT'] = df_carac_reservoir['Est']
    df_remplissage['DT'] = df_remplissage['DT'].ffill()

    # mise en forme

    # index
    df_visu = df_remplissage.reset_index()
    df_visu = df_visu.set_index(['DT', "Voies d'eau", 'index'])

    # emoji selon la valeur de référence
    table_emoji = ['🔴', '🟡', '🟢']
    df_visu['emoji'] = df_visu.apply(lambda x: 0 if x['Volume utile']< 0.8*x["Valeur de référence sur 10 ans"]
                                    else 1 if x['Volume utile']< x["Valeur de référence sur 10 ans"]
                                    else 2,
                                    axis=1)
    df_visu['emoji'] = df_visu['emoji'].apply(lambda x: table_emoji[x])

    # flèche tendance
    table_fleches = ['↘','→','↗']
    df_visu['tendance'] = df_visu.apply(lambda x:0 if x['tendance']<= -0.03
                                        else 1 if x['tendance']< 0.03
                                        else 2,
                                        axis=1)
    df_visu['tendance'] = df_visu['tendance'].apply(lambda x: table_fleches[x])

    # affichage
    df_visu_styler = df_visu.style \
        .format(precision=2) \
        .map( lambda v: 'color:red;' if v == table_fleches[0]
                else 'color:orange;' if v == table_fleches[1]
                else 'color:green;', subset=['tendance']) \
        .hide(level=2, axis=0)

    col_config = {
        "Barrages réservoirs" : "Barrages réservoirs",
        "Capacité maximale utile": st.column_config.NumberColumn(
            "Capacité maximale utile dans les ouvrages VNF (en Mm3)",
            help="Valeur mise à jour en 2025",
            format="%.2f",
        ),
        "Valeur de référence sur 10 ans": st.column_config.NumberColumn(
            "Valeur de référence sur 10 ans (volume de remplissage en $Mm^3$)",
            format="%.2f",
        ),
        "Volume utile": st.column_config.NumberColumn(
            'Volume utile en $Mm^3$',
            format="%.2f",
        ),
        'Taux de remplissage utile': st.column_config.ProgressColumn(
            'Taux de remplissage utile',
            help="volume utile actuel / capacité maximale utile",
            format='percent',
        ),
    }

    # affichage du tableau avec style de df_visu
    # st.write("Tableau de synthèse des réservoirs")
    # st.write("Légende :")
    # st.write("🔴 : volume utile inférieur à 80% de la valeur de référence sur 10 ans"
    #          " (volume de remplissage en $Mm^3$)")
    # st.write("🟡 : volume utile compris entre 80% et 100% de la valeur de référence sur 10 ans" \
    # " (volume de remplissage en $Mm^3$)")
    # st.write("🟢 : volume utile supérieur à 100% de la valeur de référence sur 10 ans" \
    # " (volume de remplissage en $Mm^3$)")
    # st.write("↘ : tendance de remplissage en baisse (taux de remplissage utile inférieur à 97%)")
    # st.write("→ : tendance de remplissage stable (taux de remplissage utile compris entre 97% et 103%)")
    # st.write("↗ : tendance de remplissage en hausse (taux de remplissage utile supérieur à 103%)")
    # st.write("Les données sont issues de la base de données aGHyre v1")
    # st.write("Les données sont mises à jour mensuellement")

    st.dataframe(df_visu_styler,
                 column_config=col_config)

#-------------------------------------------------------------------------------

def afficher_disponibilite_donnees(df_vol_utile,df_id_rub):
    """Affichage de la disponibilité des données de suivi des volumes utiles des réservoirs
    Args:
        df_vol_utile (pd.DataFrame): DataFrame contenant les chroniques des volumes utiles
        df_id_rub (pd.DataFrame): DataFrame contenant les identifiants des rubriques
    """

    fig, ax = plt.subplots(1,1)
    # fig.set_figwidth(largeur)
    # fig.set_figheight(hauteur)

    # zoom sur certaines années
    df_zoom = df_vol_utile.loc["2015":"2024"]

    # nom des colonnes : avec nom des réservoirs
    idx_nom_reservoirs = df_id_rub.to_dict(orient='dict')['nom']
    df_zoom.columns = df_zoom.columns.map(float)
    df_zoom = df_zoom.rename(columns=idx_nom_reservoirs)

    sns.heatmap(df_zoom.notna(),
                cmap='YlGnBu',
                cbar=False,
                yticklabels=12,
                ax=ax
                )
    # axes des dates
    ytick_labels = [t.strftime('%Y-%m') for t in df_zoom.index[0::12]]
    ax.set_yticklabels(ytick_labels)
    ax.tick_params(axis='x', bottom=False, top=True, labelbottom=False, labeltop=True)
    ax.set_xticklabels(df_zoom.columns, rotation=45, ha='left')

    fig.tight_layout()
    st.subheader("Disponibilité des données")
    st.write("Disponibilité des données de suivi des volumes utiles des réservoirs")
    st.write("Les données sont issues de la base de données aGHyre v1")
    st.write("Les données sont mises à jour mensuellement")
    st.write("La figure ci-dessous présente la disponibilité des données de suivi des volumes utiles des réservoirs")
    st.write("Chaque ligne correspond à un mois, chaque colonne à un réservoir")
    st.write("Les cases colorées en bleu indiquent la présence de données pour le réservoir et le mois correspondant")
    st.pyplot(fig, use_container_width=True)

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

def main():
    """Fonction principale
    """
    # titre de page
    st.set_page_config(layout='centered',
                       page_title="Suivi des réserves en eau de VNF",)
    st.title("Suivi des réserves en eau de VNF")

    dialoque_statut = st.status("Requête auprès de aghyre...")

    tab1,tab2,tab3 = st.tabs(["Volume global", "Synthèse par réservoirs", "Disponitilité des données"])

    # lancement du script python de récupération des données sur aGHyre avec la commende externe python
    with dialoque_statut:
        dialoque_statut.update(expanded=True)
        df_vol_utile, df_id_rub = get_donnees_reservoirs()
    # lecture des caractéristiques des réservoirs
    df_carac_reservoir = lire_caracteristiques_reservoirs()

    # bilan global des volumes annuels
    df_vol_annees = calculer_bilan_annuel(df_vol_utile)

    # affichages
    dialoque_statut.update(label="Requête auprès de aghyre...Terminé !", expanded=False)

    with tab1:
        afficher_volume_global(df_vol_annees, df_vol_utile)
    with tab2:
        afficher_synthsese_par_reservoirs(df_vol_utile, df_carac_reservoir)
    with tab3:
        afficher_disponibilite_donnees(df_vol_utile, df_id_rub)

#-------------------------------------------------------------------------------

if __name__ == '__main__':
    main()
