from minio import Minio
import urllib.request
import pandas as pd
import sys
import requests
from bs4 import BeautifulSoup
import re
import os
from minio import Minio
from minio.error import MinioException

def main():
    grab_data()
    write_data_minio()

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    # URL de la page contenant les liens vers les datasets
    url = "https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page"

    # Envoyer une requête HTTP pour récupérer le contenu de la page
    response = requests.get(url)

    if response.status_code == 200:
        soup = BeautifulSoup(response.text, 'html.parser')

        # Utiliser BeautifulSoup pour extraire les liens correspondant au modèle d'URL
        base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{}.parquet"
        # Utilisez une liste de dates spécifiques de "2018-01" à "2023-08"
        dates = ["20{:02d}-{:02d}".format(year, month) for year in range(23, 24) for month in range(1, 13)]
        links = [base_url.format(date) for date in dates]

        # Dossier de destination pour enregistrer les fichiers téléchargés
        destination_folder = "/home/chris/Documents/ATL-Datamart-main/src/data/raw/"

        # Assurez-vous que le dossier de destination existe
        os.makedirs(destination_folder, exist_ok=True)

        for link in links:
            # Générer le nom de fichier en utilisant la date dans l'URL
            filename = link.split("/")[-1]

            # Chemin complet du fichier de destination
            file_path = os.path.join(destination_folder, filename)

            # Tester si le lien est accessible
            link_response = requests.head(link)

            if link_response.status_code == 200:
                # Le lien est accessible, téléchargez le fichier
                file_response = requests.get(link)
                with open(file_path, 'wb') as file:
                    file.write(file_response.content)
                print(f"Téléchargement réussi : {filename}")
            else:
                # Le lien n'est pas accessible
                print(f"Échec du téléchargement pour : {filename}. Lien inaccessible.")

        print("Tous les fichiers ont été téléchargés.")

    else:
        print(f"Échec de la récupération de la page web. Code d'état : {response.status_code}")


def write_data_minio():
    """
    Cette méthode met tous les fichiers Parquet dans Minio.
    Ne pas faire cette méthode pour le moment.
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="minio",
        secret_key="minio123"
    )
    bucket: str = "alpha"  # Remplacez "NOM_DU_BUCKET_ICI" par votre nom de bucket Minio
    found = client.bucket_exists(bucket)

    if not found:
        client.make_bucket(bucket)
    else:
        print("Le bucket " + bucket + " existe déjà")

    # Chemin du dossier contenant les fichiers téléchargés
    source_folder = "/home/chris/Documents/ATL-Datamart-main/src/data/raw/"

    # Récupérer la liste des fichiers dans le dossier
    files = os.listdir(source_folder)

    for file_name in files:
        file_path = os.path.join(source_folder, file_name)
        object_name = f"{bucket}/{file_name}"

        try:
            # Télécharger le fichier dans Minio
            client.fput_object(bucket, object_name, file_path)
            print(f"Envoi réussi : {file_name}")
        except MinioException as err:  # Utilisez MinioException
            print(f"Échec de l'envoi pour {file_name}: {err}")


if __name__ == '__main__':
    sys.exit(main())
