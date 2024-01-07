import requests
import os
from datetime import datetime

# Obtenez la date actuelle au format "YYYY-MM" (par exemple, "2023-08")
current_date = datetime.now().strftime("%Y-%m")

# URL pour le dataset du mois actuel
url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{current_date}.parquet"

# Dossier de destination pour enregistrer le fichier téléchargé
destination_folder = "/workspaces/Atelier-DataMart/minio/python_datasets/"

# Assurez-vous que le dossier de destination existe
os.makedirs(destination_folder, exist_ok=True)

# Générer le nom de fichier en utilisant la date
filename = f"yellow_tripdata_{current_date}.parquet"

# Chemin complet du fichier de destination
file_path = os.path.join(destination_folder, filename)

# Télécharger le fichier
response = requests.get(url)

if response.status_code == 200:
    with open(file_path, 'wb') as file:
        file.write(response.content)
    print(f"Téléchargement réussi : {filename}")
else:
    print(f"Échec du téléchargement pour : {filename}")
