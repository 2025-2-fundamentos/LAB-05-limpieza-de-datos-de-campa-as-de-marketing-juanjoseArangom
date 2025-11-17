"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import numpy as np
from pathlib import Path

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    input_dir = Path('files/input')
    output_dir = Path('files/output')

    output_dir.mkdir(parents=True, exist_ok=True)

    zip_files = list(input_dir.glob('*.csv.zip'))
    
    if not zip_files:
        print(f"Error: No se encontraron archivos '*.csv.zip' en {input_dir}")
        return

    all_dfs = []
    for zip_file in zip_files:
        try:
            df = pd.read_csv(zip_file, compression='zip')
            all_dfs.append(df)
        except Exception as e:
            print(f"Error al leer el archivo {zip_file}: {e}")
            
    if not all_dfs:
        print("No se cargaron datos.")
        return

    df_full = pd.concat(all_dfs, ignore_index=True)

    client_cols_input = [
        'client_id', 'age', 'job', 'marital', 
        'education', 'credit_default', 'mortgage' 
    ]
    cols_to_use = [col for col in client_cols_input if col in df_full.columns]
    df_client = df_full[cols_to_use].copy()

    df_client['job'] = df_client['job'].str.replace('.', '', regex=False).str.replace('-', '_')
    df_client['education'] = df_client['education'].str.replace('.', '_', regex=False).replace('unknown', pd.NA)
    df_client['credit_default'] = np.where(df_client['credit_default'] == 'yes', 1, 0)
    df_client['mortgage'] = np.where(df_client['mortgage'] == 'yes', 1, 0)
    
    df_client.to_csv(output_dir / 'client.csv', index=False)


    campaign_cols = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome', 
        'campaign_outcome', 'day', 'month'
    ]
    df_campaign = df_full[campaign_cols].copy()

    df_campaign['previous_outcome'] = np.where(df_campaign['previous_outcome'] == 'success', 1, 0)
    df_campaign['campaign_outcome'] = np.where(df_campaign['campaign_outcome'] == 'yes', 1, 0)
    df_campaign['year'] = 2022
    
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    df_campaign['month'] = df_campaign['month'].str.lower().map(month_map)

    date_cols = ['year', 'month', 'day']
    df_campaign['last_contact_day'] = pd.to_datetime(df_campaign[date_cols])
    df_campaign['last_contact_day'] = df_campaign['last_contact_day'].dt.strftime('%Y-%m-%d')

    df_campaign = df_campaign.rename(columns={
        'last_contact_day': 'last_contact_date'
    })
    
    final_campaign_cols = [
        'client_id', 'number_contacts', 'contact_duration',
        'previous_campaign_contacts', 'previous_outcome', 
        'campaign_outcome', 'last_contact_date' 
    ]
    
    df_campaign[final_campaign_cols].to_csv(output_dir / 'campaign.csv', index=False)

    
    economics_cols = [
        'client_id', 
        'cons_price_idx',
        'euribor_three_months'
    ]
    df_economics = df_full[economics_cols].copy()
    
    df_economics.to_csv(output_dir / 'economics.csv', index=False)



if __name__ == "__main__":
    clean_campaign_data()
