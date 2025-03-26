import os
import pyodbc
import asyncio
from dotenv import load_dotenv
from pymongo import MongoClient
from concurrent.futures import ThreadPoolExecutor

load_dotenv()

mongo_uri = os.getenv("MONGODB_CONNECTION_STRING")
mongo_client = MongoClient(mongo_uri)
mongo_db = mongo_client[os.getenv("MONGO_DB")]

# SQL server connection string info
sql_server = os.getenv("SQL_SERVER")
username = os.getenv("USER_NAME")
password = os.getenv("PASSWORD")
identity_db = os.getenv("IDENTITY_DATABASE")
patient_db = os.getenv("PATIENT_DATABASE")
sql_server_driver = "{ODBC Driver 17 for SQL Server}"

# print(
#     f"sql_server:{sql_server}",
#     f"sql_server_driver: {sql_server_driver}",
#     f"username: {username}",
#     f"password: {password}",
#     f"identity_db: {identity_db}",
#     f"patient_db: {patient_db}",
#     sep="\n",
# )


collections = ["activity", "body", "daily", "sleep"]


def fetch_reference_ids():
    """
    Fetches all reference_ids from MongoDB collections.
    Returns a list of unique reference_ids.
    """
    reference_ids = set()
    for collection_name in collections:
        collection = mongo_db[collection_name]
        for document in collection.find({}, {"user.reference_id": 1}):
            if (
                "user" in document
                and "reference_id" in document["user"]
                and document["user"]["reference_id"] != ""
            ):
                reference_ids.add(document["user"]["reference_id"])
    return list(reference_ids)


def fetch_diagnoses(reference_id):
    """
    Fetches diagnoses for a given reference_id by querying:
    1. ScripsIdentity database for EntityId.
    2. ScripsPatient database for diagnoses.
    """
    try:
        with pyodbc.connect(
            f"DRIVER={sql_server_driver};"
            f"SERVER={sql_server};"
            f"DATABASE={identity_db};"
            f"UID={username};"
            f"PWD={password};"
        ) as identity_db_connection:
            with identity_db_connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                        EntityId 
                    FROM 
                        UserOrganizationRole 
                    WHERE 
                        UserId = ? 
                        AND ScripsRole = 1
                    """,
                    reference_id,
                )
                entity_id_row = cursor.fetchone()

                if not entity_id_row:
                    return reference_id, []

                entity_id = entity_id_row[0]

        with pyodbc.connect(
            f"DRIVER={sql_server_driver};"
            f"SERVER={sql_server};"
            f"DATABASE={patient_db};"
            f"UID={username};"
            f"PWD={password};"
        ) as patient_db_connection:
            with patient_db_connection.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT 
                        ConditionCode, BodySiteDisplay 
                    FROM 
                        Condition 
                    WHERE 
                        Subject = ? 
                        AND StatusCode = 'Active' 
                        AND BodySiteDisplay != 'null'
                    """,
                    entity_id,
                )
                all_data = cursor.fetchall()
                diagnoses_codes = [row[0] for row in all_data]
                body_sites = [row[1] for row in all_data]
        return reference_id, diagnoses_codes, body_sites
    except Exception as e:
        print(f"Error fetching diagnoses for reference_id {reference_id}: {e}")
        return reference_id, []


def store_diagnoses(reference_id, diagnoses, body_sites):
    """
    Stores diagnoses in MongoDB under the patient_diagnoses collection.
    """
    diagnoses_collection = mongo_db["patient_diagnoses"]
    diagnoses_collection.insert_one(
        {
            "reference_id": reference_id,
            "diagnoses_codes": diagnoses,
            "body_site_display": body_sites,
        }
    )


async def process_reference_ids():
    """
    Processes all reference_ids asynchronously.
    """
    reference_ids = fetch_reference_ids()
    with ThreadPoolExecutor(max_workers=4) as executor:
        loop = asyncio.get_event_loop()
        futures = [
            loop.run_in_executor(executor, fetch_diagnoses, reference_id)
            for reference_id in reference_ids
        ]
        for future in asyncio.as_completed(futures):
            reference_id, diagnoses, body_sites = await future
            if diagnoses and body_sites:
                store_diagnoses(reference_id, diagnoses, body_sites)


if __name__ == "__main__":
    asyncio.run(process_reference_ids())
