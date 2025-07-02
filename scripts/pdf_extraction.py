import mysql.connector
import os
import re
from PyPDF2 import PdfReader
import pdfplumber
import csv
import numpy as np

def read_pdf(pdf_path,filename):
    with pdfplumber.open(pdf_path) as pdf:
        num_pages = len(pdf.pages)
        print(f"Number of pages: {num_pages}")

        for i, page in enumerate(pdf.pages, start=0):
            page_text = page.extract_text()
            if not page_text:
                page_text = "Text could not be extracted by pdfplumber"

            text = f"--- Page {i} ---\n{page_text}\n\n"
            read_text_extracted_from_PDF_page(i, text,filename)

    return "PDF processing completed."

def read_text_extracted_from_PDF_page(page_no,text,filename):
    def get_api():
        api_id_pattern = (
            r"(?:\b\d{2}-\d{3}-\d{5}\b)|(?:\b\d{2} - \d{3} - \d{5}\b)|(?:^API(?:\s)?#:(?:\s)?\d{10}$)|(?:API(?:\s+)?\d{10})"
        )

        api_id = set(re.findall(api_id_pattern, text))
        api_id = [re.sub(r"[^\d-]", "", id) for id in api_id]

        api_id_final_pattern = r"(\d{2})(\d{3})(\d{5})"
        for i in range(len(api_id)):
            # Find all matches of the pattern in the string
            if len(api_id[i]) != 12:
                match = re.search(api_id_final_pattern, api_id[i])
                if match:
                    # Format the match into 'XX-XXX-XXXXX' format
                    api_id[i] = f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
        return api_id

    extracted_data = {}  # Dictionary to store results
    processed_keys = set()  # Set to track extracted keys
    # print(f"API : {get_api()}") if get_api() else None
    patterns = {
        "operator": r"Well Operator : (.*?)\n",
        "well_name": r"Well Name\s*:\s*(.*)\n",
        "enseco_job": r"\bJob (\d+)\b",
        "job_type": r"Type of Incident : (.*?)\n",
        "county": r"County : (.*?)\n",
        "latitude": r'(\d+°\d+\'\d+\.\d+\"[NS])',
        "longitude": r'(\d+°\d+\'\d+\.\d+\"[EW])',
        "datum": r"Vertical Datum to DDZ\s+([\d.]+ ft)",
        "date_simulated": r"Date Stimulated\s*\n\s*(\d{1,2}/\d{1,2}/\d{4})",
        "formation": r"Stimulated Formation\s*\n\s*([^\n]+)",
        "top_bottom_stimulation_stages": r"Top \(Ft\)\s*Bottom \(Ft\)\s*Stimulation Stages\n\s*(\d+)\s+(\d+)\s+(\d+)",
        "psi": r"Maximum Treatment Pressure \(PSI\)\s*\n\s*(\d+)",
        "lbs": r"Lbs Proppant\s*\n\s*(\d+)",
        "type_treatment": r"Type Treatment\s*\n\s*([^\n]+)",
        "volume": r"Volume Units\s*\n(\d+)\s*(\w+)",
        "max_treatment_rate": r"Maximum Treatment Rate \(BBLS/Min\)\s*\n\s*(\d+(?:\.\d+)?)"
    }

    for field, regex in patterns.items():
        found_match = re.search(regex, text)
        if found_match:
            extracted_data[field] = found_match.group(1)
            processed_keys.add(field)  # Track processed fields
        else:
            extracted_data[field]= np.nan

    output_csv = "extracted_data.csv"
    with open(output_csv, mode="a", newline="", encoding="utf-8") as file:
        field_names = ["pdf_name", "page_no"] + list(extracted_data.keys()) + ["API"]
        writer = csv.DictWriter(file, fieldnames=field_names)

        # Write the header only if the file is empty
        if file.tell() == 0:
            writer.writeheader()

        writer.writerow({
            "pdf_name": filename,
            "page_no": page_no,
            **extracted_data,
            "API": get_api()
        })

    # print(f"Extracted data saved to {output_csv}")

    return extracted_data

def Extract_data_from_pdfs():
    output_csv = "extracted_data.csv"

    # Check if the file exists and delete it
    if os.path.exists(output_csv):
        os.remove(output_csv)
        print(f"{output_csv} has been deleted.")
    else:
        print(f"{output_csv} does not exist.")

    directory = "DSCI560_Lab5"  # Change this to your target directory
    count = 0
    for filename in os.listdir(directory):
        file_path = os.path.join(directory, filename)

        if os.path.isfile(file_path):  # Ensure it's a file
            count += 1
            print(f"\n\n file name {file_path} \n")
            read_pdf(file_path, filename)

    print("Count of pdf files", count)
    return None


def csv_to_sql():

    db_config = {
        'host': 'localhost',
        'user': 'root',
        'password': 'Password@123',
        # 'database': 'OilWellAnalysis'  # We'll create/use a DB dynamically
    }

    # Connect to MySQL server (without specifying a database)
    connection = mysql.connector.connect(
        host=db_config["host"],
        user=db_config["user"],
        password=db_config["password"]
    )
    cursor = connection.cursor()

    # 1) Create Database if not exists
    cursor.execute("CREATE DATABASE IF NOT EXISTS Lab6_database")
    print("Database 'Lab6_database' created or already exists.")

    # 2) Switch to that database
    connection.database = "Lab6_database"

    # 3) CSV file path
    csv_file = "extracted_data.csv"
    table_name = "oilwell_data"

    # 4) If CSV exists, read its headers to create table dynamically
    if not os.path.exists(csv_file):
        print(f"CSV file '{csv_file}' not found.")
        return

    with open(csv_file, mode="r", encoding="utf-8") as file:
        reader = csv.reader(file)
        headers = next(reader, None)

        if not headers:
            print("CSV file is empty or missing headers.")
            return

        columns_sql = []
        for col in headers:
            # Escape backticks or reserved words if needed
            safe_col = col.replace("`", "``")
            columns_sql.append(f"`{safe_col}` TEXT")

        # Include an AUTO_INCREMENT PRIMARY KEY
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {", ".join(columns_sql)}
        );
        """
        cursor.execute(create_table_query)
        print(f"Table '{table_name}' created or already exists with columns from CSV headers.")

        # 6) Prepare SQL Insert Statement using placeholders
        placeholders = ", ".join(["%s"] * len(headers))
        safe_headers = [f"`{col.replace('`','``')}`" for col in headers]  # escape backticks
        insert_query = f"INSERT INTO {table_name} ({', '.join(safe_headers)}) VALUES ({placeholders})"

        # 7) Insert each row from the CSV
        row_count = 0
        for row in reader:
            cursor.execute(insert_query, row)
            row_count += 1

        connection.commit()
        print(f"Inserted {row_count} rows into '{table_name}' from '{csv_file}'.")

    # 8) Close Database Connection
    cursor.close()
    connection.close()
    print("MySQL Connection Closed.")

    return None

if __name__ == "__main__":
    Extract_data_from_pdfs()

    csv_to_sql()










# db_config = {
#     'host': 'localhost',
#     'user': 'root',
#     'password': 'Password@123',
#     'database': 'OilWellAnalysis'
# }
#
#
# # Function to create the database and tables
# def create_database_and_tables():
#     try:
#         conn = mysql.connector.connect(
#             host=db_config['host'],
#             user=db_config['user'],
#             password=db_config['password']
#         )
#         cursor = conn.cursor()
#         cursor.execute("CREATE DATABASE IF NOT EXISTS OilWellAnalysis")
#         conn.database = db_config['database']
#         cursor.execute("DROP TABLE IF EXISTS oilwelldata")
#         cursor.execute("""
#             CREATE TABLE IF NOT EXISTS stocks (
#                 id INT AUTO_INCREMENT PRIMARY KEY,
#                 ticker VARCHAR(10),
#                 name VARCHAR(100),
#                 date VARCHAR(100),
#                 sector VARCHAR(100),
#                 market_cap BIGINT,
#                 open_price DECIMAL(10, 4),
#                 high_price DECIMAL(10, 4),
#                 low_price DECIMAL(15, 4),
#                 close_price DECIMAL(15, 4),
#                 volume BIGINT
#             )
#         """)
#         print("Database and table created successfully.")
#     except mysql.connector.Error as err:
#         print(f"Error: {err}")
#     finally:
#         cursor.close()
#         conn.close()