import pandas as pd

def extract_api_values(input_file, output_file):
    # Load the dataset
    df = pd.read_csv(input_file)

    # Filter out rows where API column is not empty
    df_filtered = df[df["API"].notnull() & (df["API"] != "[]")].copy()

    # Explode the API column to create separate rows for each API value
    df_filtered.loc[:, "API"] = df_filtered["API"].apply(lambda x: x.strip("[]").split(", ") if isinstance(x, str) else [x])
    df_exploded = df_filtered.explode("API")

    # Extract only the API column and remove duplicates
    df_api_only = df_exploded[["API"]].drop_duplicates()

    # Save the API values to a new CSV file
    df_api_only.to_csv(output_file, index=False)
    print(f"Extracted API values saved to {output_file}")

# Define file paths
input_file = "extracted_data.csv"  # Replace with the actual input file path
output_file = "api_values.csv"  # Replace with the desired output file path

# Run the function
extract_api_values(input_file, output_file)

import time
import csv
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Initialize WebDriver (Ensure ChromeDriver is installed)
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # Run in headless mode
driver = webdriver.Chrome(options=options)

# Base URL
search_url = "https://www.drillingedge.com/search"

# Read API Numbers from CSV file
input_csv = "api_values.csv"  # Replace with your input CSV file
output_csv = "well_data.csv"

# Open output CSV and write header
with open(output_csv, mode="w", newline="") as file:
    writer = csv.writer(file)
    writer.writerow(["API Number", "Well Name", "Oil Produced", "Gas Produced", "Well Status", "Well Type", "Closest City"])

    # Read API numbers from input file
    with open(input_csv, mode="r", newline="") as infile:
        reader = csv.reader(infile)
        next(reader)  # Skip header if present

        for row in reader:
            api_key = row[0].strip()
            if not api_key:
                continue  # Skip empty rows

            print(f"Processing API Number: {api_key}")

            # Step 1: Open the search page and input API number
            driver.get(search_url)
            time.sleep(2)

            # Locate input field and submit API number
            try:
                input_field = driver.find_element(By.NAME, "api_no")
                input_field.clear()
                input_field.send_keys(api_key)
                input_field.send_keys(Keys.RETURN)  # Press Enter

                # Wait for search results
                well_link_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//a[contains(@href, '/north-dakota')]"))
                )
                well_url = well_link_tag.get_attribute("href")
                print(f"Extracted Well URL: {well_url}")
            except:
                print(f"Error: No results found for API {api_key}")
                writer.writerow([api_key, "N/A", "N/A", "N/A", "N/A", "N/A", "N/A"])
                continue  # Move to the next API key

            # Step 2: Navigate to the well page
            driver.get(well_url)
            time.sleep(5)

            # Extract well details
            try:
                well_name = driver.find_element(By.XPATH, "//div[contains(text(), 'Well Name:')]/span[@class='detail_point']").text.strip()
            except:
                well_name = "N/A"

            try:
                oil_produced_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, "//p[contains(@class, 'block_stat')]/span[@class='dropcap']"))
                )
                oil_produced = oil_produced_tag.text.strip()
            except:
                oil_produced = "N/A"

            try:
                gas_produced_tag = driver.find_elements(By.XPATH, "//p[contains(@class, 'block_stat')]/span[@class='dropcap']")
                gas_produced = gas_produced_tag[1].text.strip() if len(gas_produced_tag) > 1 else "N/A"
            except:
                gas_produced = "N/A"

            try:
                well_status = driver.find_element(By.XPATH, "//th[text()='Well Status']/following-sibling::td").text.strip()
            except:
                well_status = "N/A"

            try:
                well_type = driver.find_element(By.XPATH, "//th[text()='Well Type']/following-sibling::td").text.strip()
            except:
                well_type = "N/A"

            try:
                closest_city = driver.find_element(By.XPATH, "//th[text()='Closest City']/following-sibling::td").text.strip()
            except:
                closest_city = "N/A"

            # Save data to CSV
            writer.writerow([api_key, well_name, oil_produced, gas_produced, well_status, well_type, closest_city])
            print(f"Saved data for API {api_key}")

# Close the browser
driver.quit()
print(f"Data extraction completed. Saved to {output_csv}")

