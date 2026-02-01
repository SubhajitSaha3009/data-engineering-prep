import csv
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

INPUT_FILE = 'employees1.csv'
OUTPUT_FILE = 'employees_transformed.csv'

def read_data(file_path):
    """
    Reads CSV data from the given file path and returns a list of dictionaries.
    """
    try:
        with open(file_path, mode='r', newline='', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            data = [row for row in reader]
        #print(type(data))
        return data
    except FileNotFoundError:
        logging.critical(f"Error: File '{file_path}' not found.")
        return []
    except Exception as e:
        logging.error(f"An error occurred while reading the file: {e}")
        return []
def transform_data(data):
    """
    Transforms the data by:
    1. Converting 'salary' to integer, defaulting to 0 if missing.
    2. Filtering to include only employees from the 'IT' department.
    3. Filling missing 'location' with 'Unknown'.
    4. Adding a new field 'is_high_earner' which is True if salary >= 80000, else False.
    """
    transformed_data = []
    for row in data:
        # Handle missing Salary
        salary = row["salary"]
        if salary == '' or salary is None:
            salary = 0
        else:
            salary = int(salary)
        #Filter only IT department
        if row["department"] != "IT":
            continue
        #Handle missing Location
        #location = row["location"] if row["location"] else "Unknown"

        transformed_data.append(
            {
                "emp_id": row["emp_id"],
                "name": row["name"],
                "department": row["department"],
                "salary": salary,
                "location": row["location"] if row["location"] else "My Location",
                "is_high_earner": salary >= 80000
            }
        )
    #print(type(transformed_data))
    return transformed_data

def write_data(file_path, data):
    if not data:
        logging.warning("No data to write.")
        return
    """
    .keys() gets the filednames from the first dictionary in the list.
    Writes the transformed data to a CSV file at the given file path.
    """
    fieldnames = data[0].keys()
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    logging.info("Starting ETL process.")
    data = read_data(INPUT_FILE)
    transformed_data = transform_data(data)
    write_data(OUTPUT_FILE, transformed_data)
    logging.info("ETL process completed successfully.")

if __name__ == "__main__":
    main()