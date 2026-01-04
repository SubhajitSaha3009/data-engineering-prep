import csv

INPUT_FILE = 'employees.csv'
OUTPUT_FILE = 'employees_transformed.csv'

def read_data(file_path):
    with open(file_path, mode='r', newline='', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        data = [row for row in reader]
    return data

def transform_data(data):
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
        location = row["location"] if row["location"] else "Unknown"

        transformed_data.append(
            {
                "emp_id": row["emp_id"],
                "name": row["name"],
                "department": row["department"],
                "salary": salary,
                "location": location,
                "is_high_earner": salary >= 80000
            }
        )
    return transformed_data

def write_data(file_path, data):
    if not data:
        print("No data to write.")
        return
    fieldnames = data[0].keys()
    with open(file_path, mode='w', newline='', encoding='utf-8') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(data)

def main():
    data = read_data(INPUT_FILE)
    transformed_data = transform_data(data)
    write_data(OUTPUT_FILE, transformed_data)
    print("ETL process completed successfully.")

if __name__ == "__main__":
    main()