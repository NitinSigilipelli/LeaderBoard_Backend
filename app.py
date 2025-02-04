from flask import Flask, request, jsonify
import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from flask_cors import CORS
from pymongo import MongoClient
from bson import ObjectId
from werkzeug.security import check_password_hash
from werkzeug.security import generate_password_hash

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])


# Google Sheets API setup
scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
credentials = ServiceAccountCredentials.from_json_keyfile_name("credentials.json", scope)
gspread_client = gspread.authorize(credentials)


MONGO_URI = "mongodb://localhost:27017/"  # Replace with your MongoDB URI
client = MongoClient(MONGO_URI)
db = client["leaderboard_db"]  # Database name
leaderboard_collection = db["leaderboard"]  # Collection name
users_collection = db["users"] 
sheets_collection = db.sheets
email = "test@example.com"
password = "password123"

# Hash the password
hashed_password = generate_password_hash(password)

# Create the user document with the hashed password
user_data = {
    "email": email,
    "password": hashed_password,  # Store the hashed password
    "isAdmin": True  # Example isAdmin field
}

# Insert the user data into MongoDB
users_collection.insert_one(user_data)

def fetch_data(sheet_url):
    """Fetch data from a Google Sheet and return it as a DataFrame."""
    print(f"Fetching data from URL: {sheet_url}")
    try:
        sheet = gspread_client.open_by_url(sheet_url).sheet1
        data = sheet.get_all_records()
        print(f"Data fetched from {sheet_url}: {data}")
        if not data:
            print(f"No data found in sheet: {sheet_url}")
            return pd.DataFrame()  # Return an empty DataFrame if no data is found
        df = pd.DataFrame(data)
        df.rename(columns=lambda x: x.strip(), inplace=True)  # Remove extra spaces
        return df
    except Exception as e:
        print(f"Error fetching data from {sheet_url}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error


def align_columns(dataframes):
    """Align columns across all DataFrames by filling missing ones with default values."""
    if not dataframes:
        return []

    # Collect all unique column names
    all_columns = set()
    for df in dataframes:
        all_columns.update(df.columns)

    # Convert to a sorted list (FIX: Avoid using a set)
    all_columns = sorted(list(all_columns))

    aligned_dataframes = []
    for df in dataframes:
        aligned_df = df.copy()
        for col in all_columns:
            if col not in aligned_df.columns:
                aligned_df[col] = 0  # Fill missing columns with 0
        aligned_dataframes.append(aligned_df[all_columns])  # Use sorted list

    return aligned_dataframes



def objectid_to_str(obj):
    """Recursively convert all ObjectId instances in the given dictionary to strings."""
    if isinstance(obj, dict):
        return {key: objectid_to_str(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [objectid_to_str(item) for item in obj]
    elif isinstance(obj, ObjectId):
        return str(obj)
    else:
        return obj

@app.route('/getLeaderboard', methods=['GET'])
def get_leaderboard():
    """Fetch the latest leaderboard data."""
    try:
        # Retrieve all records from the leaderboard collection
        leaderboard_data = list(leaderboard_collection.find({}, {"_id": 0}))  # Exclude the _id field
        if not leaderboard_data:
            return jsonify({"message": "No leaderboard data found"}), 404
        leaderboard_data = objectid_to_str(leaderboard_data)  # Convert ObjectIds to strings
        return jsonify(leaderboard_data), 200
    except Exception as e:
        print(f"Error fetching leaderboard: {e}")
        return jsonify({"error": "Failed to fetch leaderboard"}), 500
users_collection = db["users"]  # Collection to store user details




@app.route('/api/sheets/finishScraping', methods=['POST'])
def finish_scraping():
    data = request.json
    sheet_id = data.get('sheetId')

    # Check if sheetId is provided
    if not sheet_id:
        return jsonify({"error": "sheetId is required"}), 400

    # Reset the isSelected field to false after scraping
    result = sheets_collection.update_one(
        {"_id": ObjectId(sheet_id)},
        {"$set": {"selected": False}}
    )

    if result.matched_count == 0:
        return jsonify({"error": "Sheet not found"}), 404

    return jsonify({"message": "Scraping finished, isSelected set to false"}), 200

@app.route('/login', methods=['POST'])
def login():
    """Authenticate a user and return an admin flag and user email."""
    data = request.get_json()
    email = data.get('email')
    password = data.get('password')
    print(email)
    print(password)

    if not email or not password:
        return jsonify({"message": "Email and password are required."}), 400

    user = users_collection.find_one({"email": email})  # Find user by email
    if user:
        # Check password using hashed password (assuming passwords are hashed)
        if check_password_hash(user["password"], password):
            # If valid login, return success response with isAdmin status
            return jsonify({
                "email": email,
                "isAdmin": user.get("isAdmin", False)
            }), 200
        else:
            return jsonify({"message": "Invalid password."}), 401
    else:
        return jsonify({"message": "User not found."}), 404



from bson import ObjectId
import pandas as pd
from flask import jsonify, request

@app.route('/scrape', methods=['POST'])
def scrape_selected_sheets():
    """Fetch selected Google Sheets data, process Java/Python scores, and update leaderboard."""
    print("Scraping Started")
    data = request.json
    print("Received Data:", data)
    sheet_ids = data.get("sheet_ids", [])

    if not sheet_ids:
        return jsonify({"error": "No sheet IDs provided"}), 400

    try:
        object_ids = [ObjectId(sheet_id) for sheet_id in sheet_ids]
        selected_sheets = list(sheets_collection.find({"_id": {"$in": object_ids}}))

        if not selected_sheets:
            return jsonify({"error": "No sheets found"}), 404

        # Extract sheet URLs and target columns
        sheet_urls = [sheet["url"] for sheet in selected_sheets]
        target_columns = set()
        for sheet in selected_sheets:
            target_columns_raw = sheet.get("targetColumns", [])
            target_columns_updated = [''.join(col) if isinstance(col, list) else col for col in target_columns_raw]
            target_columns.update(target_columns_updated)
        target_columns = list(target_columns)

    except Exception as e:
        print(f"Error fetching sheets from MongoDB: {e}")
        return jsonify({"error": "Failed to fetch sheets"}), 500

    try:
        # Fetch and align data
        all_data = [fetch_data(url) for url in sheet_urls]
        aligned_data = align_columns(all_data)
    except Exception as e:
        print(f"Error fetching or aligning sheets: {e}")
        return jsonify({"error": "Failed to fetch or align sheets"}), 500

    # Combine data
    combined_data = pd.concat(aligned_data, ignore_index=True)
    combined_data.fillna(0, inplace=True)
    print("Combined Data:", combined_data.columns)

    # Ensure required columns exist
    required_columns = ["Roll Number", "Name of the student", "Branch"] + target_columns
    for column in required_columns:
        if column not in combined_data.columns:
            return jsonify({"error": f"Required column {column} is missing in sheets"}), 400

    # Aggregation and ranking
    try:
        # Aggregate all numeric columns
        aggregation_rules = {col: "sum" for col in combined_data.columns if col not in ["Roll Number", "Name of the student", "Branch"]}
        print("Aggregation rules:", aggregation_rules)

        # Group by required fields and aggregate all columns
        leaderboard_full = (
            combined_data.groupby(["Roll Number", "Name of the student", "Branch"])
            .agg(aggregation_rules)
            .reset_index()
        )

        def compute_java_score(row):
            if row["Have you did problems in coding bat java"].strip().lower() == "yes":
                java_threads = [f"No.of problems solved in java thread {i}" for i in range(1, 18)]
                return sum(3 if 0 < row.get(thread, 0) < 5 else 5 for thread in java_threads if row.get(thread, 0) > 0)
            return 0

# 🟢 Compute Python Score
        def compute_python_score(row):
            if row["Have you did problems in coding bat python"].strip().lower() == "yes":
                python_threads = [f"No.of problems solved in python thread {i}" for i in range(1, 9)]
                return sum(3 if 0 < row.get(thread, 0) < 5 else 5 for thread in python_threads if row.get(thread, 0) > 0)
            return 0

        # Add computed scores to leaderboard
        leaderboard_full["Coding bat Java Total Score"] = leaderboard_full.apply(compute_java_score, axis=1)
        leaderboard_full["Coding bat Python Total Score"] = leaderboard_full.apply(compute_python_score, axis=1)
        print(leaderboard_full["Java Total Score"].values)
        print(leaderboard_full["Python Total Score"].values)

        # Select only required columns for final leaderboard
        final_columns = ["Roll Number", "Name of the student", "Branch"] + target_columns + ["Java Total Score", "Python Total Score"]
        leaderboard = leaderboard_full[final_columns]

        # 🏅 Final Total Score
        leaderboard["Total Score"] = leaderboard[target_columns + ["Java Total Score", "Python Total Score"]].sum(axis=1)
        leaderboard = leaderboard.sort_values(by="Total Score", ascending=False)

    except Exception as e:
        print(f"Error during aggregation: {e}")
        return jsonify({"error": "Failed to process data"}), 500

    try:
        # Clear existing leaderboard
        leaderboard_collection.delete_many({})
        # Insert new leaderboard data
        leaderboard_records = leaderboard.to_dict(orient="records")
        leaderboard_collection.insert_many(leaderboard_records)
    except Exception as e:
        print(f"Error saving to MongoDB: {e}")
        return jsonify({"error": "Failed to save data"}), 500

    print("Leaderboard:", leaderboard)
    return leaderboard.to_json(orient='records')


# Add a new sheet
@app.route('/addSheet', methods=['POST'])
def add_sheet():
    data = request.json
    if not data.get('name') or not data.get('url') or not data.get('targetColumns'):
        return jsonify({'error': 'Name, URL, and target columns are required'}), 400

    # Define the default column order
    default_columns = ["Roll Number", "Name of the student", "Branch", "Total Score"]
    target_columns = data['targetColumns']
    
    # Ensure Total Score is always last
    column_order = (
        default_columns[:-1]  # Everything except Total Score
        + [col for col in target_columns if col not in default_columns]
        + ["Total Score"]
    )

    sheet = {
        'name': data['name'],
        'url': data['url'],
        'targetColumns': target_columns,
        'columnOrder': column_order
    }

    result = sheets_collection.insert_one(sheet)
    sheet['_id'] = str(result.inserted_id)  # Convert ObjectId to string
    return jsonify({'message': 'Sheet added successfully', 'sheet': sheet}), 201


# Get all sheets
@app.route('/getSheets', methods=['GET'])
def get_sheets():
    sheets = list(sheets_collection.find())
    for sheet in sheets:
        sheet['_id'] = str(sheet['_id'])  # Convert ObjectId to string
    return jsonify(sheets), 200

from bson import ObjectId

@app.route('/deleteSheet', methods=['POST'])
def delete_sheet():
    try:
        # Get the sheet ID from the request body
        data = request.get_json()  # Assumes JSON format
        sheet_id = data.get('sheetId')  # Get the sheetId from the request body

        if not sheet_id:
            return jsonify({'message': 'Sheet ID is required'}), 400
        
        # Check if the sheetId is a valid ObjectId
        if len(sheet_id) != 24 or not all(c in '0123456789abcdef' for c in sheet_id.lower()):
            return jsonify({'message': 'Invalid sheet ID format'}), 400

        # Try to convert the sheet_id to ObjectId
        sheet_id = ObjectId(sheet_id)

        print(f"Received sheetId: {sheet_id}")  # Debugging line to check the sheetId
        
        # Find the sheet by its ID (use MongoDB's ObjectId)
        sheet_to_delete = sheets_collection.find_one({'_id': sheet_id})
        
        if sheet_to_delete:
            sheets_collection.delete_one({'_id': sheet_id})  # Remove the sheet from the collection
            return jsonify({'message': 'Sheet deleted successfully'}), 200
        else:
            return jsonify({'message': 'Sheet not found'}), 404

    except Exception as e:
        print(f"Error occurred: {e}")  # Log the error
        return jsonify({'message': 'Internal Server Error', 'error': str(e)}), 500
    

@app.route('/getTargetColumns/<sheet_id>', methods=['GET'])
def get_target_columns(sheet_id):
    try:
        sheet_object_id = ObjectId(sheet_id)
        sheet = sheets_collection.find_one({"_id": sheet_object_id})
        if not sheet:
            return jsonify({"error": "Sheet not found"}), 404

        target_columns = sheet.get("targetColumns", [])
        if not isinstance(target_columns, list):
            raise ValueError("Invalid targetColumns format")

        cleaned_target_columns = [
            ''.join(col) if isinstance(col, list) else col for col in target_columns
        ]

        return jsonify({"target_columns": cleaned_target_columns}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500
@app.route('/updateTargetColumns/<sheet_id>', methods=['PUT'])
def update_target_columns(sheet_id):
    try:
        sheet_object_id = ObjectId(sheet_id)
        sheet = sheets_collection.find_one({"_id": sheet_object_id})
        if not sheet:
            return jsonify({"error": "Sheet not found"}), 404
        
        new_target_columns = request.json.get("targetColumns")
        if not new_target_columns or not isinstance(new_target_columns, list):
            return jsonify({"error": "Invalid or missing targetColumns"}), 400
        
        updated_sheet = sheets_collection.update_one(
            {"_id": sheet_object_id},
            {"$set": {"targetColumns": new_target_columns}}
        )
        
        if updated_sheet.matched_count == 0:
            return jsonify({"error": "Failed to update target columns"}), 500
        
        return jsonify({"message": "Target columns updated successfully"}), 200
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(debug=True)
