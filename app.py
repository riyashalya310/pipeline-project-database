from flask import Flask, jsonify, request, send_file
from flask_cors import CORS
import psycopg2
import os

app = Flask(__name__)
CORS(app)

DATABASE_URL = "postgresql://postgres:%40Riyashalya310@localhost:5432/data-pipeline-cdac"
SAMPLE_FILES_DIR = r"C:\Users\WBL- Intern 1.WBL-INTERN1.000\Downloads\cdac-phase2\pipeline-project-database-main\pipeline-project-database-main\sample_databases"

def get_db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    return conn

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    if file:
        file_path = os.path.join(SAMPLE_FILES_DIR, file.filename)
        file.save(file_path)

        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("INSERT INTO sample_files (file_name) VALUES (%s);", (file.filename,))
        conn.commit()
        cur.close()
        conn.close()
        
        return jsonify({"message": "File uploaded successfully"}), 200

@app.route('/api/sample-files', methods=['GET'])
def list_sample_files():
    try:
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("SELECT file_name FROM sample_files;")
        files = cur.fetchall()
        cur.close()
        conn.close()
        return jsonify([file[0] for file in files])
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500

@app.route('/api/sample-files/<filename>', methods=['GET'])
def get_sample_file(filename):
    file_path = os.path.join(SAMPLE_FILES_DIR, filename)
    if os.path.exists(file_path):
        return send_file(file_path, as_attachment=True)
    else:
        return jsonify({"error": "File not found"}), 404


# New route to delete a file
@app.route('/api/sample-files/<filename>', methods=['DELETE'])
def delete_sample_file(filename):
    try:
        # Delete file record from database
        conn = get_db_connection()
        cur = conn.cursor()
        cur.execute("DELETE FROM sample_files WHERE file_name = %s;", (filename,))
        
        if cur.rowcount == 0:
            cur.close()
            conn.close()
            return jsonify({"error": "File not found in database"}), 404

        conn.commit()
        cur.close()
        conn.close()

        # Delete the file from the file system
        file_path = os.path.join(SAMPLE_FILES_DIR, filename)
        if os.path.exists(file_path):
            os.remove(file_path)
            return jsonify({"message": "File deleted successfully"}), 200
        else:
            return jsonify({"message": "Database entry deleted, but file not found on server"}), 200
    except Exception as e:
        print(f"Error: {e}")
        return jsonify({"error": "Internal server error"}), 500

if __name__ == '__main__':
    app.run(debug=True)
