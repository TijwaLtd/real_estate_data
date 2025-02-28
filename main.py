from flask import Flask, request, jsonify, send_file
import io
import os
import uuid
import tempfile
from werkzeug.utils import secure_filename
from real_estate_processor import RealEstateDataProcessor  

app = Flask(__name__)

# Configure upload settings
ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls'}
MAX_CONTENT_LENGTH = 50 * 1024 * 1024  # 50MB max file size

# Create a temporary directory for file processing
TEMP_FOLDER = tempfile.gettempdir()

def allowed_file(filename):
    """Check if the file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/health', methods=['GET'])
def health_check():
    """Simple health check endpoint"""
    return jsonify({'status': 'ok'})

@app.route('/process', methods=['POST'])
def process_files():
    """
    API endpoint to process real estate data files
    
    Expects:
        - Multipart form data with files under 'files' key
        
    Returns:
        - CSV file download with processed data
    """
    # Check if any file was uploaded
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    
    # Check if any files were selected
    if not files or all(file.filename == '' for file in files):
        return jsonify({'error': 'No files selected'}), 400
    
    # Check file types
    for file in files:
        if not allowed_file(file.filename):
            return jsonify({'error': f'File type not allowed: {file.filename}'}), 400
    
    # Store files in memory
    memory_files = []
    for file in files:
        file_content = file.read()
        file_name = secure_filename(file.filename)
        memory_files.append((file_content, file_name))
    
    try:
        # Initialize processor
        processor = RealEstateDataProcessor()
        
        # Process files directly in memory
        result_df = processor.process_memory_files(memory_files)
        
        # Check if we got any results
        if result_df.empty:
            return jsonify({'error': 'No valid data found in the uploaded files'}), 400
        
        # Create a buffer to store the CSV output
        output_buffer = io.BytesIO()
        result_df.to_csv(output_buffer, index=False)
        output_buffer.seek(0)
        
        # Generate a unique filename for the result
        result_filename = f"real_estate_data_{uuid.uuid4().hex}.csv"
        
        # Return the CSV file
        return send_file(
            output_buffer,
            mimetype='text/csv',
            as_attachment=True,
            download_name=result_filename
        )
    
    except Exception as e:
        app.logger.error(f"Error processing files: {str(e)}")
        return jsonify({'error': f'Error processing files: {str(e)}'}), 500

@app.route('/process-sync', methods=['POST'])
def process_files_sync():
    """
    Alternative API endpoint that returns JSON instead of a file download
    
    Expects:
        - Multipart form data with files under 'files' key
        
    Returns:
        - JSON with processed data
    """
    # Check if any file was uploaded
    if 'files' not in request.files:
        return jsonify({'error': 'No files provided'}), 400
    
    files = request.files.getlist('files')
    
    # Check if any files were selected
    if not files or all(file.filename == '' for file in files):
        return jsonify({'error': 'No files selected'}), 400
    
    # Check file types
    for file in files:
        if not allowed_file(file.filename):
            return jsonify({'error': f'File type not allowed: {file.filename}'}), 400
    
    # Store files in memory
    memory_files = []
    for file in files:
        file_content = file.read()
        file_name = secure_filename(file.filename)
        memory_files.append((file_content, file_name))
    
    try:
        # Initialize processor
        processor = RealEstateDataProcessor()
        
        # Process files directly in memory
        result_df = processor.process_memory_files(memory_files)
        
        # Check if we got any results
        if result_df.empty:
            return jsonify({'error': 'No valid data found in the uploaded files'}), 400
        
        # Convert DataFrame to JSON
        result_json = result_df.to_dict(orient='records')
        
        return jsonify({
            'status': 'success',
            'records_count': len(result_json),
            'data': result_json
        })
    
    except Exception as e:
        app.logger.error(f"Error processing files: {str(e)}")
        return jsonify({'error': f'Error processing files: {str(e)}'}), 500

# Add configuration route to get app settings
@app.route('/config', methods=['GET'])
def get_config():
    """Return application configuration information"""
    return jsonify({
        'allowed_extensions': list(ALLOWED_EXTENSIONS),
        'max_file_size_mb': MAX_CONTENT_LENGTH / (1024 * 1024)
    })

# Add a static file for documentation or simple web interface
@app.route('/', methods=['GET'])
def index():
    """Serve a simple HTML page for manual testing"""
    return '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Real Estate Data Processor</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 40px; }
            .container { max-width: 800px; margin: 0 auto; }
            .form-group { margin-bottom: 20px; }
            button { padding: 10px 15px; background-color: #4CAF50; color: white; border: none; cursor: pointer; }
            .info { background-color: #f8f9fa; padding: 15px; border-radius: 5px; margin-top: 20px; }
        </style>
    </head>
    <body>
        <div class="container">
            <h1>Real Estate Data Processor</h1>
            <p>Upload your real estate data files for processing.</p>
            
            <form action="/process" method="post" enctype="multipart/form-data">
                <div class="form-group">
                    <label for="files">Select files (CSV, XLSX, XLS):</label><br>
                    <input type="file" id="files" name="files" multiple accept=".csv,.xlsx,.xls">
                </div>
                <button type="submit">Process Files (Download CSV)</button>
            </form>
            
            <div class="info">
                <h3>API Endpoints:</h3>
                <ul>
                    <li><code>/process</code> - Upload files and receive processed CSV</li>
                    <li><code>/process-sync</code> - Upload files and receive JSON response</li>
                    <li><code>/health</code> - Health check endpoint</li>
                    <li><code>/config</code> - Get application configuration</li>
                </ul>
            </div>
        </div>
    </body>
    </html>
    '''

if __name__ == '__main__':
    # Get port from environment variable or use default 5000
    port = int(os.environ.get('PORT', 5000))
    
    # Get debug mode from environment variable or use default False
    debug = os.environ.get('DEBUG', 'False').lower() == 'true'
    
    print(f"Starting Real Estate Data Processor API on port {port}")
    print(f"Debug mode: {'ON' if debug else 'OFF'}")
    print(f"Allowed file types: {', '.join(ALLOWED_EXTENSIONS)}")
    print(f"Max file size: {MAX_CONTENT_LENGTH / (1024 * 1024):.1f} MB")
    
    # Run the Flask app
    app.run(host='0.0.0.0', port=port, debug=debug)