from flask import Flask, request, jsonify, send_file
import io
import os
import uuid
from flask_cors import CORS
import tempfile
from werkzeug.utils import secure_filename
from real_estate_processor import RealEstateDataProcessor  

app = Flask(__name__)
# Enable CORS for all origins
CORS(app)

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
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Chesapeake Stays - Real Estate Data Processor</title>
        <style>
            :root {
                --primary: #1a4b84;
                --secondary: #2c7873;
                --accent: #f4a261;
                --light: #f8f9fa;
                --dark: #2b2d42;
                --white: #ffffff;
                --gray: #6c757d;
            }
            
            * {
                margin: 0;
                padding: 0;
                box-sizing: border-box;
            }
            
            body {
                font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
                line-height: 1.6;
                color: var(--dark);
                background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
                min-height: 100vh;
            }
            
            .container {
                max-width: 1200px;
                margin: 0 auto;
                padding: 2rem;
            }
            
            .header {
                text-align: center;
                margin-bottom: 3rem;
                padding: 2rem 0;
                background: var(--white);
                border-radius: 15px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            
            .logo {
                max-width: 250px;
                margin-bottom: 1rem;
            }
            
            h1 {
                color: var(--primary);
                font-size: 2.5rem;
                margin-bottom: 1rem;
            }
            
            .subtitle {
                color: var(--gray);
                font-size: 1.2rem;
                margin-bottom: 2rem;
            }
            
            .upload-section {
                background: var(--white);
                padding: 2rem;
                border-radius: 15px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                margin-bottom: 2rem;
            }
            
            .form-group {
                margin-bottom: 1.5rem;
            }
            
            .form-group label {
                display: block;
                margin-bottom: 0.5rem;
                color: var(--dark);
                font-weight: 600;
            }
            
            .file-input-wrapper {
                position: relative;
                margin-bottom: 1rem;
            }
            
            .file-input {
                width: 100%;
                padding: 1rem;
                border: 2px dashed var(--primary);
                border-radius: 8px;
                background: var(--light);
                cursor: pointer;
                transition: all 0.3s ease;
            }
            
            .file-input:hover {
                border-color: var(--accent);
                background: #fff;
            }
            
            .btn {
                display: inline-block;
                padding: 1rem 2rem;
                font-size: 1.1rem;
                font-weight: 600;
                text-align: center;
                text-decoration: none;
                border-radius: 8px;
                transition: all 0.3s ease;
                cursor: pointer;
                border: none;
                width: 100%;
            }
            
            .btn-primary {
                background: var(--primary);
                color: var(--white);
            }
            
            .btn-primary:hover {
                background: var(--secondary);
                transform: translateY(-2px);
            }
            
            .info-section {
                background: var(--white);
                padding: 2rem;
                border-radius: 15px;
                box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
            }
            
            .info-section h2 {
                color: var(--primary);
                margin-bottom: 1rem;
                font-size: 1.5rem;
            }
            
            .api-list {
                list-style: none;
            }
            
            .api-list li {
                padding: 1rem;
                border-bottom: 1px solid var(--light);
                display: flex;
                align-items: center;
            }
            
            .api-list li:last-child {
                border-bottom: none;
            }
            
            .api-endpoint {
                background: var(--light);
                padding: 0.5rem 1rem;
                border-radius: 4px;
                font-family: monospace;
                margin-right: 1rem;
                color: var(--primary);
            }
            
            .api-description {
                color: var(--gray);
            }
            
            .footer {
                text-align: center;
                margin-top: 3rem;
                padding: 2rem 0;
                color: var(--gray);
            }
            
            @media (max-width: 768px) {
                .container {
                    padding: 1rem;
                }
                
                h1 {
                    font-size: 2rem;
                }
                
                .btn {
                    padding: 0.8rem 1.5rem;
                }
            }
        </style>
    </head>
    <body>
        <div class="container">
            <header class="header">
                <h1>Chesapeake Stays</h1>
                <p class="subtitle">Real Estate Data Processing Portal</p>
            </header>
            
            <main>
                <section class="upload-section">
                    <form action="/process" method="post" enctype="multipart/form-data">
                        <div class="form-group">
                            <label for="files">Upload Real Estate Data Files</label>
                            <div class="file-input-wrapper">
                                <input type="file" id="files" name="files" multiple 
                                       accept=".csv,.xlsx,.xls" class="file-input">
                            </div>
                            <small style="color: var(--gray);">
                                Accepted formats: CSV, XLSX, XLS (Max 50MB)
                            </small>
                        </div>
                        <button type="submit" class="btn btn-primary">
                            Process & Download Results
                        </button>
                    </form>
                </section>
                
                <section class="info-section">
                    <h2>Available API Endpoints</h2>
                    <ul class="api-list">
                        <li>
                            <span class="api-endpoint">/process</span>
                            <span class="api-description">Upload files and receive processed CSV</span>
                        </li>
                        <li>
                            <span class="api-endpoint">/process-sync</span>
                            <span class="api-description">Upload files and receive JSON response</span>
                        </li>
                        <li>
                            <span class="api-endpoint">/health</span>
                            <span class="api-description">Check API health status</span>
                        </li>
                        <li>
                            <span class="api-endpoint">/config</span>
                            <span class="api-description">View application configuration</span>
                        </li>
                    </ul>
                </section>
            </main>
            
            <footer class="footer">
                <p>&copy; 2025 Chesapeake Stays LLC. All rights reserved.</p>
            </footer>
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