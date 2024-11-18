import os
import pytesseract
from pdf2image import convert_from_path
import pandas as pd
import re
import sys
import logging
from tqdm import tqdm
from multiprocessing import Pool, cpu_count
from functools import partial
import PyPDF2
import csv
from PIL import Image, ImageEnhance, ImageFilter, ImageOps
import tempfile
import time
import traceback

# ==================== Environment Setup ====================

# Add Poppler's bin directory to PATH before any imports that might use it
os.environ['PATH'] += os.pathsep + r'C:\Users\thomsz\AppData\Local\Programs\Poppler\bin'

# Path to Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\thomsz\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

# ==================== Logging Configuration ====================

# Configure Logging
logging.basicConfig(
    filename='processing.log',
    level=logging.DEBUG,  # Set to DEBUG for detailed logs
    format='%(asctime)s:%(levelname)s:%(message)s'
)

# ==================== Regex Patterns ====================

# Define Route ID Pattern
route_id_pattern = re.compile(r'\bRoute[:\s]*:?(\d{8})\b', re.IGNORECASE)

# Define Store Name Patterns
to_pattern = re.compile(
    r'\bTo\s+\d+\s+(\d{4})-([A-Za-z0-9\s\(\)]+)', re.IGNORECASE
)

xdock_wh_pattern = re.compile(
    r'\bX-Dock WH\s*:\s*(\d{4})-([A-Za-z0-9\s]+)', re.IGNORECASE
)

# ==================== Directory Setup ====================

# Specify the directory to save matched pages
matched_pages_dir = 'matched_pages'

# Create the directory if it doesn't exist
os.makedirs(matched_pages_dir, exist_ok=True)

# ==================== Utility Functions ====================

def clean_temp_files(prefix="tess_"):
    temp_dir = tempfile.gettempdir()
    for filename in os.listdir(temp_dir):
        if filename.startswith(prefix):
            file_path = os.path.join(temp_dir, filename)
            try:
                os.remove(file_path)
            except Exception as e:
                logging.warning(f"Failed to remove temporary file {file_path}: {e}")

def robust_image_to_osd(image, config, retries=3, delay=1):
    for attempt in range(retries):
        try:
            return pytesseract.image_to_osd(image, config=config)
        except Exception as e:
            logging.warning(f"OSD attempt {attempt + 1} failed: {e}")
            time.sleep(delay)
    raise e  # Raise the exception after all retries fail

# ==================== Image Preprocessing ====================

def preprocess_image(image):
    # Convert to grayscale
    image = image.convert('L')

    # Enhance contrast
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2)

    # Apply sharpening filter
    image = image.filter(ImageFilter.SHARPEN)

    # Adaptive thresholding using PIL's point function
    image = image.point(lambda x: 0 if x < 128 else 255, '1')

    return image

# ==================== OCR and Data Extraction ====================

def extract_info_from_page(page_image, pdf_path, page_number):
    try:
        # Clean temporary files to prevent clutter
        clean_temp_files()

        # Configuration for OSD
        osd_config = r'--oem 3 --psm 0 -l osd'

        # Attempt to detect orientation using OSD with retries
        try:
            osd = robust_image_to_osd(page_image, config=osd_config)
            rotation = int(re.search(r'Rotate: (\d+)', osd).group(1))
            logging.debug(f"OSD for {pdf_path} page {page_number}: Rotate={rotation} degrees.")

            if rotation != 0:
                # Correct the image rotation
                corrected_image = page_image.rotate(-rotation, expand=True)
                logging.info(f"Rotated page {page_number} of {pdf_path} by {-rotation} degrees to correct orientation.")
            else:
                corrected_image = page_image

        except Exception as e:
            logging.warning(f"Orientation detection failed for {pdf_path} page {page_number}: {e}")
            corrected_image = page_image  # Proceed without rotation

        # Configuration for OCR
        ocr_config = r'--oem 3 --psm 6 -l eng'

        # Perform OCR on the (possibly rotated) image
        text = pytesseract.image_to_string(corrected_image, config=ocr_config)
        route_matches = route_id_pattern.findall(text)
        to_matches = to_pattern.findall(text)
        xdock_wh_matches = xdock_wh_pattern.findall(text)

        matches = []

        if not route_matches:
            # Fallback: Rotate by 180 degrees and try OCR again
            rotated_image = corrected_image.rotate(180, expand=True)
            logging.info(f"Rotated page {page_number} of {pdf_path} by 180 degrees as fallback.")
            text_rotated = pytesseract.image_to_string(rotated_image, config=ocr_config)
            route_matches = route_id_pattern.findall(text_rotated)
            to_matches = to_pattern.findall(text_rotated)
            xdock_wh_matches = xdock_wh_pattern.findall(text_rotated)

            if route_matches:
                logging.info(f"Found RouteID after rotating 180 degrees on page {page_number} of {pdf_path}.")
                text = text_rotated  # Use rotated text for logging and extraction

        for route_id in route_matches:
            location_number = None
            store_name = None
            source = None

            # Prioritize X-Dock WH matches over To matches
            if xdock_wh_matches:
                location_number, store_name = xdock_wh_matches[0]
                source = "X-Dock WH"
            elif to_matches:
                location_number, store_name = to_matches[0]
                source = "To"

            if location_number and store_name:
                # Clean StoreName by removing trailing unwanted text
                store_name = re.split(r'\sPhone', store_name)[0].strip()

                matches.append((route_id, location_number, store_name))

                # Debugging: Print extracted text and matches if RouteID is found
                debug_message = (
                    f"\n[DEBUG] PDF: {os.path.basename(pdf_path)} - Page: {page_number}\n"
                    f"Rotation Applied: {'-{}'.format(rotation) if 'rotation' in locals() and rotation !=0 else '0'} degrees\n"
                    f"OCR Text:\n{text}\n"
                    f"Extracted RouteID: {route_id}\n"
                    f"Extracted Location Number: {location_number}\n"
                    f"Extracted Store Name: {store_name}\n"
                    f"Source Pattern: {source}\n"
                )
                print(debug_message)  # Print to console for immediate feedback
                logging.debug(debug_message)  # Log to file for later analysis

        if not matches:
            logging.info(f"No Route IDs found on page {page_number} of {pdf_path} after all orientation attempts.")

        return matches  # Return list of all matches

    except Exception as e:
        logging.error(f"OCR extraction failed on {os.path.basename(pdf_path)} page {page_number}: {traceback.format_exc()}")
        return []

# ==================== PDF Processing ====================

def extract_and_save_page(pdf_path, page_number, route_id, location_number):
    try:
        # Open the original PDF
        with open(pdf_path, 'rb') as infile:
            reader = PyPDF2.PdfReader(infile)
            writer = PyPDF2.PdfWriter()

            # PyPDF2 uses zero-based indexing for pages
            writer.add_page(reader.pages[page_number - 1])

            # Define the new PDF's filename
            new_pdf_name = f"{route_id} - {location_number}.pdf"
            new_pdf_path = os.path.join(matched_pages_dir, new_pdf_name)

            # Handle naming conflicts by appending a count if the file already exists
            count = 1
            while os.path.exists(new_pdf_path):
                new_pdf_name = f"{route_id} - {location_number} ({count}).pdf"
                new_pdf_path = os.path.join(matched_pages_dir, new_pdf_name)
                count += 1

            # Write the new PDF
            with open(new_pdf_path, 'wb') as outfile:
                writer.write(outfile)

            logging.info(f"Saved extracted page to {new_pdf_path}")
            print(f"Saved: {new_pdf_path}")  # Inform the user
    except Exception as e:
        logging.error(f"Failed to extract/save page {page_number} from {pdf_path}: {traceback.format_exc()}")
        print(f"Error saving page {page_number} from {os.path.basename(pdf_path)}.")

def extract_route_and_store_ids(pdf_path):
    results = []
    try:
        # Convert PDF to images with sufficient DPI for OCR
        pages = convert_from_path(pdf_path, dpi=200)  # Adjust DPI as needed
        logging.info(f"Converted {pdf_path} to images. Number of pages: {len(pages)}")
        print(f"Converted {pdf_path} to images. Number of pages: {len(pages)}")

        for page_number, page in enumerate(pages, start=1):
            preprocessed_page = preprocess_image(page)
            matches = extract_info_from_page(preprocessed_page, pdf_path, page_number)

            for match in matches:
                route_id, location_number, store_name = match
                result = {
                    'RouteID': route_id.upper(),
                    'LocationNumber': location_number,
                    'StoreName': store_name,
                    'PDF': os.path.basename(pdf_path),
                    'PageNumber': page_number
                }
                results.append(result)
                logging.info(f"Found RouteID {route_id} on page {page_number} of {pdf_path}")
    except Exception as e:
        logging.error(f"Error processing {pdf_path}: {traceback.format_exc()}")
        print(f"Error processing {pdf_path}: {e}")

    return results

def process_pdfs(pdf_directory):
    pdf_files = [os.path.join(root, file)
                for root, dirs, files in os.walk(pdf_directory)
                for file in files if file.lower().endswith('.pdf')]

    all_results = []
    pool_size = min(cpu_count() - 1, 4)  # Limit to 4 processes or adjust as needed

    # Debug: Log the number of PDFs to process
    logging.info(f"Starting multiprocessing pool with {pool_size} processes for {len(pdf_files)} PDFs.")
    print(f"Starting multiprocessing pool with {pool_size} processes for {len(pdf_files)} PDFs.")

    with Pool(processes=pool_size) as pool:
        func = partial(extract_route_and_store_ids)
        for result in tqdm(pool.imap_unordered(func, pdf_files), total=len(pdf_files), desc="Processing PDFs"):
            all_results.extend(result)

    if all_results:
        results_df = pd.DataFrame(all_results)
        results_df.drop_duplicates(subset=['RouteID', 'LocationNumber', 'PDF', 'PageNumber'], inplace=True)
        logging.info(f"Total Route IDs extracted: {len(results_df)}")
    else:
        results_df = pd.DataFrame(columns=['RouteID', 'LocationNumber', 'StoreName', 'PDF', 'PageNumber'])
        logging.info("No Route IDs were extracted.")

    return results_df

# ==================== Result Saving ====================

def save_results(df, output_path):
    try:
        df.to_excel(output_path, index=False)
        logging.info(f"Results saved to {output_path}")
    except Exception as e:
        logging.error(f"Error saving results to {output_path}: {traceback.format_exc()}")
        print(f"Error saving results to {output_path}: {e}")

# ==================== Query Handling ====================

def search_and_save(df, queries, pdf_directory):
    for route_id, location_number in queries:
        matching_records = df[
            (df['RouteID'] == route_id.upper()) &
            (df['LocationNumber'] == location_number)
        ]

        if not matching_records.empty:
            print(f"\nRouteID '{route_id}' with Location Number '{location_number}' found in the following locations:")
            for _, row in matching_records.iterrows():
                print(f"- PDF: {row['PDF']}, Page: {row['PageNumber']}, Store: {row['StoreName']}")

                # Extract and save the specific page as a new PDF
                pdf_path = os.path.join(pdf_directory, row['PDF'])  # Adjust if your PDFs are in a different directory
                page_number = row['PageNumber']
                if isinstance(page_number, int):
                    extract_and_save_page(pdf_path, page_number, route_id, location_number)
                else:
                    print(f"Warning: Page number is not available for PDF {row['PDF']}. Cannot save specific page.")
        else:
            print(f"\nRouteID '{route_id}' with Location Number '{location_number}' not found in any PDF.")
            logging.info(f"RouteID '{route_id}' with Location Number '{location_number}' not found.")

def read_queries_from_csv(csv_path):
    queries = []
    try:
        with open(csv_path, 'r', newline='', encoding='utf-8') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                route_id = row.get('RouteID', '').strip()
                location_number = row.get('LocationNumber', '').strip()
                if route_id and location_number:
                    # Validate formats
                    if re.fullmatch(r'\d{8}', route_id) and re.fullmatch(r'\d{4}', location_number):
                        queries.append((route_id, location_number))
                    else:
                        logging.warning(f"Invalid formats in CSV row: RouteID='{route_id}', LocationNumber='{location_number}'")
    except Exception as e:
        logging.error(f"Error reading CSV file {csv_path}: {traceback.format_exc()}")
        print(f"Error reading CSV file {csv_path}: {e}")
    return queries

# ==================== Main Execution ====================

def main():
    # Determine the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Specify the directory containing PDFs
    pdf_directory = os.path.join(script_dir, 'pdfs')  # Ensure this folder contains your PDFs
    if not os.path.exists(pdf_directory):
        print(f"PDF directory '{pdf_directory}' does not exist. Please create it and add your PDFs.")
        logging.error(f"PDF directory '{pdf_directory}' does not exist.")
        sys.exit(1)

    # Specify the output Excel file
    output_excel = os.path.join(script_dir, 'route_id_mapping.xlsx')

    # Specify the CSV file path
    csv_filename = 'queries.csv'
    csv_path = os.path.join(script_dir, csv_filename)

    if not os.path.exists(csv_path):
        print(f"CSV file '{csv_filename}' not found in the script directory '{script_dir}'. Please create it with the required queries.")
        logging.error(f"CSV file '{csv_filename}' not found in '{script_dir}'.")
        sys.exit(1)

    # Logging
    logging.info("Script started.")

    # Process PDFs to extract Route IDs and Store Information
    print("Processing PDFs. Please wait...")
    route_id_df = process_pdfs(pdf_directory)

    # Save the mapping to Excel
    save_results(route_id_df, output_excel)

    # Print Extracted Data for Verification
    print("\nExtracted Data:")
    if not route_id_df.empty:
        print(route_id_df.to_string(index=False))
    else:
        print("No data was extracted.")

    # Read queries from CSV
    queries = read_queries_from_csv(csv_path)

    if not queries:
        print("No valid queries found in the CSV file. Please check the file and try again.")
        logging.info("No valid queries found in the CSV file.")
        sys.exit(1)

    # Search for the queries and save matched pages
    search_and_save(route_id_df, queries, pdf_directory)

    logging.info("Script finished.")
    print("\nProcessing complete. Check the 'matched_pages' directory for the extracted PDFs.")

if __name__ == "__main__":
    main()
