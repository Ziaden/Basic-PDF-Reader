import os
import pytesseract
from pdf2image import convert_from_path
import re
from PIL import Image, ImageEnhance, ImageFilter, ImageOps

# Add Poppler's bin directory to PATH temporarily
os.environ['PATH'] += os.pathsep + r'C:\Users\thomsz\AppData\Local\Programs\Poppler\bin'

# Path to Tesseract OCR executable
pytesseract.pytesseract.tesseract_cmd = r'C:\Users\thomsz\AppData\Local\Programs\Tesseract-OCR\tesseract.exe'

# Define Patterns
route_id_pattern = re.compile(r'\bRoute[:\s]*:?(\d{8})\b', re.IGNORECASE)
to_pattern = re.compile(r'\bTo\s+\d+\s+(\d{4})-([A-Za-z0-9\s\(\)]+)', re.IGNORECASE)
xdock_wh_pattern = re.compile(r'\bX-Dock WH\s*:\s*(\d{4})-([A-Za-z0-9\s]+)', re.IGNORECASE)

def preprocess_image(image):
    """
    Preprocess the image to enhance OCR accuracy.
    Steps:
    - Convert to grayscale
    - Apply noise reduction
    - Enhance contrast
    - Apply thresholding
    """
    image = image.convert('L')  # Grayscale
    image = image.filter(ImageFilter.MedianFilter())  # Noise reduction
    enhancer = ImageEnhance.Contrast(image)
    image = enhancer.enhance(2)  # Adjust contrast as needed
    image = ImageOps.autocontrast(image)  # Auto contrast
    image = image.point(lambda x: 0 if x < 140 else 255, '1')  # Thresholding
    return image

def extract_info_from_page(page_image, pdf_path, page_number):
    """
    Perform OCR on the preprocessed page and extract Route IDs and Location Numbers.
    """
    try:
        # OCR configuration
        custom_config = r'--oem 3 --psm 6 -l eng'

        # Perform OCR
        text = pytesseract.image_to_string(page_image, config=custom_config)
        print(f"\n[DEBUG] OCR Text for {os.path.basename(pdf_path)} page {page_number}:\n{text}\n")

        route_matches = route_id_pattern.findall(text)
        to_matches = to_pattern.findall(text)
        xdock_wh_matches = xdock_wh_pattern.findall(text)

        matches = []

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

                matches.append({
                    'RouteID': route_id.upper(),
                    'LocationNumber': location_number,
                    'StoreName': store_name,
                    'PDF': os.path.basename(pdf_path),
                    'PageNumber': page_number
                })

                # Debugging: Print extracted information
                print(f"[DEBUG] Extracted RouteID: {route_id}, LocationNumber: {location_number}, StoreName: {store_name}, Source: {source}")

        return matches

    except Exception as e:
        print(f"Error during OCR extraction on {os.path.basename(pdf_path)} page {page_number}: {e}")
        return []

def test_pdf_processing(pdf_path):
    """
    Test processing a single PDF file.
    """
    try:
        # Convert PDF to images
        pages = convert_from_path(pdf_path, dpi=200, poppler_path=r'C:\Users\thomsz\AppData\Local\Programs\Poppler\bin')
        print(f"Successfully converted {pdf_path} to images. Number of pages: {len(pages)}")

        all_matches = []

        for page_number, page in enumerate(pages, start=1):
            preprocessed_page = preprocess_image(page)
            matches = extract_info_from_page(preprocessed_page, pdf_path, page_number)
            all_matches.extend(matches)

        if all_matches:
            print("\nExtracted Data:")
            for match in all_matches:
                print(match)
        else:
            print("\nNo data was extracted.")

    except Exception as e:
        print(f"Error processing {pdf_path}: {e}")

if __name__ == "__main__":
    # Specify the path to your test PDF
    test_pdf_path = r'C:\Users\thomsz\Desktop\code\venv_freight_new\pdfs\0727_001.pdf'  # Replace with your PDF path

    test_pdf_processing(test_pdf_path)
