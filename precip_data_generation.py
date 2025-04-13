import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
# import schedule
import logging

# Set up logging
logging.basicConfig(
    filename='weather_data_log.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Path to save downloaded data and master Excel file
DOWNLOAD_DIR = os.path.join(os.path.expanduser("~"), "Downloads")
MASTER_EXCEL_FILE = os.path.join(os.path.expanduser("~"), "Documents", "precipitation_data.xlsx")

def setup_driver():
    """Set up and return a Chrome WebDriver with appropriate options."""
    chrome_options = Options()
    chrome_options.add_experimental_option("prefs", {
        "download.default_directory": DOWNLOAD_DIR,
        "download.prompt_for_download": False,
        "download.directory_upgrade": True,
        "safebrowsing.enabled": True
    })
    # Uncomment the line below if you want the browser to run headlessly (no UI)
    # chrome_options.add_argument("--headless")
    
    driver = webdriver.Chrome(options=chrome_options)
    return driver

def extract_weather_data():
    """Main function to navigate to the website, extract data, and update the Excel file."""
    logging.info("Starting weather data extraction")
    
    try:
        driver = setup_driver()
        
        # Step 1: Navigate to the weather station webpage
        url = "http://deltaweather.extension.msstate.edu/weather-station-result/DREC-2014"
        driver.get(url)
        logging.info(f"Navigated to {url}")
        
        # Step 2: Select options
        # Wait for the page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "time_period"))
        )
        
        # Select "Hourly"
        time_period_select = Select(driver.find_element(By.ID, "time_period"))
        time_period_select.select_by_visible_text("Hourly")
        logging.info("Selected 'Hourly' from time period dropdown")
        
        # Wait for duration dropdown to be clickable
        WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.ID, "duration"))
        )
        
        # Select "Last 24 Hours"
        duration_select = Select(driver.find_element(By.ID, "duration"))
        duration_select.select_by_visible_text("Last 24 Hours")
        logging.info("Selected 'Last 24 Hours' from duration dropdown")
        
        # Wait for parameters checkboxes to be clickable
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "parameter_check"))
        )
        
        # Uncheck all parameters first
        checkboxes = driver.find_elements(By.CLASS_NAME, "parameter_check")
        for checkbox in checkboxes:
            if checkbox.is_selected():
                checkbox.click()
        
        # Find and check specific parameters
        params_to_select = ["Record Date", "Record Time", "Precipitation"]
        for param in params_to_select:
            # Find the label containing the parameter text
            param_labels = driver.find_elements(By.XPATH, f"//label[contains(text(), '{param}')]")
            for label in param_labels:
                # Get the associated checkbox
                checkbox_id = label.get_attribute("for")
                checkbox = driver.find_element(By.ID, checkbox_id)
                if not checkbox.is_selected():
                    checkbox.click()
                    logging.info(f"Selected parameter: {param}")
        
        # Step 3: Click "Export Data"
        export_button = WebDriverWait(driver, 10).until(
            EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Export Data')]"))
        )
        export_button.click()
        logging.info("Clicked 'Export Data' button")
        
        # Wait for download to complete
        time.sleep(5)  # Adjust time as needed
        
        # Step 4: Process the downloaded file
        process_downloaded_file()
        
        driver.quit()
        logging.info("Browser session closed")
        
    except Exception as e:
        logging.error(f"Error in extract_weather_data: {str(e)}")
        if 'driver' in locals():
            driver.quit()

def process_downloaded_file():
    """Process the downloaded CSV file and update the master Excel file."""
    try:
        # Find the most recently downloaded file in the download directory
        files = [os.path.join(DOWNLOAD_DIR, f) for f in os.listdir(DOWNLOAD_DIR) if f.endswith('.csv')]
        if not files:
            logging.error("No CSV files found in download directory")
            return
        
        latest_file = max(files, key=os.path.getctime)
        logging.info(f"Processing downloaded file: {latest_file}")
        
        # Read the downloaded data
        new_data = pd.read_csv(latest_file)
        
        # Format the data as needed
        new_data['Date'] = pd.to_datetime(new_data['Record Date'] + ' ' + new_data['Record Time'])
        
        # Check if master Excel file exists
        if os.path.exists(MASTER_EXCEL_FILE):
            # Read existing data
            existing_data = pd.read_excel(MASTER_EXCEL_FILE)
            
            # Combine new data with existing data
            combined_data = pd.concat([existing_data, new_data], ignore_index=True)
            
            # Remove duplicates based on date and time
            combined_data = combined_data.drop_duplicates(subset=['Date'], keep='last').sort_values('Date')
            
            # Save updated data
            combined_data.to_excel(MASTER_EXCEL_FILE, index=False)
            logging.info(f"Updated existing file: {MASTER_EXCEL_FILE}")
        else:
            # Create new Excel file with the data
            new_data.to_excel(MASTER_EXCEL_FILE, index=False)
            logging.info(f"Created new file: {MASTER_EXCEL_FILE}")
        
        # Optionally, delete the downloaded file
        # os.remove(latest_file)
        # logging.info(f"Deleted downloaded file: {latest_file}")
        
    except Exception as e:
        logging.error(f"Error in process_downloaded_file: {str(e)}")

def daily_job():
    """Function to run daily at 7:00 AM."""
    logging.info("Running scheduled daily job")
    current_date = datetime.now().strftime("%Y-%m-%d")
    logging.info(f"Extracting data for {current_date}")
    extract_weather_data()
    logging.info("Daily job completed")

def main():
    """Main function to set up scheduling and run initial data extraction."""
    logging.info("Starting weather data automation script")
    
    # Run once immediately
    extract_weather_data()
    
    # Schedule to run daily at 7:00 AM
    # schedule.every().day.at("07:00").do(daily_job)
    logging.info("Scheduled daily job at 07:00")
    
    # Keep the script running
    """ while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute """

if __name__ == "__main__":
    main()