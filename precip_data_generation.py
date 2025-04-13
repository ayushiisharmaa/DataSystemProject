import os
import time
import pandas as pd
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
import schedule
import logging

# Set up logging
logging.basicConfig(
    filename='weather_data_scraper.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def setup_driver():
    """Set up and return a Chrome webdriver with appropriate options."""
    try:
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in background
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_experimental_option("prefs", {
            "download.default_directory": os.path.abspath("downloads"),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": False
        })
        
        # Create downloads directory if it doesn't exist
        os.makedirs("downloads", exist_ok=True)
        
        # Initialize the Chrome driver
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        return driver
    except Exception as e:
        logging.error(f"Failed to set up driver: {e}")
        raise

def navigate_and_download_data():
    """Navigate to the website, select options, and download data."""
    try:
        driver = setup_driver()
        
        # Navigate to the website
        url = "http://deltaweather.extension.msstate.edu/weather-station-result/DREC-2014"
        driver.get(url)
        logging.info(f"Successfully navigated to {url}")
        
        # Wait for page to load
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.ID, "hourly-tab"))
        )
        
        # Click on "Hourly" tab
        hourly_tab = driver.find_element(By.ID, "hourly-tab")
        hourly_tab.click()
        logging.info("Clicked on Hourly tab")
        
        # Wait for hourly form to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.ID, "formHourly"))
        )
        
        # Select "Last 24 Hours"
        time_select = Select(driver.find_element(By.ID, "hourlyTimePeriod"))
        time_select.select_by_visible_text("Last 24 Hours")
        logging.info("Selected 'Last 24 Hours'")
        
        # Wait for the form to update
        time.sleep(2)
        
        # Clear all checkboxes first
        checkboxes = driver.find_elements(By.CSS_SELECTOR, "#formHourly input[type='checkbox']")
        for checkbox in checkboxes:
            if checkbox.is_selected():
                checkbox.click()
        
        # Select required fields: Record Date, Record Time, and Precipitation
        field_ids = ["Record_Date", "Record_Time", "Precipitation"]
        for field_id in field_ids:
            try:
                checkbox = driver.find_element(By.ID, field_id)
                if not checkbox.is_selected():
                    checkbox.click()
                logging.info(f"Selected {field_id} checkbox")
            except Exception as e:
                logging.warning(f"Could not find or select {field_id} checkbox: {e}")
        
        # Click Export Data button
        export_button = driver.find_element(By.ID, "exportDataHourly")
        export_button.click()
        logging.info("Clicked Export Data button")
        
        # Wait for the download to complete (assuming a small file)
        time.sleep(5)
        
        # Close the driver
        driver.quit()
        logging.info("Chrome driver closed")
        
        # Return the timestamp to help identify the downloaded file
        return datetime.now().strftime("%Y%m%d_%H%M%S")
    except Exception as e:
        logging.error(f"Error during navigation and download: {e}")
        if 'driver' in locals():
            driver.quit()
        raise

def find_latest_download():
    """Find the most recently downloaded file in the downloads directory."""
    try:
        download_dir = os.path.abspath("downloads")
        files = [os.path.join(download_dir, f) for f in os.listdir(download_dir) if os.path.isfile(os.path.join(download_dir, f))]
        if not files:
            logging.error("No files found in downloads directory")
            return None
        
        latest_file = max(files, key=os.path.getctime)
        logging.info(f"Latest downloaded file: {latest_file}")
        return latest_file
    except Exception as e:
        logging.error(f"Error finding latest download: {e}")
        return None

def update_master_excel(master_file="precipitation_master.xlsx"):
    """Update the master Excel file with the new data."""
    try:
        latest_file = find_latest_download()
        if not latest_file:
            logging.error("No download file found to process")
            return False
        
        # Read the downloaded data
        new_data = pd.read_csv(latest_file)
        logging.info(f"Successfully read downloaded data with shape {new_data.shape}")
        
        # Check if master file exists, if not create it
        if not os.path.exists(master_file):
            logging.info(f"Master file {master_file} not found. Creating new file.")
            new_data.to_excel(master_file, index=False)
            return True
        
        # Read existing master file
        try:
            master_data = pd.read_excel(master_file)
            logging.info(f"Successfully read master data with shape {master_data.shape}")
        except Exception as e:
            logging.error(f"Failed to read master file, creating new one: {e}")
            new_data.to_excel(master_file, index=False)
            return True
        
        # Combine data, avoiding duplicates
        combined_data = pd.concat([master_data, new_data]).drop_duplicates().reset_index(drop=True)
        logging.info(f"Combined data has shape {combined_data.shape}")
        
        # Save the updated master file
        combined_data.to_excel(master_file, index=False)
        logging.info(f"Successfully updated {master_file}")
        return True
    except Exception as e:
        logging.error(f"Error updating master Excel: {e}")
        return False

def run_daily_task():
    """Run the complete process of downloading and updating the data."""
    try:
        logging.info("Starting daily data collection task")
        navigate_and_download_data()
        update_master_excel()
        logging.info("Daily task completed successfully")
    except Exception as e:
        logging.error(f"Failed to complete daily task: {e}")

def main():
    """Main function to schedule and run the data collection task."""
    logging.info("Starting weather data automation script")
    
    # Schedule the task to run every day at 7:00 AM
    schedule.every().day.at("07:00").do(run_daily_task)
    logging.info("Task scheduled to run daily at 07:00")
    
    # Run once immediately on startup
    run_daily_task()
    
    # Keep the script running
    while True:
        schedule.run_pending()
        time.sleep(60)  # Check every minute

if __name__ == "__main__":
    main()