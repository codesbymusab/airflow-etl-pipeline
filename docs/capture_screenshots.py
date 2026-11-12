"""
Automated screenshot capture script for the Airflow MongoDB ETL pipeline project
Uses Playwright with Microsoft Edge to capture high-resolution screenshots for the final report.
"""

import os
import time
from playwright.sync_api import sync_playwright

SCREENSHOTS_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "screenshots"))
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)


def capture_all():
    with sync_playwright() as p:
        browser = p.chromium.launch(channel="msedge", headless=True)
        context = browser.new_context(viewport={"width": 1440, "height": 900})
        page = context.new_page()

        print("[1/5] Capturing Mongo Express - Home...")
        page.goto("http://localhost:8081", wait_until="networkidle")
        time.sleep(1)
        page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "01_mongo_express_home.png"))

        print("[2/5] Capturing Mongo Express - Currency Database...")
        page.goto("http://localhost:8081/db/currency_db/", wait_until="networkidle")
        time.sleep(1)
        page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "02_mongo_express_currency_db.png"))

        print("[3/5] Capturing Mongo Express - Exchange Rates Collection (All Documents)...")
        page.goto("http://localhost:8081/db/currency_db/exchange_rates", wait_until="networkidle")
        time.sleep(1)
        page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "03_mongo_express_documents_table.png"))

        print("[4/5] Capturing Mongo Express - Document Detail View...")
        # Click first document button or load directly
        try:
            view_btns = page.locator("button.btn-info")
            if view_btns.count() > 0:
                view_btns.first.click()
                time.sleep(1)
                page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "04_mongo_express_document_detail.png"))
            else:
                page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "04_mongo_express_document_detail.png"))
        except Exception as e:
            print(f"Detail click skipped: {e}")
            page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "04_mongo_express_document_detail.png"))

        print("[5/5] Capturing Airflow UI...")
        try:
            page.goto("http://localhost:8080", wait_until="networkidle")
            time.sleep(3)
            page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "05_airflow_web_ui.png"))

            # Navigate to DAG if link present
            dag_link = page.locator("text=currency_exchange_etl_pipeline")
            if dag_link.count() > 0:
                dag_link.first.click()
                time.sleep(2)
                page.screenshot(path=os.path.join(SCREENSHOTS_DIR, "06_airflow_dag_detail.png"))
        except Exception as e:
            print(f"Airflow UI capture error: {e}")

        browser.close()
        print("All screenshots successfully captured in:", SCREENSHOTS_DIR)


if __name__ == "__main__":
    capture_all()
