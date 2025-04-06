from playwright.sync_api import sync_playwright

def fetch_cbbi_index_with_playwright():
    try:
        with sync_playwright() as p:
            # Launch a headless browser
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()

            # Open the CBBI page
            cbbi_url = "https://colintalkscrypto.com/cbbi/"
            page.goto(cbbi_url)

            # Wait for the CBBI index element to load
            page.wait_for_selector("h1.title.confidence-score-value")

            # Extract the CBBI index
            cbbi_index = page.locator("h1.title.confidence-score-value").text_content().strip()

            print(f"CBBI Index: {cbbi_index}")
            browser.close()
            return cbbi_index

    except Exception as e:
        print(f"Error fetching CBBI index with Playwright: {e}")
        return None

# Fetch the CBBI index
cbbi_index = fetch_cbbi_index_with_playwright()
