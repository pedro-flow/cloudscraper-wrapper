from cloudscraper_wrapper import CloudScraperWrapper

scraper = CloudScraperWrapper(delay_range=(2, 5))

# Simple GET request
response = scraper.get('https://example.com')
if response:
    print("Successfully retrieved page")
    print(f"Content length: {len(response)}")
