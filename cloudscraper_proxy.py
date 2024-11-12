from cloudscraper_wrapper import CloudScraperWrapper

# Initialize with a single proxy
scraper = CloudScraperWrapper(
    proxy='http://user:pass@host:port',
    max_retries=3
)

# Make request through proxy
response = scraper.get('https://example.com')
if response:
    print("Successfully made request through proxy")
