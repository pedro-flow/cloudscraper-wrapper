from cloudscraper_wrapper import CloudScraperWrapper

scraper = CloudScraperWrapper()

# Download file with progress bar
success = scraper.download_file(
    url='https://example.com/file.pdf',
    output_path='downloaded_file.pdf',
    show_progress=True
)

if success:
    print("File downloaded successfully")
else:
    print("Download failed")
