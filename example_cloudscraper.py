#!/usr/bin/env python3

import asyncio
import json
from cloudscraper_wrapper import CloudScraperWrapper, RequestError, ProxyError
from datetime import datetime

async def main():
    # Initialize the scraper with various configuration options
    scraper = CloudScraperWrapper(
        delay_range=(2, 5),
        max_retries=3,
        timeout=30,
        custom_headers={
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept-Language': 'en-US,en;q=0.9'
        },
        proxy_rotation=True,
        proxy_list=[
            'http://proxy1:port1',
            'http://proxy2:port2',
            'http://proxy3:port3'
        ],
        log_level='INFO',
        log_file='scraping.log'
    )

    # Example URLs for demonstration
    urls = {
        'basic': 'https://example.com',
        'api': 'https://api.example.com/data',
        'download': 'https://example.com/sample.pdf'
    }

    try:
        print("=== Basic GET Request ===")
        response = scraper.get(
            urls['basic'],
            use_cache=True,
            max_cache_age=3600
        )
        if response:
            print(f"Successfully retrieved page, length: {len(response)} characters")

        print("\n=== POST Request Example ===")
        data = {
            'key1': 'value1',
            'key2': 'value2',
            'timestamp': datetime.now().isoformat()
        }
        post_response = scraper.post(
            urls['api'],
            json_data=data
        )
        if post_response:
            print("Successfully sent POST request")
            try:
                json_response = json.loads(post_response)
                print(f"Response data: {json_response}")
            except json.JSONDecodeError:
                print("Response wasn't JSON format")

        print("\n=== Batch GET Requests ===")
        batch_urls = [
            'https://example1.com',
            'https://example2.com',
            'https://example3.com'
        ]
        responses = await scraper.batch_get(
            batch_urls,
            max_concurrent=2
        )
        for url, response in zip(batch_urls, responses):
            if response:
                print(f"Successfully retrieved {url}")
            else:
                print(f"Failed to retrieve {url}")

        print("\n=== File Download Example ===")
        download_success = scraper.download_file(
            urls['download'],
            'downloads/sample.pdf',
            show_progress=True
        )
        if download_success:
            print("File downloaded successfully")
        else:
            print("File download failed")

        print("\n=== Proxy Statistics ===")
        proxy_stats = scraper.get_proxy_stats()
        for proxy, stats in proxy_stats.items():
            print(f"\nProxy: {proxy}")
            print(f"Success rate: {stats['success_rate']:.2%}")
            print(f"Total requests: {stats['total_requests']}")

        print("\n=== General Statistics ===")
        stats = scraper.get_stats()

    except RequestError as e:
        print(f"Request error occurred: {e}")
    except ProxyError as e:
        print(f"Proxy error occurred: {e}")
    except Exception as e:
        print(f"Unexpected error occurred: {e}")
    finally:
        # Clean up resources
        scraper.cleanup()

if __name__ == "__main__":
    # Example of custom proxy management
    def handle_proxy_failure(proxy):
        print(f"Proxy {proxy} has failed, removing from rotation...")
        # Implement proxy replacement logic here

    # Run the async main function
    print("Starting CloudScraper Wrapper Example...")
    asyncio.run(main())
    print("Example completed!")
