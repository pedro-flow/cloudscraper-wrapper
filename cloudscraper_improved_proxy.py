from cloudscraper_wrapper import CloudScraperWrapper

# Setup with multiple proxies
proxies = [
    'http://user1:pass1@host1:port1',
    'http://user2:pass2@host2:port2',
    'http://user3:pass3@host3:port3'
]

scraper = CloudScraperWrapper(
    proxy_rotation=True,
    proxy_list=proxies
)

# Make multiple requests with rotating proxies
for i in range(5):
    response = scraper.get('https://example.com')
    print(f"Request {i+1} completed")

# Check proxy performance
stats = scraper.get_proxy_stats()
for proxy, stat in stats.items():
    print(f"\nProxy: {proxy}")
    print(f"Success rate: {stat['success_rate']:.2%}")
