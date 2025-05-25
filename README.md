# NYC Marathon Data Scraper

A Python script to scrape runner data from the New York City Marathon results.

## Features

- Asynchronous HTTP requests for efficient data collection
- Adaptive rate limiting to avoid server overload
- Comprehensive error handling and retry logic
- DNS resolution and connectivity verification
- Progress tracking with ETA calculation
- Incremental data saving (every 100 pages)
- Checkpointing support to resume interrupted scrapes
- Memory management for large datasets

## Requirements

- Python 3.7+
- aiohttp
- Optional: aiofiles (for better async file operations)

## Installation

```bash
pip install aiohttp
pip install aiofiles  # Optional but recommended
```

## Usage

1. Clone this repository
2. Navigate to the project directory
3. Run the script:

```bash
python "marathon NYC scrap/marathon_scraping.py"
```

The script will create:
- A `logs` directory with detailed logs
- A `marathon_data` directory with JSON files containing runner data

## Configuration

To customize the script behavior, edit these parameters in the `__main__` section:

```python
event_code_global = "m2022"           # Change this to the event code you want to scrape
max_pages = 20                        # Set to None for full scrape
max_failed_pages = 10                 # Maximum failures before stopping
min_request_interval = 2.5            # Seconds between requests
max_concurrent_requests = 3           # Maximum concurrent requests
resume_from_checkpoint = True         # Whether to resume from last checkpoint
```

## License

MIT
