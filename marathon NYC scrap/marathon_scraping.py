import asyncio
import aiohttp
import json
import logging
import logging.handlers
import time
import datetime
import random
import os
import socket
import sys
import glob
import re
import signal
from typing import List, Dict, Optional, Tuple, Union, Any
import math
from concurrent.futures import ThreadPoolExecutor
from functools import partial

# For async file operations 
try:
    import aiofiles
    HAS_AIOFILES = True
except ImportError:
    HAS_AIOFILES = False
    logging.warning("aiofiles not installed. Using synchronous file operations.")

# Configuration constants
BASE_API_URL = "https://rmsprodapi.nyrr.org/api/v2/runners/finishers-filter"
DEFAULT_DOMAIN = "rmsprodapi.nyrr.org"
DEFAULT_USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
MAX_RESPONSE_SIZE = 50 * 1024 * 1024  # 50MB

# Set up logging
def setup_logging():
    """Configure logging to file and console with timestamps, including log rotation"""
    log_dir = "logs"
    if not os.path.exists(log_dir):
        os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(log_dir, f"marathon_scraping_{timestamp}.log")
    
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    
    # Clear any existing handlers
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)
    
    # Add rotating file handler
    rotating_handler = logging.handlers.RotatingFileHandler(
        log_file, 
        maxBytes=10*1024*1024,  # 10MB
        backupCount=5
    )
    rotating_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(rotating_handler)
    
    # Add console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(console_handler)
    
    logging.info(f"Logging initialized. Log file: {log_file}")
    return log_file

def check_internet_connectivity() -> bool:
    """Check if there is internet connectivity by connecting to a reliable host"""
    reliable_hosts = [
        ("8.8.8.8", 53),  # Google DNS
        ("1.1.1.1", 53),  # Cloudflare DNS
    ]
    
    for host, port in reliable_hosts:
        try:
            # Try to create a socket connection
            socket.create_connection((host, port), timeout=3)
            return True
        except OSError:
            continue
    
    return False

async def verify_domain_reachable(domain: str) -> Tuple[bool, str]:
    """Verify if a domain is reachable by attempting DNS resolution asynchronously"""
    # Use ThreadPoolExecutor to make the blocking DNS call non-blocking
    loop = asyncio.get_running_loop()
    try:
        # Run the blocking DNS resolution in a thread pool
        with ThreadPoolExecutor() as executor:
            await loop.run_in_executor(executor, socket.gethostbyname, domain)
        
        # If DNS resolution succeeds, we'll consider the domain reachable
        # Some APIs might return 404 for the root domain but work for specific endpoints
        return True, ""
    except socket.gaierror as e:
        return False, f"DNS resolution failed: {e}"
    except Exception as e:
        return False, f"Domain verification failed: {e}"

async def fetch_page(session: aiohttp.ClientSession, page_index: int, base_payload: dict, 
                headers: dict, url: str, max_retries: int = 3) -> Tuple[List[Dict], bool]:
    """
    Fetch a single page of runners with improved retry logic and error handling
    
    Returns:
        Tuple of (list of runners, success flag)
    """
    payload = base_payload.copy()
    
    # Define retry strategies for different status codes
    retry_strategies = {
        429: {"wait": lambda attempt: (attempt + 1) * random.uniform(5.0, 10.0), "max_retries": 5},  # Rate limiting
        403: {"wait": lambda attempt: (attempt + 1) * 8 + random.uniform(0, 5), "max_retries": 3},   # Forbidden
        400: {"wait": lambda attempt: (attempt + 1) * 2, "max_retries": 3},                         # Bad request
        500: {"wait": lambda attempt: (attempt + 1) * 3, "max_retries": 5},                         # Server error
        502: {"wait": lambda attempt: (attempt + 1) * 3, "max_retries": 5},                         # Bad gateway
        503: {"wait": lambda attempt: (attempt + 1) * 3, "max_retries": 5},                         # Service unavailable
        504: {"wait": lambda attempt: (attempt + 1) * 3, "max_retries": 5},                         # Gateway timeout
    }
    # Default strategy
    default_strategy = {"wait": lambda attempt: 1 + attempt, "max_retries": 3}
    
    # Adjust page indexing - some APIs use 0-based, some use 1-based indexing
    # Try 1-based indexing for pages after page 10 since we're getting 400 errors
    if page_index >= 10:
        adjusted_index = page_index + 1
    else:
        adjusted_index = page_index
        
    payload["pageIndex"] = adjusted_index
    
    # Calculate local max_retries based on page index
    # Early pages are more important so we try harder
    local_max_retries = max_retries + 2 if page_index < 10 else max_retries
    
    for attempt in range(local_max_retries):
        try:
            async with session.post(url, json=payload, headers=headers) as response:
                # Check HTTP status code
                if response.status == 200:
                    try:
                        # Check response size first to avoid memory issues
                        text = await response.text()
                        if len(text) > MAX_RESPONSE_SIZE:
                            logging.warning(f"Page {page_index}: Unusually large response ({len(text)} bytes)")
                        
                        # Parse JSON
                        data = json.loads(text)
                        items = data.get("items", [])
                        
                        # Check if items is empty but shouldn't be
                        if not items and page_index < 1000:  # Assume there should be data in earlier pages
                            logging.warning(f"Page {page_index}: Received empty items list, but expected data")
                            if attempt < local_max_retries - 1:
                                await asyncio.sleep(2 + attempt)
                                continue
                        
                        logging.info(f"Page {page_index}: Successfully fetched {len(items)} runners")
                        return items, True
                        
                    except json.JSONDecodeError as json_err:
                        logging.error(f"Page {page_index}: JSON parsing error (Attempt {attempt+1}/{local_max_retries}): {json_err}")
                        # If we can't parse JSON, this is likely a server issue - wait longer
                        await asyncio.sleep((attempt + 1) * 3)
                    except aiohttp.ContentTypeError as json_err:
                        logging.error(f"Page {page_index}: Content type error (Attempt {attempt+1}/{local_max_retries}): {json_err}")
                        await asyncio.sleep((attempt + 1) * 3)
                else:
                    logging.error(f"Page {page_index}: HTTP Error {response.status} - {response.reason} (Attempt {attempt+1}/{local_max_retries})")
                    
                    # Get retry strategy for this status code
                    strategy = retry_strategies.get(response.status, default_strategy)
                    wait_time = strategy["wait"](attempt)
                    
                    logging.warning(f"Page {page_index}: HTTP error {response.status}, waiting {wait_time:.2f}s before retry")
                    await asyncio.sleep(wait_time)
                    
                    # For 400 errors, try tweaking the page index further
                    if response.status == 400 and attempt == 1:
                        # Try a different page index adjustment on second attempt
                        payload["pageIndex"] = page_index  # Try 0-based indexing
                        logging.info(f"Page {page_index}: Trying 0-based indexing for retry")
        
        except aiohttp.ClientConnectorError as conn_err:
            # Connection errors - server unreachable
            logging.error(f"Page {page_index}: Connection error (Attempt {attempt+1}/{local_max_retries}): {conn_err}")
            await asyncio.sleep((attempt + 1) * 2)
        except aiohttp.ClientOSError as os_err:
            # OS errors like connection reset
            logging.error(f"Page {page_index}: OS error (Attempt {attempt+1}/{local_max_retries}): {os_err}")
            await asyncio.sleep((attempt + 1) * 2)
        except asyncio.TimeoutError as timeout_err:
            # Asyncio timeout 
            logging.error(f"Page {page_index}: Timeout error (Attempt {attempt+1}/{local_max_retries}): {timeout_err}")
            await asyncio.sleep((attempt + 1) * 3)
        except aiohttp.ServerTimeoutError as timeout_err:
            # Server timeout
            logging.error(f"Page {page_index}: Server timeout (Attempt {attempt+1}/{local_max_retries}): {timeout_err}")
            await asyncio.sleep((attempt + 1) * 3)
        except aiohttp.ClientSSLError as ssl_err:
            # SSL errors
            logging.error(f"Page {page_index}: SSL error (Attempt {attempt+1}/{local_max_retries}): {ssl_err}")
            await asyncio.sleep((attempt + 1) * 2)
        except Exception as e:
            # Catch-all for other errors
            logging.error(f"Page {page_index}: Unexpected error (Attempt {attempt+1}/{local_max_retries}): {e}")
            await asyncio.sleep((attempt + 1) * 2)
    
    logging.error(f"Page {page_index}: Failed after {local_max_retries} attempts")
    return [], False

async def write_json_stream(filename: str, runners: List[Dict], chunk_size: int = 100):
    """Write JSON data as a stream to avoid memory spikes for large datasets"""
    loop = asyncio.get_running_loop()
    
    # Streaming write using ThreadPoolExecutor to avoid blocking
    async def _write_json_stream():
        with open(filename, 'w') as f:
            # Write opening bracket
            f.write('[\n')
            
            # Write items with commas between them
            for i, item in enumerate(runners):
                if i > 0:
                    f.write(',\n')
                f.write(json.dumps(item))
                
                # Periodically yield to event loop to avoid blocking
                if i % chunk_size == 0 and i > 0:
                    await asyncio.sleep(0)
            
            # Write closing bracket
            f.write('\n]')
    
    # Execute the streaming write
    with ThreadPoolExecutor() as pool:
        await loop.run_in_executor(pool, lambda: _write_json_stream())

async def save_intermediate_results(runners: List[Dict], event_code: str, start_page: int = None, end_page: int = None):
    """Save intermediate results to avoid losing progress, using async file operations when available"""
    timestamp = int(time.time())
    
    # Create directory for incremental saves if it doesn't exist
    save_dir = "marathon_data"
    os.makedirs(save_dir, exist_ok=True)
    
    # Generate filename based on whether this is a page range or full save
    if start_page is not None and end_page is not None:
        filename = os.path.join(save_dir, f"marathon_runners_{event_code}_pages_{start_page}-{end_page}.json")
    else:
        filename = os.path.join(save_dir, f"marathon_runners_{event_code}_{timestamp}_partial.json")
    
    logging.info(f"Saving {len(runners)} runners to {filename}...")
    
    try:
        # For large datasets, use streaming approach to reduce memory usage
        if len(runners) > 1000:
            await write_json_stream(filename, runners)
        else:
            # For smaller datasets, use standard approach
            json_data = json.dumps(runners, indent=4)
            
            # Use aiofiles if available, otherwise use ThreadPoolExecutor for blocking I/O
            if HAS_AIOFILES:
                async with aiofiles.open(filename, 'w') as f:
                    await f.write(json_data)
            else:
                # Run blocking file write in a thread pool
                loop = asyncio.get_running_loop()
                with ThreadPoolExecutor() as pool:
                    await loop.run_in_executor(
                        pool, 
                        lambda f=filename, d=json_data: (lambda fh: (fh.write(d), fh.close()))(open(f, 'w'))
                    )
    except Exception as e:
        logging.error(f"Failed to save results to {filename}: {e}")
        return None
    
    if start_page is not None and end_page is not None:
        logging.info(f"Saved {len(runners)} runners from pages {start_page}-{end_page} to file: {filename}")
    else:
        logging.info(f"Saved {len(runners)} runners to intermediate file: {filename}")
    
    return filename

async def find_latest_checkpoint(event_code: str, save_dir: str = "marathon_data") -> Tuple[Optional[str], int]:
    """Find the latest checkpoint file to resume scraping"""
    os.makedirs(save_dir, exist_ok=True)
    
    pattern = os.path.join(save_dir, f"marathon_runners_{event_code}_pages_*-*.json")
    files = glob.glob(pattern)
    
    if not files:
        return None, 0
    
    # Extract page ranges and find highest
    latest_file = None
    highest_page = -1
    
    for file in files:
        match = re.search(r'pages_(\d+)-(\d+)', file)
        if match:
            end_page = int(match.group(2))
            if end_page > highest_page:
                highest_page = end_page
                latest_file = file
    
    logging.info(f"Found latest checkpoint: {latest_file}, last completed page: {highest_page}")
    return latest_file, highest_page + 1

async def fetch_all_runners_async(
    event_code: str = "m2022", 
    max_memory_items: int = 10000, 
    max_pages: int = None, 
    max_failed_pages: int = 50,
    min_request_interval: float = 2.0,
    max_concurrent_requests: int = 5,
    resume_from_checkpoint: bool = True
) -> List[Dict]:
    """
    Fetch all runners using async requests with improved error handling, progress tracking, and memory management
    
    Args:
        event_code: The event code to fetch data for
        max_memory_items: Maximum number of items to keep in memory before forcing a save
        max_pages: Maximum number of pages to fetch (for testing or limiting results)
        max_failed_pages: Maximum number of failed pages before stopping (to prevent endless failures)
        min_request_interval: Minimum time between requests in seconds
        max_concurrent_requests: Maximum number of concurrent requests
        resume_from_checkpoint: Whether to try to resume from the last checkpoint
    """
    logging.info(f"Starting to fetch runners for event: {event_code}")
    
    # Initialize variables that might be accessed in exception handling
    all_runners = []  # Initialize the list to store all runners
    successful_pages = 0
    failed_pages = 0
    chunk_runners = []  # Ensure this is initialized before any possible exception
    last_page_save = 0
    start_page = 0  # Default start page
    
    # Use a lock for protecting shared resources during concurrent modification
    runner_lock = asyncio.Lock()
    
    # Try to resume from checkpoint if requested
    checkpoint_file = None
    if resume_from_checkpoint:
        checkpoint_file, start_page = await find_latest_checkpoint(event_code)
        if checkpoint_file:
            logging.info(f"Resuming from checkpoint, starting at page {start_page}")
            print(f"Resuming from checkpoint, starting at page {start_page}")
            
            # Load the previous batch data
            try:
                with open(checkpoint_file, 'r') as f:
                    prev_runners = json.load(f)
                    logging.info(f"Loaded {len(prev_runners)} runners from checkpoint")
                    all_runners.extend(prev_runners)
                    last_page_save = start_page
            except Exception as e:
                logging.error(f"Failed to load checkpoint file: {e}")
                # Continue anyway, but start from page 0
                start_page = 0
    
    # Extract domain from the URL for validation
    base_url = BASE_API_URL
    domain = DEFAULT_DOMAIN
    
    headers = {
        "accept": "application/json, text/plain, */*",
        "content-type": "application/json;charset=UTF-8",
        "origin": "https://results.nyrr.org",
        "referer": "https://results.nyrr.org/",
        "user-agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36"
    }
    
    base_payload = {
        "eventCode": event_code,
        "pageIndex": 0,
        "pageSize": 50,
        "sortColumn": "overallTime",
        "sortDescending": False,
    }
    
    # Check internet connectivity
    if not check_internet_connectivity():
        logging.error("No internet connectivity detected. Please check your network connection.")
        print("Error: No internet connectivity. Please check your network connection and try again.")
        return []

    # Verify that the domain is reachable
    domain_reachable, error_msg = await verify_domain_reachable(domain)
    if not domain_reachable:
        logging.error(f"Unable to reach the domain {domain}. {error_msg}")
        print(f"Error: Cannot connect to {domain}. The server might be down or there could be DNS issues.")
        print("Troubleshooting tips:")
        print("1. Check your internet connection")
        print("2. Verify the domain is correct")
        print("3. Try again later as the service might be temporarily unavailable")
        return []
    
    start_time = time.time()
    
    # Create session with longer timeout
    timeout = aiohttp.ClientTimeout(total=60*10)  # 10 minutes timeout
    conn = aiohttp.TCPConnector(
        limit=max_concurrent_requests, 
        ttl_dns_cache=300, 
        enable_cleanup_closed=True,
        use_dns_cache=True
    )  # DNS cache to reduce lookups
    
    # Check for HTTP proxy in environment
    proxy = os.environ.get("HTTP_PROXY")
    if proxy:
        logging.info(f"Using HTTP proxy: {proxy}")
        print(f"Using HTTP proxy from environment: {proxy}")
    
    try:
        # Use context manager to ensure proper cleanup of resources
        async with aiohttp.ClientSession(
            timeout=timeout, 
            connector=conn, 
            raise_for_status=False,
            proxy=proxy
        ) as session:
            try:
                # Get total count first
                try:
                    async with session.post(base_url, json=base_payload, headers=headers) as response:
                        if response.status != 200:
                            logging.error(f"Initial request failed with status {response.status}: {response.reason}")
                            return []
                        
                        try:
                            data = await response.json()
                            total_items = data.get("totalItems", 0)
                            if total_items <= 0:
                                logging.error("Invalid totalItems value returned from API (zero or negative)")
                                return []
                            total_pages = math.ceil(total_items / base_payload["pageSize"])
                        except aiohttp.ContentTypeError as json_err:
                            logging.error(f"JSON parsing error in initial request: {json_err}")
                            return []
                except (aiohttp.ClientError, asyncio.TimeoutError) as e:
                    logging.error(f"Failed to make initial request: {e}")
                    return []
                
                logging.info(f"Total runners: {total_items}, Total pages: {total_pages}")
                
                # Dynamically adjust batch size based on total pages, but respect max_concurrent_requests
                batch_size = min(max_concurrent_requests, max(2, 1000 // total_pages))  # Between 2 and max_concurrent_requests
                logging.info(f"Using batch size of {batch_size} concurrent requests with {min_request_interval}s interval")
                
                # Create tasks for all pages
                tasks = []
                last_save_count = 0
                chunk_runners = []  # Store runners for the current 100-page chunk
                
                # If we're resuming, log the progress
                if start_page > 0:
                    logging.info(f"Resuming from page {start_page}/{total_pages} ({start_page/total_pages*100:.1f}% completed)")
                    successful_pages = start_page  # Count resumed pages as successful
                
                # Loop through pages, starting from our checkpoint
                for page_idx in range(start_page, total_pages):
                    # Limit concurrent requests to be respectful
                    if len(tasks) >= batch_size:
                        # Process a batch of requests
                        results = await asyncio.gather(*tasks)
                        
                        # Add successful results to all_runners and chunk_runners with lock protection
                        async with runner_lock:
                            for items, success in results:
                                if success:
                                    all_runners.extend(items)
                                    chunk_runners.extend(items)
                                    successful_pages += 1
                                else:
                                    failed_pages += 1
                                    
                            # Check if we need to force a memory save
                            if len(all_runners) > max_memory_items:
                                logging.info(f"Memory limit reached ({len(all_runners)} items). Saving to disk and clearing memory.")
                                # Save all runners to disk
                                await save_intermediate_results(all_runners, event_code)
                                # Keep only the most recent items to reduce memory usage
                                all_runners = all_runners[-1000:]  # Keep last 1000 items
                        
                        # Clear tasks for next batch
                        tasks = []
                        
                        # Calculate progress
                        processed = successful_pages + failed_pages
                        progress = (processed / total_pages) * 100
                        elapsed = time.time() - start_time
                        pages_per_second = processed / elapsed if elapsed > 0 else 0
                        estimated_remaining = (total_pages - processed) / pages_per_second if pages_per_second > 0 else "unknown"
                        
                        logging.info(f"Progress: {progress:.1f}% ({processed}/{total_pages} pages) - " + 
                                   f"Success: {successful_pages}, Failed: {failed_pages} - " + 
                                   f"Speed: {pages_per_second:.2f} pages/sec - " + 
                                   f"Est. remaining: {estimated_remaining if isinstance(estimated_remaining, str) else f'{estimated_remaining:.1f}s'}")
                        
                        # Check if we've hit the maximum failed pages limit
                        if failed_pages >= max_failed_pages:
                            logging.error(f"Reached maximum failed pages limit ({max_failed_pages}). Stopping to prevent endless errors.")
                            print(f"\nStopping after {failed_pages} failed pages to prevent endless errors.")
                            print(f"Successfully fetched {len(all_runners)} runners from {successful_pages} pages.")
                            print("Check the log file for details on the failures.")
                            
                            # Save what we have so far
                            if all_runners:
                                await save_intermediate_results(all_runners, event_code)
                            
                            return all_runners
                            
                        # Check if we've hit the maximum pages limit (if specified)
                        if max_pages is not None and processed >= max_pages:
                            logging.info(f"Reached maximum pages limit ({max_pages}). Stopping as requested.")
                            print(f"\nStopped after processing {processed} pages as requested.")
                            
                            # Save what we have so far
                            if all_runners:
                                await save_intermediate_results(all_runners, event_code)
                                
                            return all_runners
                        
                        # Save results every 100 pages
                        current_page_block = (successful_pages // 100) * 100
                        if current_page_block > last_page_save:
                            start_page = last_page_save
                            end_page = current_page_block - 1
                            logging.info(f"Preparing to save 100-page chunk: pages {start_page}-{end_page}")
                            
                            # Save the chunk
                            await save_intermediate_results(chunk_runners, event_code, start_page, end_page)
                            
                            # Reset chunk tracking
                            chunk_runners = []
                            last_page_save = current_page_block
                        
                        # Also save intermediate full results periodically (every 1000 new runners)
                        if len(all_runners) - last_save_count > 1000:
                            await save_intermediate_results(all_runners, event_code)
                            last_save_count = len(all_runners)
                        
                        # Adaptive rate limiting - be more conservative with high failure rates
                        if failed_pages > 0:
                            failure_ratio = failed_pages / (successful_pages + failed_pages)
                            
                            # If more than 10% of requests are failing, slow down significantly
                            if failure_ratio > 0.1:
                                wait_time = min(15, min_request_interval + (failure_ratio * 15))  # Up to 15 seconds for high failure rates
                                logging.warning(f"High failure rate ({failure_ratio:.2f}). Increasing wait time to {wait_time:.1f}s")
                                await asyncio.sleep(wait_time)
                            else:
                                # Regular slowdown for occasional failures
                                wait_time = min(8, min_request_interval + (failed_pages / 10))
                                logging.warning(f"Increasing wait time to {wait_time:.1f}s due to failures")
                                await asyncio.sleep(wait_time)
                        else:
                            # Regular rate limiting with more randomization to avoid detection
                            # Add randomization to make the pattern less predictable
                            jitter = random.uniform(-0.2, 0.5) * min_request_interval
                            wait_time = max(1.0, min_request_interval + jitter)
                            await asyncio.sleep(wait_time)
                    
                    # Add task for this page
                    task = asyncio.create_task(fetch_page(session, page_idx, base_payload, headers, base_url))
                    tasks.append(task)
                
                # Process remaining tasks
                if tasks:
                    results = await asyncio.gather(*tasks)
                    
                    # Use lock for thread safety
                    async with runner_lock:
                        for items, success in results:
                            if success:
                                all_runners.extend(items)
                                chunk_runners.extend(items)
                                successful_pages += 1
                            else:
                                failed_pages += 1
                                
                        # Final memory check
                        if len(all_runners) > max_memory_items:
                            logging.info(f"Memory limit reached ({len(all_runners)} items). Saving final batch to disk.")
                            await save_intermediate_results(all_runners, event_code)
                    
                    # Save the final chunk if we have any unsaved pages
                    if successful_pages > last_page_save:
                        start_page = last_page_save
                        end_page = successful_pages - 1
                        logging.info(f"Preparing to save final chunk: pages {start_page}-{end_page}")
                        await save_intermediate_results(chunk_runners, event_code, start_page, end_page)
                
            except aiohttp.ClientConnectorDNSError as dns_error:
                logging.error(f"DNS resolution error when connecting to {domain}: {dns_error}")
                print(f"\nError: Cannot resolve the domain {domain}.")
                print("This could be due to:")
                print("1. The server might be down")
                print("2. DNS resolution issues")
                print("3. Network connectivity problems")
                print("\nPlease check your internet connection and try again later.")
                
                # Save what we have so far (if anything)
                if all_runners:
                    await save_intermediate_results(all_runners, event_code)
                
                if chunk_runners and successful_pages > last_page_save:
                    start_page = last_page_save
                    end_page = successful_pages - 1
                    logging.info(f"Saving partial chunk after DNS error: pages {start_page}-{end_page}")
                    await save_intermediate_results(chunk_runners, event_code, start_page, end_page)
                
                # Return what we've collected so far
                return all_runners
                
            except Exception as e:
                logging.error(f"Fatal error in fetch_all_runners_async: {e}")
                # Save what we have so far
                if all_runners:
                    await save_intermediate_results(all_runners, event_code)
                
                # Also save the current chunk if there's any data
                if chunk_runners and successful_pages > last_page_save:
                    start_page = last_page_save
                    end_page = successful_pages - 1
                    logging.info(f"Saving partial chunk after error: pages {start_page}-{end_page}")
                    await save_intermediate_results(chunk_runners, event_code, start_page, end_page)
                
                raise
    except Exception as outer_e:
        logging.error(f"Error in session creation: {outer_e}")
        print(f"\nError: Failed to establish connection. {outer_e}")
        print("Please check your internet connection and try again.")
        
        # Ensure connector is closed properly
        if 'conn' in locals() and conn is not None:
            await conn.close()
            
        return []
    # Log final stats
    total_processed = successful_pages + failed_pages
    success_rate = (successful_pages / total_pages * 100) if total_pages > 0 else 0
    logging.info(f"Finished processing {total_processed}/{total_pages} pages " +
               f"(Success rate: {success_rate:.1f}%) - " +
               f"Total runners: {len(all_runners)}")
    
    if failed_pages > 0:
        logging.warning(f"Failed to retrieve {failed_pages} pages. Check logs for details.")
    
    return all_runners

# Signal handling for graceful shutdown
is_shutting_down = False
all_runners_global = []
event_code_global = "m2022"

async def shutdown(sig, loop):
    """Perform graceful shutdown on signal"""
    global is_shutting_down, all_runners_global
    
    if is_shutting_down:
        return  # Prevent multiple shutdown calls
        
    is_shutting_down = True
    logging.info(f"Received exit signal {sig.name}...")
    print(f"\nReceived signal {sig.name}, saving progress before exit...")
    
    # Save current data
    if all_runners_global:
        logging.info(f"Saving {len(all_runners_global)} runners before shutdown")
        await save_intermediate_results(all_runners_global, event_code_global)
    
    # Cancel all running tasks
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    logging.info(f"Cancelling {len(tasks)} running tasks")
    
    for task in tasks:
        task.cancel()
    
    await asyncio.gather(*tasks, return_exceptions=True)
    logging.info("Shutdown complete")
    loop.stop()

def setup_signal_handlers(loop):
    """Set up signal handlers for graceful shutdown"""
    for sig_name in ('SIGINT', 'SIGTERM'):
        if hasattr(signal, sig_name):
            sig = getattr(signal, sig_name)
            loop.add_signal_handler(
                sig,
                lambda sig=sig: asyncio.create_task(shutdown(sig, loop))
            )
            logging.info(f"Registered signal handler for {sig_name}")

if __name__ == "__main__":
    # Setup logging
    log_file = setup_logging()
    
    # Create directory for data files if it doesn't exist
    save_dir = "marathon_data"
    os.makedirs(save_dir, exist_ok=True)
    logging.info(f"Ensuring directory for marathon data exists: {save_dir}")
    
    try:
        # Check basic internet connectivity before starting
        if not check_internet_connectivity():
            logging.error("No internet connectivity detected before starting")
            print("\nError: No internet connection detected.")
            print("Please check your network network and try again.")
            sys.exit(1)  # Exit with error code
        
        # Set up event loop with signal handlers
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        setup_signal_handlers(loop)
        
        # Run async version
        start_time = time.time()
        
        # Set parameters for the scrape
        event_code_global = "m2022"  # Global for signal handler
        runners = loop.run_until_complete(
            fetch_all_runners_async(
                event_code=event_code_global,
                max_pages=20,          # Initially fetch just 20 pages to test the API behavior
                max_failed_pages=10,   # Stop after 10 failures to prevent endless errors
                min_request_interval=2.5,  # 2.5 seconds between requests
                max_concurrent_requests=3,  # 3 concurrent requests maximum
                resume_from_checkpoint=True  # Try to resume from checkpoint
            )
        )
        
        # Keep a reference for signal handler
        all_runners_global = runners
        elapsed = time.time() - start_time
        
        # Log success
        if runners:
            pages_fetched = len(runners) // 50  # Approximate
            logging.info(f"Successfully fetched {len(runners)} runners from ~{pages_fetched} pages in {elapsed:.1f} seconds")
            
            # Save the data to a JSON file
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = os.path.join(save_dir, f"marathon_runners_{event_code_global}_{timestamp}.json")
            
            # Use streaming write for large datasets
            if len(runners) > 1000:
                loop.run_until_complete(write_json_stream(output_file, runners))
            else:
                with open(output_file, 'w') as f:
                    json.dump(runners, f, indent=4)
                
            logging.info(f"Data saved to {output_file}")
            print(f"\nSummary:")
            print(f"- Fetched {len(runners)} runners")
            print(f"- Data saved to {output_file}")
            print(f"- Log file: {log_file}")
            
            # Provide instructions for continuing
            if pages_fetched < 1111:  # Assuming there are approximately 1111 pages total
                print("\nThis was a test run with only 20 pages.")
                print("To run a full scrape, edit the script and change max_pages=None")
                print("You can continue from where you left off by setting resume_from_checkpoint=True")
        else:
            logging.warning("No runners were fetched due to connectivity issues")
            print("\nNo data was fetched due to connectivity issues.")
            print("Please check the log file for details and try again when the server is reachable.")
            
        # Clean up resources
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()
            
    except aiohttp.ClientConnectorDNSError as dns_error:
        logging.error(f"DNS resolution error: {dns_error}", exc_info=True)
        print(f"\nError: Unable to connect to the server due to DNS resolution issues.")
        print("Possible causes:")
        print("1. The server domain may be incorrect or unavailable")
        print("2. Your DNS settings might be misconfigured")
        print("3. There might be network connectivity issues")
        print(f"\nCheck log file for technical details: {log_file}")
    except KeyboardInterrupt:
        logging.info("Script was manually interrupted by user")
        print("\nScript was interrupted by user. Any saved data has been preserved.")
        print(f"Check log file for details: {log_file}")
    except Exception as e:
        logging.error(f"Script execution failed: {e}", exc_info=True)
        print(f"\nError: Script failed. Check log file for details: {log_file}")
    finally:
        # Ensure event loop is closed
        if 'loop' in locals() and loop.is_running():
            loop.close()
