import time
import random
from functools import wraps
import requests

def retry_on_exception(
    exceptions_to_catch=(requests.exceptions.RequestException,),
    max_retries=3,
    initial_delay=1,
    backoff_factor=2,
    logger=None
):
    """
    A decorator for retrying a function with exponential backoff and jitter.
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempt = 0
            delay = initial_delay
            
            while attempt <= max_retries:
                try:
                    # Call the original function (e.g., download_pdf)
                    return func(*args, **kwargs)
                except exceptions_to_catch as e:
                    attempt += 1
                    if attempt > max_retries:
                        if logger:
                            logger.error(f"Function {func.__name__} failed after {max_retries+1} attempts. Final error: {e}")
                        raise  # Re-raise the final exception

                    # Calculate delay with backoff and jitter
                    delay = (initial_delay * (backoff_factor ** (attempt - 1))) + random.uniform(0, 0.5)
                    
                    status_code = e.response.status_code if hasattr(e, 'response') and e.response is not None else "N/A"
                    if logger:
                        logger.warning(
                            f"Function {func.__name__} failed (Status: {status_code}). "
                            f"Retrying in {delay:.1f}s... (Attempt {attempt}/{max_retries})"
                        )
                    time.sleep(delay)
        return wrapper
    return decorator
