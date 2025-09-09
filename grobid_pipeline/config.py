"""
Cấu hình cho pipeline trích xuất GROBID.

Các đường dẫn, thông số API, và cấu hình logging cho toàn bộ pipeline.
"""
from pathlib import Path
from dotenv import load_dotenv
import os

# --- Đường dẫn ---
BASE_DIR = Path(__file__).parent.parent

# Input/Output
INPUT_FILE_PATH = BASE_DIR / "notebooks" / "processed_data" / "neurips_2021_2024_cleaned.jsonl"
OUTPUT_DIR = BASE_DIR / "data" / "output"
OUTPUT_FILE_PATH = OUTPUT_DIR / "processed_papers.jsonl"

# Logging và Checkpoint
LOGS_DIR = BASE_DIR / "logs"
LOG_FILE_PATH = LOGS_DIR / "pipeline.log"
REPORT_FILE_PATH = LOGS_DIR / "pipeline_reports.md"
PROGRESS_FILE_PATH = OUTPUT_DIR / "checkpoint.json"

# Tự động tạo các thư mục cần thiết
for directory in [OUTPUT_DIR, LOGS_DIR, OUTPUT_DIR / "raw_responses"]:
    directory.mkdir(parents=True, exist_ok=True)

# --- Cấu hình GROBID Server ---
load_dotenv()
GROBID_BASE_URL = os.environ.get('GROBID_SERVER', 'grobid_url')
GROBID_HEADER_URL = f"{GROBID_BASE_URL}/api/processHeaderDocument"
GROBID_REFERENCES_URL = f"{GROBID_BASE_URL}/api/processReferences"
NS = {'tei': 'http://www.tei-c.org/ns/1.0'} # TEI Namespace
XPATHS = {
    # Header
    "GET_HEADER_AUTHORS": '//tei:fileDesc//tei:author',
    "GET_AFFILIATIONS": './tei:affiliation',
    "GET_RAW_AFF_NOTE": './tei:note[@type="raw_affiliation"]',
    "GET_ORG_BY_TYPE": "./tei:orgName[@type='{field_type}']/text()",
    "GET_COUNTRY": "./tei:address/tei:country/text()",
    
    # References
    "GET_REFERENCES": '//tei:listBibl/tei:biblStruct',
    "GET_RAW_REFERENCE_TEXT": './tei:note[@type="raw_reference"]',
    "GET_ANALYTIC_NODE": './tei:analytic',
    "GET_MONOGR_NODE": './tei:monogr',
    "GET_TITLE_IN_NODE": './tei:title/text()',
    "GET_DATE_NODE": './tei:imprint/tei:date[@type="published"]',  # Đường dẫn đến date node từ monogr
    
    # Chung (cho cả Header và References)
    "GET_AUTHORS_RELATIVE": './tei:author',
    "GET_FORENAMES": './tei:persName/tei:forename/text()',
    "GET_SURNAMES": './tei:persName/tei:surname/text()',
}

# --- Cấu hình Request & Retry ---
REQUEST_TIMEOUT = 180  # (giây) Thời gian chờ tối đa cho một request
MAX_RETRIES = 5
RETRY_BACKOFF_FACTOR = 2 # 5s, 10s, 20s
INITIAL_RETRY_DELAY = 5 # (giây)

# --- Cấu hình Logging ---
LOG_LEVEL = "INFO"  # Có thể đổi thành "DEBUG" để gỡ lỗi
CONSOLE_LOG_FORMAT = "%(message)s"  # Format đơn giản cho console với rich handler
FILE_LOG_FORMAT = "%(asctime)s - %(levelname)s - [%(threadName)s] - %(message)s"

# --- Cấu hình Pipeline Behavior ---
BATCH_SIZE = 10  # Số paper xử lý mỗi batch
MAX_WORKERS = 3  # Số luồng tối đa cho ThreadPoolExecutor
DISPLAY_PROGRESS_INTERVAL = 0.5  # Cập nhật progress bar mỗi 0.5 giây
SAVE_RAW_RESPONSES = True  # Có lưu response XML gốc từ GROBID không

# --- Cấu hình Terminal UI ---
PROMPT_TEXT = """
[C]ontinue: Xử lý batch tiếp theo
[A]uto:    Tự động xử lý toàn bộ các batch còn lại
[T]une:    Điều chỉnh các tham số pipeline
[Q]uit:    Dừng pipeline
Lựa chọn của bạn: """
