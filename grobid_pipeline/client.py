import time
import logging
import requests
import io
from lxml import etree
from typing import Optional, Dict, Any, List
from requests_toolbelt.multipart.encoder import MultipartEncoder

from .config import REQUEST_TIMEOUT, MAX_RETRIES, INITIAL_RETRY_DELAY, RETRY_BACKOFF_FACTOR
from .exceptions import GrobidRequestError, PDFDownloadError
from .utils import retry_on_exception

logger = logging.getLogger(__name__)

class GrobidClient:
    def __init__(self, header_url: str, references_url: str):
        self.header_url = header_url
        self.references_url = references_url
        self.session = requests.Session() # Sử dụng session để tái sử dụng kết nối TCP

    @retry_on_exception(
        exceptions_to_catch=(requests.exceptions.RequestException, PDFDownloadError),
        max_retries=MAX_RETRIES,
        initial_delay=INITIAL_RETRY_DELAY,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        logger=logger
    )
    def download_pdf(self, pdf_url: str, paper_id: str) -> bytes:
        """
        Tải nội dung PDF từ một URL.
        Logic retry đã được decorator xử lý.
        """
        logger.debug(f"[{paper_id}] Attempting PDF download from {pdf_url}")
        response = self.session.get(pdf_url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        response.raise_for_status()
        
        pdf_content = response.content
        if not pdf_content.startswith(b'%PDF'):
            raise PDFDownloadError(f"Content from {pdf_url} is not a valid PDF file.")
        
        logger.debug(f"[{paper_id}] PDF downloaded successfully.")
        return pdf_content
    
    def _parse_xml_response(self, response: requests.Response, paper_id: str) -> etree._Element:
        """
        Hàm trợ giúp: Chịu trách nhiệm parse response XML.
        """
        content_type = response.headers.get('Content-Type', '')
        if not any(xml_type in content_type for xml_type in ['text/xml', 'application/xml']):
            logger.warning(f"[{paper_id}] Content type có thể không phải XML: {content_type}")

        try:
            xml_text = response.content.decode('utf-8').strip().lstrip('\ufeff')
            parser = etree.XMLParser(recover=True, resolve_entities=False)
            return etree.fromstring(xml_text.encode('utf-8'), parser=parser)
        except etree.XMLSyntaxError as e:
            logger.error(f"[{paper_id}] Lỗi phân tích XML: {e}")
            raise GrobidRequestError(response.request.url, 200, f"Phản hồi XML không hợp lệ: {e}")
        
    @retry_on_exception(
        max_retries=MAX_RETRIES,
        initial_delay=INITIAL_RETRY_DELAY,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        logger=logger
    )
    def _send_to_grobid(self, api_url: str, pdf_content: bytes, paper_id: str) -> etree._Element:
        """
        Gửi nội dung PDF đến GROBID. Logic retry đã được decorator xử lý.
        """
        logger.debug(f"[{paper_id}] Đang thử gửi PDF đến {api_url}")
        if not pdf_content.startswith(b'%PDF'):
            raise PDFDownloadError("Nội dung không phải là file PDF hợp lệ")

        # 1. Chuẩn bị tham số
        params: Dict[str, str] = {}
        if "processHeader" in api_url:
            params = {"consolidateHeader": "1", "includeRawAffiliations": "1"}
        elif "processReferences" in api_url:
            params = {"consolidateCitations": "0", "includeRawCitations": "1"}
        
        # 2. Gửi request bằng phương thức đơn giản và hiệu quả nhất
        files = {'input': ('input.pdf', pdf_content, 'application/pdf')}
        headers = {'Accept': 'application/xml'}
        
        response = self.session.post(
            api_url,
            files=files,
            headers=headers,
            data=params,
            timeout=REQUEST_TIMEOUT
        )
        
        # 3. Ném ra lỗi nếu thất bại -> Decorator sẽ bắt lỗi này và thử lại
        response.raise_for_status()
        
        # 4. Nếu thành công, gọi hàm parse và trả về kết quả
        return self._parse_xml_response(response, paper_id)

    def process_header_from_content(self, pdf_content: bytes, paper_id: str) -> etree._Element:
        """Gửi nội dung PDF đã tải để xử lý Header."""
        return self._send_to_grobid(self.header_url, pdf_content, paper_id)

    def process_references_from_content(self, pdf_content: bytes, paper_id: str) -> etree._Element:
        """Gửi nội dung PDF đã tải để xử lý References."""
        return self._send_to_grobid(self.references_url, pdf_content, paper_id)
        
    def process_paper_from_url(self, pdf_url: str, paper_id: str) -> tuple[etree._Element, etree._Element]:
        """
        Quy trình hoàn chỉnh tối ưu: Tải PDF MỘT LẦN DUY NHẤT và xử lý cả Header và References.
        
        Args:
            pdf_url: URL của file PDF cần xử lý
            paper_id: ID của paper để ghi log
            
        Returns:
            tuple[etree._Element, etree._Element]: Tuple chứa (header_xml, references_xml)
        """
        # Tải PDF một lần duy nhất
        pdf_content = self.download_pdf(pdf_url, paper_id)
        
        # Xử lý cả header và references với cùng nội dung PDF
        header_xml = self.process_header_from_content(pdf_content, paper_id)
        references_xml = self.process_references_from_content(pdf_content, paper_id)
        
        return (header_xml, references_xml)
        
    def close(self):
        """Close the HTTP session."""
        self.session.close()
