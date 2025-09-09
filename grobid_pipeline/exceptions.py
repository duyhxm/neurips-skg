class GrobidClientError(Exception):
    """Lỗi cơ bản cho GrobidClient."""
    pass

class GrobidRequestError(GrobidClientError):
    """Lỗi khi request đến Grobid thất bại sau tất cả các lần retry."""
    def __init__(self, url: str, status_code: int, message: str):
        self.url = url
        self.status_code = status_code
        super().__init__(f"Request to {url} failed with status {status_code}: {message}")

class PDFDownloadError(Exception):
    """Lỗi khi không thể tải file PDF."""
    pass
