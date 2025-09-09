"""
Định nghĩa các TypedDict cho các cấu trúc dữ liệu được dùng trong pipeline.

Các model này phản ánh cấu trúc dữ liệu cuối cùng sẽ được lưu xuống file JSONL.
"""
from typing import TypedDict, List, NotRequired, Literal

ProcessingStatusEnum = Literal["success", "failed", "pending"]

# --- Author Models ---
class AuthorName(TypedDict):
    """Đối tượng chứa các thành phần tên của tác giả."""
    full_name: str 
    forenames: List[str]
    surname: str

class AffiliationParsed(TypedDict):
    """Đối tượng chứa các thành phần affiliation đã được phân tích."""
    institution: NotRequired[str]
    department: NotRequired[str]
    laboratory: NotRequired[str]
    country: NotRequired[str]

class Affiliation(TypedDict):
    """Đối tượng chứa thông tin affiliation của tác giả."""
    raw_affiliation: str
    parsed_affiliation: AffiliationParsed

class GrobidAuthor(TypedDict):
    """TypedDict cho thông tin tác giả được GROBID trích xuất."""
    name: AuthorName
    affiliations: NotRequired[List[Affiliation]]

# --- Reference Models ---
class ReferenceAuthor(TypedDict):
    """TypedDict cho tác giả trong reference."""
    full_name: str
    forenames: List[str]
    surname: str

class GrobidReference(TypedDict):
    """TypedDict cho reference được GROBID trích xuất."""
    raw_text: str  # Required, minLength: 1
    title: NotRequired[str]  
    year: NotRequired[str]   # pattern: "^[0-9]{4}$"
    venue: NotRequired[str]  
    authors: NotRequired[List[ReferenceAuthor]]  

class ProcessingStatus(TypedDict):
    """TypedDict ghi lại trạng thái xử lý của từng phần (header, references)."""
    header_status: ProcessingStatusEnum  # Required
    references_status: ProcessingStatusEnum  # Required
    error_message: NotRequired[str]

class PaperResult(TypedDict):
    """TypedDict chứa kết quả trích xuất cuối cùng cho một paper."""
    paper_id: str  # Required, minLength: 1
    grobid_authors: NotRequired[List[GrobidAuthor]]  # Có thể là mảng rỗng
    grobid_references: NotRequired[List[GrobidReference]]  # Có thể là mảng rỗng
    processing_status: ProcessingStatus  # Required
