"""
Module chuyên xử lý việc trích xuất dữ liệu từ XML response của GROBID.
"""
from typing import List, Optional, Dict, Any, TypedDict, cast
from lxml import etree
import re
import logging

from .models import (
    GrobidAuthor, GrobidReference, ReferenceAuthor, 
    AuthorName, Affiliation, AffiliationParsed
)
from .config import NS, XPATHS

logger = logging.getLogger(__name__)

def _get_clean_affiliation_text(note_element: Optional[etree._Element]) -> str:
    """Lấy và dọn dẹp chuỗi affiliation thô từ một node <note>."""
    if note_element is None:
        return ""
    raw_text = "".join(note_element.itertext())
    clean_text = ' '.join(raw_text.split())
    return re.sub(r'^\s*\d+\.?\s*', '', clean_text)

def _parse_author_name(author_node: etree._Element) -> Optional[AuthorName]:
    """Trích xuất và cấu trúc thông tin tên từ một node <author>."""
    forenames = author_node.xpath(XPATHS['GET_FORENAMES'], namespaces=NS)
    surnames = author_node.xpath(XPATHS['GET_SURNAMES'], namespaces=NS)
    
    if not forenames and not surnames:
        return None
        
    name: AuthorName = {
        "full_name": ' '.join(forenames + surnames).strip(),
        "forenames": [fn.strip() for fn in forenames],
        "surname": ' '.join(surnames).strip()
    }
    return name

def _parse_single_affiliation(aff_node: etree._Element) -> Affiliation:
    """Trích xuất thông tin chi tiết từ một node <affiliation>."""
    raw_aff_note = aff_node.find(XPATHS['GET_RAW_AFF_NOTE'], namespaces=NS)
    
    parsed_aff: AffiliationParsed = {}
    
    org_fields = ["institution", "department", "laboratory"]
    for field in org_fields:
        result = aff_node.xpath(XPATHS['GET_ORG_BY_TYPE'].format(field_type=field), namespaces=NS)
        if result:
            parsed_aff[field] = result[0].strip()
            
    country_result = aff_node.xpath(XPATHS['GET_COUNTRY'], namespaces=NS)
    if country_result:
        parsed_aff["country"] = country_result[0].strip()
            
    affiliation: Affiliation = {
        "raw_affiliation": _get_clean_affiliation_text(raw_aff_note),
        "parsed_affiliation": parsed_aff
    }
    return affiliation

def _parse_author_name_for_ref(author_container: etree._Element) -> List[ReferenceAuthor]:
    """Trích xuất tác giả cho reference, trả về cấu trúc chi tiết."""
    authors_list: List[ReferenceAuthor] = []
    author_nodes = author_container.xpath(XPATHS['GET_AUTHORS_RELATIVE'], namespaces=NS)
    for author_node in author_nodes:
        name_obj = _parse_author_name(author_node)
        if name_obj:
            author: ReferenceAuthor = {
                "full_name": name_obj["full_name"],
                "forenames": name_obj["forenames"],
                "surname": name_obj["surname"]
            }
            authors_list.append(author)
    return authors_list

def _parse_single_reference(ref_node: etree._Element) -> Optional[GrobidReference]:
    """Phân tích một node <biblStruct> để trích xuất thông tin chi tiết."""
    raw_text_node = ref_node.find(XPATHS['GET_RAW_REFERENCE_TEXT'], namespaces=NS)
    if raw_text_node is None:
        return None
        
    # raw_text là trường bắt buộc
    raw_text = ' '.join(raw_text_node.itertext()).strip()
    if not raw_text:
        return None
        
    reference_obj: GrobidReference = {"raw_text": raw_text}

    analytic_node = ref_node.find(XPATHS['GET_ANALYTIC_NODE'], namespaces=NS)
    monogr_node = ref_node.find(XPATHS['GET_MONOGR_NODE'], namespaces=NS)

    if analytic_node is not None:
        title_res = analytic_node.xpath(XPATHS['GET_TITLE_IN_NODE'], namespaces=NS)
        authors = _parse_author_name_for_ref(analytic_node)
        if authors:
            reference_obj["authors"] = authors
            
        if monogr_node is not None:
            venue_res = monogr_node.xpath(XPATHS['GET_TITLE_IN_NODE'], namespaces=NS)
            if venue_res:
                reference_obj["venue"] = venue_res[0].strip()
                
    elif monogr_node is not None:
        title_res = monogr_node.xpath(XPATHS['GET_TITLE_IN_NODE'], namespaces=NS)
        authors = _parse_author_name_for_ref(monogr_node)
        if authors:
            reference_obj["authors"] = authors
    else:
        title_res = []

    if title_res:
        reference_obj["title"] = title_res[0].strip()

    # Lấy năm từ monogr node nếu có
    if monogr_node is not None:
        date_node = monogr_node.find(XPATHS['GET_DATE_NODE'], namespaces=NS)
        if date_node is not None:
            # Ưu tiên lấy text content, nếu không có thì lấy thuộc tính when
            year = date_node.text or date_node.get('when', '')
            if year and re.match(r'^\d{4}$', year):
                reference_obj["year"] = year
                
    return reference_obj

def parse_header_xml(xml_doc: etree._Element, paper_id: str) -> List[GrobidAuthor]:
    """
    Parse XML response từ GROBID /processHeaderDocument để trích xuất thông tin tác giả.
    
    Args:
        xml_doc: XML document đã được parse từ response của GROBID
        paper_id: ID của paper đang xử lý (để logging)
        
    Returns:
        List[GrobidAuthor]: Danh sách các tác giả được trích xuất
    """
    result: List[GrobidAuthor] = []
    author_nodes = xml_doc.xpath(XPATHS['GET_HEADER_AUTHORS'], namespaces=NS)
    
    for author_node in author_nodes:
        name_obj = _parse_author_name(author_node)
        if name_obj is None:
            continue
            
        author: GrobidAuthor = {"name": name_obj}
        
        # Xử lý affiliation
        aff_nodes = author_node.xpath(XPATHS['GET_AFFILIATIONS'], namespaces=NS)
        if aff_nodes:  # Chỉ thêm trường affiliations khi có dữ liệu
            affiliations = []
            for aff_node in aff_nodes:
                affiliation = _parse_single_affiliation(aff_node)
                if affiliation["raw_affiliation"]:  # Chỉ thêm khi có dữ liệu
                    affiliations.append(affiliation)
            if affiliations:
                author["affiliations"] = affiliations
            
        result.append(author)
    
    return result

def parse_references_xml(xml_doc: etree._Element, paper_id: str) -> List[GrobidReference]:
    """
    Parse XML response từ GROBID /processReferences để trích xuất references.
    
    Args:
        xml_doc: XML document đã được parse từ response của GROBID
        paper_id: ID của paper đang xử lý (để logging)
        
    Returns:
        List[GrobidReference]: Danh sách các reference được trích xuất
    """
    result: List[GrobidReference] = []
    reference_nodes = xml_doc.xpath(XPATHS['GET_REFERENCES'], namespaces=NS)
    
    for ref_node in reference_nodes:
        ref_obj = _parse_single_reference(ref_node)
        if ref_obj:  # _parse_single_reference có thể trả về None
            result.append(ref_obj)
    
    return result
