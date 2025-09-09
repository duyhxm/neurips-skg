"""
Pipeline xử lý tài liệu học thuật bằng GROBID với luồng xử lý tối ưu.

Pipeline này cung cấp:
- Xử lý theo batch với khả năng phục hồi (resumability)
- Tương tác người dùng sau mỗi batch
- Theo dõi hiệu suất thời gian thực
- Logging màu sắc và chi tiết
- Tối ưu hiệu năng (mỗi PDF chỉ tải một lần duy nhất)
"""

import json
import logging
import threading
import signal
import sys
import time
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Set, Generator, Optional, Tuple, Any, Iterator
from statistics import mean, median
from rich.logging import RichHandler
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TimeElapsedColumn, TimeRemainingColumn
from rich.prompt import Prompt, IntPrompt
import colorama

from grobid_pipeline import client, config, models, parser
from grobid_pipeline.client import GrobidClient
from grobid_pipeline.exceptions import PDFDownloadError, GrobidRequestError

# Global flag để kiểm soát việc dừng pipeline
emergency_stop = threading.Event()

# Khởi tạo rich console
console = Console()

@dataclass
class PerformanceTracker:
    """Theo dõi và tính toán các chỉ số hiệu suất xử lý."""
    
    success_count: int = 0
    failure_count: int = 0
    processing_times: List[float] = field(default_factory=list)
    author_stats: Dict[str, List[float]] = field(default_factory=lambda: {
        "author_counts": [],
        "affiliation_rates": []
    })
    reference_stats: Dict[str, List[float]] = field(default_factory=lambda: {
        "ref_counts": [],
        "title_rates": [],
        "year_rates": [],
        "venue_rates": []
    })
    
    def record_success(self, process_time: float, result: models.PaperResult) -> None:
        """Ghi nhận một lượt xử lý thành công cùng các thông số chất lượng."""
        self.success_count += 1
        self.processing_times.append(process_time)
        
        # Author stats
        if "grobid_authors" in result and result["grobid_authors"]:
            num_authors = len(result["grobid_authors"])
            if num_authors > 0:
                self.author_stats["author_counts"].append(num_authors)
                aff_count = sum(1 for author in result["grobid_authors"] 
                            if "affiliations" in author and author["affiliations"])
                self.author_stats["affiliation_rates"].append(aff_count / num_authors)
        
        # Reference stats
        if "grobid_references" in result and result["grobid_references"]:
            num_refs = len(result["grobid_references"])
            if num_refs > 0:
                self.reference_stats["ref_counts"].append(num_refs)
                
                # Tính tỷ lệ các trường metadata của reference
                title_rate = sum(1 for ref in result["grobid_references"] if "title" in ref) / num_refs
                year_rate = sum(1 for ref in result["grobid_references"] if "year" in ref) / num_refs
                venue_rate = sum(1 for ref in result["grobid_references"] if "venue" in ref) / num_refs
                
                self.reference_stats["title_rates"].append(title_rate)
                self.reference_stats["year_rates"].append(year_rate)
                self.reference_stats["venue_rates"].append(venue_rate)
    
    def record_failure(self, process_time: float) -> None:
        """Ghi nhận một lượt xử lý thất bại."""
        self.failure_count += 1
        if process_time > 0:  # Chỉ ghi nhận nếu có thời gian xử lý thực
            self.processing_times.append(process_time)
    
    def reset(self) -> None:
        """Reset toàn bộ các chỉ số theo dõi."""
        self.success_count = 0
        self.failure_count = 0
        self.processing_times.clear()
        self.author_stats = {"author_counts": [], "affiliation_rates": []}
        self.reference_stats = {
            "ref_counts": [], "title_rates": [], 
            "year_rates": [], "venue_rates": []
        }
    
    def get_report(self) -> Dict[str, Any]:
        """Tổng hợp các chỉ số thành báo cáo."""
        total_papers = self.success_count + self.failure_count
        
        # Tính toán các chỉ số cơ bản
        report = {
            "total_papers": total_papers,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": (self.success_count / total_papers * 100) if total_papers else 0
        }
        
        # Thêm các chỉ số thời gian
        if self.processing_times:
            total_time = sum(self.processing_times)
            report.update({
                "total_time": total_time,
                "avg_time": mean(self.processing_times),
                "min_time": min(self.processing_times),
                "max_time": max(self.processing_times),
                "papers_per_minute": (total_papers / total_time * 60) if total_time > 0 else 0
            })
        
        # Thêm thống kê về Author (nếu có)
        if self.author_stats["author_counts"]:
            report["author_stats"] = {
                "avg_authors_per_paper": mean(self.author_stats["author_counts"]),
                "min_authors": min(self.author_stats["author_counts"]),
                "max_authors": max(self.author_stats["author_counts"]),
                "avg_affiliation_rate": mean(self.author_stats["affiliation_rates"]) * 100
            }
        
        # Thêm thống kê về Reference (nếu có)
        if self.reference_stats["ref_counts"]:
            report["reference_stats"] = {
                "avg_refs_per_paper": mean(self.reference_stats["ref_counts"]),
                "min_refs": min(self.reference_stats["ref_counts"]),
                "max_refs": max(self.reference_stats["ref_counts"]),
                "avg_field_rates": {
                    "title": mean(self.reference_stats["title_rates"]) * 100,
                    "year": mean(self.reference_stats["year_rates"]) * 100,
                    "venue": mean(self.reference_stats["venue_rates"]) * 100
                }
            }
        
        return report

    def merge(self, other: 'PerformanceTracker') -> None:
        """Gộp dữ liệu từ một tracker khác vào tracker hiện tại."""
        self.success_count += other.success_count
        self.failure_count += other.failure_count
        self.processing_times.extend(other.processing_times)
        
        # Gộp thống kê tác giả
        self.author_stats["author_counts"].extend(other.author_stats["author_counts"])
        self.author_stats["affiliation_rates"].extend(other.author_stats["affiliation_rates"])
        
        # Gộp thống kê trích dẫn
        self.reference_stats["ref_counts"].extend(other.reference_stats["ref_counts"])
        self.reference_stats["title_rates"].extend(other.reference_stats["title_rates"])
        self.reference_stats["year_rates"].extend(other.reference_stats["year_rates"])
        self.reference_stats["venue_rates"].extend(other.reference_stats["venue_rates"])

def setup_logging() -> None:
    """Cấu hình hệ thống logging với hai output:
    - Console: Có màu sắc, level từ config
    - File: Không màu, level DEBUG
    """
    # Reset root logger
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Cấu hình rich handler cho console
    console_handler = RichHandler(
        rich_tracebacks=True,
        markup=True,
        show_path=False,
        enable_link_path=False,
    )
    console_handler.setLevel(config.LOG_LEVEL)
    console_formatter = logging.Formatter(config.CONSOLE_LOG_FORMAT)
    console_handler.setFormatter(console_formatter)
    
    # Cấu hình file handler
    file_handler = logging.FileHandler(config.LOG_FILE_PATH, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(config.FILE_LOG_FORMAT)
    file_handler.setFormatter(file_formatter)
    
    # Cấu hình root logger
    logging.root.setLevel(logging.DEBUG)
    logging.root.addHandler(console_handler)
    logging.root.addHandler(file_handler)
    
    logging.info("Đã khởi tạo logging system")

def load_processed_papers() -> Set[str]:
    """
    Tải danh sách các paper_id đã được xử lý thành công.
    
    Returns:
        Set[str]: Tập hợp các paper_id đã xử lý
    """
    processed_papers = set()
    
    if config.PROGRESS_FILE_PATH.exists():
        try:
            with open(config.PROGRESS_FILE_PATH, 'r', encoding='utf-8') as f:
                processed_papers = set(line.strip() for line in f if line.strip())
            logging.info(f"Đã tải {len(processed_papers)} paper_id đã xử lý từ checkpoint")
        except Exception as e:
            logging.error(f"Lỗi khi đọc file checkpoint: {str(e)}")
    
    return processed_papers

def read_input_file(processed_papers: Set[str]) -> Generator[Tuple[str, str], None, None]:
    """
    Đọc file JSONL đầu vào và yield các paper cần xử lý.
    
    Args:
        processed_papers: Tập hợp các paper_id đã xử lý
        
    Yields:
        Tuple[str, str]: Mỗi tuple chứa (paper_id, pdf_link)
    """
    if not config.INPUT_FILE_PATH.exists():
        logging.error(f"File đầu vào không tồn tại: {config.INPUT_FILE_PATH}")
        return
    
    total_count = 0
    skipped_count = 0
    
    with open(config.INPUT_FILE_PATH, 'r', encoding='utf-8') as f:
        for line in f:
            if not line.strip():
                continue
                
            try:
                paper_data = json.loads(line)
                paper_id = paper_data.get('paper_id')
                pdf_link = paper_data.get('pdf_link')
                
                total_count += 1
                
                # Kiểm tra xem paper đã được xử lý chưa
                if paper_id in processed_papers:
                    skipped_count += 1
                    continue
                    
                if not paper_id or not pdf_link:
                    logging.warning(f"Dòng thiếu paper_id hoặc pdf_link: {line[:100]}...")
                    continue
                    
                yield (paper_id, pdf_link)
                
            except json.JSONDecodeError:
                logging.warning(f"Dòng không phải JSON hợp lệ: {line[:100]}...")
    
    logging.info(f"Đã đọc {total_count} paper từ file đầu vào, bỏ qua {skipped_count} paper đã xử lý")

def mark_paper_as_processed(paper_id: str) -> None:
    """
    Đánh dấu một paper đã được xử lý thành công.
    
    Args:
        paper_id: ID của paper đã xử lý
    """
    try:
        with open(config.PROGRESS_FILE_PATH, 'a', encoding='utf-8') as f:
            f.write(f"{paper_id}\n")
    except Exception as e:
        logging.error(f"Lỗi khi ghi vào file checkpoint: {str(e)}")

def process_single_paper(grobid_client: GrobidClient, 
                        paper_id: str, 
                        pdf_link: str) -> Tuple[bool, float, models.PaperResult]:
    """
    Xử lý một paper duy nhất bằng GROBID.
    
    Args:
        grobid_client: Client để gọi GROBID API
        paper_id: ID của paper
        pdf_link: URL để tải PDF
        
    Returns:
        Tuple[bool, float, models.PaperResult]:
            - bool: True nếu xử lý hoàn toàn thành công
            - float: Thời gian xử lý (giây)
            - models.PaperResult: Kết quả xử lý
    """
    start_time = time.time()
    
    # Khởi tạo đối tượng kết quả với các giá trị mặc định
    result: models.PaperResult = {
        "paper_id": paper_id,
        "processing_status": {
            "header_status": "pending",
            "references_status": "pending"
        }
    }
    
    header_success = False
    references_success = False
    
    try:
        # Gọi phương thức mới - tải PDF một lần duy nhất
        header_xml, references_xml = grobid_client.process_paper_from_url(pdf_link, paper_id)
        
        # Parse kết quả XML
        # Parse kết quả XML một cách riêng biệt
        authors = parser.parse_header_xml(header_xml, paper_id)
        references = parser.parse_references_xml(references_xml, paper_id)

        # Cập nhật kết quả
        if authors:
            result["grobid_authors"] = authors
        if references:
            result["grobid_references"] = references

        # Cập nhật trạng thái
        result["processing_status"]["header_status"] = "success" if authors else "failed"
        header_success = bool(authors)

        result["processing_status"]["references_status"] = "success" if references else "failed"
        references_success = bool(references)
            
        # Log thành công ở cấp độ DEBUG thay vì INFO để không hiển thị ra console
        logging.debug(f"✅ Xử lý thành công {paper_id}")
        
    except PDFDownloadError as e:
        error_msg = f"Lỗi khi tải PDF: {str(e)}"
        logging.error(f"❌ [{paper_id}] {error_msg}")
        result["processing_status"] = {
            "header_status": "failed",
            "references_status": "failed",
            "error_message": error_msg
        }
    except GrobidRequestError as e:
        error_msg = f"Lỗi khi gọi GROBID: {str(e)}"
        logging.error(f"❌ [{paper_id}] {error_msg}")
        result["processing_status"] = {
            "header_status": "failed",
            "references_status": "failed",
            "error_message": error_msg
        }
    except Exception as e:
        error_msg = f"Lỗi không xác định: {str(e)}"
        logging.exception(f"❌ [{paper_id}] {error_msg}")
        result["processing_status"] = {
            "header_status": "failed",
            "references_status": "failed",
            "error_message": error_msg
        }
    
    process_time = time.time() - start_time
    
    # Kiểm tra xem có thành công hoàn toàn không
    overall_success = header_success and references_success
    
    return overall_success, process_time, result

def run_pipeline(overall_tracker: PerformanceTracker,
               processed_papers: Set[str],
               num_workers: int = config.MAX_WORKERS,
               batch_size: int = config.BATCH_SIZE) -> None:
    """
    Chạy pipeline xử lý batch với khả năng phục hồi và tương tác người dùng.
    
    Args:
        overall_tracker: Đối tượng theo dõi tiến trình và hiệu suất tổng thể
        processed_papers: Tập hợp các paper_id đã xử lý
        num_workers: Số lượng worker thread
        batch_size: Số lượng paper xử lý mỗi batch
    """
    # Mở file đầu ra ở chế độ ghi nối tiếp
    output_file_dir = config.OUTPUT_FILE_PATH.parent
    output_file_dir.mkdir(parents=True, exist_ok=True)
    
    # Khởi tạo generator
    paper_generator = read_input_file(processed_papers)
    
    # Kiểm tra xem có dữ liệu để xử lý không
    papers_to_process = []
    auto_mode = False  # Chế độ tự động xử lý tất cả các batch
    
    def process_batch(batch: List[Tuple[str, str]], progress, task_id, batch_tracker: PerformanceTracker) -> None:
        """Hàm helper để xử lý một batch paper."""
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = []
            
            # Chuẩn bị và submit các task
            for paper_id, pdf_link in batch:
                if emergency_stop.is_set():
                    break
                    
                future = executor.submit(
                    process_single_paper,
                    GrobidClient(
                        config.GROBID_HEADER_URL,
                        config.GROBID_REFERENCES_URL
                    ),
                    paper_id,
                    pdf_link
                )
                futures.append((paper_id, future))
            
            # Thu thập kết quả theo thứ tự hoàn thành
            with open(config.OUTPUT_FILE_PATH, 'a', encoding='utf-8') as out_file:
                for paper_id, future in [(p_id, f) for p_id, f in futures]:
                    if emergency_stop.is_set():
                        break
                        
                    try:
                        success, process_time, result = future.result()
                        
                        # Ghi kết quả vào file đầu ra
                        out_file.write(json.dumps(result, ensure_ascii=False) + '\n')
                        
                        # Cập nhật trình theo dõi tiến trình cho batch hiện tại
                        if success:
                            batch_tracker.record_success(process_time, result)
                            # Đánh dấu paper đã xử lý thành công
                            mark_paper_as_processed(paper_id)
                        else:
                            batch_tracker.record_failure(process_time)
                            
                        # Cập nhật thanh tiến trình
                        progress.update(task_id, advance=1)
                    except Exception as e:
                        logging.exception(f"Lỗi khi xử lý kết quả cho {paper_id}: {str(e)}")
                        progress.update(task_id, advance=1)
    
    # Đọc batch đầu tiên
    for _ in range(batch_size):
        try:
            paper = next(paper_generator)
            papers_to_process.append(paper)
        except StopIteration:
            break
    
    if not papers_to_process:
        logging.info("Không còn paper nào cần xử lý")
        return
    
    total_processed = 0
    batch_count = 1
    
    # Tiếp tục xử lý từng batch
    while papers_to_process:
        if emergency_stop.is_set():
            logging.warning("🛑 Nhận tín hiệu dừng, đang kết thúc...")
            break
        
        # Tạo tracker riêng cho batch hiện tại
        batch_tracker = PerformanceTracker()
        
        current_batch_size = len(papers_to_process)
        logging.info(f"Bắt đầu xử lý batch {batch_count} với {current_batch_size} paper")
        
        # Khởi tạo thanh tiến trình rich cho batch hiện tại
        with Progress(
            SpinnerColumn(),
            TextColumn("[bold blue]Processing Batch {task.fields[batch_number]}[/bold blue]"),
            BarColumn(),
            TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
            TextColumn("({task.completed}/{task.total})"),
            TimeElapsedColumn(),
            TimeRemainingColumn(),
            console=console
        ) as progress:
            # Thêm task cho batch hiện tại
            task_id = progress.add_task(
                f"Batch {batch_count}", 
                total=current_batch_size,
                batch_number=batch_count
            )
            
            # Xử lý batch hiện tại với thanh tiến trình mới
            process_batch(papers_to_process, progress, task_id, batch_tracker)
            total_processed += current_batch_size
        
        # In dòng trống để tạo khoảng cách trước báo cáo
        console.print()
        
        # Gộp dữ liệu từ batch_tracker vào overall_tracker
        overall_tracker.merge(batch_tracker)
        
        # In báo cáo chi tiết sau mỗi batch sử dụng batch_tracker
        report = batch_tracker.get_report()
        display_batch_report(batch_count, report, total_processed)
        
        # Nếu không ở chế độ tự động, hỏi người dùng
        if not auto_mode and not emergency_stop.is_set():
            try:
                choice = console.input(
                    "[bold yellow][C]ontinue, [A]uto, [Q]uit: [/bold yellow]"
                ).lower()
                
                if choice == 'q':
                    logging.info("Người dùng chọn dừng pipeline")
                    emergency_stop.set()
                    break
                elif choice == 'a':
                    auto_mode = True
                    # Hiển thị tùy chọn cấu hình cho chế độ tự động
                    console.print("[bold green]Entering Auto Mode. You can adjust settings for the remainder of the run.[/bold green]")
                    
                    # Hỏi để thay đổi batch_size
                    try:
                        new_batch_size = IntPrompt.ask(
                            f"Enter new batch size for reporting (current: {batch_size}) or press Enter to keep it",
                            default=batch_size
                        )
                        
                        # Hỏi để thay đổi num_workers
                        new_num_workers = IntPrompt.ask(
                            f"Enter new number of workers (current: {num_workers}) or press Enter to keep it",
                            default=num_workers
                        )
                        
                        # Cập nhật giá trị nếu có thay đổi
                        if new_batch_size != batch_size:
                            batch_size = new_batch_size
                        
                        if new_num_workers != num_workers:
                            num_workers = new_num_workers
                        
                        console.print(f"[bold green]Configuration updated. Starting auto mode with batch_size={batch_size}, workers={num_workers}...[/bold green]")
                        logging.info(f"Chuyển sang chế độ tự động với batch_size={batch_size}, workers={num_workers}")
                    except (KeyboardInterrupt, EOFError):
                        console.print("[bold yellow]Bạn đã gián đoạn nhập liệu. Đang dừng pipeline một cách an toàn...[/bold yellow]")
                        emergency_stop.set()
                        break
            except (KeyboardInterrupt, EOFError):
                console.print("[bold yellow]Bạn đã gián đoạn nhập liệu. Đang dừng pipeline một cách an toàn...[/bold yellow]")
                emergency_stop.set()
                break
        
        # Chuẩn bị batch tiếp theo nếu không có tín hiệu dừng
        if not emergency_stop.is_set():
            papers_to_process = []
            for _ in range(batch_size):
                try:
                    paper = next(paper_generator)
                    papers_to_process.append(paper)
                except StopIteration:
                    break
            
            batch_count += 1
        
        # Nếu không còn paper nào để xử lý
        if not papers_to_process:
            logging.info("Đã xử lý tất cả paper trong danh sách")
            break
    
    # Hiển thị báo cáo tổng kết sử dụng overall_tracker
    if not emergency_stop.is_set():
        console.print("\n[bold green]🎉 Đã hoàn thành tất cả batch![/bold green]")
        console.print("[bold cyan]📊 Báo cáo tổng kết[/bold cyan]")
        
        # Lấy báo cáo từ overall_tracker
        final_report = overall_tracker.get_report()
        display_overall_report(final_report, total_processed)

def display_batch_report(batch_number: int, report: Dict[str, Any], total_processed: int) -> None:
    """
    Hiển thị báo cáo chi tiết của một batch.
    
    Args:
        batch_number: Số thứ tự của batch
        report: Báo cáo hiệu suất từ PerformanceTracker
        total_processed: Tổng số paper đã xử lý đến hiện tại
    """
    # Tạo bảng báo cáo
    table = Table(title=f"📊 Báo cáo Batch {batch_number}",
                 show_header=True,
                 header_style="bold blue")
    
    table.add_column("Chỉ số", justify="left")
    table.add_column("Giá trị", justify="right")
    
    # Thông số cơ bản
    table.add_row("Tổng số paper", str(report['total_papers']))
    table.add_row("Xử lý thành công", str(report['success_count']))
    table.add_row("Xử lý thất bại", str(report['failure_count']))
    table.add_row("Tỷ lệ thành công", f"{report['success_rate']:.1f}%")
    table.add_row("Đã xử lý tổng cộng", str(total_processed))
    
    # Thông số thời gian
    if 'total_time' in report:
        table.add_row("Thời gian xử lý batch", f"{report['total_time']:.1f}s")
        table.add_row("Tốc độ trung bình", 
                     f"{report.get('papers_per_minute', 0):.1f} papers/phút")
        table.add_row("Thời gian trung bình/paper", f"{report.get('avg_time', 0):.2f}s")
    
    # Thông số về Author
    if 'author_stats' in report:
        auth_stats = report['author_stats']
        table.add_row("", "")  # Dòng trống để phân tách
        table.add_row("[bold]Thống kê Tác giả[/bold]", "")
        table.add_row(
            "Số tác giả trung bình",
            f"{auth_stats['avg_authors_per_paper']:.1f}"
        )
        table.add_row(
            "Số tác giả tối thiểu",
            str(auth_stats.get('min_authors', 'N/A'))
        )
        table.add_row(
            "Số tác giả tối đa",
            str(auth_stats.get('max_authors', 'N/A'))
        )
        table.add_row(
            "Tỷ lệ có affiliation",
            f"{auth_stats['avg_affiliation_rate']:.1f}%"
        )
    
    # Thông số về Reference
    if 'reference_stats' in report:
        ref_stats = report['reference_stats']
        table.add_row("", "")  # Dòng trống để phân tách
        table.add_row("[bold]Thống kê Tham chiếu[/bold]", "")
        table.add_row(
            "Số tham chiếu trung bình",
            f"{ref_stats['avg_refs_per_paper']:.1f}"
        )
        table.add_row(
            "Số tham chiếu tối thiểu",
            str(ref_stats.get('min_refs', 'N/A'))
        )
        table.add_row(
            "Số tham chiếu tối đa",
            str(ref_stats.get('max_refs', 'N/A'))
        )
        
        field_rates = ref_stats['avg_field_rates']
        table.add_row(
            "Tỷ lệ có title",
            f"{field_rates['title']:.1f}%"
        )
        table.add_row(
            "Tỷ lệ có year",
            f"{field_rates['year']:.1f}%"
        )
        table.add_row(
            "Tỷ lệ có venue",
            f"{field_rates['venue']:.1f}%"
        )
    
    # In bảng báo cáo
    console.print(table)

def display_overall_report(report: Dict[str, Any], total_processed: int) -> None:
    """
    Hiển thị báo cáo tổng kết của toàn bộ quá trình xử lý.
    
    Args:
        report: Báo cáo hiệu suất từ PerformanceTracker
        total_processed: Tổng số paper đã xử lý
    """
    # Tạo bảng báo cáo
    table = Table(title=f"📊 Báo cáo Tổng kết",
                 show_header=True,
                 header_style="bold blue")
    
    table.add_column("Chỉ số", justify="left")
    table.add_column("Giá trị", justify="right")
    
    # Thông số cơ bản
    table.add_row("Tổng số paper", str(report['total_papers']))
    table.add_row("Xử lý thành công", str(report['success_count']))
    table.add_row("Xử lý thất bại", str(report['failure_count']))
    table.add_row("Tỷ lệ thành công", f"{report['success_rate']:.1f}%")
    table.add_row("Đã xử lý tổng cộng", str(total_processed))
    
    # Thông số thời gian
    if 'total_time' in report:
        table.add_row("Thời gian xử lý tổng cộng", f"{report['total_time']:.1f}s")
        table.add_row("Tốc độ trung bình", 
                     f"{report.get('papers_per_minute', 0):.1f} papers/phút")
        table.add_row("Thời gian trung bình/paper", f"{report.get('avg_time', 0):.2f}s")
    
    # Thống kê về Author
    if 'author_stats' in report:
        auth_stats = report['author_stats']
        table.add_row("", "")  # Dòng trống để phân tách
        table.add_row("[bold]Thống kê Tác giả[/bold]", "")
        table.add_row(
            "Số tác giả trung bình",
            f"{auth_stats['avg_authors_per_paper']:.1f}"
        )
        table.add_row(
            "Số tác giả tối thiểu",
            str(auth_stats.get('min_authors', 'N/A'))
        )
        table.add_row(
            "Số tác giả tối đa",
            str(auth_stats.get('max_authors', 'N/A'))
        )
        table.add_row(
            "Tỷ lệ có affiliation",
            f"{auth_stats['avg_affiliation_rate']:.1f}%"
        )
    
    # Thống kê về Reference
    if 'reference_stats' in report:
        ref_stats = report['reference_stats']
        table.add_row("", "")  # Dòng trống để phân tách
        table.add_row("[bold]Thống kê Tham chiếu[/bold]", "")
        table.add_row(
            "Số tham chiếu trung bình",
            f"{ref_stats['avg_refs_per_paper']:.1f}"
        )
        table.add_row(
            "Số tham chiếu tối thiểu",
            str(ref_stats.get('min_refs', 'N/A'))
        )
        table.add_row(
            "Số tham chiếu tối đa",
            str(ref_stats.get('max_refs', 'N/A'))
        )
        
        field_rates = ref_stats['avg_field_rates']
        table.add_row(
            "Tỷ lệ có title",
            f"{field_rates['title']:.1f}%"
        )
        table.add_row(
            "Tỷ lệ có year",
            f"{field_rates['year']:.1f}%"
        )
        table.add_row(
            "Tỷ lệ có venue",
            f"{field_rates['venue']:.1f}%"
        )
    
    # In bảng báo cáo
    console.print(table)

def display_configuration() -> None:
    """Hiển thị bảng cấu hình hiện tại."""
    table = Table(title="⚙️ Cấu hình Pipeline",
                show_header=True,
                header_style="bold blue")
    
    table.add_column("Tham số", justify="left")
    table.add_column("Giá trị", justify="left")
    
    table.add_row("Input File", str(config.INPUT_FILE_PATH))
    table.add_row("Output File", str(config.OUTPUT_FILE_PATH))
    table.add_row("Progress File", str(config.PROGRESS_FILE_PATH))
    table.add_row("GROBID URL", config.GROBID_BASE_URL)
    table.add_row("Workers", str(config.MAX_WORKERS))
    table.add_row("Batch Size", str(config.BATCH_SIZE))
    table.add_row("Log Level", str(config.LOG_LEVEL))
    table.add_row("Log File", str(config.LOG_FILE_PATH))
    
    console.print(table)

def main() -> None:
    """Hàm main với quy trình đơn giản hơn."""
    # Khởi tạo môi trường
    colorama.init()
    setup_logging()
    
    # Đăng ký signal handler
    def signal_handler(signum, frame):
        emergency_stop.set()
        logging.info("🚦 Đã nhận tín hiệu dừng từ phím tắt")
    
    # Trên Windows, ưu tiên sử dụng các phím tắt ít phổ biến
    # Thử đăng ký nhiều signal khác nhau để tìm cái hoạt động
    is_handler_registered = False
    
    # Thử với SIGTERM (có thể gửi bằng taskkill /PID process_id /F từ command line)
    try:
        signal.signal(signal.SIGTERM, signal_handler)
        is_handler_registered = True
        logging.debug("Đã đăng ký handler cho SIGTERM")
        console.print("[bold yellow]Để dừng chương trình, mở một terminal khác và chạy 'taskkill /PID <process_id> /F'[/bold yellow]")
    except (AttributeError, ValueError):
        pass
        
    # Các phím tắt khác vẫn hoạt động thông qua KeyboardInterrupt
    # Hiển thị hướng dẫn cho người dùng để tránh nhầm lẫn
    console.print("[bold yellow]Để thoát chương trình an toàn, hãy sử dụng tùy chọn [Q]uit khi được hỏi[/bold yellow]")
    
    # Hiển thị banner
    console.print("\n" + "="*60)
    console.print("[bold cyan]🤖 GROBID PDF Processing Pipeline - Optimized[/bold cyan]")
    console.print("[bold green]Mỗi PDF chỉ tải một lần duy nhất - I/O tối ưu - Resumable[/bold green]")
    console.print("="*60 + "\n")
    
    # Hiển thị cấu hình
    display_configuration()
    
    # Xác nhận với người dùng
    choice = console.input("\n[bold yellow][S]tart the pipeline with this configuration, or [Q]uit? [/bold yellow]").lower()
    
    if choice != 's':
        console.print("[bold green]👋 Tạm biệt![/bold green]")
        return
    
    # Reset emergency stop flag
    emergency_stop.clear()
    
    # Tải danh sách paper đã xử lý
    processed_papers = load_processed_papers()
    
    # Khởi tạo progress tracker
    tracker = PerformanceTracker()
    
    # Chạy pipeline
    try:
        run_pipeline(tracker, processed_papers)
        
        # Hiển thị thông báo kết thúc
        console.print("[bold green]Pipeline đã hoàn thành![/bold green]")
    except (KeyboardInterrupt, EOFError):
        console.print("[bold yellow]Chương trình đã dừng lại theo yêu cầu. Dữ liệu đã xử lý vẫn được lưu.[/bold yellow]")
    except Exception as e:
        logging.exception("❌ Lỗi không xác định trong pipeline")
        console.print(f"[bold red]Lỗi: {str(e)}[/bold red]")
    
    console.print("[bold green]Pipeline đã hoàn thành![/bold green]")

if __name__ == "__main__":
    main()
