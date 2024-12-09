from datetime import datetime
from typing import Dict, List
from log_entry import LogEntry

class Worker:
    """Processes log chunks and reports results"""
    
    def __init__(self, worker_id: str, coordinator_url: str):
        self.worker_id = worker_id
        self.coordinator_url = coordinator_url

    async def process_chunk(self, filepath: str, start: int, size: int) -> Dict:
        """Process a chunk of log file and return metrics"""
        metrics = {"error_rate": 0, "avg_response_time": 0, "request_count": 0, "resource_usage": {}}
        with open(filepath, 'r') as file:
            file.seek(start)
            chunk = file.read(size)
            log_entries = self.parse_logs(chunk)
            # Calculate metrics for this chunk
            metrics = self.calculate_metrics(log_entries)
        return metrics

    def parse_logs(self, log_data: str):
        """Parse logs and handle errors gracefully."""
        log_entries = []
        for line in log_data.splitlines():
            parts = line.split(" ", 2)
            if len(parts) < 3:
                print(f"Skipping malformed line: {line}")  # Logging malformed lines
                continue
            
            timestamp_str, level, message = parts
            try:
                timestamp = self.parse_timestamp(timestamp_str)
                log_entries.append(LogEntry(timestamp, level, message))
            except ValueError as e:
                print(f"Error processing line: {line} -> {e}")  # Log the error
                continue

        return log_entries


    def parse_timestamp(self, timestamp_str: str) -> datetime:
        """Try to parse the timestamp in different formats"""
        formats = [
            "%Y-%m-%d %H:%M:%S.%f",  # Full timestamp with milliseconds
            "%Y-%m-%d %H:%M:%S",      # Timestamp without milliseconds
            "%Y-%m-%d",               # Timestamp with just the date (no time)
        ]
        for fmt in formats:
            try:
                # Handle the case where the timestamp is just the date (YYYY-MM-DD)
                if len(timestamp_str) == 10:  # Only YYYY-MM-DD
                    timestamp_str += " 00:00:00"  # Add default time
                return datetime.strptime(timestamp_str, fmt)
            except ValueError:
                continue  # Try the next format
        # If none of the formats matched, raise an error
        raise ValueError(f"Unrecognized timestamp format: {timestamp_str}")

    def calculate_metrics(self, entries: List[LogEntry]) -> Dict:
        """Calculate error rate, avg response time, and request count"""
        error_count = 0
        response_times = []
        request_count = 0
        
        for entry in entries:
            if "ERROR" in entry.level:
                error_count += 1
            
            if "Request processed in" in entry.message:
                request_count += 1
                # Attempt to extract response time from the message
                try:
                    # Extract the response time (assumes format: 'Request processed in 127ms')
                    response_time_str = entry.message.split(" ")[3]  # Expected: '127ms'
                    response_time = int(response_time_str[:-2])  # Remove 'ms' and convert to int
                    response_times.append(response_time)
                except (IndexError, ValueError):
                    # In case the format is not correct, skip the entry
                    print(f"Skipping malformed log entry: {entry.message}")

        # Calculate the metrics
        error_rate = error_count / len(entries) if entries else 0
        avg_response_time = sum(response_times) / len(response_times) if response_times else 0

        return {
            "error_rate": error_rate,
            "avg_response_time": avg_response_time,
            "request_count": request_count
        }

    async def report_health(self) -> None:
        """Send heartbeat to coordinator"""
        async with aiohttp.ClientSession() as session:
            async with session.post(f"{self.coordinator_url}/heartbeat", json={"worker_id": self.worker_id}) as response:
                if response.status != 200:
                    print(f"Failed to report health for {self.worker_id}")
