from typing import Dict

class Analyzer:
    """Calculates real-time metrics from results"""
    
    def __init__(self):
        self.metrics = {
            "error_rate": 0,
            "avg_response_time": 0,
            "request_count": 0,
            "resource_usage": {}
        }

    def update_metrics(self, new_data: Dict) -> None:
        """Update metrics with new data from workers"""
        self.metrics["error_rate"] += new_data.get("error_rate", 0)
        self.metrics["avg_response_time"] += new_data.get("avg_response_time", 0)
        self.metrics["request_count"] += new_data.get("request_count", 0)

    def get_current_metrics(self) -> Dict:
        """Return current calculated metrics"""
        return self.metrics

async def main():
    analyzer = Analyzer()
    # Simulate updating metrics with new data
    analyzer.update_metrics({"error_rate": 0.05, "avg_response_time": 120, "request_count": 100})
    print(analyzer.get_current_metrics())

if __name__ == "__main__":
    asyncio.run(main())
