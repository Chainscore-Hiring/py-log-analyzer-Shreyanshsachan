import os
import asyncio
from aiohttp import web
from worker import Worker
from typing import Dict

class Coordinator:
    """Manages workers and aggregates results"""
    
    def __init__(self, port: int, log_dir: str):
        self.port = port
        self.workers = {}
        self.results = {}
        self.log_dir = log_dir

    async def distribute_work(self) -> None:
        """Scan the log directory and split files into chunks for workers"""
        log_files = [f for f in os.listdir(self.log_dir) if f.endswith('.log')]
        print(f"Found log files: {log_files}")

        chunk_size = 1024  # 1 KB per chunk
        worker_urls = [f"http://localhost:8001", f"http://localhost:8002", f"http://localhost:8003"]
        
        worker_index = 0
        for log_file in log_files:
            file_path = os.path.join(self.log_dir, log_file)
            file_size = os.path.getsize(file_path)
            num_chunks = (file_size // chunk_size) + 1
            
            for i in range(num_chunks):
                worker_url = worker_urls[worker_index % len(worker_urls)]  # Round-robin assignment
                worker = Worker(worker_id=str(worker_index), coordinator_url=worker_url)
                start = i * chunk_size
                print(f"Distributing chunk {i} of {log_file} to worker {worker.worker_id}")
                await worker.process_chunk(file_path, start, chunk_size)
                worker_index += 1

    async def handle_worker_failure(self, worker_id: str) -> None:
        """Reassign work from failed worker"""
        print(f"Handling failure of worker {worker_id}")
        # Reassign the failed worker's tasks to other workers here
    
    async def handle_health_check(self, request):
        """Health check endpoint"""
        data = await request.json()
        worker_id = data.get("worker_id")
        print(f"Received health report from {worker_id}")
        return web.Response(text="OK")

    async def start_server(self):
        """Start the server to manage workers"""
        app = web.Application()
        app.router.add_post('/heartbeat', self.handle_health_check)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, 'localhost', self.port)
        await site.start()

async def main():
    coordinator = Coordinator(port=8000, log_dir='test_vectors/logs')
    await coordinator.start_server()
    # Example: Start distributing work
    await coordinator.distribute_work()

if __name__ == "__main__":
    asyncio.run(main())
