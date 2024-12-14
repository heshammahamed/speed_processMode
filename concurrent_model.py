from abc import ABC, abstractmethod
import multiprocessing
import multiprocessing.pool
import multiprocessing.process
from consumer import Consumer
from threading import Thread
from producer import Producer
from chunks_processing_info import ChunkInfo, merge_chunks_info


# we need to make hybird approach
# this class will be extend from the interface concurrentModel
#it will make thread for the producer 
#and process = equal to the core that the pc have  

# interface (abstract class)
class ConcurrentModel(ABC):

    @abstractmethod
    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        """start the concurrent work"""
        pass


class HybirdModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        producer_thread = Thread (target=producer.run)
        producer_thread.start()

        n_cores = multiprocessing.cpu_count()

        # Start consumer processes
        processes = [
            multiprocessing.Process(target=consumer.run)
            for _ in range(n_cores)
        ]

        for process in processes:
            process.start()

        producer_thread.join()

        for process in processes:
            process.join()

        return merge_chunks_info(
            producer.reading_info_queue, consumer.filtering_info_queue
        )


class ProcessesPoolModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        producer_process = multiprocessing.Process(target=producer.run)
        consumer_process1 = multiprocessing.Process(target=consumer.run)
        consumer_process2 = multiprocessing.Process(target=consumer.run)
        producer_process.start()
        consumer_process1.start()
        consumer_process2.start()
        producer_process.join()
        consumer_process1.join()
        consumer_process2.join()
        return merge_chunks_info(
            producer.reading_info_queue, consumer.filtering_info_queue
        )


class MultiProcessingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        producer_process = multiprocessing.Process(target=producer.run)
        consumer_process = multiprocessing.Process(target=consumer.run)
        producer_process.start()
        consumer_process.start()
        producer_process.join()
        consumer_process.join()
        return merge_chunks_info(
            producer.reading_info_queue, consumer.filtering_info_queue
        )


class MultiThreadingModel(ConcurrentModel):

    def start(self, producer: Producer, consumer: Consumer) -> list[ChunkInfo]:
        producer_thread = Thread(target=producer.run)
        consumer_thread = Thread(target=consumer.run)
        producer_thread.start()
        consumer_thread.start()
        producer_thread.join()
        consumer_thread.join()
        return merge_chunks_info(
            producer.reading_info_queue, consumer.filtering_info_queue
        )
