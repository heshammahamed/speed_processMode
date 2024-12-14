import logging
from queue import Queue
from time import time
from typing import Any

import pandas as pd

from arguments import Args
from chunks_processing_info import ChunkFilteringInfo, elapsed
from filter import TextFilter


class Consumer:
    def __init__(
        self,
        chunks_queue: Queue[tuple[int, pd.DataFrame]] | Any,
        filtering_info_queue: Queue[tuple[int, ChunkFilteringInfo]] | Any,
        text_filter: TextFilter,
        args: Args,
    ):
        self.text_filter = text_filter
        self.chunks_queue = chunks_queue
        self.filtering_info_queue = filtering_info_queue
        self.args = args

    def start_filtering(self):
        while True:
            if self.chunks_queue.empty():
                continue
            if (tuple := self.chunks_queue.get()) is None:
                self.chunks_queue.put(None)  # type: ignore # to handle multiple consumers case
                break
            (index, chunk) = tuple
            # filtering
            start_time = time()
            healthy_rows_number, unhealthy_rows_number = self.text_filter.filter(chunk)
            end_time = time()

            # add the time taken to filter the chunk
            self.filtering_info_queue.put(
                (
                    index,
                    ChunkFilteringInfo(
                        filtering_time=round(
                            end_time - start_time, self.args.rounding_place
                        ),
                        number_of_healthy=healthy_rows_number,
                        number_of_unhealthy=unhealthy_rows_number,
                    ),
                )
            )
            logging.info(
                "%s  Consumer: finish filtering chunk number %s",
                elapsed(self.args.starting_time),
                index + 1,
            )

    def run(self):
        logging.info(
            "%s  consumer:start filtering chunks using %s.",
            elapsed(self.args.starting_time),
            self.text_filter,
        )
        self.start_filtering()
        logging.info(
            "%s  Consumer:finish of filtering chunks using %s.",
            elapsed(self.args.starting_time),
            self.text_filter,
        )
