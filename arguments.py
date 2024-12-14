from argparse import ArgumentParser
from dataclasses import dataclass
import json
import os
from time import time
from Enums import FilterMode, ProcessingMode


@dataclass
class Args:

    data_file: str
    bad_words_file: str
    columns: list[int]
    filter_mode: FilterMode
    processing_mode: ProcessingMode
    chunk_size: int
    rounding_place: int
    starting_time: float = time()


def add_arguments(parser: ArgumentParser):
    parser.add_argument(
        "-d",
        "--data_file",
        type=str,
        help="The csv file that we will filter",
        required=True,
    )
    parser.add_argument(
        "-b",
        "--bad_words_file",
        type=str,
        help="The name of bad words file",
        required=True,
    )
    parser.add_argument(
        "-s",
        "--chunk_size",
        type=int,
        default=1000,
        help="The chunk size will be processed",
    )
    parser.add_argument(
        "-f",
        "--filter_mode",
        choices=[i.name for i in FilterMode],  # names of Enum fields
        default=FilterMode.Regex,
        help="The mode of filtering.",
    )
    parser.add_argument(
        "-p",
        "--processing_mode",
        choices=[i.name for i in ProcessingMode],  # names of Enum fields
        default=ProcessingMode.MultiThreading,
        help="the concurrent model that will work",
    )
    parser.add_argument(
        "-c",
        "--columns",
        type=lambda v: list(map(int, (v.split(",")))),
        default=[0, 2, 4],
        help="specified columns that will be filtered in format column1,column... like 1,2,3,4",
    )
    parser.add_argument(
        "-r",
        "--rounding_place",
        type=int,
        default=2,
        help="how many digits you want after the decimal point",
    )


def parse_args() -> Args:
    if os.path.exists("args.json"):
        with open("args.json", "r") as a:
            dict = json.load(a)
            # convert str to enum
            dict["filter_mode"] = FilterMode[dict["filter_mode"]]
            dict["processing_mode"] = ProcessingMode[dict["processing_mode"]]
            return Args(**dict)

    parser = ArgumentParser(
        prog="Bad Words Filter App",
        description="filter the specified columns from a big compressed csv file the bad words rows.",
    )
    add_arguments(parser)
    args = parser.parse_args()
    # if user provided enum values as str so i convert it
    if type(args.filter_mode) == str:
        args.filter_mode = FilterMode[args.filter_mode]
    if type(args.processing_mode) == str:
        args.processing_mode = ProcessingMode[args.processing_mode]
    return Args(**vars(args))
