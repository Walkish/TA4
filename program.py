import json
from statistics import median
from typing import Any
import items as items
import time

start_time = time.time()

_path = "data.jsonl"


def read_file(_path: str) -> dict[tuple[Any, Any], list]:
    grouped_data = {}
    with open(_path, "r") as file:
        for line in file:
            data = json.loads(line)

            date = data["date"]
            sensor = data["input"]
            value = data["value"]

            key = (date, sensor)

            if key not in grouped_data:
                grouped_data[key] = []

            grouped_data[key].append(value)

    return grouped_data


def group_file(grouped_data: dict) -> list[dict[str, Any]]:
    data = []
    for key, values in grouped_data.items():
        date, sensor = key
        median_value = median(values)

        new_record = {"date": date, "input": sensor, "median_value": median_value}
        data.append(new_record)
    return data


def write_file(data: dict) -> None:
    with open("new_data.jsonl", "w") as file:
        for record in data:
            file.write(json.dumps(record) + "\n")


write_file(group_file(read_file(_path)))

print("time elapsed: {:.2f}s".format(time.time() - start_time))
