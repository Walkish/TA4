From an IOT system we have gathered a log of sensor observations as a JSONL text file with the
following format:
```json lines
{"date": "2019-01-01", "time": "14:09:13.010", "input": "sensor_17", "value": 56.65788628257326}
{"date": "2019-01-01", "time": "16:07:09.166", "input": "sensor_23", "value": -6.939272293750748}
{"date": "2019-01-01", "time": "12:18:39.988", "input": "sensor_103", "value": 14.45649620272106}
```

The dates in the input file are guaranteed to appear in order.

For reporting, we would like you to write a small tool to aggregate the data per day, and for each day to report the median value for each sensor. 

It is imperative to write the program in such a way that it can scale as the data grows.

Also, the code must be covered with unit tests.

The output file should be in the
following format:
```json lines
{"date": "2019-01-01", "input": "sensor_134", "median_value": 19.09421548912573}
{"date": "2019-01-01", "input": "sensor_121", "median_value": 12.99225035624281}
{"date": "2019-01-01", "input": "sensor_33", "median_value": -30.332243582193147}
```

