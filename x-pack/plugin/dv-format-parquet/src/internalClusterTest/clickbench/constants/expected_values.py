"""Prints the first values, min, max and the sum of some numeric columns of the first three row groups of hits.parquet.
Usage: expected_values.py hits.parquet"""
import sys, pyarrow.parquet as pq, pyarrow.compute as pc
f = pq.ParquetFile(sys.argv[1])
rgs = [0, 1, 2]
print("row groups", rgs, [f.metadata.row_group(i).num_rows for i in rgs])
for col in ["CounterID", "EventTime", "EventDate", "UserID", "WatchID", "ClientIP"]:
    t = f.read_row_groups(rgs, columns=[col]).column(0).combine_chunks()
    vals = t.to_pylist()
    # sum with python ints (UserID/WatchID overflow int64 sums)
    total = sum(vals)
    print(col, "rows", len(vals), "first5", vals[:5], "last", vals[-1], "min", min(vals), "max", max(vals), "sum", total)
