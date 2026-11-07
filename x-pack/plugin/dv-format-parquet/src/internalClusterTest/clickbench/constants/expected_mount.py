"""Prints the values that ParquetMountIT-style checks on hits.parquet compare with: row count, min/max EventTime and EventDate, and counts for
some CounterID and date ranges.
Usage: expected_mount.py hits.parquet"""
import pyarrow.parquet as pq, pyarrow.compute as pc, pyarrow as pa, sys
t = pq.read_table(sys.argv[1], columns=["EventTime", "EventDate", "CounterID"])
print("rows", t.num_rows)
et = t["EventTime"]
print("min/max EventTime", pc.min(et).as_py(), pc.max(et).as_py())
d = t["EventDate"]
print("min/max EventDate", pc.min(d).as_py(), pc.max(d).as_py())
m = pc.and_(pc.equal(t["CounterID"], 62), pc.and_(pc.greater_equal(d, 15887), pc.less_equal(d, 15917)))
print("count counter 62 in July 2013", pc.sum(pc.cast(m, pa.int64())).as_py())
# distinct days with EventTime in 2013-07-14..2013-07-15 (seconds)
lo, hi = 1373760000, 1373846400
m2 = pc.and_(pc.greater_equal(et, lo), pc.less(et, hi))
print("EventTime in [2013-07-14,2013-07-15)", pc.sum(pc.cast(m2, pa.int64())).as_py())
m3 = pc.and_(pc.equal(t["CounterID"], 62), pc.and_(pc.greater_equal(d, 15900), pc.less_equal(d, 15905)))
print("count counter 62, EventDate in [2013-07-14, 2013-07-19]", pc.sum(pc.cast(m3, pa.int64())).as_py())
m4 = pc.and_(pc.greater_equal(d, 15900), pc.less_equal(d, 15902))
print("count EventDate in [2013-07-14, 2013-07-16]", pc.sum(pc.cast(m4, pa.int64())).as_py())
