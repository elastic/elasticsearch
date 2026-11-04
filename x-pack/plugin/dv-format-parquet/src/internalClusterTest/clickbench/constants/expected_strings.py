"""Prints the byte count, the number of empty values and an md5 of some string columns of the first three row groups of hits.parquet.
Usage: expected_strings.py hits.parquet"""
import sys, hashlib, pyarrow.parquet as pq
f = pq.ParquetFile(sys.argv[1])
rgs = [0, 1, 2]
for col in ["URL", "Title", "SearchPhrase", "Referer", "HitColor", "UserAgentMinor"]:
    vals = f.read_row_groups(rgs, columns=[col]).column(0).to_pylist()
    md5 = hashlib.md5()
    total = empty = 0
    for v in vals:
        b = v.encode("utf-8")
        md5.update(b); md5.update(b"\x00")
        total += len(b); empty += (len(b) == 0)
    print(col, "rows", len(vals), "bytes", total, "empty", empty, "md5", md5.hexdigest(), "first", vals[0].encode()[:30], "last", vals[-1].encode()[:30])
