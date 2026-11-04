"""Checks that the min/max statistics in the footer of a Parquet file are exact for the given columns (we need exact statistics for the skippers).
Usage: check_stats.py hits.parquet column [column ...]"""
import pyarrow.parquet as pq, pyarrow.compute as pc, sys
f = pq.ParquetFile(sys.argv[1])
names = [f.schema_arrow.field(i).name for i in range(len(f.schema_arrow))]
cols = sys.argv[2:]
bad = 0
for rg in range(f.num_row_groups):
    md = f.metadata.row_group(rg)
    for c in cols:
        i = names.index(c)
        st = md.column(i).statistics
        data = f.read_row_group(rg, columns=[c])[c]
        lo, hi = pc.min(data).as_py(), pc.max(data).as_py()
        if st is None or not st.has_min_max:
            print(f"rg {rg} {c}: no stats"); bad += 1; continue
        if st.min != lo or st.max != hi:
            print(f"rg {rg} {c}: footer min/max {st.min}/{st.max} actual {lo}/{hi}"); bad += 1
print("row groups", f.num_row_groups, "mismatches", bad)
