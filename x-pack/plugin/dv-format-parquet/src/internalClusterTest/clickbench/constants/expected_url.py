"""Prints the counts of URLs that contain google (with and without an empty SearchPhrase) in hits.parquet.
Usage: expected_url.py hits.parquet"""
import sys, pyarrow.parquet as pq, pyarrow.compute as pc
f = pq.ParquetFile(sys.argv[1])
google = 0; empty_phrase_google = 0
for i in range(f.metadata.num_row_groups):
    t = f.read_row_group(i, columns=["URL", "SearchPhrase"])
    m = pc.match_substring(t.column("URL"), "google")
    google += pc.sum(pc.cast(m, "int64")).as_py()
    both = pc.and_(m, pc.not_equal(t.column("SearchPhrase"), ""))
    empty_phrase_google += pc.sum(pc.cast(both, "int64")).as_py()
print("url like %google%", google, "and phrase not empty", empty_phrase_google)
