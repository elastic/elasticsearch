"""Prints aggregates over hits.parquet (AdvEngineID, ResolutionWidth, EventDate, SearchPhrase) that the first ClickBench checks compare with.
Usage: expected_queries.py hits.parquet"""
import sys, collections, pyarrow.parquet as pq, pyarrow.compute as pc
f = pq.ParquetFile(sys.argv[1])
n = f.metadata.num_row_groups
rows = adv_nonzero = 0; adv_sum = 0; width_sum = 0; dmin = 10**9; dmax = -1
adv_groups = collections.Counter(); phrase_nonempty = 0; phrases = collections.Counter()
for i in range(n):
    t = f.read_row_group(i, columns=["AdvEngineID", "ResolutionWidth", "EventDate", "SearchPhrase"])
    adv = t.column("AdvEngineID"); w = t.column("ResolutionWidth"); d = t.column("EventDate"); sp = t.column("SearchPhrase")
    rows += len(adv)
    nz = pc.not_equal(adv, 0)
    adv_nonzero += pc.sum(pc.cast(nz, "int64")).as_py()
    adv_sum += pc.sum(adv).as_py()
    width_sum += pc.sum(w).as_py()
    dmin = min(dmin, pc.min(d).as_py()); dmax = max(dmax, pc.max(d).as_py())
    for v in pc.value_counts(adv.filter(nz)).to_pylist():
        adv_groups[v["values"]] += v["counts"]
    ne = pc.not_equal(sp, "")
    phrase_nonempty += pc.sum(pc.cast(ne, "int64")).as_py()
    for v in pc.value_counts(sp.filter(ne)).to_pylist():
        phrases[v["values"]] += v["counts"]
print("rows", rows)
print("adv_nonzero", adv_nonzero, "adv_sum", adv_sum, "avg_width", width_sum / rows, "width_sum", width_sum)
print("event_date min", dmin, "max", dmax)
print("adv_groups top", adv_groups.most_common(5))
print("phrase_nonempty", phrase_nonempty, "distinct phrases", len(phrases))
print("phrases top", [(k, c) for k, c in sorted(phrases.items(), key=lambda kv: (-kv[1], kv[0]))[:10]])
