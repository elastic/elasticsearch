Corpus for JsonParserBenchmark.

Each file is a `source` @Param value. They are chosen to cover different document shapes, because a
parser's cost profile depends far more on shape than on size: nesting depth, key repetition, the
string/number ratio, and whether string values can be handed back as raw input bytes.

monitor_cluster_stats.json  33,811 B  monitoring payload: deeply nested, numeric-heavy, short ASCII keys
monitor_index_stats.json     3,604 B  same shape, smaller
monitor_node_stats.json      4,139 B  same shape, smaller

small_log_doc.json             377 B  one flat http-log record, 12 fields, no nesting, plain ASCII.
                                      Emphasizes per-document fixed costs (buffer setup, padding, symbol-table
                                      warmup).

flat_log_batch.json         38,806 B  array of 100 records with fixed and repeated keys (same keys as
                                      small_log_doc.json) and varying values. It mimics a typical o11y logs
                                      shape: wide, shallow and string-dominated.

escaped_unicode.json         6,915 B  string-heavy and with deliberately "difficult" UTF-8 cases: it
                                      contains escaped quotes, backslashes, \n and \t, \uXXXX escapes
                                      including surrogate pairs, and literal 2-, 3- and 4-byte UTF-8
                                      (accented Latin, CJK, emoji). Also carries one long clean-ASCII
                                      record as an in-document control.
