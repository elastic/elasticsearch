Corpus for JsonParserBenchmark.

Each file is a `source` @Param value. They are chosen to cover different document shapes, because a
parser's cost profile depends far more on shape than on size: nesting depth, key repetition, the
string/number ratio, and whether string values can be handed back as raw input bytes.

monitor_cluster_stats.json  33,811 B  monitoring payload: deeply nested, numeric-heavy, short ASCII keys
monitor_index_stats.json     3,604 B  same shape, smaller
monitor_node_stats.json      4,139 B  same shape, smaller

    The three monitoring documents are one shape sampled three times. They were the entire corpus
    originally; the three files below were added to widen it.

small_log_doc.json             377 B  one flat http-log record, 12 fields, no nesting, plain ASCII.
                                      Bulk indexing is overwhelmingly many small documents, and the
                                      per-document fixed costs (buffer setup, padding, symbol-table
                                      warmup) amortize worst here.

flat_log_batch.json         38,806 B  array of 100 records with exactly the keys of small_log_doc.json
                                      and varying values. Wide, shallow, string-dominated: the o11y
                                      logs shape. The key repetition is the point -- it is what
                                      exercises Jackson's ByteQuadsCanonicalizer field-name interning,
                                      which parseFieldNames measures.
                                      The array is wrapped in a single-field object rather than being
                                      the root, because XContentParser.mapOrdered() on a document
                                      whose root is an array returns an empty map without reading it,
                                      which would make parseToMap measure nothing for this file.

escaped_unicode.json         6,915 B  string-heavy and deliberately hostile to zero-copy UTF-8: it
                                      contains escaped quotes, backslashes, \n and \t, \uXXXX escapes
                                      including surrogate pairs, and literal 2-, 3- and 4-byte UTF-8
                                      (accented Latin, CJK, emoji). ESUTF8StreamJsonParser's
                                      getValueAsText() fast path only applies when it can hand back
                                      raw input bytes unmodified, so a corpus of clean ASCII flatters
                                      it; this file is where that shows. It also carries one long
                                      clean-ASCII record as an in-document control.

The bottom three files are written by hand or synthesized locally, and should stay that way: keep new
corpus entries small, self-explanatory and of known provenance rather than importing third-party
datasets.
