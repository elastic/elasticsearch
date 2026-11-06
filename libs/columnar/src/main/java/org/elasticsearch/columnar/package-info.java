/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

/**
 * ColumNAR is a Lucene {@link DocValuesFormat} for columnar, analytics-oriented workloads. Numeric
 * values are stored in a binary substrate and encoded per block by an adaptive pipeline (Native
 * Adaptive Representation). It is selected per field through {@link PerFieldDocValuesFormat}, so it
 * coexists with the default format field by field.
 *
 * <h2>Read profiling</h2>
 *
 * <p>A ColumNAR reader can count and time its reads for the codec profiler. This section lists the instrumented read
 * operations, defines every ColumNAR metric and describes how the instrumentation is built. The profiler documents
 * scopes, collectors, snapshots and timing: see {@link org.elasticsearch.codec.profiler.Scopes},
 * {@link org.elasticsearch.codec.profiler.MetricRecorder}, {@link org.elasticsearch.codec.profiler.Timer} and
 * {@link org.elasticsearch.codec.profiler.TimingPolicy}. The frontend, the search profile or the ES|QL profile, owns
 * the response format and the selection of segments and fields.
 *
 * <h3>Activation</h3>
 *
 * <p>A frontend binds a scope with {@link org.elasticsearch.codec.profiler.Scopes#bind} around the code that opens
 * readers. The frontend decides whether to bind a scope from the {@code codec_read_profiling} feature flag and the
 * {@code profile} option of the request. ColumNAR has no switch of its own.
 *
 * <p>{@code ColumNARDocValuesProducer#getBinary} calls {@link org.elasticsearch.codec.profiler.Scopes#current} once,
 * when it opens a reader. It passes the codec name, the segment, the field and one fact about the stored column,
 * {@code skip_index}. The result is the recorder of the reader, or null when no scope is bound. The producer puts the
 * recorder in {@link org.elasticsearch.columnar.substrate.ColumnInputs#metrics}, and every component of the reader
 * takes it from there when it is built. A reader keeps its recorder for its life. The producer builds a new reader
 * for every call and never caches one, so the work of a reader goes to the scope that was bound when it opened. A
 * merge reader from {@code getMergeInstance} gets no recorder, also when a merge runs on a thread with a bound scope.
 *
 * <h3>Instrumented operations</h3>
 *
 * <ul>
 * <li>Opening a reader for a column ({@code readers}).</li>
 * <li>Inflating a compressed chunk of a byte stream, and finding a needed chunk already held ({@code chunks}).</li>
 * <li>Reading a byte stream that is stored uncompressed ({@code uncompressed}).</li>
 * <li>Fetching, inflating and decoding a block of an integer stream ({@code blocks}).</li>
 * <li>Reading a page of values, or the byte lengths of a page, from a string column ({@code pages}).</li>
 * <li>Resolving a term from the dictionary of a DICTIONARY column ({@code dictionary}).</li>
 * <li>Stepping over spans of documents with the ordinal skip index ({@code ordinal_skip}).</li>
 * </ul>
 *
 * <h3>Decorators and in-place measurement</h3>
 *
 * <p>Most measurement is in decorators, the {@code Profiled*} classes. A decorator wraps one codec component, owns the
 * metrics of its operation and asks the recorder for its handles when it is built. The codec logic does not change
 * when the reads are profiled, and a call site that gets its component from the same factory is measured without a
 * change. A factory such as {@link org.elasticsearch.columnar.substrate.BlockBytesCodec#forReading} returns the plain
 * component when the recorder is null.
 *
 * <ul>
 * <li>{@code ProfiledChunkDecompressor}: chunk inflation, uncompressed stream reads, and the inflation of a compressed
 * block. Each use reports to its own set of metrics.</li>
 * <li>{@code ProfiledBlockBytesCodec}: block fetches.</li>
 * <li>{@code ProfiledBlockDecoder}: block decodes.</li>
 * <li>{@code ProfiledStringBlockSink}: the form a page is handed over in.</li>
 * <li>{@code ProfiledIntervalSkipObserver}: the spans the ordinal skip index steps over.</li>
 * </ul>
 *
 * <p>Some measurement points are in the codec classes, because no component boundary sees the event:
 *
 * <ul>
 * <li>{@code ColumNARDocValuesProducer} counts and times the open of a reader. No reader component exists before the
 * open.</li>
 * <li>{@code ChunkedBytesReader} counts {@code chunks.cache_hits}. A cache hit is a call to the decompressor that does
 * not occur, so no decorator sees it.</li>
 * <li>{@code StringColumnReader#readBlock} and {@code StringColumnReader#readByteLengths} time the whole call, because
 * the metric is defined as that public call. Both methods are final, so a subclass cannot bypass the timer.</li>
 * <li>{@code DictionaryStringColumnReader#termAt} counts and times each lookup. Every term lookup goes through this
 * method.</li>
 * <li>{@code OrdinalRangePresence} times its navigation with a timer it receives. The navigation is a private loop of
 * the iterator.</li>
 * </ul>
 *
 * <h3>Uninstrumented path</h3>
 *
 * <p>Without a recorder, every factory returns the plain component and no decorator is built. The in-place timers are
 * {@link org.elasticsearch.codec.profiler.Timer#INERT}, which reads no clock, and the in-place counters are null and
 * skipped. The uninstrumented path reads no clock and allocates nothing for profiling. A recorder whose collector does
 * not measure time also hands out {@link org.elasticsearch.codec.profiler.Timer#INERT}, and its counters still count.
 *
 * <h3>Metric names and presence</h3>
 *
 * <p>A metric key is {@code ColumNAR.<component>.<name>}. The tables below write a metric as
 * {@code <component>.<name>}. A metric is registered when the component that records it is built, so it is present
 * at zero when nothing happened. An absent metric was not registered: the reader did not build that component, or
 * the collector does not measure time. For example, a column that stores its chunks uncompressed reports
 * {@code uncompressed} metrics and no {@code chunks} metrics.
 *
 * <h3>Rules for reading the metrics</h3>
 *
 * <ul>
 * <li><b>Counters are exact.</b> A counter of a call, such as an open, a fetch, an inflation, a decode, a read or a
 * lookup, changes only after the call returns, so a call that throws is not counted. A page counter changes when the
 * page begins, and a skip counter when the span is rejected. A timer also measures a call that throws.</li>
 * <li><b>Times are elapsed time.</b> Every ColumNAR time is elapsed (wall-clock) time in nanoseconds, except
 * {@code blocks.decode_cpu_time_nanos}, which is CPU time. Elapsed time includes waits for page faults, a cache or
 * remote storage. Each table row says whether a time is measured exactly or estimated: see
 * {@link org.elasticsearch.codec.profiler.TimingPolicy}.</li>
 * <li><b>Times nest.</b> A page call contains the chunk, block and dictionary work of the page. A block fetch
 * contains the inflation of the block. A dictionary lookup contains the uncompressed reads and block fetches it
 * triggers. Do not add times, and do not subtract one time from another. An estimated outer time can be lower than
 * an exact time inside it.</li>
 * <li><b>Times include the measurement cost.</b> A clock read costs tens of nanoseconds. That is a large part of a
 * short call, such as a dictionary lookup, so the times of short calls are overstated most. Compare times of the
 * same kind of call only.</li>
 * <li><b>Counts are not I/O.</b> The metrics count calls into the codec, not bytes read from a disk, a cache or an
 * object store. A byte counter adds the bytes of each operation, so a block or chunk read twice counts twice. The
 * stored bytes of the three stream kinds, {@code blocks.bytes_fetched}, {@code chunks.bytes_compressed} and
 * {@code uncompressed.bytes_read}, do not overlap.</li>
 * </ul>
 *
 * <h3>Readers</h3>
 *
 * <table>
 * <caption>Metrics for opening readers</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code readers.opened}</td><td>Readers opened, one for each call that asks for a column.</td>
 * <td>How many readers the work built. Each segment, field and caller builds its own.</td>
 * <td>It is not the number of columns of the index.</td></tr>
 * <tr><td>{@code readers.plain}, {@code readers.dictionary}, {@code readers.numeric}</td><td>Readers opened, by the
 * layout of the column. Each open counts in one of them, so their sum is {@code readers.opened}.</td>
 * <td>The layout the column was written with.</td>
 * <td>A DICTIONARY column with escaped values still inflates chunks.</td></tr>
 * <tr><td>{@code readers.open_time_nanos}</td><td>Elapsed time of building the reader. Exact.</td>
 * <td>The cost of opening.</td>
 * <td>It does not include the reads that follow.</td></tr>
 * </table>
 *
 * <h3>Chunks</h3>
 *
 * <p>A byte stream stores its bytes in chunks. A reader holds the last chunk it inflated.
 *
 * <table>
 * <caption>Metrics for chunks</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code chunks.decompressed}</td><td>Compressed chunks inflated.</td>
 * <td>The decompression the reads caused.</td>
 * <td>A chunk inflated twice counts twice.</td></tr>
 * <tr><td>{@code chunks.bytes_compressed}</td><td>Stored size of the inflated chunks.</td>
 * <td>The input of the decompressor.</td><td>It is not the bytes read from storage.</td></tr>
 * <tr><td>{@code chunks.bytes_inflated}</td><td>Size of the inflated chunks, including bytes the read did not
 * use.</td>
 * <td>Amplification: divide by the values returned. A full scan gives about the size of one value. A sparse read
 * gives about the size of one chunk.</td>
 * <td>It is not the bytes the query needed.</td></tr>
 * <tr><td>{@code chunks.cache_hits}</td><td>Times a read needed a chunk that the reader already held. Nothing is
 * inflated for a cache hit.</td>
 * <td>With {@code chunks.decompressed}, every chunk the reads needed.</td>
 * <td>It depends on the access pattern: a range read in one call counts fewer cache hits than the same values read
 * one by one.</td></tr>
 * <tr><td>{@code chunks.decompress_time_nanos}</td><td>Elapsed time of the decompressor calls. Exact.</td>
 * <td>The time spent inflating. The decompressor reads the stored bytes where they are, so a cold read on a mapped
 * file pays its page faults here.</td>
 * <td>It is not CPU time.</td></tr>
 * </table>
 *
 * <h3>Uncompressed streams</h3>
 *
 * <p>A stream stored uncompressed is read where it is, with no chunk to inflate. The dictionary of a string column is
 * always stored uncompressed.
 *
 * <table>
 * <caption>Metrics for uncompressed reads</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code uncompressed.reads}</td><td>Bulk reads, one for each contiguous range copied off the file. A read
 * served from the one chunk the reader holds is not counted.</td>
 * <td>How often the reads went to the file.</td>
 * <td>One read can serve one value or a range.</td></tr>
 * <tr><td>{@code uncompressed.bytes_read}</td><td>Bytes those reads copied off the file.</td>
 * <td>The traffic of the uncompressed streams.</td><td>It is not the bytes read from a device.</td></tr>
 * <tr><td>{@code uncompressed.read_time_nanos}</td><td>Elapsed time of the reads. Estimated.</td>
 * <td>The whole storage cost of the uncompressed streams, including page faults on a mapped file.</td>
 * <td>Page, byte length and dictionary times contain it.</td></tr>
 * </table>
 *
 * <h3>Blocks</h3>
 *
 * <p>A block holds values of an integer stream: the ordinals of a DICTIONARY column, the length codes of a PLAIN
 * column, the slot counts of a multivalued column, or the values of a numeric column. The block metrics count all
 * integer streams of a column together. A reader holds one block of each stream. Block compression and chunk
 * compression are separate: a byte is in a block or in a chunk, never in both.
 *
 * <table>
 * <caption>Metrics for blocks</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code blocks.fetched}</td><td>Blocks fetched because the reader did not hold them.</td>
 * <td>The block work of the reads. A DICTIONARY column that returns ordinals does its work here.</td>
 * <td>A block needed again after the reader moved on is fetched and counted again.</td></tr>
 * <tr><td>{@code blocks.bytes_fetched}</td><td>Stored size of the fetched blocks, compressed or not.</td>
 * <td>The input of the block codec.</td><td>It is not the bytes read from storage, or the size of the column.</td></tr>
 * <tr><td>{@code blocks.decompressed}, {@code blocks.bytes_inflated}</td><td>Fetched blocks that were stored
 * compressed, and their inflated size.</td>
 * <td>Compressed block work. The writer compresses blocks only when it gains space, so random ordinals often give
 * zero.</td>
 * <td>A block stored plain is not counted.</td></tr>
 * <tr><td>{@code blocks.fetch_time_nanos}</td><td>Elapsed time of a fetch, until the block is available.
 * Estimated.</td>
 * <td>The fetch cost, including inflation and waits for data.</td>
 * <td>It does not include the decode. It can be lower than {@code blocks.decompress_time_nanos}, which it
 * contains.</td></tr>
 * <tr><td>{@code blocks.decompress_time_nanos}</td><td>Elapsed time of block inflation. Exact.</td>
 * <td>The time spent inflating blocks.</td><td>It is part of {@code blocks.fetch_time_nanos}.</td></tr>
 * <tr><td>{@code blocks.decode_time_nanos}</td><td>Elapsed time of decoding a fetched block into values.
 * Estimated.</td>
 * <td>The decode cost. It follows the fetch and does not overlap it. A block stored plain is read off the file while
 * it is decoded, so a cold read on a mapped file pays its page faults here.</td>
 * <td>It is not CPU time.</td></tr>
 * <tr><td>{@code blocks.decode_cpu_time_nanos}</td><td>CPU time of the thread in the decode calls, measured with the
 * same calls as {@code blocks.decode_time_nanos}. Estimated. Present only when the collector measures thread CPU time
 * and the clock of the platform stays valid. The search and ES|QL profiles do not measure it.</td>
 * <td>Elapsed time minus CPU time approximates the off-CPU part of decoding, such as page faults, scheduling or
 * blocking.</td>
 * <td>CPU time includes memory stalls and can include kernel time. It is not an I/O time.</td></tr>
 * <tr><td>{@code blocks.values_decoded}</td><td>Integer values decoded: a full block for each fetch, of every
 * integer stream of the column.</td>
 * <td>Decode amplification, divided by the values returned, for a column of one integer stream: a numeric column or a
 * single-valued DICTIONARY column. The ratio is approximate.</td>
 * <td>It mixes streams for a PLAIN or multivalued column. It never counts value bytes.</td></tr>
 * </table>
 *
 * <h3>Pages</h3>
 *
 * <p>A consumer that loads many values of a string column at once asks for a page. The reader hands over a page as
 * ordinals into a dictionary of the page, or as the values.
 *
 * <table>
 * <caption>Metrics for pages</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code pages.as_ordinals}, {@code pages.as_values}</td><td>Pages handed over in each form, counted when the
 * page begins. An empty page is not counted.</td>
 * <td>The form the consumer received. Ordinals are the cheap form for repeated values.</td>
 * <td>They do not say what the consumer did with the page.</td></tr>
 * <tr><td>{@code pages.values_in_ordinal_pages}, {@code pages.values_in_value_pages}</td><td>The values those pages
 * carried.</td>
 * <td>The values returned: the divisor for amplification.</td>
 * <td>Values read one document at a time are not in pages.</td></tr>
 * <tr><td>{@code pages.call_time_nanos}</td><td>Elapsed time of every call that asks for a page, including calls that
 * decline the page and calls that throw. Exact.</td>
 * <td>The cost of the page calls. It contains the chunk, block and dictionary work of the page, the work that has no
 * metric, building the page, and the work of the consumer on the page.</td>
 * <td>It is not codec time only. Its call count can exceed the pages counted.</td></tr>
 * <tr><td>{@code pages.byte_lengths_call_time_nanos}</td><td>Elapsed time of every call that asks for the byte
 * lengths of a page. Exact.</td>
 * <td>The cost of reading lengths. These calls hand over no page.</td>
 * <td>Lengths read one document at a time are not covered.</td></tr>
 * </table>
 *
 * <h3>Dictionary lookups</h3>
 *
 * <p>A DICTIONARY column resolves a term when it reads a value one document at a time, when it bisects for a term, a
 * range or a prefix, and when it builds the terms of a page. A bisection resolves about log2 of the dictionary size
 * terms, and each lookup depends on the one before it.
 *
 * <table>
 * <caption>Metrics for dictionary lookups</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code dictionary.term_lookups}</td><td>Terms resolved, one for each lookup. An escaped value is not a term
 * and is not counted.</td>
 * <td>How often the reads went to the dictionary.</td>
 * <td>A term looked up twice counts twice.</td></tr>
 * <tr><td>{@code dictionary.resolve_time_nanos}</td><td>Elapsed time of the lookups. Estimated.</td>
 * <td>The cost of resolution, including the reads it triggers. A high time with few lookups on a cold read shows the
 * dependent reads of a bisection.</td>
 * <td>The uncompressed and page times overlap it.</td></tr>
 * </table>
 *
 * <h3>Ordinal skipping</h3>
 *
 * <p>A DICTIONARY column with an ordinal skip index can step over a span of documents whose ordinals cannot match a
 * term, a range or a prefix. A span runs from the current document to the end of the rejected interval, at the
 * highest level of the skip index that rejects it. The metrics are registered only for a column with an ordinal skip
 * index. A column with escaped values has none.
 *
 * <table>
 * <caption>Metrics for ordinal skipping</caption>
 * <tr><th>Metric</th><th>Definition</th><th>Use</th><th>Limitation</th></tr>
 * <tr><td>{@code ordinal_skip.intervals_rejected}</td><td>Spans stepped over because no ordinal in them can
 * match.</td>
 * <td>Whether the read skipped. Zero shows a read that could skip and did not.</td>
 * <td>It is not a number of documents.</td></tr>
 * <tr><td>{@code ordinal_skip.docid_span}</td><td>Document ids those spans cover, including ids with no value.</td>
 * <td>How much of the id range the skip index ruled out.</td>
 * <td>It is not the documents or the time saved.</td></tr>
 * <tr><td>{@code ordinal_skip.navigation_time_nanos}</td><td>Elapsed time of the calls that move past the current
 * interval of the skip index, including calls that reject nothing and the advances of the underlying iterator.
 * Estimated.</td>
 * <td>The cost of the navigation.</td>
 * <td>It is not a saving. The cost of the same read without skipping is not measured.</td></tr>
 * </table>
 *
 * <h3>Reading metrics together</h3>
 *
 * <ul>
 * <li><b>Amplification.</b> {@code chunks.bytes_inflated} divided by the values returned. About the width of one
 * value shows a full scan. Thousands of times that width shows a sparse read that inflates a chunk for few
 * values.</li>
 * <li><b>A good DICTIONARY read.</b> Pages as ordinals, block work, and no chunk work.</li>
 * <li><b>Rebuilt readers.</b> A {@code readers.opened} much larger than the expected number of readers, with
 * {@code chunks.decompressed} growing with it, shows readers that were rebuilt for each read.</li>
 * <li><b>Page time.</b> {@code chunks.decompress_time_nanos} as a share of {@code pages.call_time_nanos}. The rest of
 * the page time is work without a metric, such as finding the values of the documents and the work of the
 * consumer.</li>
 * <li><b>Skipping.</b> Rejected intervals and fewer {@code blocks.fetched} than the same query on a column that is
 * not clustered.</li>
 * </ul>
 *
 * <h3>Not covered</h3>
 *
 * <ul>
 * <li>Writes and merges.</li>
 * <li>Readers opened while no scope is bound. Which reads a frontend covers is documented by the frontend.</li>
 * <li>A page-level time for values read one document at a time. Their chunk, block, uncompressed and dictionary
 * operations are counted and timed individually.</li>
 * <li>The page, chunk and skip metrics of numeric columns. A numeric column reports reader and block metrics only.
 * It uses its skip index without metrics, so {@code skip_index} can be true with no metric that shows its use.</li>
 * <li>The column iterator, the slot addresses, the escape ranks and the skip index reads. Their cost is inside the
 * page or navigation time that contains them, or inside no time on the document-at-a-time path.</li>
 * </ul>
 *
 * <h3>Adding a metric</h3>
 *
 * <ul>
 * <li>Declare the metric definition as a constant next to the code that records it, with {@code CounterMetric.of} or
 * {@code TimerMetric.of}. Add {@code withCpuTime} for a timer that also measures thread CPU time.</li>
 * <li>Put the unit in the name, for example {@code _nanos}.</li>
 * <li>Ask the recorder for the handle when the component is built, never on the read path.</li>
 * <li>Measure in a decorator at a component boundary. Measure in place only when no boundary sees the event, and
 * use {@link org.elasticsearch.codec.profiler.Timer#INERT} when there is no recorder.</li>
 * <li>A collector accepts at most 64 metrics, and a CPU time companion counts as one.</li>
 * <li>Add a table row above. {@code ColumnarMetricCatalogueTests} fails when a declared metric has no row, and it pins
 * every definition against a golden file.</li>
 * </ul>
 */
package org.elasticsearch.columnar;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
