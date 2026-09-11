/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.index.fielddata.plain.MultiValuedBinaryDocValuesSortField;
import org.elasticsearch.lucene.queries.SearchAfterSortedDocQuery;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

/**
 * Measures sorted {@code search_after} page latency across three index modes and three document
 * counts.
 *
 * <p>When the index sort matches the query sort, Elasticsearch's {@code QueryPhase} injects a
 * {@code SearchAfterSortedDocQuery} filter that binary-searches each sorted segment to the
 * continuation position. The cost per page is O(log({@code numDocs}) + {@code size}), so latency
 * stays flat as {@code numDocs} grows. When the gate does not fire the fallback scans all
 * documents from the beginning on every page, costing O({@code numDocs}). The benchmark exercises
 * all three index modes to show which ones benefit.
 *
 * <p>Run all parameter combinations:
 * <pre>{@code
 * ./gradlew :benchmarks:run \
 *   --args 'KeywordSearchAfterBenchmark -f 3 -wi 5 -i 5 \
 *     -rf json -rff /tmp/keyword-searchafter.json \
 *     -o /tmp/keyword-searchafter.txt'
 * }</pre>
 *
 * <p>Quick smoke test (single mode, small doc count):
 * <pre>{@code
 * ./gradlew :benchmarks:run \
 *   --args 'KeywordSearchAfterBenchmark -f 1 -wi 1 -i 1 \
 *     -p indexMode=logsdb,logsdb_columnar \
 *     -p numDocs=10000 -p size=10'
 * }</pre>
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Benchmark)
@Fork(3)
@Warmup(iterations = 5, time = 1)
@Measurement(iterations = 5, time = 1)
public class KeywordSearchAfterBenchmark {

    private static final String HOST_NAME = "host.name";
    private static final String TIMESTAMP = "@timestamp";
    private static final int CARDINALITY = 100_000;

    @Param({ "logsdb", "logsdb_columnar", "columnar" })
    private String indexMode;

    @Param({ "10000", "100000", "1000000" })
    private int numDocs;

    @Param({ "10", "100", "1000" })
    private int size;

    private Path path;
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private Sort indexSort;
    private Sort querySort;
    private FieldDoc afterDoc;

    @Setup
    public void setup() throws IOException {
        path = Files.createTempDirectory("keyword-searchafter-bench-");
        directory = MMapDirectory.open(path);
        if ("logsdb".equals(indexMode)) {
            setupLogsdb();
        } else {
            setupColumnar();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        // NOTE: midpoint positioning ensures the skip distance scales with numDocs.
        final int midHost = Math.min(CARDINALITY, numDocs) / 2;
        afterDoc = new FieldDoc(Integer.MAX_VALUE, 0.0f, new Object[] { new BytesRef(String.format(Locale.ROOT, "host-%06d", midHost)) });
    }

    @Benchmark
    public TopDocs searchAfterPage(Blackhole bh) throws IOException {
        final TopDocs docs = fetchPage();
        bh.consume(docs);
        return docs;
    }

    @TearDown
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
        try (Stream<Path> files = Files.walk(path).sorted(Comparator.reverseOrder())) {
            files.forEach(p -> p.toFile().delete());
        }
    }

    private TopDocs fetchPage() throws IOException {
        final Sort segmentSort = reader.leaves().isEmpty() ? null : reader.leaves().get(0).reader().getMetaData().sort();
        if (segmentSort != null && Lucene.canEarlyTerminate(querySort, segmentSort)) {
            final BooleanQuery query = new BooleanQuery.Builder().add(MatchAllDocsQuery.INSTANCE, BooleanClause.Occur.MUST)
                .add(new SearchAfterSortedDocQuery(querySort, afterDoc), BooleanClause.Occur.FILTER)
                .build();
            return searcher.search(query, size, querySort);
        }
        return searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(querySort, size, afterDoc, Integer.MAX_VALUE));
    }

    private void setupLogsdb() throws IOException {
        indexSort = new Sort(new SortedSetSortField(HOST_NAME, false), timestampIndexSort());
        querySort = new Sort(new SortedSetSortField(HOST_NAME, false));
        buildIndex((doc, i) -> doc.add(new SortedSetDocValuesField(HOST_NAME, hostValue(i))));
    }

    private void setupColumnar() throws IOException {
        final MultiValuedBinaryDocValuesSortField hostIndexSort = new MultiValuedBinaryDocValuesSortField(
            HOST_NAME,
            false,
            SortField.STRING_LAST,
            false
        );
        indexSort = new Sort(hostIndexSort, timestampIndexSort());
        querySort = new Sort(new MultiValuedBinaryDocValuesSortField(HOST_NAME, false, SortField.STRING_LAST, false));
        buildIndex((doc, i) -> doc.add(new BinaryDocValuesField(HOST_NAME, hostValue(i))));
    }

    private void buildIndex(DocConsumer addHostField) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setIndexSort(indexSort).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                addHostField.accept(doc, i);
                doc.add(new SortedNumericDocValuesField(TIMESTAMP, (long) i * 1_000L));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private static BytesRef hostValue(int index) {
        return new BytesRef(String.format(Locale.ROOT, "host-%06d", index % CARDINALITY));
    }

    private static SortedNumericSortField timestampIndexSort() {
        return new SortedNumericSortField(TIMESTAMP, SortField.Type.LONG, true);
    }

    @FunctionalInterface
    private interface DocConsumer {
        void accept(Document doc, int index) throws IOException;
    }
}
