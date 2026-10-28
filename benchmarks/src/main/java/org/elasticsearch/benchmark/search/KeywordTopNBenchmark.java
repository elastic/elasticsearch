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
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.index.fielddata.fieldcomparator.BytesRefFieldComparatorSource;
import org.elasticsearch.index.fielddata.plain.BinaryIndexFieldData;
import org.elasticsearch.index.fielddata.plain.MultiValuedBinaryDocValuesSortField;
import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
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
 * Measures sorted top-N search latency across three index modes and three document counts.
 *
 * <p>When the index sort matches the query sort prefix, Lucene's
 * {@code TopFieldCollector.canEarlyTerminateOnPrefix()} lets the collector stop once its heap is
 * full. The cost is O({@code size}) regardless of segment size, so latency stays flat as
 * {@code numDocs} grows. When the optimization does not fire the collector scans every document,
 * so latency grows roughly linearly with {@code numDocs}. The benchmark exercises all three index
 * modes to show which ones benefit.
 *
 * <p>Run all parameter combinations:
 * <pre>{@code
 * ./gradlew :benchmarks:run \
 *   --args 'KeywordTopNBenchmark -f 3 -wi 5 -i 5 \
 *     -rf json -rff /tmp/keyword-topn.json \
 *     -o /tmp/keyword-topn.txt'
 * }</pre>
 *
 * <p>Quick smoke test (single mode, small doc count):
 * <pre>{@code
 * ./gradlew :benchmarks:run \
 *   --args 'KeywordTopNBenchmark -f 1 -wi 1 -i 1 \
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
public class KeywordTopNBenchmark {

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

    @Setup
    public void setup() throws IOException {
        path = Files.createTempDirectory("keyword-topn-bench-");
        directory = MMapDirectory.open(path);
        if ("logsdb".equals(indexMode)) {
            setupLogsdb();
        } else {
            setupColumnar();
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);
    }

    @Benchmark
    public TopDocs topN(Blackhole bh) throws IOException {
        // NOTE: totalHitsThreshold=size lets the collector switch to TOP_DOCS mode once the heap
        // is full, which is the precondition for canEarlyTerminateOnPrefix() to fire.
        final TopDocs docs = searcher.search(MatchAllDocsQuery.INSTANCE, new TopFieldCollectorManager(querySort, size, null, size));
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

    private void setupLogsdb() throws IOException {
        final SortedSetSortField hostIndexSort = new SortedSetSortField(HOST_NAME, false);
        indexSort = new Sort(hostIndexSort, timestampIndexSort());
        querySort = new Sort(new SortedSetSortField(HOST_NAME, false));
        buildIndex(doc -> doc.add(new SortedSetDocValuesField(HOST_NAME, hostValue())));
    }

    private void setupColumnar() throws IOException {
        final MultiValuedBinaryDocValuesSortField hostIndexSort = new MultiValuedBinaryDocValuesSortField(
            HOST_NAME,
            false,
            SortField.STRING_LAST,
            false
        );
        indexSort = new Sort(hostIndexSort, timestampIndexSort());
        final BinaryIndexFieldData fieldData = new BinaryIndexFieldData(HOST_NAME, CoreValuesSourceType.KEYWORD);
        final IndexFieldData.XFieldComparatorSource comparatorSource = new BytesRefFieldComparatorSource(
            fieldData,
            "_last",
            MultiValueMode.MIN,
            null
        );
        querySort = new Sort(new SortField(HOST_NAME, comparatorSource, false));
        buildIndex(doc -> doc.add(new BinaryDocValuesField(HOST_NAME, hostValue())));
    }

    private void buildIndex(DocConsumer addHostField) throws IOException {
        final IndexWriterConfig config = new IndexWriterConfig().setIndexSort(indexSort).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                addHostField.accept(doc);
                doc.add(new SortedNumericDocValuesField(TIMESTAMP, (long) i * 1_000L));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
    }

    private BytesRef hostValue() {
        return new BytesRef(String.format(Locale.ROOT, "host-%06d", randomHostOrdinal()));
    }

    private int docCounter = 0;

    private int randomHostOrdinal() {
        return (docCounter++) % CARDINALITY;
    }

    private static SortedNumericSortField timestampIndexSort() {
        return new SortedNumericSortField(TIMESTAMP, SortField.Type.LONG, true);
    }

    @FunctionalInterface
    private interface DocConsumer {
        void accept(Document doc) throws IOException;
    }
}
