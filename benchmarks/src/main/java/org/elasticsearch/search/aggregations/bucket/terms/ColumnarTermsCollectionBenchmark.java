/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.aggregations.bucket.terms;

import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.benchmark.internal.BenchmarkLogging;
import org.elasticsearch.columnar.ColumNARDocValuesFormat;
import org.elasticsearch.columnar.ColumnarFieldType;
import org.elasticsearch.columnar.string.PageBudget;
import org.elasticsearch.columnar.string.StringBinaryPayload;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.index.codec.Elasticsearch96Codec;
import org.elasticsearch.index.fielddata.ColumnarPayloadSortableBinaryDocValues;
import org.elasticsearch.index.fielddata.ValueDeduplicator;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
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
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Collecting a {@code terms} aggregation over a columnar keyword field, one segment, through a real Lucene search so
 * that the documents reach the collector the way a query gives them: all of them as one run, or a share of them in
 * windows found in a bit set.
 *
 * <p>The {@code perDocument} benchmarks hand the aggregator's loop for this field to Lucene as it is today, which
 * decodes every document's value, copies its bytes and hashes them. The {@code pages} benchmarks wrap the same loop in
 * {@link ColumnarPageTermsCollector}, which reads the documents of each stream a page at a time and hashes each
 * distinct value of a page once. The query, and the cost of Lucene finding the matches, are the same for both. Setup
 * checks that they end with the same counts, because a faster way of counting something else is the easiest mistake to
 * make here.
 *
 * <p>Lives in the aggregator's package because the collector is not public. This leaves out what a whole search adds
 * around the collector: the doc count lookup, the circuit breaker checks, and building the result.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 2, time = 3)
@Measurement(iterations = 3, time = 3)
public class ColumnarTermsCollectionBenchmark {

    static {
        BenchmarkLogging.configure();
    }

    private static final String FIELD = "kw";
    /**
     * Shares of the documents a filter matches. A window of 4,096 documents then holds about forty to twelve hundred
     * matches, which is the size of the page the collector reads for each, so this is a sweep of page sizes.
     */
    private static final int[] PERCENTS = { 1, 2, 5, 10, 20, 30 };

    /**
     * Distinct values in the field. 5 is a version string. The larger one is where a page repeats little and a
     * dictionary has less to save.
     */
    @Param({ "5", "100" })
    private int distinctValues;

    @Param({ "2000000" })
    private int docCount;

    /** {@code single}: one value a document. {@code multi}: one in ten holds two or three, some repeated. */
    @Param({ "single", "multi" })
    private String arity;

    /** Documents read as one page: the length of the array the collector copies a stream's doc IDs into. */
    @Param({ "4096" })
    private int pageDocs;

    private final BigArrays bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), null, "REQUEST");

    private Path path;
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private final Query all = new MatchAllDocsQuery();
    /** One filter for each of {@link #PERCENTS}, matching that share of the documents. */
    private final Query[] filters = new Query[PERCENTS.length];

    @Setup
    public void setup() throws IOException {
        path = Files.createTempDirectory("columnar-terms-collection");
        directory = new MMapDirectory(path);
        final Random random = new Random(7);
        final FieldType type = new FieldType();
        type.setDocValuesType(DocValuesType.BINARY);
        type.freeze();
        final DocValuesFormat columnar = new ColumNARDocValuesFormat(field -> ColumnarFieldType.STRING);
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(new Elasticsearch96Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return columnar;
            }
        }).setUseCompoundFile(false);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < docCount; i++) {
                final Document doc = new Document();
                doc.add(new Field(FIELD, payload(random), type));
                for (int percent : PERCENTS) {
                    doc.add(new StringField("share" + percent, random.nextInt(100) < percent ? "yes" : "no", Field.Store.NO));
                }
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != 1) {
            throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
        }
        for (int i = 0; i < PERCENTS.length; i++) {
            filters[i] = new TermQuery(new Term("share" + PERCENTS[i], "yes"));
        }
        searcher = new IndexSearcher(reader);
        // A cached result would be collected without the scorer that produces the streams being measured.
        searcher.setQueryCache(null);

        // The two have to agree, for every query, before either is worth timing.
        final List<Query> queries = new ArrayList<>(List.of(all));
        queries.addAll(List.of(filters));
        for (Query query : queries) {
            try (Buckets expected = search(query, false); Buckets actual = search(query, true)) {
                final TreeMap<String, Long> want = expected.snapshot();
                final TreeMap<String, Long> got = actual.snapshot();
                if (want.equals(got) == false) {
                    throw new IllegalStateException(
                        "pages and per-document counts differ for " + query + ": " + got.size() + " terms against " + want.size()
                    );
                }
            }
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        IOUtils.close(reader, directory);
        IOUtils.rm(path);
    }

    @Benchmark
    public void perDocumentAll(Blackhole bh) throws IOException {
        consume(bh, all, false);
    }

    @Benchmark
    public void pagesAll(Blackhole bh) throws IOException {
        consume(bh, all, true);
    }

    @Benchmark
    public void perDocument1(Blackhole bh) throws IOException {
        consume(bh, filters[0], false);
    }

    @Benchmark
    public void pages1(Blackhole bh) throws IOException {
        consume(bh, filters[0], true);
    }

    @Benchmark
    public void perDocument2(Blackhole bh) throws IOException {
        consume(bh, filters[1], false);
    }

    @Benchmark
    public void pages2(Blackhole bh) throws IOException {
        consume(bh, filters[1], true);
    }

    @Benchmark
    public void perDocument5(Blackhole bh) throws IOException {
        consume(bh, filters[2], false);
    }

    @Benchmark
    public void pages5(Blackhole bh) throws IOException {
        consume(bh, filters[2], true);
    }

    @Benchmark
    public void perDocument10(Blackhole bh) throws IOException {
        consume(bh, filters[3], false);
    }

    @Benchmark
    public void pages10(Blackhole bh) throws IOException {
        consume(bh, filters[3], true);
    }

    @Benchmark
    public void perDocument20(Blackhole bh) throws IOException {
        consume(bh, filters[4], false);
    }

    @Benchmark
    public void pages20(Blackhole bh) throws IOException {
        consume(bh, filters[4], true);
    }

    @Benchmark
    public void perDocument30(Blackhole bh) throws IOException {
        consume(bh, filters[5], false);
    }

    @Benchmark
    public void pages30(Blackhole bh) throws IOException {
        consume(bh, filters[5], true);
    }

    private void consume(Blackhole bh, Query query, boolean pages) throws IOException {
        try (Buckets buckets = search(query, pages)) {
            bh.consume(buckets.ords.size());
        }
    }

    /** One search of the segment, counting what it matches into fresh buckets. */
    private Buckets search(Query query, boolean pages) throws IOException {
        final Buckets buckets = new Buckets();
        searcher.search(query, new CollectorManager<Collector, Void>() {
            @Override
            public Collector newCollector() {
                return new Collector() {
                    @Override
                    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
                        final ColumnarPayloadSortableBinaryDocValues values = ColumnarPayloadSortableBinaryDocValues.from(
                            context.reader(),
                            FIELD
                        );
                        final LeafBucketCollector perDocument = perDocumentCollector(values, buckets);
                        return pages ? new ColumnarPageTermsCollector(values.columnSource().reader(), buckets, perDocument) : perDocument;
                    }

                    @Override
                    public ScoreMode scoreMode() {
                        return ScoreMode.COMPLETE_NO_SCORES;
                    }
                };
            }

            @Override
            public Void reduce(Collection<Collector> collectors) {
                return null;
            }
        });
        return buckets;
    }

    /** What the aggregator's ordinary collector does for one document of this field, without what surrounds it. */
    private static LeafBucketCollector perDocumentCollector(ColumnarPayloadSortableBinaryDocValues values, Buckets buckets) {
        final ValueDeduplicator duplicates = new ValueDeduplicator(values);
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                if (values.advanceExact(doc) == false) {
                    return;
                }
                final int valueCount = values.docValueCount();
                duplicates.reset(valueCount);
                for (int i = 0; i < valueCount; i++) {
                    final BytesRef value = values.nextValue();
                    if (duplicates.seen(value) == false) {
                        buckets.count(value, 1);
                    }
                }
            }
        };
    }

    /** A document's value or values, skewed towards the first few of the vocabulary as a real field is. */
    private BytesRef payload(Random random) {
        final int slots = arity.equals("multi") && random.nextInt(10) == 0 ? 2 + random.nextInt(2) : 1;
        final List<BytesRef> values = new ArrayList<>(slots);
        for (int s = 0; s < slots; s++) {
            // A repeat of the previous slot now and then, which a document holding a value twice does.
            if (s > 0 && random.nextInt(4) == 0) {
                values.add(values.get(s - 1));
            } else {
                final int index = (int) (distinctValues * Math.pow(random.nextDouble(), 2.0));
                values.add(new BytesRef("9." + index / 100 + "." + index % 100));
            }
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(values));
    }

    /** Where counted terms go: the same hash and counts array the aggregator keeps, for either way of counting. */
    private final class Buckets implements ColumnarPageTermsCollector.Target, Releasable {
        final BytesKeyedBucketOrds ords = BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
        LongArray counts = bigArrays.newLongArray(256, true);
        private int[] scratchDocs;

        @Override
        public void count(BytesRef term, long docCount) {
            long ord = ords.add(0, term);
            if (ord < 0) {
                ord = -1 - ord;
            } else {
                counts = bigArrays.grow(counts, ord + 1);
            }
            counts.increment(ord, docCount);
        }

        @Override
        public PageBudget budget() {
            return PageBudget.UNLIMITED;
        }

        @Override
        public int[] scratchDocs() {
            if (scratchDocs == null) {
                scratchDocs = new int[pageDocs];
            }
            return scratchDocs;
        }

        TreeMap<String, Long> snapshot() {
            final TreeMap<String, Long> snapshot = new TreeMap<>();
            final BytesKeyedBucketOrds.BucketOrdsEnum all = ords.ordsEnum(0);
            final BytesRef scratch = new BytesRef();
            while (all.next()) {
                all.readValue(scratch);
                snapshot.put(scratch.utf8ToString(), counts.get(all.ord()));
            }
            return snapshot;
        }

        @Override
        public void close() {
            Releasables.close(ords, counts);
        }
    }
}
