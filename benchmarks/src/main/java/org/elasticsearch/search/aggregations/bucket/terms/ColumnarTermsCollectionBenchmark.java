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
import org.elasticsearch.columnar.string.StringColumnReader;
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
 * that the documents reach the collector the way a query gives them: a share of them, in windows found in a bit set.
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
     * Distinct values in the field. 5 is a version string and 100 a service name. From about 5,000 a page repeats little
     * and a dictionary has less to save.
     */
    @Param({ "5", "100", "1000", "5000" })
    private int distinctValues;

    /**
     * How often each value is drawn. {@code skewed}: a few values are common and there is a long tail, with the most
     * common taking {@code 1 / sqrt(distinctValues)} of the draws. {@code uniform}: all alike, which is the worst case
     * for a page, since it repeats the least.
     */
    @Param({ "skewed", "uniform" })
    private String distribution;

    @Param({ "1000000" })
    private int docCount;

    /**
     * Values in a document. {@code single}: one. {@code multi2}: one to three, picked at random. A value is drawn from the
     * vocabulary each time, so a document can hold one more than once.
     */
    @Param({ "single", "multi2" })
    private String arity;

    /**
     * Percent of the documents the filter matches. A window of 4,096 documents then holds about forty to twelve hundred
     * matches, which is the size of the page the collector reads for each, so this is a sweep of page sizes. Only the
     * field for the share in use is indexed, which keeps setup short, so the index is built again for each share.
     */
    @Param({ "1", "5", "10", "30" })
    private int percent;

    /** Documents read as one page: the length of the array the collector copies a stream's doc IDs into. */
    @Param({ "4096" })
    private int pageDocs;

    /** Fewest documents a batch needs to be read as a page. Zero reads every batch as a page. */
    @Param({ "0" })
    private int minPageDocs;

    private final BigArrays bigArrays = new BigArrays(new PageCacheRecycler(Settings.EMPTY), null, "REQUEST");

    private Path path;
    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private final Query filter = new TermQuery(new Term("share", "yes"));

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
                doc.add(new StringField("share", random.nextInt(100) < percent ? "yes" : "no", Field.Store.NO));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        if (reader.leaves().size() != 1) {
            throw new IllegalStateException("expected one segment, found " + reader.leaves().size());
        }
        // Which reader serves the column decides what a page is made of, so say which it is.
        final StringColumnReader column = ColumnarPayloadSortableBinaryDocValues.from(reader.leaves().get(0).reader(), FIELD)
            .columnSource()
            .reader();
        System.out.println(
            "columnar reader "
                + column.getClass().getSimpleName()
                + ", dictionary size "
                + column.dictionarySize()
                + ", value addresses "
                + column.hasValueAddresses()
        );
        searcher = new IndexSearcher(reader);
        // A cached result would be collected without the scorer that produces the streams being measured.
        searcher.setQueryCache(null);

        // The two have to agree before either is worth timing.
        try (Buckets expected = search(false); Buckets actual = search(true)) {
            final TreeMap<String, Long> want = expected.snapshot();
            final TreeMap<String, Long> got = actual.snapshot();
            if (want.equals(got) == false) {
                throw new IllegalStateException("pages and per-document counts differ: " + got.size() + " terms against " + want.size());
            }
        }
    }

    @TearDown
    public void tearDown() throws IOException {
        IOUtils.close(reader, directory);
        IOUtils.rm(path);
    }

    @Benchmark
    public void perDocument(Blackhole bh) throws IOException {
        consume(bh, false);
    }

    @Benchmark
    public void pages(Blackhole bh) throws IOException {
        consume(bh, true);
    }

    private void consume(Blackhole bh, boolean pages) throws IOException {
        try (Buckets buckets = search(pages)) {
            bh.consume(buckets.ords.size());
        }
    }

    /** One search of the segment for the filter, counting what it matches into fresh buckets. */
    private Buckets search(boolean pages) throws IOException {
        final Buckets buckets = new Buckets();
        searcher.search(filter, new CollectorManager<Collector, Void>() {
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
                        if (pages == false) {
                            return perDocument;
                        }
                        return new ColumnarPageTermsCollector(values.columnSource().reader(), buckets, perDocument, minPageDocs);
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
        final int slots = switch (arity) {
            case "single" -> 1;
            case "multi2" -> 1 + random.nextInt(3);
            default -> throw new IllegalStateException("unknown arity [" + arity + "]");
        };
        final List<BytesRef> values = new ArrayList<>(slots);
        for (int s = 0; s < slots; s++) {
            final int index = switch (distribution) {
                case "skewed" -> (int) (distinctValues * Math.pow(random.nextDouble(), 2.0));
                case "uniform" -> random.nextInt(distinctValues);
                default -> throw new IllegalStateException("unknown distribution [" + distribution + "]");
            };
            values.add(new BytesRef("9." + index / 100 + "." + index % 100));
        }
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(values));
    }

    /** Where counted terms go: the same hash and counts array the aggregator keeps, for either way of counting. */
    private final class Buckets implements ColumnarPageTermsCollector.Target, Releasable {
        final BytesKeyedBucketOrds ords = BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
        LongArray counts = bigArrays.newLongArray(256, true);
        private int[] scratchDocs;
        private final ColumnarPageTermsCollector.SlotScratch slotScratch = new ColumnarPageTermsCollector.SlotScratch(PageBudget.UNLIMITED);

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

        @Override
        public ColumnarPageTermsCollector.SlotScratch slotScratch() {
            return slotScratch;
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
