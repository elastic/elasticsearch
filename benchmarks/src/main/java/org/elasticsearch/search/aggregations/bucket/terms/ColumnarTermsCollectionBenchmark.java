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
import org.elasticsearch.search.aggregations.bucket.terms.MapStringTermsAggregator.BucketCollectConsumer;
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
import java.util.Collection;
import java.util.List;
import java.util.Random;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

/**
 * Collects a {@code terms} aggregation over a columnar keyword field in one segment, through a real Lucene search, so the
 * documents reach the collector as a query delivers them. {@code perDocument} is the aggregator's loop as it is today;
 * {@code ordinals} is {@link ColumnarOrdinalTermsCollector}, which hashes each term once for each segment. Setup checks
 * that both give the same counts. In the aggregator's package because the collector is not public. It leaves out the
 * rest of a search.
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

    /** Distinct values in the field. */
    @Param({ "5", "100", "1000", "5000" })
    private int distinctValues;

    /** How often each value is drawn: {@code skewed} has a few common values and a long tail, {@code uniform} has none. */
    @Param({ "skewed", "uniform" })
    private String distribution;

    @Param({ "1000000" })
    private int docCount;

    /** Percent of the documents the filter matches. */
    @Param({ "1", "5", "10", "30" })
    private int percent;

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
        // Say which reader serves the column, since only a dictionary has ordinals.
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
        // A cached result would skip the scorer being measured.
        searcher.setQueryCache(null);

        // Both have to agree before either is worth timing.
        try (Buckets expected = search(false); Buckets actual = search(true)) {
            final TreeMap<String, Long> want = expected.snapshot();
            final TreeMap<String, Long> got = actual.snapshot();
            if (want.equals(got) == false) {
                throw new IllegalStateException("ordinal and per-document counts differ: " + got.size() + " terms against " + want.size());
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
    public void ordinals(Blackhole bh) throws IOException {
        consume(bh, true);
    }

    private void consume(Blackhole bh, boolean ordinals) throws IOException {
        try (Buckets buckets = search(ordinals)) {
            bh.consume(buckets.ords.size());
        }
    }

    /** One search for the filter, counting into fresh buckets. */
    private Buckets search(boolean ordinals) throws IOException {
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
                        if (ordinals == false) {
                            return perDocument;
                        }
                        final LeafBucketCollector byOrdinal = ColumnarOrdinalTermsCollector.create(
                            values,
                            null,
                            LeafBucketCollector.NO_OP_COLLECTOR,
                            buckets,
                            buckets.ordinalBuckets,
                            bytes -> {},
                            perDocument
                        );
                        if (byOrdinal == null) {
                            throw new IllegalStateException("the column has no ordinals to read");
                        }
                        return byOrdinal;
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

    /** The aggregator's ordinary per-document collector for this field. */
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
                        buckets.accept(LeafBucketCollector.NO_OP_COLLECTOR, doc, 0, value);
                    }
                }
            }
        };
    }

    /** A document's value. */
    private BytesRef payload(Random random) {
        final int index = switch (distribution) {
            case "skewed" -> (int) (distinctValues * Math.pow(random.nextDouble(), 2.0));
            case "uniform" -> random.nextInt(distinctValues);
            default -> throw new IllegalStateException("unknown distribution [" + distribution + "]");
        };
        final BytesRef value = new BytesRef("value-" + index);
        return BytesRef.deepCopyOf(new StringBinaryPayload.Builder().encode(List.of(value)));
    }

    /** Where counted terms go: the hash and counts the aggregator keeps. */
    private final class Buckets implements BucketCollectConsumer, Releasable {
        final BytesKeyedBucketOrds ords = BytesKeyedBucketOrds.build(bigArrays, CardinalityUpperBound.ONE);
        LongArray counts = bigArrays.newLongArray(256, true);
        final ColumnarOrdinalTermsCollector.OrdinalBuckets ordinalBuckets = new ColumnarOrdinalTermsCollector.OrdinalBuckets();

        @Override
        public long accept(LeafBucketCollector sub, int doc, long owningBucketOrd, BytesRef term) {
            long ord = ords.add(owningBucketOrd, term);
            if (ord < 0) {
                ord = -1 - ord;
            } else {
                counts = bigArrays.grow(counts, ord + 1);
            }
            counts.increment(ord, 1);
            return ord;
        }

        @Override
        public void acceptExisting(LeafBucketCollector sub, int doc, long bucketOrdinal) {
            counts.increment(bucketOrdinal, 1);
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
