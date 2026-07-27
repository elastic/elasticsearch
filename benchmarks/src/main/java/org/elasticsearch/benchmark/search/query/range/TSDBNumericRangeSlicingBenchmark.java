/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.range;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.index.codec.Elasticsearch93Lucene104Codec;
import org.elasticsearch.index.codec.tsdb.es819.ES819TSDBDocValuesFormat;
import org.elasticsearch.index.codec.tsdb.es95.ES95TSDBDocValuesFormat;
import org.elasticsearch.lucene.queries.SortedNumericDocValuesRangeQuery;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
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
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Measures a numeric range query under ESQL-style {@code DataPartitioning.DOC}: a single segment is
 * split into {@code numSlices} contiguous doc-id windows, and each window is scored by its own fresh
 * {@link org.apache.lucene.search.BulkScorer} (as separate ESQL drivers would).
 *
 * <p>The field is a single-valued numeric doc value with a RANGE skipper, filled with high-variance
 * (unsorted) data so skipper blocks straddle the query bounds and cannot be skipped. A plain
 * {@code DocIdSetIterator} matches eagerly in {@code advance(min)}, so a slice whose window has no
 * early match scans past the slice's max, and that work compounds with the number of slices. The
 * two-phase iterator bounds the per-slice cost to the slice window, so wall-clock should stay roughly
 * flat as {@code numSlices} grows. This benchmark guards against reintroducing that over-scan.
 */
@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@State(Scope.Benchmark)
@Fork(1)
@Threads(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
public class TSDBNumericRangeSlicingBenchmark {

    static {
        // Bootstrap ES logging so the ES95/ES819 SIMD range path can initialize native access.
        Utils.configureBenchmarkLogging();
    }

    private static final String FIELD = "value";
    private static final long VALUE_BOUND = 1_000_000_000L;

    @Param("1000000")
    private int numDocs;

    /** Number of DOC-partitioning slices the single segment is split into. */
    @Param({ "1", "64", "1024" })
    private int numSlices;

    /** TSDB doc-values codec under test; both route through the shared tryRangeIterator. */
    @Param({ "ES95", "ES819" })
    private String format;

    /** Fraction of docs the (mid-domain) range matches. 0.0 is the near-empty worst case for over-scan. */
    @Param({ "0.0", "0.001" })
    private double selectivity;

    @Param("42")
    private int seed;

    private Directory directory;
    private DirectoryReader reader;
    private Weight weight;
    private int maxDoc;

    @Setup(Level.Trial)
    public void setup() throws IOException {
        directory = FSDirectory.open(Files.createTempDirectory("tsdb-range-slicing-"));

        final DocValuesFormat dvFormat = switch (format) {
            case "ES95" -> new ES95TSDBDocValuesFormat();
            case "ES819" -> new ES819TSDBDocValuesFormat();
            default -> throw new IllegalArgumentException("unknown format [" + format + "]");
        };
        final Codec codec = new Elasticsearch93Lucene104Codec() {
            @Override
            public DocValuesFormat getDocValuesFormatForField(String field) {
                return dvFormat;
            }
        };
        // No index sort on FIELD: keeps data high-variance and avoids the primary-sort fast path,
        // so the query exercises tryRangeIterator.
        final IndexWriterConfig config = new IndexWriterConfig().setCodec(codec);

        final Random random = new Random(seed);
        try (IndexWriter writer = new IndexWriter(directory, config)) {
            for (int i = 0; i < numDocs; i++) {
                final Document doc = new Document();
                doc.add(SortedNumericDocValuesField.indexedField(FIELD, Math.floorMod(random.nextLong(), VALUE_BOUND)));
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        maxDoc = reader.maxDoc();

        // Mid-domain range so the global skipper min/max can't early-exit the query; with high-variance
        // data the per-block skipper still straddles it, forcing the over-scan path in the old iterator.
        final long lower = VALUE_BOUND / 2;
        final long upper = lower + (long) (VALUE_BOUND * selectivity);
        final SortedNumericDocValuesRangeQuery query = new SortedNumericDocValuesRangeQuery(FIELD, lower, upper);
        weight = new IndexSearcher(reader).createWeight(query, ScoreMode.COMPLETE_NO_SCORES, 1f);
    }

    /**
     * Scores the whole segment as {@code numSlices} independent windows, each via its own scorer,
     * mirroring ESQL DataPartitioning.DOC.
     */
    @Benchmark
    public void sliceAndScore(final Blackhole bh) throws IOException {
        final LeafReaderContext leaf = reader.leaves().getFirst();
        final CountingCollector collector = new CountingCollector();
        final int sliceSize = Math.max(1, (maxDoc + numSlices - 1) / numSlices);
        for (int min = 0; min < maxDoc; min += sliceSize) {
            final int max = Math.min(min + sliceSize, maxDoc);
            final ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier == null) {
                continue;
            }
            scorerSupplier.bulkScorer().score(collector, leaf.reader().getLiveDocs(), min, max);
        }
        bh.consume(collector.count);
    }

    @TearDown(Level.Trial)
    public void tearDown() throws IOException {
        reader.close();
        directory.close();
    }

    private static final class CountingCollector implements LeafCollector {
        private long count;

        @Override
        public void setScorer(Scorable scorer) {}

        @Override
        public void collect(int doc) {
            count++;
        }
    }
}
