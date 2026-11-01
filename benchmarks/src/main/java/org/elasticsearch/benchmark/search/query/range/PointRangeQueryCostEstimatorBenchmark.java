/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.search.query.range;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PointRangeQuery;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.store.ByteBuffersDirectory;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.DocIdSetBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.lucene.search.cost.PointRangeQueryCostEstimator;
import org.openjdk.jmh.annotations.AuxCounters;
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
import org.openjdk.jmh.annotations.Warmup;

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Compares the per-leaf circuit-breaker estimate (structural {@link RamUsageEstimator} size plus
 * {@link PointRangeQueryCostEstimator#executionBytesForLeaf} summed over the searched leaves, fed the
 * real {@link ScorerSupplier#cost()} and leaf {@code maxDoc}) against the exact retained RAM of a real
 * {@link PointRangeQuery} run over a real index. The measured baseline is the query's structural size
 * plus the actual per-segment {@link DocIdSet} it materialises (rebuilt with the same
 * {@link DocIdSetBuilder}/{@link PointValues.IntersectVisitor} Lucene uses, so it reflects the real
 * dense/sparse allocation).
 * <p>
 * The estimate should over-estimate without much slack: ratio just above {@code 1.0} for dense
 * queries, and — unlike the previous worst-case-per-segment model — close to the measured value for
 * selective ranges rather than orders of magnitude above it.
 * <p>
 * The {@link Metrics} aux counters are JMH {@code EVENTS}, scaled by the iteration count, so divide
 * each by {@code Cnt} to recover absolute bytes.
 */
@Fork(1)
@Warmup(iterations = 3)
@Measurement(iterations = 5)
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@BenchmarkMode(Mode.AverageTime)
@SuppressWarnings("unused") // invoked by JMH
public class PointRangeQueryCostEstimatorBenchmark {

    static final String FIELD = "f";

    /**
     * Real Lucene {@link PointRangeQuery} shapes so the measured side reflects an actual query object
     * and execution. {@code numDims}/{@code bytesPerDim} are intrinsic to each type. Each type knows
     * how to index a document's point and how to build a range query matching {@code [0, upper]} in
     * every dimension.
     */
    public enum PointType {
        INT_1D(1, Integer.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new IntPoint(FIELD, (int) docId));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) IntPoint.newRangeQuery(FIELD, 0, (int) upper);
            }
        },
        LONG_1D(1, Long.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new LongPoint(FIELD, docId));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) LongPoint.newRangeQuery(FIELD, 0L, upper);
            }
        },
        DOUBLE_1D(1, Double.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new DoublePoint(FIELD, (double) docId));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) DoublePoint.newRangeQuery(FIELD, 0.0, (double) upper);
            }
        },
        INT_2D(2, Integer.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new IntPoint(FIELD, filled(2, (int) docId)));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) IntPoint.newRangeQuery(FIELD, filled(2, 0), filled(2, (int) upper));
            }
        },
        INT_4D(4, Integer.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new IntPoint(FIELD, filled(4, (int) docId)));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) IntPoint.newRangeQuery(FIELD, filled(4, 0), filled(4, (int) upper));
            }
        },
        INT_8D(8, Integer.BYTES) {
            @Override
            void addPoint(Document doc, long docId) {
                doc.add(new IntPoint(FIELD, filled(8, (int) docId)));
            }

            @Override
            PointRangeQuery rangeQuery(long upper) {
                return (PointRangeQuery) IntPoint.newRangeQuery(FIELD, filled(8, 0), filled(8, (int) upper));
            }
        };

        private final int numDims;
        private final int bytesPerDim;

        PointType(int numDims, int bytesPerDim) {
            this.numDims = numDims;
            this.bytesPerDim = bytesPerDim;
        }

        abstract void addPoint(Document doc, long docId);

        abstract PointRangeQuery rangeQuery(long upper);

        private static int[] filled(int dims, int value) {
            int[] point = new int[dims];
            Arrays.fill(point, value);
            return point;
        }
    }

    @Param({ "INT_1D", "LONG_1D", "DOUBLE_1D", "INT_2D", "INT_4D", "INT_8D" })
    public PointType pointType;

    @Param({ "1000000" })
    public int nDocs;

    /** Fraction of indexed documents the range query matches; drives sparse-vs-dense doc-set behaviour. */
    @Param({ "1.0", "0.1" })
    public double matchFraction;

    private Directory directory;
    private DirectoryReader reader;
    private IndexSearcher searcher;
    private PointRangeQuery query;
    private long precomputedEstimate;
    private long precomputedMeasured;
    private double precomputedRatio;

    @AuxCounters(AuxCounters.Type.EVENTS)
    @State(Scope.Thread)
    public static class Metrics {
        public double estimatedBytes;
        public double measuredBytes;
        public double estimateOverMeasuredRatio;
    }

    @Setup(Level.Trial)
    public void setupTrial() throws IOException {
        directory = new ByteBuffersDirectory();
        try (IndexWriter writer = new IndexWriter(directory, new IndexWriterConfig(null))) {
            for (long docId = 0; docId < nDocs; docId++) {
                Document doc = new Document();
                pointType.addPoint(doc, docId);
                writer.addDocument(doc);
            }
            writer.forceMerge(1);
        }
        reader = DirectoryReader.open(directory);
        searcher = new IndexSearcher(reader);

        long upper = Math.max(0L, (long) (nDocs * matchFraction) - 1L);
        query = pointType.rangeQuery(upper);

        precomputedEstimate = estimateRetainedRam(searcher, reader, query);
        precomputedMeasured = measureRetainedRam(reader, query);
        precomputedRatio = precomputedMeasured == 0 ? 0.0 : (double) precomputedEstimate / (double) precomputedMeasured;
    }

    @TearDown(Level.Trial)
    public void tearDownTrial() throws IOException {
        if (reader != null) {
            reader.close();
        }
        if (directory != null) {
            directory.close();
        }
    }

    @Benchmark
    public long estimate(Metrics metrics) throws IOException {
        publish(metrics);
        return estimateRetainedRam(searcher, reader, query);
    }

    @Benchmark
    public long measure(Metrics metrics) throws IOException {
        publish(metrics);
        return measureRetainedRam(reader, query);
    }

    private void publish(Metrics metrics) {
        metrics.estimatedBytes = precomputedEstimate;
        metrics.measuredBytes = precomputedMeasured;
        metrics.estimateOverMeasuredRatio = precomputedRatio;
    }

    /**
     * The breaker estimate: the query's structural RAM plus the per-leaf execution ceiling from
     * {@link PointRangeQueryCostEstimator#executionBytesForLeaf}, fed the real {@link ScorerSupplier#cost()}
     * and leaf {@code maxDoc} — exactly what {@code ContextIndexSearcher} charges per leaf.
     */
    private static long estimateRetainedRam(IndexSearcher searcher, DirectoryReader reader, PointRangeQuery query) throws IOException {
        long total = measureStructuralRam(query);
        Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.COMPLETE_NO_SCORES, 1.0f);
        for (LeafReaderContext leaf : reader.leaves()) {
            PointValues values = leaf.reader().getPointValues(query.getField());
            if (values == null) {
                continue;
            }
            ScorerSupplier scorerSupplier = weight.scorerSupplier(leaf);
            if (scorerSupplier == null) {
                continue;
            }
            boolean singleValued = values.size() == values.getDocCount();
            boolean matchAll = coversAllValues(values, query);
            total += PointRangeQueryCostEstimator.executionBytesForLeaf(
                scorerSupplier.cost(),
                leaf.reader().maxDoc(),
                singleValued,
                matchAll,
                query.getNumDims(),
                query.getBytesPerDim()
            );
        }
        return total;
    }

    /** Whether the query range covers the leaf's entire indexed value range (Lucene's match-all path). */
    private static boolean coversAllValues(PointValues values, PointRangeQuery query) throws IOException {
        byte[] minPackedValue = values.getMinPackedValue();
        byte[] maxPackedValue = values.getMaxPackedValue();
        byte[] lower = query.getLowerPoint();
        byte[] upper = query.getUpperPoint();
        int numDims = query.getNumDims();
        int bytesPerDim = query.getBytesPerDim();
        for (int dim = 0; dim < numDims; dim++) {
            int offset = dim * bytesPerDim;
            int to = offset + bytesPerDim;
            if (Arrays.compareUnsigned(lower, offset, to, minPackedValue, offset, to) > 0
                || Arrays.compareUnsigned(upper, offset, to, maxPackedValue, offset, to) < 0) {
                return false;
            }
        }
        return true;
    }

    /** Structural RAM of the query object plus the actual per-segment doc sets it materialises. */
    private static long measureRetainedRam(DirectoryReader reader, PointRangeQuery query) throws IOException {
        return measureStructuralRam(query) + measureExecutionRam(reader, query);
    }

    private static long measureStructuralRam(PointRangeQuery query) {
        return RamUsageEstimator.shallowSizeOf(query) + RamUsageEstimator.sizeOf(query.getLowerPoint()) + RamUsageEstimator.sizeOf(
            query.getUpperPoint()
        );
    }

    /**
     * Rebuilds, per segment, the exact {@link DocIdSet} a {@link PointRangeQuery} produces and sums its
     * {@link DocIdSet#ramBytesUsed()}. Uses the same {@link DocIdSetBuilder} and range-matching
     * {@link PointValues.IntersectVisitor} as Lucene's internal scorer, so the measured bytes reflect
     * the real {@code FixedBitSet} (dense) or sparse-buffer (selective) allocation.
     */
    private static long measureExecutionRam(DirectoryReader reader, PointRangeQuery query) throws IOException {
        long total = 0L;
        for (LeafReaderContext leaf : reader.leaves()) {
            PointValues values = leaf.reader().getPointValues(query.getField());
            if (values == null) {
                continue;
            }
            DocIdSetBuilder builder = new DocIdSetBuilder(leaf.reader().maxDoc(), values);
            values.intersect(rangeVisitor(builder, query));
            DocIdSet docIdSet = builder.build();
            total += docIdSet.ramBytesUsed();
        }
        return total;
    }

    /**
     * The range-matching visitor used by {@link org.apache.lucene.search.PointRangeQuery}: a cell is
     * inside/outside/crossing the query box based on per-dimension unsigned comparison of the packed
     * min/max values against the query's lower/upper points, and each candidate doc is kept only if
     * every dimension falls within {@code [lower, upper]}.
     */
    private static PointValues.IntersectVisitor rangeVisitor(DocIdSetBuilder builder, PointRangeQuery query) {
        byte[] lower = query.getLowerPoint();
        byte[] upper = query.getUpperPoint();
        int numDims = query.getNumDims();
        int bytesPerDim = query.getBytesPerDim();
        return new PointValues.IntersectVisitor() {
            DocIdSetBuilder.BulkAdder adder;

            @Override
            public void grow(int count) {
                adder = builder.grow(count);
            }

            @Override
            public void visit(int docID) {
                adder.add(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                for (int dim = 0; dim < numDims; dim++) {
                    int offset = dim * bytesPerDim;
                    if (Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, lower, offset, offset + bytesPerDim) < 0
                        || Arrays.compareUnsigned(packedValue, offset, offset + bytesPerDim, upper, offset, offset + bytesPerDim) > 0) {
                        return;
                    }
                }
                adder.add(docID);
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                boolean crosses = false;
                for (int dim = 0; dim < numDims; dim++) {
                    int offset = dim * bytesPerDim;
                    if (Arrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, lower, offset, offset + bytesPerDim) < 0
                        || Arrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, upper, offset, offset + bytesPerDim) > 0) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }
                    crosses |= Arrays.compareUnsigned(minPackedValue, offset, offset + bytesPerDim, lower, offset, offset + bytesPerDim) < 0
                        || Arrays.compareUnsigned(maxPackedValue, offset, offset + bytesPerDim, upper, offset, offset + bytesPerDim) > 0;
                }
                return crosses ? PointValues.Relation.CELL_CROSSES_QUERY : PointValues.Relation.CELL_INSIDE_QUERY;
            }
        };
    }
}
