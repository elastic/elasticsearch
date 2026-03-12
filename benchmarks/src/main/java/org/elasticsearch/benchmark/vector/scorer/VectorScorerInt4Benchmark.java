/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.vector.scorer;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.benchmark.Utils;
import org.elasticsearch.simdvec.VectorSimilarityType;
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

import java.io.IOException;
import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.int4QuantizedVectorValues;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104ScoreSupplier;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.lucene104Scorer;
import static org.elasticsearch.benchmark.vector.scorer.BenchmarkUtils.supportsHeapSegments;
import static org.elasticsearch.benchmark.vector.scorer.ScalarOperations.dotProductI4SinglePacked;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.packNibbles;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.randomInt4Bytes;
import static org.elasticsearch.simdvec.internal.vectorization.VectorScorerTestUtils.unpackNibbles;

/**
 * Benchmark that compares int4 packed-nibble quantized vector similarity scoring:
 * scalar vs Lucene's Lucene104ScalarQuantizedVectorScorer.
 * Run with ./gradlew -p benchmarks run --args 'VectorScorerInt4Benchmark'
 */
@Fork(value = 1, jvmArgsPrepend = { "--add-modules=jdk.incubator.vector" })
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 3)
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@State(Scope.Thread)
public class VectorScorerInt4Benchmark {

    static {
        Utils.configureBenchmarkLogging();
    }

    private static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    @Param({ "96", "768", "1024" })
    public int dims;
    public static int numVectors = 2;

    @Param({ "SCALAR", "LUCENE" })
    public VectorImplementation implementation;

    @Param({ "DOT_PRODUCT", "EUCLIDEAN" })
    public VectorSimilarityType function;

    private static class ScalarDotProduct implements UpdateableRandomVectorScorer {
        private final InMemoryInt4QuantizedByteVectorValues values;
        private byte[] queryUnpacked;
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        private final int dims;

        ScalarDotProduct(InMemoryInt4QuantizedByteVectorValues values) {
            this.values = values;
            this.dims = values.dimension();
        }

        @Override
        public float score(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            int rawDot = dotProductI4SinglePacked(queryUnpacked, packed);
            var nodeCorrections = values.getCorrectiveTerms(node);
            float ax = nodeCorrections.lowerInterval();
            float lx = (nodeCorrections.upperInterval() - ax) * FOUR_BIT_SCALE;
            float ay = queryCorrections.lowerInterval();
            float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
            float x1 = nodeCorrections.quantizedComponentSum();
            float y1 = queryCorrections.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawDot;
            score += queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - values.getCentroidDP();
            return (1f + Math.clamp(score, -1, 1)) / 2f;
        }

        @Override
        public int maxOrd() {
            return values.size();
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            queryUnpacked = unpackNibbles(packed, dims);
            queryCorrections = values.getCorrectiveTerms(node);
        }
    }

    private static class ScalarEuclidean implements UpdateableRandomVectorScorer {
        private final InMemoryInt4QuantizedByteVectorValues values;
        private byte[] queryUnpacked;
        private OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        private final int dims;

        ScalarEuclidean(InMemoryInt4QuantizedByteVectorValues values) {
            this.values = values;
            this.dims = values.dimension();
        }

        @Override
        public float score(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            int rawDot = dotProductI4SinglePacked(queryUnpacked, packed);
            var nodeCorrections = values.getCorrectiveTerms(node);
            float ax = nodeCorrections.lowerInterval();
            float lx = (nodeCorrections.upperInterval() - ax) * FOUR_BIT_SCALE;
            float ay = queryCorrections.lowerInterval();
            float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
            float x1 = nodeCorrections.quantizedComponentSum();
            float y1 = queryCorrections.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawDot;
            score = queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - 2 * score;
            return 1 / (1f + Math.max(score, 0f));
        }

        @Override
        public int maxOrd() {
            return values.size();
        }

        @Override
        public void setScoringOrdinal(int node) throws IOException {
            byte[] packed = values.vectorValue(node);
            queryUnpacked = unpackNibbles(packed, dims);
            queryCorrections = values.getCorrectiveTerms(node);
        }
    }

    static RandomVectorScorer createScalarQueryScorer(
        InMemoryInt4QuantizedByteVectorValues values,
        VectorSimilarityFunction sim,
        float[] queryVec
    ) throws IOException {
        int dims = values.dimension();
        OptimizedScalarQuantizer quantizer = values.getQuantizer();
        float[] centroid = values.getCentroid();
        ScalarEncoding encoding = values.getScalarEncoding();

        byte[] queryQuantized = new byte[encoding.getDiscreteDimensions(dims)];
        float[] queryCopy = Arrays.copyOf(queryVec, queryVec.length);
        if (sim == VectorSimilarityFunction.COSINE) {
            VectorUtil.l2normalize(queryCopy);
        }
        var queryCorrections = quantizer.scalarQuantize(queryCopy, queryQuantized, encoding.getQueryBits(), centroid);
        float centroidDP = values.getCentroidDP();

        return new RandomVectorScorer.AbstractRandomVectorScorer(values) {
            @Override
            public float score(int node) throws IOException {
                byte[] packed = values.vectorValue(node);
                int rawDot = dotProductI4SinglePacked(queryQuantized, packed);
                var nodeCorrections = values.getCorrectiveTerms(node);
                float ax = nodeCorrections.lowerInterval();
                float lx = (nodeCorrections.upperInterval() - ax) * FOUR_BIT_SCALE;
                float ay = queryCorrections.lowerInterval();
                float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
                float x1 = nodeCorrections.quantizedComponentSum();
                float y1 = queryCorrections.quantizedComponentSum();
                float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawDot;
                if (sim == VectorSimilarityFunction.EUCLIDEAN) {
                    score = queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - 2 * score;
                    return 1 / (1f + Math.max(score, 0f));
                } else {
                    score += queryCorrections.additionalCorrection() + nodeCorrections.additionalCorrection() - centroidDP;
                    return (1f + Math.clamp(score, -1, 1)) / 2f;
                }
            }
        };
    }

    private UpdateableRandomVectorScorer scorer;
    private RandomVectorScorer queryScorer;

    static class VectorData {
        final InMemoryInt4QuantizedByteVectorValues values;
        final float[] queryVector;

        VectorData(int dims) {
            byte[][] packedVectors = new byte[numVectors][];
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int v = 0; v < numVectors; v++) {
                byte[] unpacked = new byte[dims];
                randomInt4Bytes(random, unpacked);
                packedVectors[v] = packNibbles(unpacked);
            }
            values = int4QuantizedVectorValues(dims, packedVectors);
            queryVector = new float[dims];
            for (int i = 0; i < dims; i++) {
                queryVector[i] = random.nextFloat();
            }
        }
    }

    @Setup
    public void setup() throws IOException {
        setup(new VectorData(dims));
    }

    void setup(VectorData vectorData) throws IOException {
        VectorSimilarityFunction sim = function.function();
        var values = vectorData.values;

        switch (implementation) {
            case SCALAR:
                scorer = switch (sim) {
                    case DOT_PRODUCT, COSINE -> new ScalarDotProduct(values);
                    case EUCLIDEAN -> new ScalarEuclidean(values);
                    case MAXIMUM_INNER_PRODUCT -> throw new IllegalArgumentException("MIP not implemented for scalar int4 benchmark");
                };
                queryScorer = createScalarQueryScorer(values, sim, vectorData.queryVector);
                break;
            case LUCENE:
                scorer = lucene104ScoreSupplier(values, sim).scorer();
                if (supportsHeapSegments()) {
                    queryScorer = lucene104Scorer(values, sim, vectorData.queryVector);
                }
                break;
        }

        scorer.setScoringOrdinal(0);
    }

    @TearDown
    public void teardown() throws IOException {}

    @Benchmark
    public float score() throws IOException {
        return scorer.score(1);
    }

    @Benchmark
    public float scoreQuery() throws IOException {
        if (queryScorer == null) {
            return 0f;
        }
        return queryScorer.score(1);
    }
}
