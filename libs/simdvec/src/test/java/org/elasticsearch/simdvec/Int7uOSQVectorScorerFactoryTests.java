/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorsFormat;
import org.apache.lucene.codecs.lucene104.QuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.core.SuppressForbidden;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;

public class Int7uOSQVectorScorerFactoryTests extends org.elasticsearch.simdvec.AbstractVectorTestCase {
    private static final float LIMIT_SCALE = 1f / ((1 << 7) - 1);

    @SuppressForbidden(reason = "require usage of OptimizedScalarQuantizer")
    private static OptimizedScalarQuantizer scalarQuantizer(VectorSimilarityFunction sim) {
        return new OptimizedScalarQuantizer(sim);
    }

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    static final byte MIN_INT7_VALUE = 0;
    static final byte MAX_INT7_VALUE = 127;

    // Tests that the provider instance is present or not on expected platforms/architectures
    public void testSupport() {
        supported();
    }

    public void testSimple() throws IOException {
        testSimpleImpl(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    }

    public void testSimpleMaxChunkSizeSmall() throws IOException {
        long maxChunkSize = randomLongBetween(4, 16);
        logger.info("maxChunkSize=" + maxChunkSize);
        testSimpleImpl(maxChunkSize);
    }

    void testSimpleImpl(long maxChunkSize) throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testSimpleImpl"), maxChunkSize)) {
            for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                var scalarQuantizer = scalarQuantizer(sim.function());
                for (int dims : List.of(31, 32, 33)) {
                    // dimensions that cross the scalar / native boundary (stride)
                    byte[] vec1 = new byte[dims];
                    byte[] vec2 = new byte[dims];
                    float[] query1 = new float[dims];
                    float[] query2 = new float[dims];
                    float[] centroid = new float[dims];
                    float centroidDP = 0f;
                    OptimizedScalarQuantizer.QuantizationResult vec1Correction, vec2Correction;
                    String fileName = "testSimpleImpl-" + sim + "-" + dims + ".vex";
                    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                        for (int i = 0; i < dims; i++) {
                            query1[i] = (float) i;
                            query2[i] = (float) (dims - i);
                            centroid[i] = (query1[i] + query2[i]) / 2f;
                            centroidDP += centroid[i] * centroid[i];
                        }
                        vec1Correction = scalarQuantizer.scalarQuantize(query1, vec1, (byte) 7, centroid);
                        vec2Correction = scalarQuantizer.scalarQuantize(query2, vec2, (byte) 7, centroid);
                        out.writeBytes(vec1, 0, vec1.length);
                        out.writeInt(Float.floatToIntBits(vec1Correction.lowerInterval()));
                        out.writeInt(Float.floatToIntBits(vec1Correction.upperInterval()));
                        out.writeInt(Float.floatToIntBits(vec1Correction.additionalCorrection()));
                        out.writeInt(vec1Correction.quantizedComponentSum());
                        out.writeBytes(vec2, 0, vec2.length);
                        out.writeInt(Float.floatToIntBits(vec2Correction.lowerInterval()));
                        out.writeInt(Float.floatToIntBits(vec2Correction.upperInterval()));
                        out.writeInt(Float.floatToIntBits(vec2Correction.additionalCorrection()));
                        out.writeInt(vec2Correction.quantizedComponentSum());
                    }
                    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                        var values = vectorValues(dims, 2, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(sim, vec1, vec2, centroidDP, vec1Correction, vec2Correction);

                        var luceneSupplier = luceneScoreSupplier(values, sim.function()).scorer();
                        luceneSupplier.setScoringOrdinal(1);
                        assertFloatEquals(expected, luceneSupplier.score(0), 1e-6f);
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(1);
                        assertFloatEquals(expected, scorer.score(0), 1e-6f);

                        if (supportsHeapSegments()) {
                            var qScorer = factory.getInt7uOSQVectorScorer(
                                sim.function(),
                                values,
                                vec2,
                                vec2Correction.lowerInterval(),
                                vec2Correction.upperInterval(),
                                vec2Correction.additionalCorrection(),
                                vec2Correction.quantizedComponentSum()
                            ).get();
                            assertFloatEquals(expected, qScorer.score(0), 1e-6f);
                        }
                    }
                }
            }
        }
    }

    public void testRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_RANDOM_INT7_FUNC);
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomSupplier(maxChunkSize, BYTE_ARRAY_RANDOM_INT7_FUNC);
    }

    public void testRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MAX_INT7_FUNC);
    }

    public void testRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MIN_INT7_FUNC);
    }

    void testRandomSupplier(long maxChunkSize, Function<Integer, byte[]> byteArraySupplier) throws IOException {
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            final int dims = randomIntBetween(1, 4096);
            final int size = randomIntBetween(2, 100);
            final byte[][] vectors = new byte[size][];
            final OptimizedScalarQuantizer.QuantizationResult[] quantizationResults = new OptimizedScalarQuantizer.QuantizationResult[size];
            final float[] centroid = new float[dims];

            String fileName = "testRandom-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = byteArraySupplier.apply(dims);
                    int componentSum = 0;
                    for (int d = 0; d < dims; d++) {
                        componentSum += Byte.toUnsignedInt(vec[d]);
                    }
                    float lowerInterval = randomFloat();
                    float upperInterval = randomFloat() + lowerInterval;
                    quantizationResults[i] = new OptimizedScalarQuantizer.QuantizationResult(
                        lowerInterval,
                        upperInterval,
                        randomFloat(),
                        componentSum
                    );
                    out.writeBytes(vec, 0, vec.length);
                    out.writeInt(Float.floatToIntBits(lowerInterval));
                    out.writeInt(Float.floatToIntBits(upperInterval));
                    out.writeInt(Float.floatToIntBits(quantizationResults[i].additionalCorrection()));
                    out.writeInt(componentSum);
                    vectors[i] = vec;
                }
            }
            for (int i = 0; i < dims; i++) {
                centroid[i] = randomFloat();
            }
            float centroidDP = VectorUtil.dotProduct(centroid, centroid);
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(
                            sim,
                            vectors[idx0],
                            vectors[idx1],
                            centroidDP,
                            quantizationResults[idx0],
                            quantizationResults[idx1]
                        );
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx1);
                        assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                    }
                }
            }
        }
    }

    public void testRandomScorer() throws IOException {
        testRandomScorerImpl(
            MMapDirectory.DEFAULT_MAX_CHUNK_SIZE,
            org.elasticsearch.simdvec.Int7SQVectorScorerFactoryTests.FLOAT_ARRAY_RANDOM_FUNC
        );
    }

    public void testRandomScorerMax() throws IOException {
        testRandomScorerImpl(
            MMapDirectory.DEFAULT_MAX_CHUNK_SIZE,
            org.elasticsearch.simdvec.Int7SQVectorScorerFactoryTests.FLOAT_ARRAY_MAX_FUNC
        );
    }

    public void testRandomScorerChunkSizeSmall() throws IOException {
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomScorerImpl(maxChunkSize, FLOAT_ARRAY_RANDOM_FUNC);
    }

    void testRandomScorerImpl(long maxChunkSize, Function<Integer, float[]> floatArraySupplier) throws IOException {
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                var scalarQuantizer = new OptimizedScalarQuantizer(sim.function());
                final int dims = randomIntBetween(1, 4096);
                final int size = randomIntBetween(2, 100);
                final float[] centroid = new float[dims];
                for (int i = 0; i < dims; i++) {
                    centroid[i] = randomFloat();
                }
                final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
                final float[][] vectors = new float[size][];
                final byte[][] qVectors = new byte[size][];
                final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

                String fileName = "testRandom-" + sim + "-" + dims + ".vex";
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    for (int i = 0; i < size; i++) {
                        vectors[i] = floatArraySupplier.apply(dims);
                        qVectors[i] = new byte[dims];
                        corrections[i] = scalarQuantizer.scalarQuantize(vectors[i], qVectors[i], (byte) 7, centroid);
                        out.writeBytes(qVectors[i], 0, qVectors[i].length);
                        out.writeInt(Float.floatToIntBits(corrections[i].lowerInterval()));
                        out.writeInt(Float.floatToIntBits(corrections[i].upperInterval()));
                        out.writeInt(Float.floatToIntBits(corrections[i].additionalCorrection()));
                        out.writeInt(corrections[i].quantizedComponentSum());
                    }
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    for (int times = 0; times < TIMES; times++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1);
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());

                        var expected = luceneScore(sim, qVectors[idx0], qVectors[idx1], centroidDP, corrections[idx0], corrections[idx1]);
                        var scorer = factory.getInt7uOSQVectorScorer(
                            sim.function(),
                            values,
                            qVectors[idx0],
                            corrections[idx0].lowerInterval(),
                            corrections[idx0].upperInterval(),
                            corrections[idx0].additionalCorrection(),
                            corrections[idx0].quantizedComponentSum()
                        ).get();
                        assertFloatEquals(expected, scorer.score(idx1), 1e-6f);
                    }
                }
            }
        }
    }

    public void testRandomSlice() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSliceImpl(30, 64, 1, BYTE_ARRAY_RANDOM_INT7_FUNC);
    }

    void testRandomSliceImpl(int dims, long maxChunkSize, int initialPadding, Function<Integer, byte[]> byteArraySupplier)
        throws IOException {
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandomSliceImpl"), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int size = randomIntBetween(2, 100);
                final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
                final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
                final byte[][] vectors = new byte[size][];
                final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

                String fileName = "testRandomSliceImpl-" + times + "-" + dims;
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[initialPadding];
                    out.writeBytes(ba, 0, ba.length);
                    for (int i = 0; i < size; i++) {
                        var vec = byteArraySupplier.apply(dims);
                        var correction = randomCorrection(vec);
                        writeVectorWithCorrection(out, vec, correction);
                        vectors[i] = vec;
                        corrections[i] = correction;
                    }
                }
                try (
                    var outter = dir.openInput(fileName, IOContext.DEFAULT);
                    var in = outter.slice("slice", initialPadding, outter.length() - initialPadding)
                ) {
                    for (int itrs = 0; itrs < TIMES / 10; itrs++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                        for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                            var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                            float expected = luceneScore(
                                sim,
                                vectors[idx0],
                                vectors[idx1],
                                centroidDP,
                                corrections[idx0],
                                corrections[idx1]
                            );
                            var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                            var scorer = supplier.scorer();
                            scorer.setScoringOrdinal(idx1);
                            assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                        }
                    }
                }
            }
        }
    }

    // Tests with a large amount of data (> 2GB), which ensures that data offsets do not overflow
    @Nightly
    public void testLarge() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testLarge"))) {
            final int dims = 8192;
            final int size = 262144;
            final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
            final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
            final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

            String fileName = "testLarge-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    var correction = randomCorrection(vec);
                    writeVectorWithCorrection(out, vec, correction);
                    corrections[i] = correction;
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = size - 1;
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(
                            sim,
                            vector(idx0, dims),
                            vector(idx1, dims),
                            centroidDP,
                            corrections[idx0],
                            corrections[idx1]
                        );
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx1);
                        assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                    }
                }
            }
        }
    }

    // Test that the scorer works well when the IndexInput is greater than the directory segment chunk size
    public void testDatasetGreaterThanChunkSize() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testDatasetGreaterThanChunkSize"), 8192)) {
            final int dims = 1024;
            final int size = 128;
            final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
            final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
            final byte[][] vectors = new byte[size][];
            final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

            String fileName = "testDatasetGreaterThanChunkSize-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    var correction = randomCorrection(vec);
                    writeVectorWithCorrection(out, vec, correction);
                    vectors[i] = vec;
                    corrections[i] = correction;
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = size - 1;
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(sim, vectors[idx0], vectors[idx1], centroidDP, corrections[idx0], corrections[idx1]);
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx1);
                        assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                    }
                }
            }
        }
    }

    public void testBulk() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        final int dims = 1024;
        final int size = randomIntBetween(1, 102);
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        final byte[][] vectors = new byte[size][];
        final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];
        // Set maxChunkSize to be less than dims * size
        try (Directory dir = new MMapDirectory(createTempDir("testBulk"))) {
            String fileName = "testBulk-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    var correction = randomCorrection(vec);
                    writeVectorWithCorrection(out, vec, correction);
                    vectors[i] = vec;
                    corrections[i] = correction;
                }
            }

            List<Integer> ids = IntStream.range(0, size).boxed().collect(Collectors.toList());
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int[] nodes = shuffledList(ids).stream().mapToInt(i -> i).toArray();
                    for (var sim : List.of(EUCLIDEAN)) {
                        QuantizedByteVectorValues values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float[] expected = new float[nodes.length];
                        float[] scores = new float[nodes.length];
                        var referenceScorer = luceneScoreSupplier(values, sim.function()).scorer();
                        referenceScorer.setScoringOrdinal(idx0);
                        referenceScorer.bulkScore(nodes, expected, nodes.length);
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).orElseThrow();
                        var testScorer = supplier.scorer();
                        testScorer.setScoringOrdinal(idx0);
                        testScorer.bulkScore(nodes, scores, nodes.length);
                        // applying the corrections in even a slightly different order can impact the score
                        // account for this during bulk scoring
                        assertFloatArrayEquals(expected, scores, 2e-5f);
                    }
                }
            }
        }
    }

    public void testBulkWithDatasetGreaterThanChunkSize() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        final int dims = 1024;
        final int size = 128;
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        final byte[][] vectors = new byte[size][];
        final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];
        // Set maxChunkSize to be less than dims * size
        try (Directory dir = new MMapDirectory(createTempDir("testBulkWithDatasetGreaterThanChunkSize"), 8192)) {
            String fileName = "testBulkWithDatasetGreaterThanChunkSize-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    var correction = randomCorrection(vec);
                    writeVectorWithCorrection(out, vec, correction);
                    vectors[i] = vec;
                    corrections[i] = correction;
                }
            }

            List<Integer> ids = IntStream.range(0, size).boxed().collect(Collectors.toList());
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int[] nodes = shuffledList(ids).stream().mapToInt(i -> i).toArray();
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        QuantizedByteVectorValues values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float[] expected = new float[nodes.length];
                        float[] scores = new float[nodes.length];
                        var referenceScorer = luceneScoreSupplier(values, sim.function()).scorer();
                        referenceScorer.setScoringOrdinal(idx0);
                        referenceScorer.bulkScore(nodes, expected, nodes.length);
                        var supplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).orElseThrow();
                        var testScorer = supplier.scorer();
                        testScorer.setScoringOrdinal(idx0);
                        testScorer.bulkScore(nodes, scores, nodes.length);
                        assertFloatArrayEquals(expected, scores, 1e-6f);
                    }
                }
            }
        }
    }

    public void testRace() throws Exception {
        testRaceImpl(DOT_PRODUCT);
        testRaceImpl(EUCLIDEAN);
        testRaceImpl(MAXIMUM_INNER_PRODUCT);
    }

    // Tests that copies in threads do not interfere with each other
    void testRaceImpl(org.elasticsearch.simdvec.VectorSimilarityType sim) throws Exception {
        assumeTrue(notSupportedMsg(), supported());
        var factory = org.elasticsearch.simdvec.AbstractVectorTestCase.factory.get();

        final long maxChunkSize = 32;
        final int dims = 34; // dimensions that are larger than the chunk size, to force fallback
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        byte[] vec1 = new byte[dims];
        byte[] vec2 = new byte[dims];
        IntStream.range(0, dims).forEach(i -> vec1[i] = 1);
        IntStream.range(0, dims).forEach(i -> vec2[i] = 2);
        var correction1 = randomCorrection(vec1);
        var correction2 = randomCorrection(vec2);
        try (Directory dir = new MMapDirectory(createTempDir("testRace"), maxChunkSize)) {
            String fileName = "testRace-" + dims;
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                writeVectorWithCorrection(out, vec1, correction1);
                writeVectorWithCorrection(out, vec1, correction1);
                writeVectorWithCorrection(out, vec2, correction2);
                writeVectorWithCorrection(out, vec2, correction2);
            }
            var expectedScore1 = luceneScore(sim, vec1, vec1, centroidDP, correction1, correction1);
            var expectedScore2 = luceneScore(sim, vec2, vec2, centroidDP, correction2, correction2);

            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(dims, 4, centroid, centroidDP, in, sim.function());
                var scoreSupplier = factory.getInt7uOSQVectorScorerSupplier(sim, in, values).get();
                var tasks = List.<Callable<Optional<Throwable>>>of(
                    new ScoreCallable(scoreSupplier.copy().scorer(), 0, 1, expectedScore1),
                    new ScoreCallable(scoreSupplier.copy().scorer(), 2, 3, expectedScore2)
                );
                var executor = Executors.newFixedThreadPool(2);
                var results = executor.invokeAll(tasks);
                executor.shutdown();
                assertTrue(executor.awaitTermination(60, TimeUnit.SECONDS));
                assertThat(results.stream().filter(Predicate.not(Future::isDone)).count(), equalTo(0L));
                for (var res : results) {
                    assertThat("Unexpected exception" + res.get(), res.get(), isEmpty());
                }
            }
        }
    }

    static class ScoreCallable implements Callable<Optional<Throwable>> {

        final UpdateableRandomVectorScorer scorer;
        final int ord;
        final float expectedScore;

        ScoreCallable(UpdateableRandomVectorScorer scorer, int queryOrd, int ord, float expectedScore) {
            try {
                this.scorer = scorer;
                this.scorer.setScoringOrdinal(queryOrd);
                this.ord = ord;
                this.expectedScore = expectedScore;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Optional<Throwable> call() {
            try {
                for (int i = 0; i < 100; i++) {
                    assertFloatEquals(expectedScore, scorer.score(ord), 1e-6f);
                }
            } catch (Throwable t) {
                return Optional.of(t);
            }
            return Optional.empty();
        }
    }

    private static OptimizedScalarQuantizer.QuantizationResult randomCorrection(byte[] vec) {
        int componentSum = 0;
        for (byte value : vec) {
            componentSum += Byte.toUnsignedInt(value);
        }
        float lowerInterval = randomFloat();
        float upperInterval = lowerInterval + randomFloat();
        return new OptimizedScalarQuantizer.QuantizationResult(lowerInterval, upperInterval, randomFloat(), componentSum);
    }

    private static void writeVectorWithCorrection(IndexOutput out, byte[] vec, OptimizedScalarQuantizer.QuantizationResult correction)
        throws IOException {
        out.writeBytes(vec, 0, vec.length);
        out.writeInt(Float.floatToIntBits(correction.lowerInterval()));
        out.writeInt(Float.floatToIntBits(correction.upperInterval()));
        out.writeInt(Float.floatToIntBits(correction.additionalCorrection()));
        out.writeInt(correction.quantizedComponentSum());
    }

    QuantizedByteVectorValues vectorValues(
        int dims,
        int size,
        float[] centroid,
        float centroidDP,
        IndexInput in,
        VectorSimilarityFunction sim
    ) throws IOException {
        var slice = in.slice("values", 0, in.length());
        return new DenseOffHeapScalarQuantizedVectorValues(dims, size, sim, slice, centroid, centroidDP);
    }

    /** Computes the score using the Lucene implementation. */
    public float luceneScore(
        org.elasticsearch.simdvec.VectorSimilarityType similarityFunc,
        byte[] a,
        byte[] b,
        float centroidDP,
        OptimizedScalarQuantizer.QuantizationResult aCorrection,
        OptimizedScalarQuantizer.QuantizationResult bCorrection
    ) {
        OSQScorer scorer = OSQScorer.fromSimilarity(similarityFunc);
        return scorer.score(a, b, centroidDP, aCorrection, bCorrection);
    }

    private abstract static class OSQScorer {
        static OSQScorer fromSimilarity(org.elasticsearch.simdvec.VectorSimilarityType sim) {
            return switch (sim) {
                case DOT_PRODUCT -> new DotProductOSQScorer();
                case MAXIMUM_INNER_PRODUCT -> new MaxInnerProductOSQScorer();
                case EUCLIDEAN -> new EuclideanOSQScorer();
                default -> throw new IllegalArgumentException("Unsupported similarity: " + sim);
            };
        }

        final float score(
            byte[] a,
            byte[] b,
            float centroidDP,
            OptimizedScalarQuantizer.QuantizationResult aCorrection,
            OptimizedScalarQuantizer.QuantizationResult bCorrection
        ) {
            float ax = aCorrection.lowerInterval();
            float lx = (aCorrection.upperInterval() - ax) * LIMIT_SCALE;
            float ay = bCorrection.lowerInterval();
            float ly = (bCorrection.upperInterval() - ay) * LIMIT_SCALE;
            float y1 = bCorrection.quantizedComponentSum();
            float x1 = aCorrection.quantizedComponentSum();
            float score = ax * ay * a.length + ay * lx * x1 + ax * ly * y1 + lx * ly * VectorUtil.dotProduct(a, b);
            return scaleScore(score, aCorrection.additionalCorrection(), bCorrection.additionalCorrection(), centroidDP);
        }

        abstract float scaleScore(float score, float aCorrection, float bCorrection, float centroidDP);

        private static class DotProductOSQScorer extends OSQScorer {
            @Override
            float scaleScore(float score, float aCorrection, float bCorrection, float centroidDP) {
                score += aCorrection + bCorrection - centroidDP;
                score = Math.clamp(score, -1, 1);
                return VectorUtil.normalizeToUnitInterval(score);
            }
        }

        private static class MaxInnerProductOSQScorer extends OSQScorer {
            @Override
            float scaleScore(float score, float aCorrection, float bCorrection, float centroidDP) {
                score += aCorrection + bCorrection - centroidDP;
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
        }

        private static class EuclideanOSQScorer extends OSQScorer {
            @Override
            float scaleScore(float score, float aCorrection, float bCorrection, float centroidDP) {
                score = aCorrection + bCorrection - 2 * score;
                return VectorUtil.normalizeDistanceToUnitInterval(Math.max(score, 0f));
            }
        }
    }

    static void assertFloatArrayEquals(float[] expected, float[] actual, float delta) {
        assertThat(actual.length, equalTo(expected.length));
        for (int i = 0; i < expected.length; i++) {
            assertEquals("differed at element [" + i + "]", expected[i], actual[i], Math.abs(expected[i]) * delta + delta);
        }
    }

    static void assertFloatEquals(float expected, float actual, float delta) {
        assertEquals(expected, actual, Math.abs(expected) * delta + delta);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(QuantizedByteVectorValues values, VectorSimilarityFunction sim)
        throws IOException {
        return new Lucene104ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
    }

    // creates the vector based on the given ordinal, which is reproducible given the ord and dims
    static byte[] vector(int ord, int dims) {
        var random = new Random(Objects.hash(ord, dims));
        byte[] ba = new byte[dims];
        for (int i = 0; i < dims; i++) {
            ba[i] = (byte) RandomNumbers.randomIntBetween(random, MIN_INT7_VALUE, MAX_INT7_VALUE);
        }
        return ba;
    }

    static Function<Integer, float[]> FLOAT_ARRAY_RANDOM_FUNC = size -> {
        float[] fa = new float[size];
        for (int i = 0; i < size; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    };

    static Function<Integer, byte[]> BYTE_ARRAY_RANDOM_INT7_FUNC = size -> {
        byte[] ba = new byte[size];
        randomBytesBetween(ba, MIN_INT7_VALUE, MAX_INT7_VALUE);
        return ba;
    };

    static Function<Integer, byte[]> BYTE_ARRAY_MAX_INT7_FUNC = size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, MAX_INT7_VALUE);
        return ba;
    };

    static Function<Integer, byte[]> BYTE_ARRAY_MIN_INT7_FUNC = size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, MIN_INT7_VALUE);
        return ba;
    };

    static final int TIMES = 100; // a loop iteration times

    static class DenseOffHeapScalarQuantizedVectorValues extends QuantizedByteVectorValues {
        final int dimension;
        final int size;
        final VectorSimilarityFunction similarityFunction;

        final IndexInput slice;
        final byte[] vectorValue;
        final ByteBuffer byteBuffer;
        final int byteSize;
        private int lastOrd = -1;
        final float[] correctiveValues;
        int quantizedComponentSum;
        final float[] centroid;
        final float centroidDp;

        DenseOffHeapScalarQuantizedVectorValues(
            int dimension,
            int size,
            VectorSimilarityFunction similarityFunction,
            IndexInput slice,
            float[] centroid,
            float centroidDp
        ) {
            this.dimension = dimension;
            this.size = size;
            this.similarityFunction = similarityFunction;
            this.slice = slice;
            this.centroid = centroid;
            this.centroidDp = centroidDp;
            this.correctiveValues = new float[3];
            this.byteSize = dimension + (Float.BYTES * 3) + Integer.BYTES;
            this.byteBuffer = ByteBuffer.allocate(dimension);
            this.vectorValue = byteBuffer.array();
        }

        @Override
        public IndexInput getSlice() {
            return slice;
        }

        @Override
        public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int vectorOrd) throws IOException {
            if (lastOrd != vectorOrd) {
                slice.seek((long) vectorOrd * byteSize);
                slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), vectorValue.length);
                slice.readFloats(correctiveValues, 0, 3);
                quantizedComponentSum = slice.readInt();
                lastOrd = vectorOrd;
            }
            return new OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }

        @Override
        public OptimizedScalarQuantizer getQuantizer() {
            return scalarQuantizer(similarityFunction);
        }

        @Override
        public Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding getScalarEncoding() {
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.SEVEN_BIT;
        }

        @Override
        public float[] getCentroid() throws IOException {
            return centroid;
        }

        @Override
        public float getCentroidDP() throws IOException {
            return centroidDp;
        }

        @Override
        public VectorScorer scorer(float[] query) throws IOException {
            assert false;
            return null;
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            if (lastOrd == ord) {
                return vectorValue;
            }
            slice.seek((long) ord * byteSize);
            slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), vectorValue.length);
            slice.readFloats(correctiveValues, 0, 3);
            quantizedComponentSum = slice.readInt();
            lastOrd = ord;
            return vectorValue;
        }

        @Override
        public int dimension() {
            return dimension;
        }

        @Override
        public int size() {
            return size;
        }

        @Override
        public QuantizedByteVectorValues copy() throws IOException {
            return new DenseOffHeapScalarQuantizedVectorValues(dimension, size, similarityFunction, slice.clone(), centroid, centroidDp);
        }
    }
}
