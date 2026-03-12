/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import com.carrotsearch.randomizedtesting.annotations.Nightly;
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
import java.util.function.IntFunction;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;

public class Int4VectorScorerFactoryTests extends AbstractVectorTestCase {
    private static final float LIMIT_SCALE = 1f / ((1 << 4) - 1);

    @SuppressForbidden(reason = "require usage of OptimizedScalarQuantizer")
    private static OptimizedScalarQuantizer scalarQuantizer(VectorSimilarityFunction sim) {
        return new OptimizedScalarQuantizer(sim);
    }

    // bounds of the range of values for int4 packed nibble (4-bit)
    static final byte MIN_INT4_VALUE = 0;
    static final byte MAX_INT4_VALUE = 15;

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
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testSimpleImpl"), maxChunkSize)) {
            for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                var scalarQuantizer = scalarQuantizer(sim.function());
                var encoding = Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
                for (int dims : List.of(30, 32, 34)) {
                    float[] query1 = new float[dims];
                    float[] query2 = new float[dims];
                    float[] centroid = new float[dims];
                    float centroidDP = 0f;
                    byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];
                    OptimizedScalarQuantizer.QuantizationResult vec1Correction, vec2Correction;
                    byte[] packed1, packed2;
                    String fileName = "testSimpleImpl-" + sim + "-" + dims + ".vex";
                    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                        for (int i = 0; i < dims; i++) {
                            query1[i] = (float) i;
                            query2[i] = (float) (dims - i);
                            centroid[i] = (query1[i] + query2[i]) / 2f;
                            centroidDP += centroid[i] * centroid[i];
                        }
                        vec1Correction = scalarQuantizer.scalarQuantize(query1, scratch, (byte) 4, centroid);
                        packed1 = packNibbles(Arrays.copyOf(scratch, dims));
                        vec2Correction = scalarQuantizer.scalarQuantize(query2, scratch, (byte) 4, centroid);
                        packed2 = packNibbles(Arrays.copyOf(scratch, dims));
                        writePackedVectorWithCorrection(out, packed1, vec1Correction);
                        writePackedVectorWithCorrection(out, packed2, vec2Correction);
                    }
                    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                        var values = vectorValues(dims, 2, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(sim, packed1, packed2, dims, centroidDP, vec1Correction, vec2Correction);

                        var luceneSupplier = luceneScoreSupplier(values, sim.function()).scorer();
                        luceneSupplier.setScoringOrdinal(1);
                        assertFloatEquals(expected, luceneSupplier.score(0), 1e-6f);
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(1);
                        assertFloatEquals(expected, scorer.score(0), 1e-6f);

                        if (supportsHeapSegments()) {
                            byte[] unpackedQuery = unpackNibbles(packed2, dims);
                            var qScorer = factory.getInt4VectorScorer(
                                sim.function(),
                                values,
                                unpackedQuery,
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
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_RANDOM_INT4_FUNC);
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomSupplier(maxChunkSize, BYTE_ARRAY_RANDOM_INT4_FUNC);
    }

    public void testRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MAX_INT4_FUNC);
    }

    public void testRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MIN_INT4_FUNC);
    }

    void testRandomSupplier(long maxChunkSize, IntFunction<byte[]> packedByteArraySupplier) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            final int dims = randomIntBetween(1, 2048) * 2;
            final int size = randomIntBetween(2, 100);
            final byte[][] packedVectors = new byte[size][];
            final OptimizedScalarQuantizer.QuantizationResult[] quantizationResults = new OptimizedScalarQuantizer.QuantizationResult[size];
            final float[] centroid = new float[dims];

            String fileName = "testRandom-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var packed = packedByteArraySupplier.apply(dims);
                    int componentSum = componentSumUnpacked(packed);
                    float lowerInterval = randomFloat();
                    float upperInterval = randomFloat() + lowerInterval;
                    quantizationResults[i] = new OptimizedScalarQuantizer.QuantizationResult(
                        lowerInterval,
                        upperInterval,
                        randomFloat(),
                        componentSum
                    );
                    writePackedVectorWithCorrection(out, packed, quantizationResults[i]);
                    packedVectors[i] = packed;
                }
            }
            for (int i = 0; i < dims; i++) {
                centroid[i] = randomFloat();
            }
            float centroidDP = VectorUtil.dotProduct(centroid, centroid);
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1);
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                        float expected = luceneScore(
                            sim,
                            packedVectors[idx0],
                            packedVectors[idx1],
                            dims,
                            centroidDP,
                            quantizationResults[idx0],
                            quantizationResults[idx1]
                        );
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx1);
                        assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                    }
                }
            }
        }
    }

    public void testRandomScorer() throws IOException {
        testRandomScorerImpl(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, Int7SQVectorScorerFactoryTests.FLOAT_ARRAY_RANDOM_FUNC);
    }

    public void testRandomScorerMax() throws IOException {
        testRandomScorerImpl(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, Int7SQVectorScorerFactoryTests.FLOAT_ARRAY_MAX_FUNC);
    }

    public void testRandomScorerChunkSizeSmall() throws IOException {
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomScorerImpl(maxChunkSize, FLOAT_ARRAY_RANDOM_FUNC);
    }

    void testRandomScorerImpl(long maxChunkSize, IntFunction<float[]> floatArraySupplier) throws IOException {
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                var scalarQuantizer = scalarQuantizer(sim.function());
                var encoding = Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
                final int dims = randomIntBetween(1, 2048) * 2;
                final int size = randomIntBetween(2, 100);
                final float[] centroid = new float[dims];
                for (int i = 0; i < dims; i++) {
                    centroid[i] = randomFloat();
                }
                final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
                final float[][] vectors = new float[size][];
                final byte[][] packedVectors = new byte[size][];
                final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];
                byte[] scratch = new byte[encoding.getDiscreteDimensions(dims)];

                String fileName = "testRandom-" + sim + "-" + dims + ".vex";
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    for (int i = 0; i < size; i++) {
                        vectors[i] = floatArraySupplier.apply(dims);
                        corrections[i] = scalarQuantizer.scalarQuantize(vectors[i], scratch, (byte) 4, centroid);
                        packedVectors[i] = packNibbles(Arrays.copyOf(scratch, dims));
                        writePackedVectorWithCorrection(out, packedVectors[i], corrections[i]);
                    }
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    for (int times = 0; times < TIMES; times++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1);
                        var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());

                        var expected = luceneScore(
                            sim,
                            packedVectors[idx0],
                            packedVectors[idx1],
                            dims,
                            centroidDP,
                            corrections[idx0],
                            corrections[idx1]
                        );
                        byte[] unpackedQuery = unpackNibbles(packedVectors[idx0], dims);
                        var scorer = factory.getInt4VectorScorer(
                            sim.function(),
                            values,
                            unpackedQuery,
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
        testRandomSliceImpl(30, 64, 1, BYTE_ARRAY_RANDOM_INT4_FUNC);
    }

    void testRandomSliceImpl(int dims, long maxChunkSize, int initialPadding, IntFunction<byte[]> packedByteArraySupplier)
        throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandomSliceImpl"), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int size = randomIntBetween(2, 100);
                final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
                final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
                final byte[][] packedVectors = new byte[size][];
                final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

                String fileName = "testRandomSliceImpl-" + times + "-" + dims;
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[initialPadding];
                    out.writeBytes(ba, 0, ba.length);
                    for (int i = 0; i < size; i++) {
                        var packed = packedByteArraySupplier.apply(dims);
                        var correction = randomCorrectionPacked(packed);
                        writePackedVectorWithCorrection(out, packed, correction);
                        packedVectors[i] = packed;
                        corrections[i] = correction;
                    }
                }
                try (
                    var outter = dir.openInput(fileName, IOContext.DEFAULT);
                    var in = outter.slice("slice", initialPadding, outter.length() - initialPadding)
                ) {
                    for (int itrs = 0; itrs < TIMES / 10; itrs++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1);
                        for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                            var values = vectorValues(dims, size, centroid, centroidDP, in, sim.function());
                            float expected = luceneScore(
                                sim,
                                packedVectors[idx0],
                                packedVectors[idx1],
                                dims,
                                centroidDP,
                                corrections[idx0],
                                corrections[idx1]
                            );
                            var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
                            var scorer = supplier.scorer();
                            scorer.setScoringOrdinal(idx1);
                            assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                        }
                    }
                }
            }
        }
    }

    @Nightly
    public void testLarge() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

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
                    var packed = vector(i, dims);
                    var correction = randomCorrectionPacked(packed);
                    writePackedVectorWithCorrection(out, packed, correction);
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
                            dims,
                            centroidDP,
                            corrections[idx0],
                            corrections[idx1]
                        );
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx1);
                        assertFloatEquals(expected, scorer.score(idx0), 1e-6f);
                    }
                }
            }
        }
    }

    public void testDatasetGreaterThanChunkSize() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testDatasetGreaterThanChunkSize"), 8192)) {
            final int dims = 1024;
            final int size = 128;
            final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
            final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
            final byte[][] packedVectors = new byte[size][];
            final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];

            String fileName = "testDatasetGreaterThanChunkSize-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var packed = vector(i, dims);
                    var correction = randomCorrectionPacked(packed);
                    writePackedVectorWithCorrection(out, packed, correction);
                    packedVectors[i] = packed;
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
                            packedVectors[idx0],
                            packedVectors[idx1],
                            dims,
                            centroidDP,
                            corrections[idx0],
                            corrections[idx1]
                        );
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
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
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = 1024;
        final int size = randomIntBetween(1, 102);
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        final byte[][] packedVectors = new byte[size][];
        final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];
        try (Directory dir = new MMapDirectory(createTempDir("testBulk"))) {
            String fileName = "testBulk-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var packed = vector(i, dims);
                    var correction = randomCorrectionPacked(packed);
                    writePackedVectorWithCorrection(out, packed, correction);
                    packedVectors[i] = packed;
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
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).orElseThrow();
                        var testScorer = supplier.scorer();
                        testScorer.setScoringOrdinal(idx0);
                        testScorer.bulkScore(nodes, scores, nodes.length);
                        assertFloatArrayEquals(expected, scores, 2e-5f);
                    }
                }
            }
        }
    }

    public void testBulkWithDatasetGreaterThanChunkSize() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = 1024;
        final int size = 128;
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        final byte[][] packedVectors = new byte[size][];
        final OptimizedScalarQuantizer.QuantizationResult[] corrections = new OptimizedScalarQuantizer.QuantizationResult[size];
        try (Directory dir = new MMapDirectory(createTempDir("testBulkWithDatasetGreaterThanChunkSize"), 8192)) {
            String fileName = "testBulkWithDatasetGreaterThanChunkSize-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var packed = vector(i, dims);
                    var correction = randomCorrectionPacked(packed);
                    writePackedVectorWithCorrection(out, packed, correction);
                    packedVectors[i] = packed;
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
                        var supplier = factory.getInt4VectorScorerSupplier(sim, in, values).orElseThrow();
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

    void testRaceImpl(VectorSimilarityType sim) throws Exception {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        final long maxChunkSize = 32;
        final int dims = 34;
        final float[] centroid = FLOAT_ARRAY_RANDOM_FUNC.apply(dims);
        final float centroidDP = VectorUtil.dotProduct(centroid, centroid);
        byte[] unpacked1 = new byte[dims];
        byte[] unpacked2 = new byte[dims];
        IntStream.range(0, dims).forEach(i -> unpacked1[i] = 1);
        IntStream.range(0, dims).forEach(i -> unpacked2[i] = 2);
        byte[] packed1 = packNibbles(unpacked1);
        byte[] packed2 = packNibbles(unpacked2);
        var correction1 = randomCorrectionPacked(packed1);
        var correction2 = randomCorrectionPacked(packed2);
        try (Directory dir = new MMapDirectory(createTempDir("testRace"), maxChunkSize)) {
            String fileName = "testRace-" + dims;
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                writePackedVectorWithCorrection(out, packed1, correction1);
                writePackedVectorWithCorrection(out, packed1, correction1);
                writePackedVectorWithCorrection(out, packed2, correction2);
                writePackedVectorWithCorrection(out, packed2, correction2);
            }
            var expectedScore1 = luceneScore(sim, packed1, packed1, dims, centroidDP, correction1, correction1);
            var expectedScore2 = luceneScore(sim, packed2, packed2, dims, centroidDP, correction2, correction2);

            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(dims, 4, centroid, centroidDP, in, sim.function());
                var scoreSupplier = factory.getInt4VectorScorerSupplier(sim, in, values).get();
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

    static byte[] packNibbles(byte[] unpacked) {
        int packedLength = unpacked.length / 2;
        byte[] packed = new byte[packedLength];
        for (int i = 0; i < packedLength; i++) {
            packed[i] = (byte) ((unpacked[i] << 4) | (unpacked[i + packedLength] & 0x0F));
        }
        return packed;
    }

    static byte[] unpackNibbles(byte[] packed, int dims) {
        byte[] unpacked = new byte[dims];
        int packedLen = packed.length;
        for (int i = 0; i < packedLen; i++) {
            unpacked[i] = (byte) ((packed[i] & 0xFF) >>> 4);
            unpacked[i + packedLen] = (byte) (packed[i] & 0x0F);
        }
        return unpacked;
    }

    static int componentSumUnpacked(byte[] packed) {
        byte[] unpacked = unpackNibbles(packed, packed.length * 2);
        int sum = 0;
        for (byte value : unpacked) {
            sum += Byte.toUnsignedInt(value) & 0x0F;
        }
        return sum;
    }

    private static OptimizedScalarQuantizer.QuantizationResult randomCorrectionPacked(byte[] packed) {
        int componentSum = componentSumUnpacked(packed);
        float lowerInterval = randomFloat();
        float upperInterval = lowerInterval + randomFloat();
        return new OptimizedScalarQuantizer.QuantizationResult(lowerInterval, upperInterval, randomFloat(), componentSum);
    }

    static void writePackedVectorWithCorrection(IndexOutput out, byte[] packed, OptimizedScalarQuantizer.QuantizationResult correction)
        throws IOException {
        out.writeBytes(packed, 0, packed.length);
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
        return new DenseOffHeapInt4VectorValues(dims, size, sim, slice, centroid, centroidDP);
    }

    public float luceneScore(
        VectorSimilarityType similarityFunc,
        byte[] packedA,
        byte[] packedB,
        int dims,
        float centroidDP,
        OptimizedScalarQuantizer.QuantizationResult aCorrection,
        OptimizedScalarQuantizer.QuantizationResult bCorrection
    ) {
        OSQScorer scorer = OSQScorer.fromSimilarity(similarityFunc);
        return scorer.score(packedA, packedB, dims, centroidDP, aCorrection, bCorrection);
    }

    private abstract static class OSQScorer {
        static OSQScorer fromSimilarity(VectorSimilarityType sim) {
            return switch (sim) {
                case DOT_PRODUCT -> new DotProductOSQScorer();
                case MAXIMUM_INNER_PRODUCT -> new MaxInnerProductOSQScorer();
                case EUCLIDEAN -> new EuclideanOSQScorer();
                default -> throw new IllegalArgumentException("Unsupported similarity: " + sim);
            };
        }

        final float score(
            byte[] packedA,
            byte[] packedB,
            int dims,
            float centroidDP,
            OptimizedScalarQuantizer.QuantizationResult aCorrection,
            OptimizedScalarQuantizer.QuantizationResult bCorrection
        ) {
            byte[] unpackedB = unpackNibbles(packedB, dims);
            float rawDot = VectorUtil.int4DotProductSinglePacked(unpackedB, packedA);
            float ax = aCorrection.lowerInterval();
            float lx = (aCorrection.upperInterval() - ax) * LIMIT_SCALE;
            float ay = bCorrection.lowerInterval();
            float ly = (bCorrection.upperInterval() - ay) * LIMIT_SCALE;
            float y1 = bCorrection.quantizedComponentSum();
            float x1 = aCorrection.quantizedComponentSum();
            float score = ax * ay * dims + ay * lx * x1 + ax * ly * y1 + lx * ly * rawDot;
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

    static byte[] vector(int ord, int dims) {
        var random = new Random(Objects.hash(ord, dims));
        byte[] unpacked = new byte[dims];
        for (int i = 0; i < dims; i++) {
            unpacked[i] = (byte) RandomNumbers.randomIntBetween(random, MIN_INT4_VALUE, MAX_INT4_VALUE);
        }
        return packNibbles(unpacked);
    }

    static IntFunction<float[]> FLOAT_ARRAY_RANDOM_FUNC = size -> {
        float[] fa = new float[size];
        for (int i = 0; i < size; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    };

    static IntFunction<byte[]> BYTE_ARRAY_RANDOM_INT4_FUNC = dims -> {
        byte[] unpacked = new byte[dims];
        for (int i = 0; i < dims; i++) {
            unpacked[i] = (byte) randomIntBetween(MIN_INT4_VALUE, MAX_INT4_VALUE);
        }
        return packNibbles(unpacked);
    };

    static IntFunction<byte[]> BYTE_ARRAY_MAX_INT4_FUNC = dims -> {
        byte[] unpacked = new byte[dims];
        Arrays.fill(unpacked, MAX_INT4_VALUE);
        return packNibbles(unpacked);
    };

    static IntFunction<byte[]> BYTE_ARRAY_MIN_INT4_FUNC = dims -> {
        byte[] unpacked = new byte[dims];
        Arrays.fill(unpacked, MIN_INT4_VALUE);
        return packNibbles(unpacked);
    };

    static final int TIMES = 100;

    static class DenseOffHeapInt4VectorValues extends QuantizedByteVectorValues {
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

        DenseOffHeapInt4VectorValues(
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
            this.byteSize = dimension / 2 + (Float.BYTES * 3) + Integer.BYTES;
            this.byteBuffer = ByteBuffer.allocate(dimension / 2);
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
            return Lucene104ScalarQuantizedVectorsFormat.ScalarEncoding.PACKED_NIBBLE;
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
            return new DenseOffHeapInt4VectorValues(dimension, size, similarityFunction, slice.clone(), centroid, centroidDp);
        }
    }
}
