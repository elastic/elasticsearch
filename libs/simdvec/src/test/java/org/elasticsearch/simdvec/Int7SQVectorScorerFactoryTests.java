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

import org.apache.lucene.codecs.lucene99.Lucene99ScalarQuantizedVectorScorer;
import org.apache.lucene.codecs.lucene99.OffHeapQuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizedVectorSimilarity;
import org.apache.lucene.util.quantization.ScalarQuantizer;

import java.io.IOException;
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
import java.util.stream.IntStream;

import static org.apache.lucene.codecs.hnsw.ScalarQuantizedVectorScorer.quantizeQuery;
import static org.elasticsearch.simdvec.VectorSimilarityType.COSINE;
import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class Int7SQVectorScorerFactoryTests extends AbstractVectorTestCase {

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
        var factory = AbstractVectorTestCase.factory.get();
        var scalarQuantizer = new ScalarQuantizer(0.1f, 0.9f, (byte) 7);

        try (Directory dir = new MMapDirectory(createTempDir("testSimpleImpl"), maxChunkSize)) {
            for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                for (int dims : List.of(31, 32, 33)) {
                    // dimensions that cross the scalar / native boundary (stride)
                    byte[] vec1 = new byte[dims];
                    byte[] vec2 = new byte[dims];
                    float[] query1 = new float[dims];
                    float[] query2 = new float[dims];
                    float vec1Correction, vec2Correction;
                    String fileName = "testSimpleImpl-" + sim + "-" + dims + ".vex";
                    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                        for (int i = 0; i < dims; i++) {
                            query1[i] = (float) i;
                            query2[i] = (float) (dims - i);
                        }
                        vec1Correction = quantizeQuery(query1, vec1, VectorSimilarityType.of(sim), scalarQuantizer);
                        vec2Correction = quantizeQuery(query2, vec2, VectorSimilarityType.of(sim), scalarQuantizer);
                        byte[] bytes = concat(vec1, floatToByteArray(vec1Correction), vec2, floatToByteArray(vec2Correction));
                        out.writeBytes(bytes, 0, bytes.length);
                    }
                    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                        var values = vectorValues(dims, 2, in, VectorSimilarityType.of(sim));
                        float scc = values.getScalarQuantizer().getConstantMultiplier();
                        float expected = luceneScore(sim, vec1, vec2, scc, vec1Correction, vec2Correction);

                        var luceneSupplier = luceneScoreSupplier(values, VectorSimilarityType.of(sim)).scorer();
                        luceneSupplier.setScoringOrdinal(0);
                        assertThat(luceneSupplier.score(1), equalTo(expected));
                        var supplier = factory.getInt7SQVectorScorerSupplier(sim, in, values, scc).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(0);
                        assertThat(scorer.score(1), equalTo(expected));

                        if (supportsHeapSegments()) {
                            var qScorer = factory.getInt7SQVectorScorer(VectorSimilarityType.of(sim), values, query1).get();
                            assertThat(qScorer.score(1), equalTo(expected));
                        }
                    }
                }
            }
        }
    }

    public void testNonNegativeDotProduct() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testNonNegativeDotProduct"), MMapDirectory.DEFAULT_MAX_CHUNK_SIZE)) {
            // keep vecs `0` so dot product is `0`
            byte[] vec1 = new byte[32];
            byte[] vec2 = new byte[32];
            String fileName = "testNonNegativeDotProduct-32";
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                var negativeOffset = floatToByteArray(-5f);
                byte[] bytes = concat(vec1, negativeOffset, vec2, negativeOffset);
                out.writeBytes(bytes, 0, bytes.length);
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(32, 2, in, VectorSimilarityType.of(DOT_PRODUCT));
                // dot product
                float expected = 0f;
                assertThat(luceneScore(DOT_PRODUCT, vec1, vec2, 1, -5, -5), equalTo(expected));
                var supplier = factory.getInt7SQVectorScorerSupplier(DOT_PRODUCT, in, values, 1).get();
                var scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                // max inner product
                expected = luceneScore(MAXIMUM_INNER_PRODUCT, vec1, vec2, 1, -5, -5);
                supplier = factory.getInt7SQVectorScorerSupplier(MAXIMUM_INNER_PRODUCT, in, values, 1).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                assertThat(scorer.score(1), equalTo(expected));
                // cosine
                expected = 0f;
                assertThat(luceneScore(COSINE, vec1, vec2, 1, -5, -5), equalTo(expected));
                supplier = factory.getInt7SQVectorScorerSupplier(COSINE, in, values, 1).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                // euclidean
                expected = luceneScore(EUCLIDEAN, vec1, vec2, 1, -5, -5);
                supplier = factory.getInt7SQVectorScorerSupplier(EUCLIDEAN, in, values, 1).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
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
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            final int dims = randomIntBetween(1, 4096);
            final int size = randomIntBetween(2, 100);
            final float correction = randomFloat();
            final byte[][] vectors = new byte[size][];
            final float[] offsets = new float[size];

            String fileName = "testRandom-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = byteArraySupplier.apply(dims);
                    var off = randomFloat();
                    out.writeBytes(vec, 0, vec.length);
                    out.writeInt(Float.floatToIntBits(off));
                    vectors[i] = vec;
                    offsets[i] = off;
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                        float expected = luceneScore(sim, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                        var supplier = factory.getInt7SQVectorScorerSupplier(sim, in, values, correction).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);
                        assertThat(scorer.score(idx1), equalTo(expected));
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

    void testRandomScorerImpl(long maxChunkSize, Function<Integer, float[]> floatArraySupplier) throws IOException {
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();
        var scalarQuantizer = new ScalarQuantizer(0.1f, 0.9f, (byte) 7);

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                // Use the random supplier for COSINE, which returns values in the normalized range
                floatArraySupplier = sim == COSINE ? FLOAT_ARRAY_RANDOM_FUNC : floatArraySupplier;
                final int dims = randomIntBetween(1, 4096);
                final int size = randomIntBetween(2, 100);
                final float[][] vectors = new float[size][];
                final byte[][] qVectors = new byte[size][];
                final float[] corrections = new float[size];

                float delta = 1e-6f * dims;

                String fileName = "testRandom-" + sim + "-" + dims + ".vex";
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    for (int i = 0; i < size; i++) {
                        vectors[i] = floatArraySupplier.apply(dims);
                        qVectors[i] = new byte[dims];
                        corrections[i] = quantizeQuery(vectors[i], qVectors[i], VectorSimilarityType.of(sim), scalarQuantizer);
                        out.writeBytes(qVectors[i], 0, dims);
                        out.writeBytes(floatToByteArray(corrections[i]), 0, 4);
                    }
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    for (int times = 0; times < TIMES; times++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1);
                        var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                        var correction = scalarQuantizer.getConstantMultiplier();

                        var expected = luceneScore(sim, qVectors[idx0], qVectors[idx1], correction, corrections[idx0], corrections[idx1]);
                        var scorer = factory.getInt7SQVectorScorer(VectorSimilarityType.of(sim), values, vectors[idx0]).get();
                        assertEquals(scorer.score(idx1), expected, delta);
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
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandomSliceImpl"), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int size = randomIntBetween(2, 100);
                final float correction = randomFloat();
                final byte[][] vectors = new byte[size][];
                final float[] offsets = new float[size];

                String fileName = "testRandomSliceImpl-" + times + "-" + dims;
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[initialPadding];
                    out.writeBytes(ba, 0, ba.length);
                    for (int i = 0; i < size; i++) {
                        var vec = byteArraySupplier.apply(dims);
                        var off = randomFloat();
                        out.writeBytes(vec, 0, vec.length);
                        out.writeInt(Float.floatToIntBits(off));
                        vectors[i] = vec;
                        offsets[i] = off;
                    }
                }
                try (
                    var outter = dir.openInput(fileName, IOContext.DEFAULT);
                    var in = outter.slice("slice", initialPadding, outter.length() - initialPadding)
                ) {
                    for (int itrs = 0; itrs < TIMES / 10; itrs++) {
                        int idx0 = randomIntBetween(0, size - 1);
                        int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                        for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                            var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                            float expected = luceneScore(sim, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                            var supplier = factory.getInt7SQVectorScorerSupplier(sim, in, values, correction).get();
                            var scorer = supplier.scorer();
                            scorer.setScoringOrdinal(idx0);
                            assertThat(scorer.score(idx1), equalTo(expected));
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
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testLarge"))) {
            final int dims = 8192;
            final int size = 262144;
            final float correction = randomFloat();

            String fileName = "testLarge-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    var off = (float) i;
                    out.writeBytes(vec, 0, vec.length);
                    out.writeInt(Float.floatToIntBits(off));
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = size - 1;
                    float off0 = (float) idx0;
                    float off1 = (float) idx1;
                    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                        float expected = luceneScore(sim, vector(idx0, dims), vector(idx1, dims), correction, off0, off1);
                        var supplier = factory.getInt7SQVectorScorerSupplier(sim, in, values, correction).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);
                        assertThat(scorer.score(idx1), equalTo(expected));
                    }
                }
            }
        }
    }

    public void testRace() throws Exception {
        testRaceImpl(COSINE);
        testRaceImpl(DOT_PRODUCT);
        testRaceImpl(EUCLIDEAN);
        testRaceImpl(MAXIMUM_INNER_PRODUCT);
    }

    // Tests that copies in threads do not interfere with each other
    void testRaceImpl(VectorSimilarityType sim) throws Exception {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        final long maxChunkSize = 32;
        final int dims = 34; // dimensions that are larger than the chunk size, to force fallback
        byte[] vec1 = new byte[dims];
        byte[] vec2 = new byte[dims];
        IntStream.range(0, dims).forEach(i -> vec1[i] = 1);
        IntStream.range(0, dims).forEach(i -> vec2[i] = 2);
        try (Directory dir = new MMapDirectory(createTempDir("testRace"), maxChunkSize)) {
            String fileName = "testRace-" + dims;
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                var one = floatToByteArray(1f);
                byte[] bytes = concat(vec1, one, vec1, one, vec2, one, vec2, one);
                out.writeBytes(bytes, 0, bytes.length);
            }
            var expectedScore1 = luceneScore(sim, vec1, vec1, 1, 1, 1);
            var expectedScore2 = luceneScore(sim, vec2, vec2, 1, 1, 1);

            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(dims, 4, in, VectorSimilarityType.of(sim));
                var scoreSupplier = factory.getInt7SQVectorScorerSupplier(sim, in, values, 1f).get();
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
                    assertThat(scorer.score(ord), equalTo(expectedScore));
                }
            } catch (Throwable t) {
                return Optional.of(t);
            }
            return Optional.empty();
        }
    }

    QuantizedByteVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var sq = new ScalarQuantizer(0.1f, 0.9f, (byte) 7);
        var slice = in.slice("values", 0, in.length());
        return new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(dims, size, sq, false, sim, null, slice);
    }

    /** Computes the score using the Lucene implementation. */
    public static float luceneScore(
        VectorSimilarityType similarityFunc,
        byte[] a,
        byte[] b,
        float correction,
        float aOffsetValue,
        float bOffsetValue
    ) {
        var scorer = ScalarQuantizedVectorSimilarity.fromVectorSimilarity(VectorSimilarityType.of(similarityFunc), correction, (byte) 7);
        return scorer.score(a, aOffsetValue, b, bOffsetValue);
    }

    RandomVectorScorerSupplier luceneScoreSupplier(QuantizedByteVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return new Lucene99ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
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

    static Function<Integer, float[]> FLOAT_ARRAY_MAX_FUNC = size -> {
        float[] fa = new float[size];
        Arrays.fill(fa, Float.MAX_VALUE);
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
}
