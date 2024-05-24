/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import com.carrotsearch.randomizedtesting.generators.RandomNumbers;

import org.apache.lucene.codecs.lucene99.OffHeapQuantizedByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.quantization.RandomAccessQuantizedByteVectorValues;

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

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.elasticsearch.vec.VectorSimilarityType.COSINE;
import static org.elasticsearch.vec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.vec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.vec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
public class VectorScorerFactoryTests extends AbstractVectorTestCase {

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

        try (Directory dir = new MMapDirectory(createTempDir("testSimpleImpl"), maxChunkSize)) {
            for (int dims : List.of(31, 32, 33)) {
                // dimensions that cross the scalar / native boundary (stride)
                byte[] vec1 = new byte[dims];
                byte[] vec2 = new byte[dims];
                String fileName = "testSimpleImpl" + "-" + dims;
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    for (int i = 0; i < dims; i++) {
                        vec1[i] = (byte) i;
                        vec2[i] = (byte) (dims - i);
                    }
                    var oneFactor = floatToByteArray(1f);
                    byte[] bytes = concat(vec1, oneFactor, vec2, oneFactor);
                    out.writeBytes(bytes, 0, bytes.length);
                }
                try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, 2, in, VectorSimilarityType.of(sim));
                        float expected = luceneScore(sim, vec1, vec2, 1, 1, 1);
                        var supplier = factory.getInt7ScalarQuantizedVectorScorer(sim, in, values, 1).get();
                        assertThat(supplier.scorer(0).score(1), equalTo(expected));
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
                float expected = 0f; // TODO fix in Lucene: https://github.com/apache/lucene/pull/13356 luceneScore(DOT_PRODUCT, vec1, vec2,
                                     // 1, -5, -5);
                var supplier = factory.getInt7ScalarQuantizedVectorScorer(DOT_PRODUCT, in, values, 1).get();
                assertThat(supplier.scorer(0).score(1), equalTo(expected));
                assertThat(supplier.scorer(0).score(1), greaterThanOrEqualTo(0f));
                // max inner product
                expected = luceneScore(MAXIMUM_INNER_PRODUCT, vec1, vec2, 1, -5, -5);
                supplier = factory.getInt7ScalarQuantizedVectorScorer(MAXIMUM_INNER_PRODUCT, in, values, 1).get();
                assertThat(supplier.scorer(0).score(1), greaterThanOrEqualTo(0f));
                assertThat(supplier.scorer(0).score(1), equalTo(expected));
                // cosine
                expected = 0f; // TODO fix in Lucene: https://github.com/apache/lucene/pull/13356 luceneScore(COSINE, vec1, vec2, 1, -5,
                               // -5);
                supplier = factory.getInt7ScalarQuantizedVectorScorer(COSINE, in, values, 1).get();
                assertThat(supplier.scorer(0).score(1), equalTo(expected));
                assertThat(supplier.scorer(0).score(1), greaterThanOrEqualTo(0f));
                // euclidean
                expected = luceneScore(EUCLIDEAN, vec1, vec2, 1, -5, -5);
                supplier = factory.getInt7ScalarQuantizedVectorScorer(EUCLIDEAN, in, values, 1).get();
                assertThat(supplier.scorer(0).score(1), equalTo(expected));
                assertThat(supplier.scorer(0).score(1), greaterThanOrEqualTo(0f));
            }
        }
    }

    public void testRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_RANDOM_INT7_FUNC);
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandom(maxChunkSize, BYTE_ARRAY_RANDOM_INT7_FUNC);
    }

    public void testRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MAX_INT7_FUNC);
    }

    public void testRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, BYTE_ARRAY_MIN_INT7_FUNC);
    }

    void testRandom(long maxChunkSize, Function<Integer, byte[]> byteArraySupplier) throws IOException {
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
                        var supplier = factory.getInt7ScalarQuantizedVectorScorer(sim, in, values, correction).get();
                        assertThat(supplier.scorer(idx0).score(idx1), equalTo(expected));
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
                            var supplier = factory.getInt7ScalarQuantizedVectorScorer(sim, in, values, correction).get();
                            assertThat(supplier.scorer(idx0).score(idx1), equalTo(expected));
                        }
                    }
                }
            }
        }
    }

    // Tests with a large amount of data (> 2GB), which ensures that data offsets do not overflow
    @Nightly
    public void testLarge() throws IOException {
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
                        var supplier = factory.getInt7ScalarQuantizedVectorScorer(sim, in, values, correction).get();
                        assertThat(supplier.scorer(idx0).score(idx1), equalTo(expected));
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
                var scoreSupplier = factory.getInt7ScalarQuantizedVectorScorer(sim, in, values, 1f).get();
                var tasks = List.<Callable<Optional<Throwable>>>of(
                    new ScoreCallable(scoreSupplier.copy().scorer(0), 1, expectedScore1),
                    new ScoreCallable(scoreSupplier.copy().scorer(2), 3, expectedScore2)
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

        final RandomVectorScorer scorer;
        final int ord;
        final float expectedScore;

        ScoreCallable(RandomVectorScorer scorer, int ord, float expectedScore) {
            this.scorer = scorer;
            this.ord = ord;
            this.expectedScore = expectedScore;
        }

        @Override
        public Optional<Throwable> call() throws Exception {
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

    RandomAccessQuantizedByteVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        return new OffHeapQuantizedByteVectorValues.DenseOffHeapVectorValues(dims, size, in.slice("values", 0, in.length()));
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
