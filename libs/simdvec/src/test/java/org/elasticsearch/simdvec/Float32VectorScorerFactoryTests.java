/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.codecs.hnsw.DefaultFlatVectorScorer;
import org.apache.lucene.codecs.lucene95.OffHeapFloatVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.hnsw.UpdateableRandomVectorScorer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.VectorSimilarityType.COSINE;
import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.test.hamcrest.OptionalMatchers.isEmpty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class Float32VectorScorerFactoryTests extends AbstractVectorTestCase {

    static final double BASE_DELTA = 1e-5;

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
            for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                for (int dims : List.of(31, 32, 33)) {
                    // dimensions that cross the scalar / native boundary (stride)
                    float[] vector1 = new float[dims];
                    float[] vector2 = new float[dims];
                    String fileName = "testSimpleImpl-" + sim + "-" + dims + ".vex";
                    try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                        for (int i = 0; i < dims; i++) {
                            vector1[i] = (float) i;
                            vector2[i] = (float) (dims - i);
                        }
                        writeFloat32Vectors(out, vector1, vector2);
                    }
                    try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                        var values = vectorValues(dims, 2, in, VectorSimilarityType.of(sim));
                        float expected = luceneFloat32Score(sim, vector1, vector2);

                        var luceneSupplier = luceneScoreSupplier(values, VectorSimilarityType.of(sim)).scorer();
                        luceneSupplier.setScoringOrdinal(0);
                        assertEquals(expected, luceneSupplier.score(1), BASE_DELTA);
                        var supplier = factory.getFloat32VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(0);
                        assertEquals(expected, scorer.score(1), BASE_DELTA);

                        if (supportsHeapSegments()) {
                            var qScorer = factory.getFloat32VectorScorer(VectorSimilarityType.of(sim), values, vector1).get();
                            assertEquals(expected, qScorer.score(1), BASE_DELTA);
                        }
                    }
                }
            }
        }
    }

    public void testNonNegative() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testNonNegativeDotProduct"), MMapDirectory.DEFAULT_MAX_CHUNK_SIZE)) {
            float[] vec1 = new float[32];
            float[] vec2 = new float[32];
            String fileName = "testNonNegativeDotProduct-32";
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                vec1[0] = -2.0f;  // values to trigger a negative dot product
                vec2[0] = 1.0f;
                writeFloat32Vectors(out, vec1, vec2);
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(32, 2, in, VectorSimilarityType.of(DOT_PRODUCT));
                // dot product
                float expected = 0.0f;
                assertThat(luceneFloat32Score(DOT_PRODUCT, vec1, vec2), equalTo(expected));
                var supplier = factory.getFloat32VectorScorerSupplier(DOT_PRODUCT, in, values).get();
                var scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                // max inner product
                expected = luceneFloat32Score(MAXIMUM_INNER_PRODUCT, vec1, vec2);
                supplier = factory.getFloat32VectorScorerSupplier(MAXIMUM_INNER_PRODUCT, in, values).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                assertThat(scorer.score(1), equalTo(expected));
                // cosine
                expected = 0f;
                assertThat(luceneFloat32Score(COSINE, vec1, vec2), equalTo(expected));
                supplier = factory.getFloat32VectorScorerSupplier(COSINE, in, values).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
                // euclidean
                expected = luceneFloat32Score(EUCLIDEAN, vec1, vec2);
                supplier = factory.getFloat32VectorScorerSupplier(EUCLIDEAN, in, values).get();
                scorer = supplier.scorer();
                scorer.setScoringOrdinal(0);
                assertThat(scorer.score(1), equalTo(expected));
                assertThat(scorer.score(1), greaterThanOrEqualTo(0f));
            }
        }
    }

    public void testRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomWithChunkSize(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE);
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomWithChunkSize(maxChunkSize);
    }

    void testRandomWithChunkSize(long maxChunkSize) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            final int dims = randomIntBetween(1, 4096);
            final int size = randomIntBetween(2, 100);
            final float[][] vectors = IntStream.range(0, size).mapToObj(i -> randomVector(dims)).toArray(float[][]::new);
            final double delta = BASE_DELTA * dims; // scale the delta with the size

            String fileName = "testRandom-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                writeFloat32Vectors(out, vectors);
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                        float expected = luceneFloat32Score(sim, vectors[idx0], vectors[idx1]);
                        var supplier = factory.getFloat32VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);
                        assertEquals(expected, scorer.score(idx1), delta);

                        if (supportsHeapSegments()) {
                            var qScorer = factory.getFloat32VectorScorer(VectorSimilarityType.of(sim), values, vectors[idx0]).get();
                            assertEquals(expected, qScorer.score(idx1), delta);
                        }
                    }
                }
            }
        }
    }

    public void testRandomSlice() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSliceImpl(30, 64, 1);
    }

    void testRandomSliceImpl(int dims, long maxChunkSize, int initialPadding) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandomSliceImpl"), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int size = randomIntBetween(2, 100);
                final float[][] vectors = IntStream.range(0, size).mapToObj(i -> randomVector(dims)).toArray(float[][]::new);
                final double delta = BASE_DELTA * dims; // scale the delta with the size

                String fileName = "testRandomSliceImpl-" + times + "-" + dims;
                logger.info("Testing " + fileName);
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    byte[] ba = new byte[initialPadding];
                    out.writeBytes(ba, 0, ba.length);
                    writeFloat32Vectors(out, vectors);
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
                            float expected = luceneFloat32Score(sim, vectors[idx0], vectors[idx1]);
                            var supplier = factory.getFloat32VectorScorerSupplier(sim, in, values).get();
                            var scorer = supplier.scorer();
                            scorer.setScoringOrdinal(idx0);
                            assertEquals(expected, scorer.score(idx1), delta);

                            if (supportsHeapSegments()) {
                                var qScorer = factory.getFloat32VectorScorer(VectorSimilarityType.of(sim), values, vectors[idx0]).get();
                                assertEquals(expected, qScorer.score(idx1), delta);
                            }
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
            final int dims = 4096;
            final int size = 262144;
            assert (long) dims * Float.BYTES * size > Integer.MAX_VALUE;
            final double delta = BASE_DELTA * dims; // scale the delta with the size

            String fileName = "testLarge-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    writeFloat32Vectors(out, vector(i, dims));
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = size - 1;
                    for (var sim : List.of(COSINE, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, in, VectorSimilarityType.of(sim));
                        float expected = luceneFloat32Score(sim, vector(idx0, dims), vector(idx1, dims));
                        var supplier = factory.getFloat32VectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);
                        assertEquals(expected, scorer.score(idx1), delta);

                        if (supportsHeapSegments()) {
                            var qScorer = factory.getFloat32VectorScorer(VectorSimilarityType.of(sim), values, vector(idx0, dims)).get();
                            assertEquals(expected, qScorer.score(idx1), delta);
                        }
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
        float[] vec1 = new float[dims];
        float[] vec2 = new float[dims];
        IntStream.range(0, dims).forEach(i -> vec1[i] = 1);
        IntStream.range(0, dims).forEach(i -> vec2[i] = 2);
        try (Directory dir = new MMapDirectory(createTempDir("testRace"), maxChunkSize)) {
            String fileName = "testRace-" + dims;
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                writeFloat32Vectors(out, vec1, vec1, vec2, vec2);
            }
            var expectedScore1 = luceneFloat32Score(sim, vec1, vec1);
            var expectedScore2 = luceneFloat32Score(sim, vec2, vec2);

            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                var values = vectorValues(dims, 4, in, VectorSimilarityType.of(sim));
                var scoreSupplier = factory.getFloat32VectorScorerSupplier(sim, in, values).get();
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

    FloatVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) throws IOException {
        var slice = in.slice("values", 0, in.length());
        var byteSize = dims * Float.BYTES;
        return new OffHeapFloatVectorValues.DenseOffHeapVectorValues(dims, size, slice, byteSize, DefaultFlatVectorScorer.INSTANCE, sim);
    }

    /** Computes the score using the Lucene implementation. */
    static float luceneFloat32Score(VectorSimilarityType similarityFunc, float[] a, float[] b) {
        return VectorSimilarityType.of(similarityFunc).compare(a, b);
    }

    RandomVectorScorerSupplier luceneScoreSupplier(KnnVectorValues values, VectorSimilarityFunction sim) throws IOException {
        return DefaultFlatVectorScorer.INSTANCE.getRandomVectorScorerSupplier(sim, values);
    }

    static float[] randomVector(int dim) {
        float[] v = new float[dim];
        Random random = random();
        for (int i = 0; i < dim; i++) {
            v[i] = random.nextFloat();
        }
        return v;
    }

    // creates the vector based on the given ordinal, which is reproducible given the ord and dims
    static float[] vector(int ord, int dims) {
        var random = new Random(Objects.hash(ord, dims));
        float[] fa = new float[dims];
        for (int i = 0; i < dims; i++) {
            fa[i] = random.nextFloat();
        }
        return fa;
    }

    static void writeFloat32Vectors(IndexOutput out, float[]... vectors) throws IOException {
        var buffer = ByteBuffer.allocate(vectors[0].length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (var v : vectors) {
            buffer.asFloatBuffer().put(v);
            out.writeBytes(buffer.array(), buffer.array().length);
        }
    }

    static final int TIMES = 100; // a loop iteration times
}
