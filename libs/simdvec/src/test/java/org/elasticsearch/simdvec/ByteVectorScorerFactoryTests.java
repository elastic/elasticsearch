/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.IntFunction;

import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.hamcrest.Matchers.closeTo;

public class ByteVectorScorerFactoryTests extends AbstractVectorTestCase {

    private static final double DELTA = 1e-6;

    // Tests that the provider instance is present or not on expected platforms/architectures
    public void testSupport() {
        supported();
    }

    public void testZeros() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, byte[]::new, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT);
    }

    public void testRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandomSupplier(MMapDirectory.DEFAULT_MAX_CHUNK_SIZE, ESTestCase::randomByteArrayOfLength, VectorSimilarityType.values());
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        testRandomSupplier(maxChunkSize, ESTestCase::randomByteArrayOfLength, VectorSimilarityType.values());
    }

    public void testArrayBackedRandomSupplier() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Not supported on current JDK", supportsHeapSegments());
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = randomIntBetween(1, 1024);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];
        for (int i = 0; i < size; i++) {
            vectors[i] = randomByteArrayOfLength(dims);
        }
        final ByteVectorValues values = arrayBackedVectorValues(vectors);

        for (int times = 0; times < TIMES; times++) {
            int idx0 = randomIntBetween(0, size - 1);
            int idx1 = randomIntBetween(0, size - 1);
            for (var sim : VectorSimilarityType.values()) {
                float expected = luceneScore(sim, vectors[idx0], vectors[idx1]);
                var scorerSupplier = factory.getByteVectorScorerSupplier(sim, values);
                assertTrue(scorerSupplier.isPresent());
                var scorer = scorerSupplier.get().scorer();
                scorer.setScoringOrdinal(idx0);

                double expectedDelta = expected * DELTA;
                assertThat(sim.toString(), (double) scorer.score(idx1), closeTo(expected, expectedDelta));
            }
        }
    }

    public void testArrayBackedRandomSupplierBulkScore() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("Not supported on current JDK", supportsHeapSegments());
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = randomIntBetween(1, 1024);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];
        for (int i = 0; i < size; i++) {
            vectors[i] = randomByteArrayOfLength(dims);
        }
        final ByteVectorValues values = arrayBackedVectorValues(vectors);

        for (int times = 0; times < TIMES; times++) {
            final int queryOrd = randomIntBetween(0, size - 1);
            final int numNodes = randomIntBetween(1, size);
            final int[] nodes = new int[numNodes];
            for (int i = 0; i < numNodes; i++) {
                nodes[i] = randomIntBetween(0, size - 1);
            }

            for (var sim : VectorSimilarityType.values()) {
                final var scorerSupplier = factory.getByteVectorScorerSupplier(sim, values);
                assertTrue(scorerSupplier.isPresent());
                final var scorer = scorerSupplier.get().scorer();
                scorer.setScoringOrdinal(queryOrd);

                final float[] scores = new float[numNodes];
                scorer.bulkScore(nodes, scores, numNodes);

                for (int i = 0; i < numNodes; i++) {
                    float expected = luceneScore(sim, vectors[queryOrd], vectors[nodes[i]]);
                    double expectedDelta = expected * DELTA;
                    assertThat(sim.toString(), (double) scores[i], closeTo(expected, expectedDelta));
                }
            }
        }
    }

    void testRandomSupplier(long maxChunkSize, IntFunction<byte[]> bytesSupplier, VectorSimilarityType... types) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testRandom"), maxChunkSize)) {
            final int dims = randomIntBetween(1, 4096);
            final int size = randomIntBetween(2, 100);
            final byte[][] vectors = new byte[size][];

            String fileName = "testRandom-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    byte[] vec = bytesSupplier.apply(dims);
                    out.writeBytes(vec, vec.length);
                    vectors[i] = vec;
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    for (var sim : types) {
                        var values = vectorValues(dims, size, in, sim.function());
                        float expected = luceneScore(sim, vectors[idx0], vectors[idx1]);

                        var supplier = factory.getByteVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);

                        // scale the delta to the magnitude of the score
                        double expectedDelta = expected * DELTA;
                        assertThat(sim.toString(), (double) scorer.score(idx1), closeTo(expected, expectedDelta));
                    }
                }
            }
        }
    }

    // Test that the scorer works well when the IndexInput is greater than the directory segment chunk size
    public void testDatasetGreaterThanChunkSize() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir("testDatasetGreaterThanChunkSize"), 8192)) {
            final int dims = 1024;
            final int size = 128;

            String fileName = "testDatasetGreaterThanChunkSize-" + dims;
            logger.info("Testing " + fileName);
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    var vec = vector(i, dims);
                    out.writeBytes(vec, vec.length);
                }
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (int times = 0; times < TIMES; times++) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = size - 1;
                    // not COSINE, as we normalize vectors to always use dot product
                    for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                        var values = vectorValues(dims, size, in, sim.function());
                        float expected = luceneScore(sim, vector(idx0, dims), vector(idx1, dims));

                        var supplier = factory.getByteVectorScorerSupplier(sim, in, values).get();
                        var scorer = supplier.scorer();
                        scorer.setScoringOrdinal(idx0);

                        // scale the delta to the magnitude of the score
                        double expectedDelta = expected * DELTA;
                        assertThat(sim.toString(), (double) scorer.score(idx1), closeTo(expected, expectedDelta));
                    }
                }
            }
        }
    }

    static ByteVectorValues vectorValues(int dims, int size, IndexInput in, VectorSimilarityFunction sim) {
        return new OffHeapByteVectorValues.DenseOffHeapVectorValues(dims, size, in, dims, null, sim);
    }

    static float luceneScore(VectorSimilarityType similarityFunc, byte[] a, byte[] b) {
        return similarityFunc.function().compare(a, b);
    }

    // creates the vector based on the given ordinal, which is reproducible given the ord and dims
    static byte[] vector(int ord, int dims) {
        var random = new Random(Objects.hash(ord, dims));
        byte[] vec = new byte[dims];
        random.nextBytes(vec);
        return vec;
    }

    static final int TIMES = 100; // a loop iteration times

    static ByteVectorValues arrayBackedVectorValues(byte[][] vectors) {
        return new ByteVectorValues() {
            @Override
            public int dimension() {
                return vectors[0].length;
            }

            @Override
            public int size() {
                return vectors.length;
            }

            @Override
            public byte[] vectorValue(int targetOrd) {
                return vectors[targetOrd];
            }

            @Override
            public ByteVectorValues copy() {
                return this;
            }
        };
    }
}
