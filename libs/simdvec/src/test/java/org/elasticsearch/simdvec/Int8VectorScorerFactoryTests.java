/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.codecs.lucene95.OffHeapByteVectorValues;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.searchablesnapshots.store.SearchableSnapshotDirectoryFactory;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.Random;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.simdvec.VectorSimilarityType.COSINE;
import static org.elasticsearch.simdvec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.simdvec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.simdvec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.simdvec.internal.vectorization.JdkFeatures.SUPPORTS_HEAP_SEGMENTS;
import static org.hamcrest.Matchers.closeTo;

public class Int8VectorScorerFactoryTests extends AbstractVectorTestCase {

    private static final double DELTA = 1e-6;

    @BeforeClass
    public static void requiresHeapSegments() {
        assumeTrue("scorer only supported on JDK 22+", SUPPORTS_HEAP_SEGMENTS);
    }

    // Tests that the provider instance is present or not on expected platforms/architectures
    public void testSupport() {
        supported();
    }

    public void testZeros() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        try (Directory dir = new MMapDirectory(createTempDir("testZeros"), MMapDirectory.DEFAULT_MAX_CHUNK_SIZE)) {
            testRandomSupplier(dir, byte[]::new, DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT);
        }
    }

    public void testRandomMMap() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        try (Directory dir = new MMapDirectory(createTempDir("testRandomMMap"), MMapDirectory.DEFAULT_MAX_CHUNK_SIZE)) {
            testRandomSupplier(dir, ESTestCase::randomByteArrayOfLength, VectorSimilarityType.values());
        }
    }

    public void testRandomNIO() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        try (Directory dir = new NIOFSDirectory(createTempDir("testRandomNIO"))) {
            testRandomSupplier(dir, ESTestCase::randomByteArrayOfLength, VectorSimilarityType.values());
        }
    }

    public void testRandomMaxChunkSizeSmall() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        long maxChunkSize = randomLongBetween(32, 128);
        logger.info("maxChunkSize=" + maxChunkSize);
        try (Directory dir = new MMapDirectory(createTempDir("testRandomMaxChunkSizeSmall"), maxChunkSize)) {
            testRandomSupplier(dir, ESTestCase::randomByteArrayOfLength, VectorSimilarityType.values());
        }
    }

    void testRandomSupplier(Directory dir, IntFunction<byte[]> bytesSupplier, VectorSimilarityType... types) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = randomIntBetween(1, 4096);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];

        String fileName = "testRandom-" + dir.getClass().getSimpleName() + "-" + dims;
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

                    var supplier = factory.getInt8VectorScorerSupplier(sim, in, values).get();
                    var scorer = supplier.scorer();
                    scorer.setScoringOrdinal(idx0);

                    // scale the delta to the magnitude of the score
                    double expectedDelta = expected * DELTA;
                    assertThat(sim.toString(), (double) scorer.score(idx1), closeTo(expected, expectedDelta));
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

                        var supplier = factory.getInt8VectorScorerSupplier(sim, in, values).get();
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

    public void testSupplierBulkWithMMap() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        try (var dir = new MMapDirectory(createTempDir("testBulkWithMMap"))) {
            testSupplierBulkImpl(dir);
        }
    }

    public void testSupplierBulkWithNIO() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        try (var dir = new NIOFSDirectory(createTempDir("testBulkWithNIO"))) {
            testSupplierBulkImpl(dir);
        }
    }

    private void testSupplierBulkImpl(Directory dir) throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        final int dims = randomIntBetween(1, 4096);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];
        String fileName = "testBulk-" + dir.getClass().getSimpleName() + "-" + dims;
        logger.info("Testing " + fileName);
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
            for (int i = 0; i < size; i++) {
                byte[] vec = randomByteArrayOfLength(dims);
                out.writeBytes(vec, vec.length);
                vectors[i] = vec;
            }
            CodecUtil.writeFooter(out);
        }
        List<Integer> ids = IntStream.range(0, size).boxed().collect(Collectors.toList());
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
            for (int times = 0; times < TIMES; times++) {
                int idx0 = randomIntBetween(0, size - 1);
                int[] nodes = shuffledList(ids).stream().mapToInt(i -> i).toArray();
                for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, MAXIMUM_INNER_PRODUCT)) {
                    var values = vectorValues(dims, size, in, sim.function());
                    float[] expected = new float[size];
                    float[] scores = new float[size];
                    for (int i = 0; i < size; i++) {
                        expected[i] = luceneScore(sim, vectors[idx0], vectors[nodes[i]]);
                    }
                    var supplier = factory.getInt8VectorScorerSupplier(sim, in, values).get();
                    var scorer = supplier.scorer();
                    scorer.setScoringOrdinal(idx0);
                    scorer.bulkScore(nodes, scores, nodes.length);
                    for (int i = 0; i < size; i++) {
                        double expectedDelta = Math.max(Math.abs(expected[i]) * DELTA, DELTA);
                        assertThat(sim.toString(), (double) scores[i], closeTo(expected[i], expectedDelta));
                        // assert single scoring returns the same expected score as bulk
                        assertThat(sim.toString(), (double) scorer.score(nodes[i]), closeTo(expected[i], expectedDelta));
                    }
                }
            }
        }
    }

    // -- Query-side scorer tests (Int8VectorScorer via getInt8VectorScorer, JDK 22+) --
    // These test the query scorer which accepts both MMap and DirectAccessInput (SNAP).

    public void testScorerWithMMap() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        try (var dir = new MMapDirectory(createTempDir("testScorerWithMMap"))) {
            testScorerImpl(dir);
        }
    }

    public void testScorerWithSNAP() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        try (var dir = SearchableSnapshotDirectoryFactory.newDirectory(createTempDir("testScorerWithSNAP"))) {
            testScorerImpl(dir);
        }
    }

    private void testScorerImpl(Directory dir) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();
        final int dims = randomIntBetween(1, 4096);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];
        final byte[] queryVector = randomByteArrayOfLength(dims);

        String fileName = "testScorerImpl-" + dir.getClass().getSimpleName() + "-" + dims;
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
            for (int i = 0; i < size; i++) {
                byte[] vec = randomByteArrayOfLength(dims);
                out.writeBytes(vec, vec.length);
                vectors[i] = vec;
            }
            CodecUtil.writeFooter(out);
        }
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
            for (int times = 0; times < TIMES; times++) {
                int idx = randomIntBetween(0, size - 1);
                for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, COSINE, MAXIMUM_INNER_PRODUCT)) {
                    var values = vectorValues(dims, size, in, sim.function());
                    float expected = luceneScore(sim, queryVector, vectors[idx]);
                    var scorer = factory.getInt8VectorScorer(sim.function(), values, queryVector).get();
                    double expectedDelta = Math.max(Math.abs(expected) * DELTA, DELTA);
                    assertThat(sim.toString(), (double) scorer.score(idx), closeTo(expected, expectedDelta));
                }
            }
        }
    }

    public void testScorerBulkWithMMap() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        try (var dir = new MMapDirectory(createTempDir("testScorerBulkWithMMap"))) {
            testScorerBulkImpl(dir);
        }
    }

    public void testScorerBulkFallback() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        // Small chunk size forces multi-segment mmap; segmentSliceOrNull(0, length) returns null,
        // so bulkScoreWithSparse falls back to super.bulkScore() (one-at-a-time scoring).
        try (var dir = new MMapDirectory(createTempDir("testScorerBulkFallback"), 32)) {
            testScorerBulkImpl(dir);
        }
    }

    public void testScorerBulkWithSNAP() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        try (var dir = SearchableSnapshotDirectoryFactory.newDirectory(createTempDir("testScorerBulkWithSNAP"))) {
            testScorerBulkImpl(dir);
        }
    }

    private void testScorerBulkImpl(Directory dir) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();
        final int dims = randomIntBetween(64, 4096);
        final int size = randomIntBetween(2, 100);
        final byte[][] vectors = new byte[size][];
        final byte[] queryVector = randomByteArrayOfLength(dims);

        String fileName = "testScorerBulk-" + dir.getClass().getSimpleName() + "-" + dims;
        try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
            for (int i = 0; i < size; i++) {
                byte[] vec = randomByteArrayOfLength(dims);
                out.writeBytes(vec, vec.length);
                vectors[i] = vec;
            }
            CodecUtil.writeFooter(out);
        }
        List<Integer> ids = IntStream.range(0, size).boxed().collect(Collectors.toList());
        try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
            for (int times = 0; times < TIMES; times++) {
                int[] nodes = shuffledList(ids).stream().mapToInt(i -> i).toArray();
                for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, COSINE, MAXIMUM_INNER_PRODUCT)) {
                    var values = vectorValues(dims, size, in, sim.function());
                    float[] expected = new float[size];
                    float[] scores = new float[size];
                    for (int i = 0; i < size; i++) {
                        expected[i] = luceneScore(sim, queryVector, vectors[nodes[i]]);
                    }
                    var scorer = factory.getInt8VectorScorer(sim.function(), values, queryVector).get();
                    scorer.bulkScore(nodes, scores, nodes.length);
                    for (int i = 0; i < size; i++) {
                        double expectedDelta = Math.max(Math.abs(expected[i]) * DELTA, DELTA);
                        assertThat(sim.toString(), (double) scores[i], closeTo(expected[i], expectedDelta));
                        // assert single scoring returns the same expected score as bulk
                        assertThat(sim.toString(), (double) scorer.score(nodes[i]), closeTo(expected[i], expectedDelta));
                    }
                }
            }
        }
    }

    // Verifies that bulkScore with zero nodes returns NEGATIVE_INFINITY without throwing,
    // as Lucene's exactSearch path can call bulkScore with an empty batch when filters exclude all docs.
    public void testScorerBulkWithZeroNodes() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        assumeTrue("scorer only supported on JDK 22+", Runtime.version().feature() >= 22);
        var factory = AbstractVectorTestCase.factory.get();
        final int dims = randomIntBetween(64, 4096);
        final int size = randomIntBetween(2, 100);
        final byte[] queryVector = randomByteArrayOfLength(dims);

        try (var dir = new MMapDirectory(createTempDir("testScorerBulkWithZeroNodes"))) {
            String fileName = "testScorerBulkWithZeroNodes-" + dims;
            try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                for (int i = 0; i < size; i++) {
                    byte[] vec = randomByteArrayOfLength(dims);
                    out.writeBytes(vec, vec.length);
                }
                CodecUtil.writeFooter(out);
            }
            try (IndexInput in = dir.openInput(fileName, IOContext.DEFAULT)) {
                for (var sim : List.of(DOT_PRODUCT, EUCLIDEAN, COSINE, MAXIMUM_INNER_PRODUCT)) {
                    var values = vectorValues(dims, size, in, sim.function());
                    var scorer = factory.getInt8VectorScorer(sim.function(), values, queryVector).get();
                    float result = scorer.bulkScore(new int[0], new float[0], 0);
                    assertEquals(Float.NEGATIVE_INFINITY, result, 0f);
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
}
