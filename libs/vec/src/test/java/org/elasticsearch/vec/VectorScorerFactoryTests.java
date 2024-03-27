/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.MMapDirectory;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.vec.VectorSimilarityType.COSINE;
import static org.elasticsearch.vec.VectorSimilarityType.DOT_PRODUCT;
import static org.elasticsearch.vec.VectorSimilarityType.EUCLIDEAN;
import static org.elasticsearch.vec.VectorSimilarityType.MAXIMUM_INNER_PRODUCT;
import static org.hamcrest.Matchers.equalTo;

// @com.carrotsearch.randomizedtesting.annotations.Repeat(iterations = 100)
public class VectorScorerFactoryTests extends AbstractVectorTestCase {

    // Tests that the provider instance is present or not on expected platforms/architectures
    public void testSupport() {
        supported();
    }

    public void testDotProductSimple() throws IOException {
        testSimple(DOT_PRODUCT);
    }

    public void testCosineSimple() throws IOException {
        testSimple(COSINE);
    }

    public void testMaxInnerProductSimple() throws IOException {
        testSimple(MAXIMUM_INNER_PRODUCT);
    }

    @AwaitsFix(bugUrl = "http://") // TODO:
    public void testEuclideanSimple() throws IOException {
        testSimple(EUCLIDEAN);
    }

    void testSimple(VectorSimilarityType similarityType) throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir(getTestName() + "-" + similarityType))) {
            for (int dims : List.of(31, 32, 33)) {
                // dimensions that cross the scalar / native boundary (stride)
                byte[] vec1 = new byte[dims];
                byte[] vec2 = new byte[dims];
                String fileName = getTestName() + "-" + similarityType + "-" + dims;
                try (IndexOutput out = dir.createOutput(fileName, IOContext.DEFAULT)) {
                    for (int i = 0; i < dims; i++) {
                        vec1[i] = (byte) i;
                        vec2[i] = (byte) (dims - i);
                    }
                    var oneFactor = floatToByteArray(1f);
                    byte[] bytes = concat(vec1, oneFactor, vec2, oneFactor);
                    out.writeBytes(bytes, 0, bytes.length);
                }
                final IndexInput in = dir.openInput(fileName, IOContext.DEFAULT);
                try (var scorer = factory.getScalarQuantizedVectorScorer(dims, 2, 1, similarityType, in).get()) {
                    float expected = luceneScore(similarityType, vec1, vec2, 1, 1, 1);
                    assertThat(scorer.score(0, 1), equalTo(expected));
                }
            }
        }
    }

    public void testDotProductRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(DOT_PRODUCT, ESTestCase::randomByteArrayOfLength);
    }

    public void testDotProductRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(DOT_PRODUCT, BYTE_ARRAY_MAX_FUNC);
    }

    public void testDotProductRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(DOT_PRODUCT, BYTE_ARRAY_MIN_FUNC);
    }

    public void testMaxInnerProductRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MAXIMUM_INNER_PRODUCT, ESTestCase::randomByteArrayOfLength);
    }

    public void testMaxInnerProductRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MAXIMUM_INNER_PRODUCT, BYTE_ARRAY_MAX_FUNC);
    }

    public void testMaxInnerProductRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(MAXIMUM_INNER_PRODUCT, BYTE_ARRAY_MIN_FUNC);
    }

    public void testCosineRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(COSINE, ESTestCase::randomByteArrayOfLength);
    }

    public void testCosineRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(COSINE, BYTE_ARRAY_MAX_FUNC);
    }

    public void testCosineRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(COSINE, BYTE_ARRAY_MIN_FUNC);
    }

    @AwaitsFix(bugUrl = "http://") // TODO:
    public void testEuclideanRandom() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(EUCLIDEAN, ESTestCase::randomByteArrayOfLength);
    }

    @AwaitsFix(bugUrl = "http://") // TODO:
    public void testEuclideanRandomMax() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(EUCLIDEAN, BYTE_ARRAY_MAX_FUNC);
    }

    @AwaitsFix(bugUrl = "http://") // TODO:
    public void testEuclideanRandomMin() throws IOException {
        assumeTrue(notSupportedMsg(), supported());
        testRandom(EUCLIDEAN, BYTE_ARRAY_MIN_FUNC);
    }

    void testRandom(VectorSimilarityType similarityType, Function<Integer, byte[]> byteArraySupplier) throws IOException {
        var factory = AbstractVectorTestCase.factory.get();

        try (Directory dir = new MMapDirectory(createTempDir(getTestName() + "-" + similarityType))) {
            for (int times = 0; times < TIMES; times++) {
                final int dims = randomIntBetween(1, 4096);
                final int size = randomIntBetween(2, 100);
                final float correction = randomFloat();
                final byte[][] vectors = new byte[size][];
                final float[] offsets = new float[size];

                String fileName = getTestName() + "-" + similarityType + "-" + times + "-" + dims;
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
                final IndexInput in = dir.openInput(fileName, IOContext.DEFAULT);

                try (var scorer = factory.getScalarQuantizedVectorScorer(dims, size, correction, similarityType, in).get()) {
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    float expected = luceneScore(similarityType, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                }
            }
        }
    }

    static Function<Integer, byte[]> BYTE_ARRAY_MAX_FUNC = size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, Byte.MAX_VALUE);
        return ba;
    };

    static Function<Integer, byte[]> BYTE_ARRAY_MIN_FUNC = size -> {
        byte[] ba = new byte[size];
        Arrays.fill(ba, Byte.MIN_VALUE);
        return ba;
    };

    static final int TIMES = 100; // a loop iteration times
}
