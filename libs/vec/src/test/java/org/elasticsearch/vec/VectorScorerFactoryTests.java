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

        try (Directory dir = new MMapDirectory(createTempDir(getTestName()), maxChunkSize)) {
            for (int dims : List.of(31, 32, 33)) {
                // dimensions that cross the scalar / native boundary (stride)
                byte[] vec1 = new byte[dims];
                byte[] vec2 = new byte[dims];
                String fileName = getTestName() + "-" + dims;
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
                    // dot product
                    float expected = luceneScore(DOT_PRODUCT, vec1, vec2, 1, 1, 1);
                    var scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, 2, 1, DOT_PRODUCT, in).get();
                    assertThat(scorer.score(0, 1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(0).score(1), equalTo(expected));
                    // max inner product
                    expected = luceneScore(MAXIMUM_INNER_PRODUCT, vec1, vec2, 1, 1, 1);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, 2, 1, MAXIMUM_INNER_PRODUCT, in).get();
                    assertThat(scorer.score(0, 1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(0).score(1), equalTo(expected));
                    // cosine
                    expected = luceneScore(COSINE, vec1, vec2, 1, 1, 1);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, 2, 1, COSINE, in).get();
                    assertThat(scorer.score(0, 1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(0).score(1), equalTo(expected));
                    // euclidean
                    expected = luceneScore(EUCLIDEAN, vec1, vec2, 1, 1, 1);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, 2, 1, EUCLIDEAN, in).get();
                    assertThat(scorer.score(0, 1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(0).score(1), equalTo(expected));
                }
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

        try (Directory dir = new MMapDirectory(createTempDir(getTestName()), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int dims = randomIntBetween(1, 4096);
                final int size = randomIntBetween(2, 100);
                final float correction = randomFloat();
                final byte[][] vectors = new byte[size][];
                final float[] offsets = new float[size];

                String fileName = getTestName() + "-" + times + "-" + dims;
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
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    // dot product
                    float expected = luceneScore(DOT_PRODUCT, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    var scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, DOT_PRODUCT, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // max inner product
                    expected = luceneScore(MAXIMUM_INNER_PRODUCT, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, MAXIMUM_INNER_PRODUCT, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // cosine
                    expected = luceneScore(COSINE, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, COSINE, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // euclidean
                    expected = luceneScore(EUCLIDEAN, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, EUCLIDEAN, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
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

        try (Directory dir = new MMapDirectory(createTempDir(getTestName()), maxChunkSize)) {
            for (int times = 0; times < TIMES; times++) {
                final int size = randomIntBetween(2, 100);
                final float correction = randomFloat();
                final byte[][] vectors = new byte[size][];
                final float[] offsets = new float[size];

                String fileName = getTestName() + "-" + times + "-" + dims;
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
                    int idx0 = randomIntBetween(0, size - 1);
                    int idx1 = randomIntBetween(0, size - 1); // may be the same as idx0 - which is ok.
                    // dot product
                    float expected = luceneScore(DOT_PRODUCT, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    var scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, DOT_PRODUCT, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // max inner product
                    expected = luceneScore(MAXIMUM_INNER_PRODUCT, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, MAXIMUM_INNER_PRODUCT, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // cosine
                    expected = luceneScore(COSINE, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, COSINE, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                    // euclidean
                    expected = luceneScore(EUCLIDEAN, vectors[idx0], vectors[idx1], correction, offsets[idx0], offsets[idx1]);
                    scorer = factory.getInt7ScalarQuantizedVectorScorer(dims, size, correction, EUCLIDEAN, in).get();
                    assertThat(scorer.score(idx0, idx1), equalTo(expected));
                    assertThat((new VectorScorerSupplierAdapter(scorer)).scorer(idx0).score(idx1), equalTo(expected));
                }
            }
        }
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
