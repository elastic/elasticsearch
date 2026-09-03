/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.vectors.script;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.script.field.vectors.BFloat16RankVectors;
import org.elasticsearch.script.field.vectors.BitRankVectors;
import org.elasticsearch.script.field.vectors.ByteRankVectors;
import org.elasticsearch.script.field.vectors.FloatRankVectors;
import org.elasticsearch.script.field.vectors.RankVectors;
import org.elasticsearch.script.field.vectors.VectorIterator;
import org.elasticsearch.test.ESTestCase;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.function.IntFunction;

public class RankVectorsTests extends ESTestCase {

    public void testByteUnsupported() {
        int count = randomIntBetween(1, 16);
        int dims = randomIntBetween(1, 16);
        byte[][] docVector = new byte[count][dims];
        float[][] queryVector = new float[count][dims];
        for (int i = 0; i < docVector.length; i++) {
            random().nextBytes(docVector[i]);
            for (int j = 0; j < dims; j++) {
                queryVector[i][j] = randomFloat();
            }
        }

        RankVectors knn = newByteVector(docVector);
        UnsupportedOperationException e;

        e = expectThrows(UnsupportedOperationException.class, () -> knn.maxSimDotProduct(queryVector));
        assertEquals(e.getMessage(), "use [float maxSimDotProduct(byte[][] queryVector)] instead");
    }

    public void testFloatUnsupported() {
        int count = randomIntBetween(1, 16);
        int dims = randomIntBetween(1, 16);
        float[][] docVector = new float[count][dims];
        byte[][] queryVector = new byte[count][dims];
        for (int i = 0; i < docVector.length; i++) {
            random().nextBytes(queryVector[i]);
            for (int j = 0; j < dims; j++) {
                docVector[i][j] = randomFloat();
            }
        }

        RankVectors knn = newFloatVector(docVector);

        UnsupportedOperationException e = expectThrows(UnsupportedOperationException.class, () -> knn.maxSimDotProduct(queryVector));
        assertEquals(e.getMessage(), "use [float maxSimDotProduct(float[][] queryVector)] instead");
    }

    /**
     * Anchors the max-sim semantics on a hand-computed example: each query vector contributes only its best match
     * against the document's vectors, and those per-query maxima are summed.
     */
    public void testMaxSimDotProductSumsPerQueryMaxima() {
        float[][] docVectors = { { 1, 0 }, { 0, 1 } };
        // dot products per query vector: [2, 0] -> max 2, [0, 3] -> max 3, [1, 1] -> max 1
        float[][] queryVectors = { { 2, 0 }, { 0, 3 }, { 1, 1 } };

        assertEquals(6.0f, newFloatVector(docVectors).maxSimDotProduct(queryVectors), 0.0f);
    }

    public void testFloatMaxSimDotProduct() {
        int dims = randomIntBetween(1, 64);
        float[][] docVectors = randomFloatVectors(randomIntBetween(1, 16), dims);
        float[][] queryVectors = randomFloatVectors(randomIntBetween(1, 16), dims);

        float expected = maxSim(queryVectors, docVectors, RankVectorsTests::dotProduct);
        assertEquals(expected, newFloatVector(docVectors).maxSimDotProduct(queryVectors), delta(expected));

        // bfloat16 doc values are already decoded by the time they reach the scorer, so truncating them the way
        // decoding would leaves the same arithmetic against the same reference
        for (float[] docVector : docVectors) {
            for (int i = 0; i < dims; i++) {
                docVector[i] = BFloat16.truncateToBFloat16(docVector[i]);
            }
        }
        expected = maxSim(queryVectors, docVectors, RankVectorsTests::dotProduct);
        assertEquals(expected, newBFloat16Vector(docVectors).maxSimDotProduct(queryVectors), delta(expected));
    }

    public void testByteMaxSimDotProduct() {
        int dims = randomIntBetween(1, 64);
        byte[][] docVectors = randomByteVectors(randomIntBetween(1, 16), dims);
        byte[][] queryVectors = randomByteVectors(randomIntBetween(1, 16), dims);

        // byte dot products are exact in float, so no tolerance is needed
        float expected = maxSim(queryVectors, docVectors, RankVectorsTests::dotProduct);
        assertEquals(expected, newByteVector(docVectors).maxSimDotProduct(queryVectors), 0.0f);
    }

    /**
     * Bit vectors accept three query shapes: another bit vector, scored as the population count of the AND, and a byte
     * or float vector with one value per bit, which uses the stored vector as a mask over the query's dimensions.
     */
    public void testBitMaxSimDotProduct() {
        int numBytes = randomIntBetween(1, 8);
        int numQueryVectors = randomIntBetween(1, 16);
        byte[][] docVectors = randomByteVectors(randomIntBetween(1, 16), numBytes);
        RankVectors bitVectors = newBitVector(docVectors, numBytes);

        byte[][] bitQuery = randomByteVectors(numQueryVectors, numBytes);
        float expected = maxSim(bitQuery, docVectors, RankVectorsTests::andBitCount);
        assertEquals(expected, bitVectors.maxSimDotProduct(bitQuery), 0.0f);

        byte[][] byteQuery = randomByteVectors(numQueryVectors, numBytes * Byte.SIZE);
        expected = maxSim(byteQuery, docVectors, RankVectorsTests::maskedSum);
        assertEquals(expected, bitVectors.maxSimDotProduct(byteQuery), 0.0f);

        float[][] floatQuery = randomFloatVectors(numQueryVectors, numBytes * Byte.SIZE);
        expected = maxSim(floatQuery, docVectors, RankVectorsTests::maskedSum);
        assertEquals(expected, bitVectors.maxSimDotProduct(floatQuery), delta(expected));
    }

    /**
     * Byte and bit vectors share one implementation, which derives the bit count from the stored byte length, so both
     * have to agree with the same reference.
     */
    public void testMaxSimInvHamming() {
        int dims = randomIntBetween(1, 64);
        int numQueryVectors = randomIntBetween(1, 16);
        byte[][] docVectors = randomByteVectors(randomIntBetween(1, 16), dims);
        byte[][] queryVectors = randomByteVectors(numQueryVectors, dims);

        float expected = maxSim(queryVectors, docVectors, (q, d) -> invHamming(q, d, dims * Byte.SIZE));
        assertEquals(expected, newByteVector(docVectors).maxSimInvHamming(queryVectors), 0.0f);
        assertEquals(expected, newBitVector(docVectors, dims).maxSimInvHamming(queryVectors), 0.0f);
    }

    /**
     * The implementations rewind the vector iterator and reuse a per-query scratch buffer on each call, so scoring the
     * same instance again - in particular with a shorter query vector - must not pick up stale state.
     */
    public void testScoringWithReusedScratchBuffers() {
        float[][] docVectors = { { 1, 0 }, { 0, 1 } };
        float[][] wideQuery = { { 2, 0 }, { 0, 3 }, { 1, 1 } };
        float[][] narrowQuery = { { 2, 0 } };

        RankVectors floatVectors = newFloatVector(docVectors);
        assertEquals(6.0f, floatVectors.maxSimDotProduct(wideQuery), 0.0f);
        assertEquals(6.0f, floatVectors.maxSimDotProduct(wideQuery), 0.0f);
        assertEquals(2.0f, floatVectors.maxSimDotProduct(narrowQuery), 0.0f);

        byte[][] bitDocs = { { 0b0000_0011 }, { 0b0000_0101 } };
        byte[][] wideBitQuery = { { 0b0000_0111 }, { 0b0000_0001 }, { 0b0000_0100 } };
        byte[][] narrowBitQuery = { { 0b0000_0111 } };

        RankVectors bitVectors = newBitVector(bitDocs, 1);
        assertEquals(maxSim(wideBitQuery, bitDocs, RankVectorsTests::andBitCount), bitVectors.maxSimDotProduct(wideBitQuery), 0.0f);
        assertEquals(maxSim(narrowBitQuery, bitDocs, RankVectorsTests::andBitCount), bitVectors.maxSimDotProduct(narrowBitQuery), 0.0f);
        assertEquals(
            maxSim(narrowBitQuery, bitDocs, (q, d) -> invHamming(q, d, Byte.SIZE)),
            bitVectors.maxSimInvHamming(narrowBitQuery),
            0.0f
        );
    }

    // Reference implementations. These are deliberately naive so that they are an independent oracle for the
    // vectorized production code.

    private interface Similarity<Q, D> {
        float apply(Q query, D doc);
    }

    private static <Q, D> float maxSim(Q[] queryVectors, D[] docVectors, Similarity<Q, D> similarity) {
        float sum = 0;
        for (Q queryVector : queryVectors) {
            float max = Float.NEGATIVE_INFINITY;
            for (D docVector : docVectors) {
                max = Math.max(max, similarity.apply(queryVector, docVector));
            }
            sum += max;
        }
        return sum;
    }

    private static float dotProduct(float[] query, float[] doc) {
        float sum = 0;
        for (int i = 0; i < query.length; i++) {
            sum += query[i] * doc[i];
        }
        return sum;
    }

    private static float dotProduct(byte[] query, byte[] doc) {
        int sum = 0;
        for (int i = 0; i < query.length; i++) {
            sum += query[i] * doc[i];
        }
        return sum;
    }

    private static float andBitCount(byte[] query, byte[] doc) {
        int sum = 0;
        for (int i = 0; i < doc.length; i++) {
            sum += Integer.bitCount((query[i] & doc[i]) & 0xFF);
        }
        return sum;
    }

    /** Sums the query dimensions whose corresponding bit is set in the doc vector, most significant bit first. */
    private static float maskedSum(byte[] query, byte[] doc) {
        int sum = 0;
        for (int i = 0; i < doc.length; i++) {
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                if (((doc[i] >> (Byte.SIZE - 1 - bit)) & 1) == 1) {
                    sum += query[i * Byte.SIZE + bit];
                }
            }
        }
        return sum;
    }

    private static float maskedSum(float[] query, byte[] doc) {
        float sum = 0;
        for (int i = 0; i < doc.length; i++) {
            for (int bit = 0; bit < Byte.SIZE; bit++) {
                if (((doc[i] >> (Byte.SIZE - 1 - bit)) & 1) == 1) {
                    sum += query[i * Byte.SIZE + bit];
                }
            }
        }
        return sum;
    }

    private static float invHamming(byte[] query, byte[] doc, int bitCount) {
        int differing = 0;
        for (int i = 0; i < doc.length; i++) {
            differing += Integer.bitCount((query[i] ^ doc[i]) & 0xFF);
        }
        return (bitCount - differing) / (float) bitCount;
    }

    private static float delta(float expected) {
        return Math.max(1e-4f, Math.abs(expected) * 1e-4f);
    }

    private static float[][] randomFloatVectors(int count, int dims) {
        float[][] vectors = new float[count][dims];
        for (int i = 0; i < count; i++) {
            for (int j = 0; j < dims; j++) {
                vectors[i][j] = randomFloat();
            }
        }
        return vectors;
    }

    private static byte[][] randomByteVectors(int count, int dims) {
        byte[][] vectors = new byte[count][dims];
        for (int i = 0; i < count; i++) {
            random().nextBytes(vectors[i]);
        }
        return vectors;
    }

    static RankVectors newFloatVector(float[][] vector) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new FloatRankVectors(VectorIterator.from(vector), magnitudes, vector.length, vector[0].length);
    }

    static RankVectors newBFloat16Vector(float[][] vector) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new BFloat16RankVectors(VectorIterator.from(vector), magnitudes, vector.length, vector[0].length, null);
    }

    static RankVectors newByteVector(byte[][] vector) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new ByteRankVectors(VectorIterator.from(vector), magnitudes, vector.length, vector[0].length);
    }

    /** @param numBytes the number of bytes per vector, i.e. an eighth of the field's dimension count */
    static RankVectors newBitVector(byte[][] vector, int numBytes) {
        BytesRef magnitudes = magnitudes(vector.length, i -> (float) Math.sqrt(VectorUtil.dotProduct(vector[i], vector[i])));
        return new BitRankVectors(VectorIterator.from(vector), magnitudes, vector.length, numBytes);
    }

    static BytesRef magnitudes(int count, IntFunction<Float> magnitude) {
        ByteBuffer magnitudeBuffer = ByteBuffer.allocate(count * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (int i = 0; i < count; i++) {
            magnitudeBuffer.putFloat(magnitude.apply(i));
        }
        return new BytesRef(magnitudeBuffer.array());
    }
}
