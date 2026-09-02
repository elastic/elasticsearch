/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.UnicodeUtil;
import org.elasticsearch.index.codec.vectors.BFloat16;
import org.elasticsearch.index.codec.vectors.BQVectorUtils;
import org.elasticsearch.index.codec.vectors.VectorTestUtils;
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.ShortBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;
import java.util.function.ToLongBiFunction;

import static org.elasticsearch.index.codec.vectors.VectorTestUtils.randomFloatVector;
import static org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport.B_QUERY;
import static org.hamcrest.Matchers.closeTo;

public class ESVectorUtilTests extends BaseVectorizationTests {

    static final ESVectorizationProvider defaultedProvider = BaseVectorizationTests.defaultProvider();
    static final ESVectorizationProvider panamaProvider = BaseVectorizationTests.panamaProvider();
    // only a few methods have a native version - use this when required
    static final ESVectorizationProvider nativeProvider = BaseVectorizationTests.nativeProvider();

    public void testMaxSimDotProductFloatDefaultEqualsPanama() {
        int dims = randomIntBetween(1, 512);
        int numDocVectors = randomIntBetween(1, 256);
        int numQueryVectors = randomIntBetween(1, 64);
        float[][] docVectors = generateRandomFloatVectors(numDocVectors, dims);
        float[][] queryVectors = generateRandomFloatVectors(numQueryVectors, dims);
        MultiFloatVectorsSource source = new TestMultiFloatVectorsSource(docVectors, encodeFloatVectors(docVectors), dims);
        float[] defaultScoresScratch = new float[numDocVectors];
        float[] defOrPanamaScoresScratch = new float[numDocVectors];
        float expected = defaultedProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defaultScoresScratch);
        float actual = panamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
        actual = nativeProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
    }

    public void testMaxSimDotProductBFloat16DefaultEqualsPanama() {
        int dims = randomIntBetween(1, 512);
        int numDocVectors = randomIntBetween(1, 256);
        int numQueryVectors = randomIntBetween(1, 64);
        float[][] docVectors = generateRandomBFloat16Vectors(numDocVectors, dims);
        float[][] queryVectors = generateRandomFloatVectors(numQueryVectors, dims);
        MultiBFloat16VectorsSource source = new TestMultiBFloat16VectorsSource(docVectors, encodeBFloat16Vectors(docVectors), dims);
        float[] defaultScoresScratch = new float[numDocVectors];
        float[] defOrPanamaScoresScratch = new float[numDocVectors];
        float expected = defaultedProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defaultScoresScratch);
        float actual = panamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
        actual = nativeProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
    }

    public void testMaxSimDotProductByteDefaultEqualsPanama() {
        int dims = randomIntBetween(1, 512);
        int numDocVectors = randomIntBetween(1, 256);
        int numQueryVectors = randomIntBetween(1, 64);
        byte[][] docVectors = generateRandomByteVectors(numDocVectors, dims);
        byte[][] queryVectors = generateRandomByteVectors(numQueryVectors, dims);
        MultiByteVectorsSource source = new TestMultiByteVectorsSource(docVectors, encodeByteVectors(docVectors), dims);
        float[] defaultScoresScratch = new float[numDocVectors];
        float[] defOrPanamaScoresScratch = new float[numDocVectors];
        float expected = defaultedProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defaultScoresScratch);
        float actual = panamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
        actual = nativeProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
    }

    public void testBFloat16ToFloat() {
        Random r = random();
        int dims = r.nextInt(1025);
        ByteOrder bo = randomFrom(ByteOrder.LITTLE_ENDIAN, ByteOrder.LITTLE_ENDIAN);
        float[] floats = new float[dims];
        byte[] bFloats = new byte[dims * BFloat16.BYTES];
        ShortBuffer bfloatShorts = ByteBuffer.wrap(bFloats).order(bo).asShortBuffer();
        for (int i = 0; i < dims; i++) {
            floats[i] = BFloat16.truncateToBFloat16(r.nextFloat());
            bfloatShorts.put(BFloat16.floatToBFloat16(floats[i]));
        }

        float[] defaultFloats = new float[dims];
        defaultedProvider.getVectorUtilSupport().bFloat16ToFloat(bFloats, 0, defaultFloats, 0, dims, bo);
        assertArrayEquals(floats, defaultFloats, 0f);

        float[] panamaFloats = new float[dims];
        panamaProvider.getVectorUtilSupport().bFloat16ToFloat(bFloats, 0, panamaFloats, 0, dims, bo);
        assertArrayEquals(floats, panamaFloats, 0f);
    }

    public void testFloatToBFloat16() {
        Random r = random();
        int dims = r.nextInt(1025);
        ByteOrder bo = randomFrom(ByteOrder.LITTLE_ENDIAN, ByteOrder.LITTLE_ENDIAN);
        float[] floats = new float[dims];
        byte[] bFloats = new byte[dims * BFloat16.BYTES];
        ShortBuffer bfloatShorts = ByteBuffer.wrap(bFloats).order(bo).asShortBuffer();
        for (int i = 0; i < dims; i++) {
            floats[i] = r.nextFloat();
            bfloatShorts.put(BFloat16.floatToBFloat16(floats[i]));
        }

        byte[] defaultBFloats = new byte[bFloats.length];
        defaultedProvider.getVectorUtilSupport().floatToBFloat16(floats, 0, defaultBFloats, 0, dims, bo);
        assertArrayEquals(bFloats, defaultBFloats);

        byte[] panamaBFloats = new byte[bFloats.length];
        panamaProvider.getVectorUtilSupport().floatToBFloat16(floats, 0, panamaBFloats, 0, dims, bo);
        assertArrayEquals(bFloats, panamaBFloats);
    }

    public void testIpByteBit() {
        byte[] d = new byte[random().nextInt(128)];
        byte[] q = new byte[d.length * 8];
        random().nextBytes(d);
        random().nextBytes(q);

        int sum = 0;
        for (int i = 0; i < q.length; i++) {
            if (((d[i / 8] << (i % 8)) & 0x80) == 0x80) {
                sum += q[i];
            }
        }

        assertEquals(sum, ESVectorUtil.ipByteBit(q, d));
        assertEquals(sum, defaultedProvider.getVectorUtilSupport().ipByteBit(q, d));
        assertEquals(sum, panamaProvider.getVectorUtilSupport().ipByteBit(q, d));
    }

    public void testIpFloatBit() {
        float[] q = VectorTestUtils.randomFloatVector(randomIntBetween(8, 128));
        byte[] d = new byte[(q.length + Byte.SIZE - 1) / Byte.SIZE];
        random().nextBytes(d);

        int qOffset = randomInt(q.length / 2) & -Byte.SIZE;  // multiple of 8
        int qLength = randomInt(q.length / 2 - randomInt(1));    // can be arbitrary
        int dOffset = randomInt(d.length - qLength / Byte.SIZE - 1);

        float sum = 0;
        for (int i = 0; i < qLength; i++) {
            if (((d[dOffset + i / 8] << (i % 8)) & 0x80) == 0x80) {
                sum += q[qOffset + i];
            }
        }

        double delta = 1e-5 * qLength;

        assertEquals(sum, ESVectorUtil.ipFloatBit(q, qOffset, d, dOffset, qLength), delta);
        assertEquals(sum, defaultedProvider.getVectorUtilSupport().ipFloatBit(q, qOffset, d, dOffset, qLength), delta);
        assertEquals(sum, panamaProvider.getVectorUtilSupport().ipFloatBit(q, qOffset, d, dOffset, qLength), delta);
    }

    public void testIpFloatByte() {
        int vectorSize = randomIntBetween(1, 1024);
        // scale the delta according to the vector size
        double delta = 1e-5 * vectorSize;

        float[] q = new float[vectorSize];
        byte[] d = new byte[vectorSize];
        for (int i = 0; i < q.length; i++) {
            q[i] = random().nextFloat();
        }
        random().nextBytes(d);

        float expected = 0;
        for (int i = 0; i < q.length; i++) {
            expected += q[i] * d[i];
        }
        assertThat((double) ESVectorUtil.ipFloatByte(q, d), closeTo(expected, delta));
        assertThat((double) defaultedProvider.getVectorUtilSupport().ipFloatByte(q, d), closeTo(expected, delta));
        assertThat((double) panamaProvider.getVectorUtilSupport().ipFloatByte(q, d), closeTo(expected, delta));
    }

    public void testIntBitAndCount() {
        testBasicBitAndImpl(ESVectorUtil::andBitCountInt);
    }

    public void testLongBitAndCount() {
        testBasicBitAndImpl(ESVectorUtil::andBitCountLong);
    }

    public void testIpByteBinInvariants() {
        int iterations = atLeast(10);
        for (int i = 0; i < iterations; i++) {
            int size = randomIntBetween(1, 10);
            var d = new byte[size];
            var q = new byte[size * B_QUERY - 1];
            expectThrows(IllegalArgumentException.class, () -> ESVectorUtil.ipByteBinByte(q, d));
        }
    }

    public void testBasicIpByteBin() {
        testBasicIpByteBinImpl(ESVectorUtil::ipByteBinByte);
        testBasicIpByteBinImpl(defaultedProvider.getVectorUtilSupport()::ipByteBinByte);
        testBasicIpByteBinImpl(panamaProvider.getVectorUtilSupport()::ipByteBinByte);
    }

    private interface BitAnd {
        int bitAnd(byte[] a, int aOffset, byte[] b, int bOffset, int length);
    }

    void testBasicBitAndImpl(BitAnd bitAnd) {
        assertEquals(0, bitAnd.bitAnd(new byte[] { 0 }, 0, new byte[] { 0 }, 0, 1));
        assertEquals(0, bitAnd.bitAnd(new byte[] { 1 }, 0, new byte[] { 0 }, 0, 1));
        assertEquals(0, bitAnd.bitAnd(new byte[] { 0 }, 0, new byte[] { 1 }, 0, 1));
        assertEquals(1, bitAnd.bitAnd(new byte[] { 1 }, 0, new byte[] { 1 }, 0, 1));
        byte[] a = new byte[33];
        byte[] b = new byte[33];
        random().nextBytes(a);
        random().nextBytes(b);
        int expected = scalarBitAnd(a, 1, b, 1, 31);
        assertEquals(expected, bitAnd.bitAnd(a, 1, b, 1, 31));
    }

    void testBasicIpByteBinImpl(ToLongBiFunction<byte[], byte[]> ipByteBinFunc) {
        assertEquals(15L, ipByteBinFunc.applyAsLong(new byte[] { 1, 1, 1, 1 }, new byte[] { 1 }));
        assertEquals(30L, ipByteBinFunc.applyAsLong(new byte[] { 1, 2, 1, 2, 1, 2, 1, 2 }, new byte[] { 1, 2 }));

        var d = new byte[] { 1, 2, 3 };
        var q = new byte[] { 1, 2, 3, 1, 2, 3, 1, 2, 3, 1, 2, 3 };
        assert scalarIpByteBin(q, d) == 60L; // 4 + 8 + 16 + 32
        assertEquals(60L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4 };
        q = new byte[] { 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4, 1, 2, 3, 4 };
        assert scalarIpByteBin(q, d) == 75L; // 5 + 10 + 20 + 40
        assertEquals(75L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4, 5 };
        q = new byte[] { 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5, 1, 2, 3, 4, 5 };
        assert scalarIpByteBin(q, d) == 105L; // 7 + 14 + 28 + 56
        assertEquals(105L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4, 5, 6 };
        q = new byte[] { 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6 };
        assert scalarIpByteBin(q, d) == 135L; // 9 + 18 + 36 + 72
        assertEquals(135L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4, 5, 6, 7 };
        q = new byte[] { 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7, 1, 2, 3, 4, 5, 6, 7 };
        assert scalarIpByteBin(q, d) == 180L; // 12 + 24 + 48 + 96
        assertEquals(180L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 };
        q = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8 };
        assert scalarIpByteBin(q, d) == 195L; // 13 + 26 + 52 + 104
        assertEquals(195L, ipByteBinFunc.applyAsLong(q, d));

        d = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        q = new byte[] { 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        assert scalarIpByteBin(q, d) == 225L; // 15 + 30 + 60 + 120
        assertEquals(225L, ipByteBinFunc.applyAsLong(q, d));
    }

    public void testIpByteBin() {
        testIpByteBinImpl(ESVectorUtil::ipByteBinByte);
        testIpByteBinImpl(defaultedProvider.getVectorUtilSupport()::ipByteBinByte);
        testIpByteBinImpl(panamaProvider.getVectorUtilSupport()::ipByteBinByte);
    }

    public void testSoarDistance() {
        int size = random().nextInt(128, 512);
        float deltaEps = 1e-3f * size;
        var vector = new float[size];
        var centroid = new float[size];
        var preResidual = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            centroid[i] = random().nextFloat();
            preResidual[i] = random().nextFloat();
        }
        float soarLambda = random().nextFloat();
        float rnorm = random().nextFloat();
        var expected = defaultedProvider.getVectorUtilSupport().soarDistance(vector, centroid, preResidual, soarLambda, rnorm);
        var result = panamaProvider.getVectorUtilSupport().soarDistance(vector, centroid, preResidual, soarLambda, rnorm);
        assertEquals(expected, result, deltaEps);
    }

    public void testSoarDistanceByte() {
        int size = random().nextInt(128, 512);
        var vector = new byte[size];
        var centroid = new byte[size];
        var preResidual = new float[size];
        random().nextBytes(vector);
        random().nextBytes(centroid);
        for (int i = 0; i < size; ++i) {
            preResidual[i] = random().nextFloat();
        }
        float soarLambda = random().nextFloat();
        float rnorm = random().nextFloat() + 0.01f; // avoid division by near-zero
        var expected = defaultedProvider.getVectorUtilSupport().soarDistance(vector, centroid, preResidual, soarLambda, rnorm);
        var result = panamaProvider.getVectorUtilSupport().soarDistance(vector, centroid, preResidual, soarLambda, rnorm);
        assertEquals(expected, result, Math.abs(expected) * 1e-5f + 1e-3f);
    }

    public void testSquareDistanceRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] a = randomFloatVector(vectorSize);
        float[] b = randomFloatVector(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        float actual = panamaProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testSquareDistanceRangeByte() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        byte[] a = randomByteArrayOfLength(vectorSize);
        byte[] b = randomByteArrayOfLength(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        float actual = panamaProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testSquareDistanceByteFloat() {
        int vectorSize = randomIntBetween(64, 2048);
        byte[] a = randomByteArrayOfLength(vectorSize);
        float[] b = randomFloatVector(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().squareDistance(a, b);
        float actual = panamaProvider.getVectorUtilSupport().squareDistance(a, b);
        assertEquals(expected, actual, Math.abs(expected) * 1e-5f);
        actual = nativeProvider.getVectorUtilSupport().squareDistance(a, b);
        assertEquals(expected, actual, Math.abs(expected) * 1e-5f);
    }

    public void testDotProductRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] a = randomFloatVector(vectorSize);
        float[] b = randomFloatVector(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().dotProduct(a, offset, b, offset, length);
        float actual = panamaProvider.getVectorUtilSupport().dotProduct(a, offset, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().dotProduct(a, offset, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testDotProductOffsetRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int aOffset = randomIntBetween(0, vectorSize - 1);
        int bOffset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - Math.max(aOffset, bOffset));
        float[] a = randomFloatVector(vectorSize);
        float[] b = randomFloatVector(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().dotProduct(a, aOffset, b, bOffset, length);
        float actual = panamaProvider.getVectorUtilSupport().dotProduct(a, aOffset, b, bOffset, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().dotProduct(a, aOffset, b, bOffset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testDotProductOffsetDifferentLengthArrays() {
        // Regression test: the offset-based dotProduct must work when a and b have different total
        // lengths. A previous short-circuit optimization incorrectly delegated to dotProduct(a, b)
        // which requires a.length == b.length.
        int aSize = randomIntBetween(16, 128);
        int bSize = randomIntBetween(aSize + 1, aSize * 4);
        int length = aSize; // dot over the full extent of a, but only a prefix of b
        float[] a = randomFloatVector(aSize);
        float[] b = randomFloatVector(bSize);
        // Manual reference dot product
        float expected = 0f;
        for (int i = 0; i < length; i++) {
            expected += a[i] * b[i];
        }
        float actual = defaultedProvider.getVectorUtilSupport().dotProduct(a, 0, b, 0, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = panamaProvider.getVectorUtilSupport().dotProduct(a, 0, b, 0, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().dotProduct(a, 0, b, 0, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testDotProductRangeByte() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        byte[] a = randomByteArrayOfLength(vectorSize);
        byte[] b = randomByteArrayOfLength(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().dotProduct(a, b, offset, length);
        float actual = panamaProvider.getVectorUtilSupport().dotProduct(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
        actual = nativeProvider.getVectorUtilSupport().dotProduct(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testL2NormalizePrefixDefaultEqualsPanama() {
        float[] expected = { 3f, 4f, 99f, 99f };
        float[] panama = expected.clone();
        defaultedProvider.getVectorUtilSupport().l2Normalize(expected, 0, 2);
        panamaProvider.getVectorUtilSupport().l2Normalize(panama, 0, 2);
        assertArrayEquals(expected, panama, 1e-5f);
        assertArrayEquals(new float[] { 0.6f, 0.8f, 99f, 99f }, expected, 1e-5f);
        float[] util = { 3f, 4f, 99f, 99f };
        ESVectorUtil.l2Normalize(util, 2);
        assertArrayEquals(expected, util, 1e-5f);
    }

    public void testL2NormalizePrefixZeroIsNoOp() {
        float[] v = { 0f, 0f, 5f };
        ESVectorUtil.l2Normalize(v, 2);
        assertArrayEquals(new float[] { 0f, 0f, 5f }, v, 0f);
    }

    public void testByteDotProductRangeDefaultEqualsPanama() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        byte[] a = randomByteArrayOfLength(vectorSize);
        byte[] b = randomByteArrayOfLength(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().dotProduct(a, b, offset, length);
        float actual = panamaProvider.getVectorUtilSupport().dotProduct(a, b, offset, length);
        assertEquals(expected, actual, 0f);
        assertEquals(expected, ESVectorUtil.dotProduct(a, b, offset, length), 0f);
    }

    public void testByteDotProductLengthMatchesFullWhenEqual() {
        int vectorSize = randomIntBetween(1, 128);
        byte[] a = randomByteArrayOfLength(vectorSize);
        byte[] b = randomByteArrayOfLength(vectorSize);
        assertEquals(ESVectorUtil.dotProduct(a, b), ESVectorUtil.dotProduct(a, b, vectorSize), 0f);
    }

    public void testByteL2NormalizePrefixDefaultEqualsPanama() {
        byte[] expected = { 3, 4, 99 };
        byte[] panama = expected.clone();
        defaultedProvider.getVectorUtilSupport().l2Normalize(expected, 0, 2);
        panamaProvider.getVectorUtilSupport().l2Normalize(panama, 0, 2);
        assertArrayEquals(expected, panama);
        assertArrayEquals(new byte[] { 0, 0, 99 }, expected);
        byte[] util = { 3, 4, 99 };
        ESVectorUtil.l2Normalize(util, 2);
        assertArrayEquals(expected, util);
    }

    public void testByteL2NormalizePrefixZeroIsNoOp() {
        byte[] v = { 0, 0, 5 };
        ESVectorUtil.l2Normalize(v, 2);
        assertArrayEquals(new byte[] { 0, 0, 5 }, v);
    }

    public void testL2NormalizeRangeDefaultEqualsPanama() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] expected = randomFloatVector(vectorSize);
        float[] panama = expected.clone();
        float[] util = expected.clone();
        defaultedProvider.getVectorUtilSupport().l2Normalize(expected, offset, length);
        panamaProvider.getVectorUtilSupport().l2Normalize(panama, offset, length);
        ESVectorUtil.l2Normalize(util, offset, length);
        assertArrayEquals(expected, panama, 1e-5f);
        assertArrayEquals(expected, util, 1e-5f);
    }

    public void testByteL2NormalizeRangeDefaultEqualsPanama() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        byte[] expected = randomByteArrayOfLength(vectorSize);
        byte[] panama = expected.clone();
        defaultedProvider.getVectorUtilSupport().l2Normalize(expected, offset, length);
        panamaProvider.getVectorUtilSupport().l2Normalize(panama, offset, length);
        assertArrayEquals(expected, panama);
    }

    public void testSquareDistanceBulkRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] query = randomFloatVector(vectorSize);
        float[] v0 = randomFloatVector(vectorSize);
        float[] v1 = randomFloatVector(vectorSize);
        float[] v2 = randomFloatVector(vectorSize);
        float[] v3 = randomFloatVector(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, v0, v1, v2, v3, 0, expectedDistances, length);
        panamaProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, v0, v1, v2, v3, 0, panamaDistances, length);
        assertArrayEquals(expectedDistances, panamaDistances, 1e-3f * length);
    }

    public void testSquareDistanceBulkRangeByte() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        byte[] query = randomByteArrayOfLength(vectorSize);
        byte[] v0 = randomByteArrayOfLength(vectorSize);
        byte[] v1 = randomByteArrayOfLength(vectorSize);
        byte[] v2 = randomByteArrayOfLength(vectorSize);
        byte[] v3 = randomByteArrayOfLength(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, v0, v1, v2, v3, 0, expectedDistances, length);
        panamaProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, v0, v1, v2, v3, 0, panamaDistances, length);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedDistances[i], panamaDistances[i], Math.abs(expectedDistances[i]) * 1e-3f);
        }
    }

    public void testDotProductBulk() {
        int vectorSize = randomIntBetween(1, 2048);
        float[] query = randomFloatVector(vectorSize);
        float[] v0 = randomFloatVector(vectorSize);
        float[] v1 = randomFloatVector(vectorSize);
        float[] v2 = randomFloatVector(vectorSize);
        float[] v3 = randomFloatVector(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().dotProductBulk(query, v0, v1, v2, v3, 0, expectedDistances);
        panamaProvider.getVectorUtilSupport().dotProductBulk(query, v0, v1, v2, v3, 0, panamaDistances);
        assertArrayEquals(expectedDistances, panamaDistances, 1e-3f * vectorSize);
    }

    public void testDotProductBulkByte() {
        int vectorSize = randomIntBetween(1, 2048);
        byte[] query = randomByteArrayOfLength(vectorSize);
        byte[] v0 = randomByteArrayOfLength(vectorSize);
        byte[] v1 = randomByteArrayOfLength(vectorSize);
        byte[] v2 = randomByteArrayOfLength(vectorSize);
        byte[] v3 = randomByteArrayOfLength(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().dotProductBulk(query, v0, v1, v2, v3, 0, expectedDistances);
        panamaProvider.getVectorUtilSupport().dotProductBulk(query, v0, v1, v2, v3, 0, panamaDistances);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedDistances[i], panamaDistances[i], Math.abs(expectedDistances[i]) * 1e-3f);
        }
    }

    public void testCosineBulkByte() {
        int vectorSize = randomIntBetween(1, 2048);
        // ensure no zero-norm vectors to avoid NaN
        byte[] query = randomNonZeroByteVector(vectorSize);
        byte[] v0 = randomNonZeroByteVector(vectorSize);
        byte[] v1 = randomNonZeroByteVector(vectorSize);
        byte[] v2 = randomNonZeroByteVector(vectorSize);
        byte[] v3 = randomNonZeroByteVector(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().cosineBulk(query, v0, v1, v2, v3, 0, expectedDistances);
        panamaProvider.getVectorUtilSupport().cosineBulk(query, v0, v1, v2, v3, 0, panamaDistances);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedDistances[i], panamaDistances[i], Math.abs(expectedDistances[i]) * 1e-3f);
        }
    }

    private static byte[] randomNonZeroByteVector(int length) {
        byte[] v;
        for (;;) {
            v = randomByteArrayOfLength(length);
            for (byte b : v) {
                if (b != 0) return v;
            }
        }
    }

    public void testSoarDistanceBulk() {
        int vectorSize = randomIntBetween(1, 2048);
        float deltaEps = 1e-3f * vectorSize;
        float[] query = randomFloatVector(vectorSize);
        float[] v0 = randomFloatVector(vectorSize);
        float[] v1 = randomFloatVector(vectorSize);
        float[] v2 = randomFloatVector(vectorSize);
        float[] v3 = randomFloatVector(vectorSize);
        float[] diff = randomFloatVector(vectorSize);
        float soarLambda = random().nextFloat();
        float rnorm = random().nextFloat(10);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().soarDistanceBulk(query, v0, v1, v2, v3, diff, soarLambda, rnorm, expectedDistances);
        panamaProvider.getVectorUtilSupport().soarDistanceBulk(query, v0, v1, v2, v3, diff, soarLambda, rnorm, panamaDistances);
        assertArrayEquals(expectedDistances, panamaDistances, deltaEps);
    }

    public void testSoarDistanceBulkByte() {
        int vectorSize = randomIntBetween(1, 2048);
        byte[] query = randomByteArrayOfLength(vectorSize);
        byte[] c0 = randomByteArrayOfLength(vectorSize);
        byte[] c1 = randomByteArrayOfLength(vectorSize);
        byte[] c2 = randomByteArrayOfLength(vectorSize);
        byte[] c3 = randomByteArrayOfLength(vectorSize);
        float[] diff = randomFloatVector(vectorSize);
        float soarLambda = random().nextFloat();
        float rnorm = random().nextFloat(10);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().soarDistanceBulk(query, c0, c1, c2, c3, diff, soarLambda, rnorm, expectedDistances);
        panamaProvider.getVectorUtilSupport().soarDistanceBulk(query, c0, c1, c2, c3, diff, soarLambda, rnorm, panamaDistances);
        for (int i = 0; i < 4; i++) {
            assertEquals(expectedDistances[i], panamaDistances[i], Math.abs(expectedDistances[i]) * 1e-5f + 1e-3f);
        }
    }

    public void testLinearCombinationByte() {
        int vectorSize = randomIntBetween(1, 2048);
        byte[] src = randomByteArrayOfLength(vectorSize);
        float[] destDefault = randomFloatVector(vectorSize);
        float[] destPanama = new float[vectorSize];
        System.arraycopy(destDefault, 0, destPanama, 0, vectorSize);
        float scaleSrc = random().nextFloat() * 2 - 1;
        float scaleDest = random().nextFloat() * 2 - 1;
        defaultedProvider.getVectorUtilSupport().linearCombination(scaleSrc, src, scaleDest, destDefault);
        panamaProvider.getVectorUtilSupport().linearCombination(scaleSrc, src, scaleDest, destPanama);
        for (int i = 0; i < vectorSize; i++) {
            assertEquals(destDefault[i], destPanama[i], Math.abs(destDefault[i]) * 1e-6f + 1e-6f);
        }
    }

    public void testPackAsBytesCorrectness() {
        // values that fit in a byte unchanged
        int[] src = { 0, 1, 127 };
        byte[] dst = new byte[3];
        ESVectorUtil.packAsBytes(src, dst, 3);
        assertArrayEquals(new byte[] { 0, 1, 127 }, dst);

        // values > 127 are narrowed via signed cast (two's complement truncation)
        src = new int[] { 128, 255, 256 };
        dst = new byte[3];
        ESVectorUtil.packAsBytes(src, dst, 3);
        assertArrayEquals(new byte[] { (byte) 128, (byte) 255, (byte) 256 }, dst);

        // partial fill: only len elements are written, remainder is untouched
        src = new int[] { 10, 20, 30 };
        dst = new byte[] { 0, 0, 99 };
        ESVectorUtil.packAsBytes(src, dst, 2);
        assertArrayEquals(new byte[] { 10, 20, 99 }, dst);
    }

    public void testPack1BitValues() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(1);
        }
        int length = BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().pack1BitValues(toPack, packedLegacy);
        panamaProvider.getVectorUtilSupport().pack1BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testPack1BitValuesCorrectness() {
        // 5 bits
        int[] toPack = new int[] { 1, 1, 0, 0, 1 };
        byte[] packed = new byte[1];
        ESVectorUtil.pack1BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001000 }, packed);

        // 8 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0 };
        packed = new byte[1];
        ESVectorUtil.pack1BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010 }, packed);

        // 10 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1 };
        packed = new byte[2];
        ESVectorUtil.pack1BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11000000 }, packed);

        // 16 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 0 };
        packed = new byte[2];
        ESVectorUtil.pack1BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11100110 }, packed);
    }

    public void testPack1BitValuesDuel() {
        int dims = random().nextInt(16, 2049);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = random().nextInt(2);
        }
        int length = BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        pack1BitValuesLegacy(toPack, packedLegacy);
        ESVectorUtil.pack1BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testStride4BitValuesDuel() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        stride4BitValuesLegacy(toPack, packedLegacy);
        ESVectorUtil.stride4BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testStride4BitValues() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().stride4BitValues(toPack, packedLegacy);
        panamaProvider.getVectorUtilSupport().stride4BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testStride2BitValues() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(3);
        }
        int length = ES940DiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY_STRIPED.getDocPackedLength(dims);
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().stride2BitValues(toPack, packedLegacy);
        panamaProvider.getVectorUtilSupport().stride2BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testPack2BitValues() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(3);
        }
        int length = ES940DiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY_PACKED.getDocPackedLength(dims);
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().pack2BitValues(toPack, packedLegacy);
        panamaProvider.getVectorUtilSupport().pack2BitValues(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testStride2BitValuesCorrectness() {
        // 5 bits
        // binary lower bits 1 1 0 0 1
        // binary upper bits 0 1 1 0 0
        // resulting dibit 1 3 2 0 1
        int[] toPack = new int[] { 1, 3, 2, 0, 1 };
        byte[] packed = new byte[2];
        ESVectorUtil.stride2BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001000, (byte) 0b01100000 }, packed);

        // 8 bits
        // binary lower bits 1 1 0 0 1 0 1 0
        // binary upper bits 0 1 1 0 0 1 0 1
        // resulting dibit 1 3 2 0 1 2 1 2
        toPack = new int[] { 1, 3, 2, 0, 1, 2, 1, 2 };
        packed = new byte[2];
        ESVectorUtil.stride2BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b01100101 }, packed);
    }

    public void testPack2BitValuesCorrectness() {
        int[] toPack = new int[] { 1, 3, 2, 0, 1 };
        byte[] packed = new byte[2];
        ESVectorUtil.pack2BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b01111000, (byte) 0b01000000 }, packed);

        toPack = new int[] { 1, 3, 2, 0, 1, 2, 1, 2 };
        packed = new byte[2];
        ESVectorUtil.pack2BitValues(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b01111000, (byte) 0b01100110 }, packed);
    }

    private float[][] generateRandomFloatVectors(int vectorCount, int dims) {
        float[][] vectors = new float[vectorCount][];
        for (int i = 0; i < vectorCount; i++) {
            vectors[i] = VectorTestUtils.randomFloatVector(dims);
        }
        return vectors;
    }

    private float[][] generateRandomBFloat16Vectors(int vectorCount, int dims) {
        float[][] vectors = new float[vectorCount][dims];
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dims; j++) {
                vectors[i][j] = BFloat16.truncateToBFloat16(randomFloat() * 2f - 1f);
            }
        }
        return vectors;
    }

    private byte[][] generateRandomByteVectors(int vectorCount, int dims) {
        byte[][] vectors = new byte[vectorCount][];
        for (int i = 0; i < vectorCount; i++) {
            vectors[i] = VectorTestUtils.randomByteVector(dims);
        }
        return vectors;
    }

    private static BytesRef encodeFloatVectors(float[][] vectors) {
        int dims = vectors[0].length;
        ByteBuffer buffer = ByteBuffer.allocate(vectors.length * dims * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        var floatBuffer = buffer.asFloatBuffer();
        for (float[] vector : vectors) {
            floatBuffer.put(vector);
        }
        return new BytesRef(buffer.array());
    }

    private static BytesRef encodeByteVectors(byte[][] vectors) {
        int dims = vectors[0].length;
        byte[] bytes = new byte[vectors.length * dims];
        int offset = 0;
        for (byte[] vector : vectors) {
            System.arraycopy(vector, 0, bytes, offset, dims);
            offset += dims;
        }
        return new BytesRef(bytes);
    }

    private static BytesRef encodeBFloat16Vectors(float[][] vectors) {
        int dims = vectors[0].length;
        byte[] buffer = new byte[vectors.length * dims * BFloat16.BYTES];
        for (int i = 0; i < vectors.length; i++) {
            ESVectorUtil.floatToBFloat16(vectors[i], 0, buffer, i * dims * BFloat16.BYTES, dims, ByteOrder.LITTLE_ENDIAN);
        }
        return new BytesRef(buffer);
    }

    private static class TestMultiFloatVectorsSource implements MultiFloatVectorsSource {
        private final float[][] vectors;
        private final BytesRef vectorBytes;
        private final int dims;

        TestMultiFloatVectorsSource(float[][] vectors, BytesRef vectorBytes, int dims) {
            this.vectors = vectors;
            this.vectorBytes = vectorBytes;
            this.dims = dims;
        }

        @Override
        public BytesRef vectorBytes() {
            return vectorBytes;
        }

        @Override
        public int vectorCount() {
            return vectors.length;
        }

        @Override
        public int vectorDims() {
            return dims;
        }

        @Override
        public int vectorByteSize() {
            return dims * Float.BYTES;
        }

        @Override
        public Iterator<float[]> vectorValues() {
            return Arrays.asList(vectors).iterator();
        }
    }

    private static class TestMultiBFloat16VectorsSource implements MultiBFloat16VectorsSource {
        private final float[][] vectors;
        private final BytesRef vectorBytes;
        private final int dims;

        TestMultiBFloat16VectorsSource(float[][] vectors, BytesRef vectorBytes, int dims) {
            this.vectors = vectors;
            this.vectorBytes = vectorBytes;
            this.dims = dims;
        }

        @Override
        public BytesRef vectorBytes() {
            return vectorBytes;
        }

        @Override
        public int vectorCount() {
            return vectors.length;
        }

        @Override
        public int vectorDims() {
            return dims;
        }

        @Override
        public int vectorByteSize() {
            return dims * BFloat16.BYTES;
        }

        @Override
        public Iterator<float[]> vectorValues() {
            return Arrays.asList(vectors).iterator();
        }
    }

    private static class TestMultiByteVectorsSource implements MultiByteVectorsSource {
        private final byte[][] vectors;
        private final BytesRef vectorBytes;
        private final int dims;

        TestMultiByteVectorsSource(byte[][] vectors, BytesRef vectorBytes, int dims) {
            this.vectors = vectors;
            this.vectorBytes = vectorBytes;
            this.dims = dims;
        }

        @Override
        public BytesRef vectorBytes() {
            return vectorBytes;
        }

        @Override
        public int vectorCount() {
            return vectors.length;
        }

        @Override
        public int vectorDims() {
            return dims;
        }

        @Override
        public int vectorByteSize() {
            return dims;
        }

        @Override
        public Iterator<byte[]> vectorValues() {
            return Arrays.asList(vectors).iterator();
        }
    }

    void testIpByteBinImpl(ToLongBiFunction<byte[], byte[]> ipByteBinFunc) {
        int iterations = atLeast(50);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(5000);
            var d = new byte[size];
            var q = new byte[size * B_QUERY];
            random().nextBytes(d);
            random().nextBytes(q);
            assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.applyAsLong(q, d));

            Arrays.fill(d, Byte.MAX_VALUE);
            Arrays.fill(q, Byte.MAX_VALUE);
            assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.applyAsLong(q, d));

            Arrays.fill(d, Byte.MIN_VALUE);
            Arrays.fill(q, Byte.MIN_VALUE);
            assertEquals(scalarIpByteBin(q, d), ipByteBinFunc.applyAsLong(q, d));
        }
    }

    static int scalarIpByteBin(byte[] q, byte[] d) {
        int res = 0;
        for (int i = 0; i < B_QUERY; i++) {
            res += (popcount(q, i * d.length, d, d.length) << i);
        }
        return res;
    }

    static int scalarBitAnd(byte[] a, int aOffset, byte[] b, int bOffset, int length) {
        int res = 0;
        for (int i = 0; i < length; i++) {
            res += Integer.bitCount((a[aOffset + i] & b[bOffset + i]) & 0xFF);
        }
        return res;
    }

    public static int popcount(byte[] a, int aOffset, byte[] b, int length) {
        int res = 0;
        for (int j = 0; j < length; j++) {
            int value = (a[aOffset + j] & b[j]) & 0xFF;
            for (int k = 0; k < Byte.SIZE; k++) {
                if ((value & (1 << k)) != 0) {
                    ++res;
                }
            }
        }
        return res;
    }

    // -- indexOf

    static final Class<IndexOutOfBoundsException> IOOBE = IndexOutOfBoundsException.class;

    public void testIndexOfBounds() {
        int iterations = atLeast(50);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(2, 5000);
            var bytes = new byte[size];
            expectThrows(IOOBE, () -> ESVectorUtil.indexOf(bytes, 0, bytes.length + 1, (byte) 0x0A));
            expectThrows(IOOBE, () -> ESVectorUtil.indexOf(bytes, 1, bytes.length, (byte) 0x0A));
            expectThrows(IOOBE, () -> ESVectorUtil.indexOf(bytes, bytes.length, 1, (byte) 0x0A));
            expectThrows(IOOBE, () -> ESVectorUtil.indexOf(bytes, bytes.length - 1, 2, (byte) 0x0A));
            expectThrows(IOOBE, () -> ESVectorUtil.indexOf(bytes, randomIntBetween(2, size), bytes.length, (byte) 0x0A));
        }
    }

    public void testIndexOfSimple() {
        int iterations = atLeast(50);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(2, 5000);
            var bytes = new byte[size];
            byte marker = (byte) 0x0A;
            int markerIdx = randomIntBetween(0, bytes.length - 1);
            bytes[markerIdx] = marker;

            assertEquals(markerIdx, ESVectorUtil.indexOf(bytes, 0, bytes.length, marker));
            assertEquals(markerIdx, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));
            assertEquals(markerIdx, panamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            bytes = new byte[size];
            bytes[bytes.length - 1] = marker;
            assertEquals(bytes.length - 1, ESVectorUtil.indexOf(bytes, 0, bytes.length, marker));
            assertEquals(bytes.length - 1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));
            assertEquals(bytes.length - 1, panamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            assertEquals(bytes.length - 2, ESVectorUtil.indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(bytes.length - 2, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(bytes.length - 2, panamaProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));

            // not found
            assertEquals(-1, ESVectorUtil.indexOf(bytes, 0, bytes.length - 1, marker));
            assertEquals(-1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length - 1, marker));
            assertEquals(-1, panamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length - 1, marker));

            bytes = new byte[size];
            bytes[0] = marker;
            assertEquals(0, ESVectorUtil.indexOf(bytes, 0, bytes.length, marker));
            assertEquals(0, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));
            assertEquals(0, panamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            // not found
            assertEquals(-1, ESVectorUtil.indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(-1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(-1, panamaProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
        }
    }

    public void testIndexOfRandom() {
        int iterations = atLeast(50);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(2, 5000);
            var bytes = new byte[size];
            random().nextBytes(bytes);
            byte marker = randomByte();
            int markerIdx = randomIntBetween(0, bytes.length - 1);
            bytes[markerIdx] = marker;

            final int offset = randomIntBetween(0, bytes.length - 2);
            final int length = randomIntBetween(0, bytes.length - offset);
            final int expectedIdx = scalarIndexOf(bytes, offset, length, marker);
            assertEquals(expectedIdx, ESVectorUtil.indexOf(bytes, offset, length, marker));
            assertEquals(expectedIdx, defaultedProvider.getVectorUtilSupport().indexOf(bytes, offset, length, marker));
            assertEquals(expectedIdx, panamaProvider.getVectorUtilSupport().indexOf(bytes, offset, length, marker));
        }
    }

    public void testCodePointCountSimple() {
        assertCodePoint(new BytesRef(""), 0);
        assertCodePoint(new BytesRef("a"), 1); // 1 byte
        assertCodePoint(new BytesRef("£"), 1); // 2 byte
        assertCodePoint(new BytesRef("€"), 1); // 3 byte
        assertCodePoint(new BytesRef("\uD83D\uDE80"), 1); // 4 byte
    }

    public void testCodePointCountRandom() {
        int iterations = atLeast(1000);
        for (int i = 0; i < iterations; i++) {
            int size = random().nextInt(1000);
            var bytes = new BytesRef(randomUnicodeOfLength(size));
            final int expectedCount = UnicodeUtil.codePointCount(bytes);
            assertCodePoint(bytes, expectedCount);
        }
    }

    private void assertCodePoint(BytesRef bytes, int expected) {
        assertEquals(expected, ESVectorUtil.codePointCount(bytes));
        assertEquals(expected, defaultedProvider.getVectorUtilSupport().codePointCount(bytes));
        assertEquals(expected, panamaProvider.getVectorUtilSupport().codePointCount(bytes));
    }

    // -- contains

    public void testContainsSimple() {
        assertContains("foobar", "foo", true);
        assertContains("foobar", "bar", true);
        assertContains("foobar", "oob", true);
        assertContains("foobar", "foobar", true);
        assertContains("foobar", "b", true);
        assertContains("foobar", "baz", false);
        assertContains("foo", "foobar", false);
        assertContains("a", "a", true);
        assertContains("a", "b", false);
        assertContains("Ω≈ç√∫", "≈ç√", true);
    }

    public void testContainsEmpty() {
        byte[] value = "hello".getBytes(StandardCharsets.UTF_8);
        byte[] emptyTerm = new byte[0];
        assertTrue(ESVectorUtil.contains(value, 0, value.length, emptyTerm, 0, 0));
        assertFalse(ESVectorUtil.contains(emptyTerm, 0, 0, value, 0, value.length));
    }

    public void testContainsWithOffset() {
        byte[] backing = "XXXXXhello worldXXXXX".getBytes(StandardCharsets.UTF_8);
        byte[] term = "world".getBytes(StandardCharsets.UTF_8);
        assertTrue(ESVectorUtil.contains(backing, 5, 11, term, 0, term.length));
        assertFalse(ESVectorUtil.contains(backing, 5, 5, term, 0, term.length));
    }

    public void testContainsRandom() {
        int iterations = atLeast(500);
        for (int iter = 0; iter < iterations; iter++) {
            int valueLen = randomIntBetween(1, 500);
            byte[] value = new byte[valueLen];
            random().nextBytes(value);
            int termLen = randomIntBetween(1, Math.min(valueLen, 50));
            byte[] term;
            if (randomBoolean()) {
                int startPos = randomIntBetween(0, valueLen - termLen);
                term = Arrays.copyOfRange(value, startPos, startPos + termLen);
            } else {
                term = new byte[termLen];
                random().nextBytes(term);
            }
            boolean expected = scalarContains(value, 0, valueLen, term, 0, termLen);
            assertEquals(expected, ESVectorUtil.contains(value, 0, valueLen, term, 0, termLen));
            assertEquals(expected, defaultedProvider.getVectorUtilSupport().contains(value, 0, valueLen, term, 0, termLen));
            assertEquals(expected, panamaProvider.getVectorUtilSupport().contains(value, 0, valueLen, term, 0, termLen));
        }
    }

    public void testContainsRandomWithOffset() {
        int iterations = atLeast(200);
        for (int iter = 0; iter < iterations; iter++) {
            int padding = randomIntBetween(0, 20);
            int valueLen = randomIntBetween(1, 500);
            byte[] value = new byte[padding + valueLen + padding];
            random().nextBytes(value);
            int termLen = randomIntBetween(1, Math.min(valueLen, 50));
            int termPadding = randomIntBetween(0, 10);
            byte[] term = new byte[termPadding + termLen + termPadding];
            random().nextBytes(term);
            if (randomBoolean()) {
                int startPos = randomIntBetween(0, valueLen - termLen);
                System.arraycopy(value, padding + startPos, term, termPadding, termLen);
            }
            boolean expected = scalarContains(value, padding, valueLen, term, termPadding, termLen);
            assertEquals(expected, ESVectorUtil.contains(value, padding, valueLen, term, termPadding, termLen));
            assertEquals(expected, defaultedProvider.getVectorUtilSupport().contains(value, padding, valueLen, term, termPadding, termLen));
            assertEquals(expected, panamaProvider.getVectorUtilSupport().contains(value, padding, valueLen, term, termPadding, termLen));
        }
    }

    private void assertContains(String value, String term, boolean expected) {
        byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
        byte[] termBytes = term.getBytes(StandardCharsets.UTF_8);
        assertEquals(expected, ESVectorUtil.contains(valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length));
        assertEquals(
            expected,
            defaultedProvider.getVectorUtilSupport().contains(valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length)
        );
        assertEquals(
            expected,
            panamaProvider.getVectorUtilSupport().contains(valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length)
        );
    }

    static boolean scalarContains(byte[] value, int vOff, int vLen, byte[] term, int tOff, int tLen) {
        if (tLen > vLen) {
            return false;
        }
        for (int i = vOff; i <= vOff + vLen - tLen; i++) {
            boolean match = true;
            for (int j = 0; j < tLen; j++) {
                if (value[i + j] != term[tOff + j]) {
                    match = false;
                    break;
                }
            }
            if (match) {
                return true;
            }
        }
        return false;
    }

    static int scalarIndexOf(byte[] bytes, final int offset, final int length, final byte marker) {
        final int end = offset + length;
        for (int i = offset; i < end; i++) {
            if (bytes[i] == marker) {
                return i - offset;
            }
        }
        return -1;
    }

    private static void pack1BitValuesLegacy(int[] vector, byte[] packed) {
        for (int i = 0; i < vector.length;) {
            byte result = 0;
            for (int j = 7; j >= 0 && i < vector.length; j--) {
                assert vector[i] == 0 || vector[i] == 1;
                result |= (byte) ((vector[i] & 1) << j);
                ++i;
            }
            int index = ((i + 7) / 8) - 1;
            assert index < packed.length;
            packed[index] = result;
        }
    }

    private static void stride4BitValuesLegacy(int[] vector, byte[] packed) {
        for (int i = 0; i < vector.length;) {
            assert vector[i] >= 0 && vector[i] <= 15;
            int lowerByte = 0;
            int lowerMiddleByte = 0;
            int upperMiddleByte = 0;
            int upperByte = 0;
            for (int j = 7; j >= 0 && i < vector.length; j--) {
                lowerByte |= (vector[i] & 1) << j;
                lowerMiddleByte |= ((vector[i] >> 1) & 1) << j;
                upperMiddleByte |= ((vector[i] >> 2) & 1) << j;
                upperByte |= ((vector[i] >> 3) & 1) << j;
                i++;
            }
            int index = ((i + 7) / 8) - 1;
            packed[index] = (byte) lowerByte;
            packed[index + packed.length / 4] = (byte) lowerMiddleByte;
            packed[index + packed.length / 2] = (byte) upperMiddleByte;
            packed[index + 3 * packed.length / 4] = (byte) upperByte;
        }
    }

    public void testLogSumExpNQT() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
        }

        float referenceResult = defaultedProvider.getVectorUtilSupport().logSumExpNQT(x);
        assertEquals(referenceResult, panamaProvider.getVectorUtilSupport().logSumExpNQT(x), 0.025 * referenceResult);
    }

    public void testLinearCombination() {
        int xLength = randomIntBetween(10, 50);
        int yLength = randomIntBetween(10, 50);
        float[] x = VectorTestUtils.randomFloatVector(xLength);
        float[] y1 = VectorTestUtils.randomFloatVector(yLength);
        float[] y2 = y1.clone();

        int xOffset = randomIntBetween(0, xLength - 5);
        int yOffset = randomIntBetween(0, yLength - 5);
        int length = Math.min(xLength - xOffset, yLength - yOffset);

        float scaleX = randomFloat();
        float scaleY = randomFloat();

        defaultedProvider.getVectorUtilSupport().linearCombination(scaleX, x, xOffset, scaleY, y1, yOffset, length);
        panamaProvider.getVectorUtilSupport().linearCombination(scaleX, x, xOffset, scaleY, y2, yOffset, length);

        assertArrayEquals(y1, y2, 1e-5f);
    }

    public void testLinearCombinationNoScaleDest() {
        int xLength = randomIntBetween(10, 50);
        int yLength = randomIntBetween(10, 50);
        float[] x = VectorTestUtils.randomFloatVector(xLength);
        float[] y1 = VectorTestUtils.randomFloatVector(yLength);
        float[] y2 = y1.clone();

        int xOffset = randomIntBetween(0, xLength - 5);
        int yOffset = randomIntBetween(0, yLength - 5);
        int length = Math.min(xLength - xOffset, yLength - yOffset);

        float scaleX = randomFloat();

        defaultedProvider.getVectorUtilSupport().linearCombination(scaleX, x, xOffset, y1, yOffset, length);
        panamaProvider.getVectorUtilSupport().linearCombination(scaleX, x, xOffset, y2, yOffset, length);

        assertArrayEquals(y1, y2, 1e-5f);
    }

    public void testLinearCombinationByteNoScaleDest() {
        int vectorSize = randomIntBetween(1, 2048);
        byte[] src = randomByteArrayOfLength(vectorSize);
        float[] destDefault = randomFloatVector(vectorSize);
        float[] destPanama = new float[vectorSize];
        System.arraycopy(destDefault, 0, destPanama, 0, vectorSize);
        float scaleSrc = random().nextFloat() * 2 - 1;
        defaultedProvider.getVectorUtilSupport().linearCombination(scaleSrc, src, destDefault);
        panamaProvider.getVectorUtilSupport().linearCombination(scaleSrc, src, destPanama);
        for (int i = 0; i < vectorSize; i++) {
            assertEquals(destDefault[i], destPanama[i], Math.abs(destDefault[i]) * 1e-6f + 1e-6f);
        }
    }

    public void testLogSumExpDiff() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        float[] y = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y[i] = randomFloat();
        }

        float eps = randomFloat();

        float referenceResult = defaultedProvider.getVectorUtilSupport().logSumExpNQTDiff(x, y, eps);
        assertEquals(referenceResult, panamaProvider.getVectorUtilSupport().logSumExpNQTDiff(x, y, eps), 3.5e-2 * referenceResult);
    }

    public void testPow2DiffAndScale() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        float[] y = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y[i] = randomFloat();
        }

        float a = randomFloat();
        float eps = randomFloat();

        float[] result1 = new float[19];
        float[] result2 = new float[19];

        defaultedProvider.getVectorUtilSupport().pow2DiffAndScaleNQT(x, y, a, eps, result1);
        panamaProvider.getVectorUtilSupport().pow2DiffAndScaleNQT(x, y, a, eps, result2);

        assertArrayEqualsPercent(result1, result2, 0.15f);
    }

}
