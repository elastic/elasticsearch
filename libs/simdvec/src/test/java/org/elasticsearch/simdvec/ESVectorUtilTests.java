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
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.diskbbq.es94.ES940DiskBBQVectorsFormat;
import org.elasticsearch.simdvec.internal.vectorization.BaseVectorizationTests;
import org.elasticsearch.simdvec.internal.vectorization.ESVectorizationProvider;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Iterator;
import java.util.function.ToLongBiFunction;

import static org.elasticsearch.simdvec.internal.vectorization.ESVectorUtilSupport.B_QUERY;
import static org.hamcrest.Matchers.closeTo;

public class ESVectorUtilTests extends BaseVectorizationTests {

    static final ESVectorizationProvider defaultedProvider = BaseVectorizationTests.defaultProvider();
    static final ESVectorizationProvider defOrPanamaProvider = BaseVectorizationTests.maybePanamaProvider();

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
        float actual = defOrPanamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
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
        float actual = defOrPanamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
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
        float actual = defOrPanamaProvider.getVectorUtilSupport().maxSimDotProduct(source, queryVectors, defOrPanamaScoresScratch);
        assertEquals(expected, actual, 1e-3f * dims * numQueryVectors);
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
        assertEquals(sum, defOrPanamaProvider.getVectorUtilSupport().ipByteBit(q, d));
    }

    public void testIpFloatBit() {
        byte[] d = new byte[random().nextInt(128)];
        float[] q = new float[d.length * 8];
        random().nextBytes(d);

        float sum = 0;
        for (int i = 0; i < q.length; i++) {
            q[i] = random().nextFloat();
            if (((d[i / 8] << (i % 8)) & 0x80) == 0x80) {
                sum += q[i];
            }
        }

        double delta = 1e-5 * q.length;

        assertEquals(sum, ESVectorUtil.ipFloatBit(q, d), delta);
        assertEquals(sum, defaultedProvider.getVectorUtilSupport().ipFloatBit(q, d), delta);
        assertEquals(sum, defOrPanamaProvider.getVectorUtilSupport().ipFloatBit(q, d), delta);
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
        assertThat((double) defOrPanamaProvider.getVectorUtilSupport().ipFloatByte(q, d), closeTo(expected, delta));
    }

    public void testBitAndCount() {
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
        testBasicIpByteBinImpl(defOrPanamaProvider.getVectorUtilSupport()::ipByteBinByte);
    }

    void testBasicBitAndImpl(ToLongBiFunction<byte[], byte[]> bitAnd) {
        assertEquals(0, bitAnd.applyAsLong(new byte[] { 0 }, new byte[] { 0 }));
        assertEquals(0, bitAnd.applyAsLong(new byte[] { 1 }, new byte[] { 0 }));
        assertEquals(0, bitAnd.applyAsLong(new byte[] { 0 }, new byte[] { 1 }));
        assertEquals(1, bitAnd.applyAsLong(new byte[] { 1 }, new byte[] { 1 }));
        byte[] a = new byte[31];
        byte[] b = new byte[31];
        random().nextBytes(a);
        random().nextBytes(b);
        int expected = scalarBitAnd(a, b);
        assertEquals(expected, bitAnd.applyAsLong(a, b));
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
        testIpByteBinImpl(defOrPanamaProvider.getVectorUtilSupport()::ipByteBinByte);
    }

    public void testCenterAndCalculateOSQStatsDp() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new float[size];
        var centroid = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            centroid[i] = random().nextFloat();
        }
        var centeredLucene = new float[size];
        var statsLucene = new float[6];
        defaultedProvider.getVectorUtilSupport().centerAndCalculateOSQStatsDp(vector, centroid, centeredLucene, statsLucene);
        var centeredPanama = new float[size];
        var statsPanama = new float[6];
        defOrPanamaProvider.getVectorUtilSupport().centerAndCalculateOSQStatsDp(vector, centroid, centeredPanama, statsPanama);
        assertArrayEquals(centeredLucene, centeredPanama, delta);
        assertArrayEquals(statsLucene, statsPanama, delta);
    }

    public void testCenterAndCalculateOSQStatsEuclidean() {
        int size = random().nextInt(128, 512);
        float delta = 1e-3f * size;
        var vector = new float[size];
        var centroid = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            centroid[i] = random().nextFloat();
        }
        var centeredLucene = new float[size];
        var statsLucene = new float[5];
        defaultedProvider.getVectorUtilSupport().centerAndCalculateOSQStatsEuclidean(vector, centroid, centeredLucene, statsLucene);
        var centeredPanama = new float[size];
        var statsPanama = new float[5];
        defOrPanamaProvider.getVectorUtilSupport().centerAndCalculateOSQStatsEuclidean(vector, centroid, centeredPanama, statsPanama);
        assertArrayEquals(centeredLucene, centeredPanama, delta);
        assertArrayEquals(statsLucene, statsPanama, delta);
    }

    public void testOsqLoss() {
        int size = random().nextInt(128, 512);
        float deltaEps = 1e-5f * size;
        var vector = new float[size];
        var min = Float.MAX_VALUE;
        var max = -Float.MAX_VALUE;
        float vecMean = 0;
        float vecVar = 0;
        float norm2 = 0;
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            float delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = vector[i] - vecMean;
            vecVar += delta * delta2;
            norm2 += vector[i] * vector[i];
        }
        vecVar /= size;
        float vecStd = (float) Math.sqrt(vecVar);

        int[] destinationDefault = new int[size];
        int[] destinationPanama = new int[size];
        for (byte bits : new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }) {
            int points = 1 << bits;
            float[] initInterval = new float[2];
            OptimizedScalarQuantizer.initInterval(bits, vecStd, vecMean, min, max, initInterval);
            float step = ((initInterval[1] - initInterval[0]) / (points - 1f));
            float stepInv = 1f / step;
            float expected = defaultedProvider.getVectorUtilSupport()
                .calculateOSQLoss(vector, initInterval[0], initInterval[1], step, stepInv, norm2, 0.1f, destinationDefault);
            float result = defOrPanamaProvider.getVectorUtilSupport()
                .calculateOSQLoss(vector, initInterval[0], initInterval[1], step, stepInv, norm2, 0.1f, destinationPanama);
            assertEquals(expected, result, deltaEps);
            assertArrayEquals(destinationDefault, destinationPanama);
        }
    }

    public void testOsqGridPoints() {
        int size = random().nextInt(128, 512);
        float deltaEps = 1e-5f * size;
        var vector = new float[size];
        var min = Float.MAX_VALUE;
        var max = -Float.MAX_VALUE;
        var norm2 = 0f;
        float vecMean = 0;
        float vecVar = 0;
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
            min = Math.min(min, vector[i]);
            max = Math.max(max, vector[i]);
            float delta = vector[i] - vecMean;
            vecMean += delta / (i + 1);
            float delta2 = vector[i] - vecMean;
            vecVar += delta * delta2;
            norm2 += vector[i] * vector[i];
        }
        vecVar /= size;
        float vecStd = (float) Math.sqrt(vecVar);
        int[] destinationDefault = new int[size];
        int[] destinationPanama = new int[size];
        for (byte bits : new byte[] { 1, 2, 3, 4, 5, 6, 7, 8 }) {
            int points = 1 << bits;
            float[] initInterval = new float[2];
            OptimizedScalarQuantizer.initInterval(bits, vecStd, vecMean, min, max, initInterval);
            float step = ((initInterval[1] - initInterval[0]) / (points - 1f));
            float stepInv = 1f / step;
            float[] expected = new float[5];
            defaultedProvider.getVectorUtilSupport()
                .calculateOSQLoss(vector, initInterval[0], initInterval[1], step, stepInv, norm2, 0.1f, destinationDefault);
            defaultedProvider.getVectorUtilSupport().calculateOSQGridPoints(vector, destinationDefault, points, expected);

            float[] result = new float[5];
            defOrPanamaProvider.getVectorUtilSupport()
                .calculateOSQLoss(vector, initInterval[0], initInterval[1], step, stepInv, norm2, 0.1f, destinationPanama);
            defOrPanamaProvider.getVectorUtilSupport().calculateOSQGridPoints(vector, destinationPanama, points, result);
            assertArrayEquals(expected, result, deltaEps);
            assertArrayEquals(destinationDefault, destinationPanama);
        }
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
        var result = defOrPanamaProvider.getVectorUtilSupport().soarDistance(vector, centroid, preResidual, soarLambda, rnorm);
        assertEquals(expected, result, deltaEps);
    }

    public void testQuantizeVectorWithIntervals() {
        int vectorSize = randomIntBetween(1, 2048);
        float[] vector = new float[vectorSize];

        byte bits = (byte) randomIntBetween(1, 8);
        for (int i = 0; i < vectorSize; ++i) {
            vector[i] = random().nextFloat();
        }
        float low = random().nextFloat();
        float high = random().nextFloat();
        if (low > high) {
            float tmp = low;
            low = high;
            high = tmp;
        }
        int[] quantizeExpected = new int[vectorSize];
        int[] quantizeResult = new int[vectorSize];
        var expected = defaultedProvider.getVectorUtilSupport().quantizeVectorWithIntervals(vector, quantizeExpected, low, high, bits);
        var result = defOrPanamaProvider.getVectorUtilSupport().quantizeVectorWithIntervals(vector, quantizeResult, low, high, bits);
        assertArrayEquals(quantizeExpected, quantizeResult);
        assertEquals(expected, result, 0f);
    }

    public void testSquareDistanceBulk() {
        int vectorSize = randomIntBetween(1, 2048);
        float[] query = generateRandomVector(vectorSize);
        float[] v0 = generateRandomVector(vectorSize);
        float[] v1 = generateRandomVector(vectorSize);
        float[] v2 = generateRandomVector(vectorSize);
        float[] v3 = generateRandomVector(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().squareDistanceBulk(query, v0, v1, v2, v3, 0, expectedDistances);
        defOrPanamaProvider.getVectorUtilSupport().squareDistanceBulk(query, v0, v1, v2, v3, 0, panamaDistances);
        assertArrayEquals(expectedDistances, panamaDistances, 1e-3f);
    }

    public void testSquareDistanceRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] a = generateRandomVector(vectorSize);
        float[] b = generateRandomVector(vectorSize);
        float expected = defaultedProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        float actual = defOrPanamaProvider.getVectorUtilSupport().squareDistance(a, b, offset, length);
        assertEquals(expected, actual, 1e-3f * length);
    }

    public void testSquareDistanceBulkRange() {
        int vectorSize = randomIntBetween(64, 2048);
        int offset = randomIntBetween(0, vectorSize - 1);
        int length = randomIntBetween(1, vectorSize - offset);
        float[] query = generateRandomVector(vectorSize);
        float[] v0 = generateRandomVector(vectorSize);
        float[] v1 = generateRandomVector(vectorSize);
        float[] v2 = generateRandomVector(vectorSize);
        float[] v3 = generateRandomVector(vectorSize);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, length, v0, v1, v2, v3, 0, expectedDistances);
        defOrPanamaProvider.getVectorUtilSupport().squareDistanceBulk(query, offset, length, v0, v1, v2, v3, 0, panamaDistances);
        assertArrayEquals(expectedDistances, panamaDistances, 1e-3f * length);
    }

    public void testSoarDistanceBulk() {
        int vectorSize = randomIntBetween(1, 2048);
        float deltaEps = 1e-3f * vectorSize;
        float[] query = generateRandomVector(vectorSize);
        float[] v0 = generateRandomVector(vectorSize);
        float[] v1 = generateRandomVector(vectorSize);
        float[] v2 = generateRandomVector(vectorSize);
        float[] v3 = generateRandomVector(vectorSize);
        float[] diff = generateRandomVector(vectorSize);
        float soarLambda = random().nextFloat();
        float rnorm = random().nextFloat(10);
        float[] expectedDistances = new float[4];
        float[] panamaDistances = new float[4];
        defaultedProvider.getVectorUtilSupport().soarDistanceBulk(query, v0, v1, v2, v3, diff, soarLambda, rnorm, expectedDistances);
        defOrPanamaProvider.getVectorUtilSupport().soarDistanceBulk(query, v0, v1, v2, v3, diff, soarLambda, rnorm, panamaDistances);
        assertArrayEquals(expectedDistances, panamaDistances, deltaEps);
    }

    public void testPackAsBinary() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(1);
        }
        int length = BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().packAsBinary(toPack, packedLegacy);
        defOrPanamaProvider.getVectorUtilSupport().packAsBinary(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testPackAsBinaryCorrectness() {
        // 5 bits
        int[] toPack = new int[] { 1, 1, 0, 0, 1 };
        byte[] packed = new byte[1];
        ESVectorUtil.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001000 }, packed);

        // 8 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0 };
        packed = new byte[1];
        ESVectorUtil.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010 }, packed);

        // 10 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1 };
        packed = new byte[2];
        ESVectorUtil.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11000000 }, packed);

        // 16 bits
        toPack = new int[] { 1, 1, 0, 0, 1, 0, 1, 0, 1, 1, 1, 0, 0, 1, 1, 0 };
        packed = new byte[2];
        ESVectorUtil.packAsBinary(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b11100110 }, packed);
    }

    public void testPackAsBinaryDuel() {
        int dims = random().nextInt(16, 2049);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = random().nextInt(2);
        }
        int length = BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        packAsBinaryLegacy(toPack, packedLegacy);
        ESVectorUtil.packAsBinary(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testIntegerTransposeHalfByte() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        transposeHalfByteLegacy(toPack, packedLegacy);
        ESVectorUtil.transposeHalfByte(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testTransposeHalfByte() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(15);
        }
        int length = 4 * BQVectorUtils.discretize(dims, 64) / 8;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().transposeHalfByte(toPack, packedLegacy);
        defOrPanamaProvider.getVectorUtilSupport().transposeHalfByte(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testPackAsDibit() {
        int dims = randomIntBetween(16, 2048);
        int[] toPack = new int[dims];
        for (int i = 0; i < dims; i++) {
            toPack[i] = randomInt(3);
        }
        int length = ES940DiskBBQVectorsFormat.QuantEncoding.TWO_BIT_4BIT_QUERY.getDocPackedLength(dims);
        ;
        byte[] packed = new byte[length];
        byte[] packedLegacy = new byte[length];
        defaultedProvider.getVectorUtilSupport().packDibit(toPack, packedLegacy);
        defOrPanamaProvider.getVectorUtilSupport().packDibit(toPack, packed);
        assertArrayEquals(packedLegacy, packed);
    }

    public void testPackDibitCorrectness() {
        // 5 bits
        // binary lower bits 1 1 0 0 1
        // binary upper bits 0 1 1 0 0
        // resulting dibit 1 3 2 0 1
        int[] toPack = new int[] { 1, 3, 2, 0, 1 };
        byte[] packed = new byte[2];
        ESVectorUtil.packDibit(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001000, (byte) 0b01100000 }, packed);

        // 8 bits
        // binary lower bits 1 1 0 0 1 0 1 0
        // binary upper bits 0 1 1 0 0 1 0 1
        // resulting dibit 1 3 2 0 1 2 1 2
        toPack = new int[] { 1, 3, 2, 0, 1, 2, 1, 2 };
        packed = new byte[2];
        ESVectorUtil.packDibit(toPack, packed);
        assertArrayEquals(new byte[] { (byte) 0b11001010, (byte) 0b01100101 }, packed);
    }

    private float[] generateRandomVector(int size) {
        float[] vector = new float[size];
        for (int i = 0; i < size; ++i) {
            vector[i] = random().nextFloat();
        }
        return vector;
    }

    private float[][] generateRandomFloatVectors(int vectorCount, int dims) {
        float[][] vectors = new float[vectorCount][dims];
        for (int i = 0; i < vectorCount; i++) {
            for (int j = 0; j < dims; j++) {
                vectors[i][j] = randomFloat() * 2f - 1f;
            }
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
        byte[][] vectors = new byte[vectorCount][dims];
        for (int i = 0; i < vectorCount; i++) {
            vectors[i] = randomByteArrayOfLength(dims);
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
        ByteBuffer buffer = ByteBuffer.allocate(vectors.length * dims * BFloat16.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        var bFloat16Buffer = buffer.asShortBuffer();
        for (float[] vector : vectors) {
            BFloat16.floatToBFloat16(vector, bFloat16Buffer);
        }
        return new BytesRef(buffer.array());
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

    static int scalarBitAnd(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += Integer.bitCount((a[i] & b[i]) & 0xFF);
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
            assertEquals(markerIdx, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            bytes = new byte[size];
            bytes[bytes.length - 1] = marker;
            assertEquals(bytes.length - 1, ESVectorUtil.indexOf(bytes, 0, bytes.length, marker));
            assertEquals(bytes.length - 1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));
            assertEquals(bytes.length - 1, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            assertEquals(bytes.length - 2, ESVectorUtil.indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(bytes.length - 2, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(bytes.length - 2, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));

            // not found
            assertEquals(-1, ESVectorUtil.indexOf(bytes, 0, bytes.length - 1, marker));
            assertEquals(-1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length - 1, marker));
            assertEquals(-1, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length - 1, marker));

            bytes = new byte[size];
            bytes[0] = marker;
            assertEquals(0, ESVectorUtil.indexOf(bytes, 0, bytes.length, marker));
            assertEquals(0, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));
            assertEquals(0, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 0, bytes.length, marker));

            // not found
            assertEquals(-1, ESVectorUtil.indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(-1, defaultedProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
            assertEquals(-1, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, 1, bytes.length - 1, marker));
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
            assertEquals(expectedIdx, defOrPanamaProvider.getVectorUtilSupport().indexOf(bytes, offset, length, marker));
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
        assertEquals(expected, defOrPanamaProvider.getVectorUtilSupport().codePointCount(bytes));
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
            assertEquals(expected, defOrPanamaProvider.getVectorUtilSupport().contains(value, 0, valueLen, term, 0, termLen));
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
            assertEquals(
                expected,
                defOrPanamaProvider.getVectorUtilSupport().contains(value, padding, valueLen, term, termPadding, termLen)
            );
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
            defOrPanamaProvider.getVectorUtilSupport().contains(valueBytes, 0, valueBytes.length, termBytes, 0, termBytes.length)
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

    private static void packAsBinaryLegacy(int[] vector, byte[] packed) {
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

    private static void transposeHalfByteLegacy(int[] q, byte[] quantQueryByte) {
        for (int i = 0; i < q.length;) {
            assert q[i] >= 0 && q[i] <= 15;
            int lowerByte = 0;
            int lowerMiddleByte = 0;
            int upperMiddleByte = 0;
            int upperByte = 0;
            for (int j = 7; j >= 0 && i < q.length; j--) {
                lowerByte |= (q[i] & 1) << j;
                lowerMiddleByte |= ((q[i] >> 1) & 1) << j;
                upperMiddleByte |= ((q[i] >> 2) & 1) << j;
                upperByte |= ((q[i] >> 3) & 1) << j;
                i++;
            }
            int index = ((i + 7) / 8) - 1;
            quantQueryByte[index] = (byte) lowerByte;
            quantQueryByte[index + quantQueryByte.length / 4] = (byte) lowerMiddleByte;
            quantQueryByte[index + quantQueryByte.length / 2] = (byte) upperMiddleByte;
            quantQueryByte[index + 3 * quantQueryByte.length / 4] = (byte) upperByte;
        }
    }

    public void testLogSumExpNQT() {
        // Choosing 19 dimensions so that it is a rugged number that does not align with any SIMD length
        float[] x = new float[19];
        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
        }

        float referenceResult = defaultedProvider.getVectorUtilSupport().logSumExpNQT(x);
        assertEquals(referenceResult, defOrPanamaProvider.getVectorUtilSupport().logSumExpNQT(x), 1e-2 * referenceResult);
    }

    public void testLinearCombination() {
        float[] x = new float[19];
        float[] y1 = new float[19];

        for (int i = 0; i < x.length; i++) {
            x[i] = randomFloat();
            y1[i] = randomFloat();
        }
        float[] y2 = new float[19];
        System.arraycopy(y1, 0, y2, 0, 19);

        float scaleX = randomFloat();
        float scaleY = randomFloat();

        defaultedProvider.getVectorUtilSupport().linearCombination(scaleX, x, scaleY, y1);
        defOrPanamaProvider.getVectorUtilSupport().linearCombination(scaleX, x, scaleY, y2);

        assertArrayEquals(y1, y2, 1e-5f);
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
        assertEquals(referenceResult, defOrPanamaProvider.getVectorUtilSupport().logSumExpNQTDiff(x, y, eps), 1.5e-2 * referenceResult);
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
        defOrPanamaProvider.getVectorUtilSupport().pow2DiffAndScaleNQT(x, y, a, eps, result2);

        assertArrayEqualsPercent(result1, result2, 0.1f);
    }
}
