/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.codecs.lucene104.Lucene104ScalarQuantizedVectorScorer;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.test.ESTestCase;
import org.junit.AssumptionViolatedException;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public abstract class AbstractVectorTestCase extends ESTestCase {

    static VectorScorerFactory factory;

    protected static final float DELTA = 1e-6f;

    /**
     * Use a slightly larger delta for bulk scoring to account for floating point precision
     * issues: applying the corrections in even a slightly different order can impact the score.
     */
    protected static final float BULK_DELTA = 2e-5f;

    @BeforeClass
    public static void getVectorScorerFactory() {
        factory = ESVectorizationProvider.getInstance().getVectorScorerFactory();

        // check the factory is resolved as expected on the arches we expect
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if ((arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux"))
            || arch.equals("amd64") && osName.equals("Linux"))) {
            assertTrue(factory.usesNative());
        } else {
            // not an arch with native support, so shouldn't be native
            assertFalse(factory.usesNative());

            // there's only native implementations of these scorers at the moment,
            // if this changes, the tests will need to check the Optionals returned themselves
            throw new AssumptionViolatedException(notSupportedMsg());
        }
    }

    protected AbstractVectorTestCase() {
        logger.info(platformMsg());
    }

    private static String notSupportedMsg() {
        return "Not supported on [" + platformMsg() + "]";
    }

    private static String platformMsg() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        return "JDK=" + jdkVersion + ", os=" + osName + ", arch=" + arch;
    }

    /** Converts a float value to a byte array. */
    public static byte[] floatToByteArray(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    /** Concatenates byte arrays. */
    public static byte[] concat(byte[]... arrays) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (var ba : arrays) {
                baos.write(ba);
            }
            return baos.toByteArray();
        }
    }

    public static float[] maxFloatArray(int size) {
        float[] fa = new float[size];
        Arrays.fill(fa, Float.MAX_VALUE);
        return fa;
    }

    // bounds of the range of values that can be seen by int7 scalar quantized vectors
    public static final byte MIN_INT7_VALUE = 0;
    public static final byte MAX_INT7_VALUE = 127;

    public static byte[] randomInt7ByteVector(int size) {
        byte[] ba = new byte[size];
        randomBytesBetween(ba, MIN_INT7_VALUE, MAX_INT7_VALUE);
        return ba;
    }

    public static byte[] maxInt7ByteVector(int size) {
        byte[] ba = new byte[size];
        Arrays.fill(ba, MAX_INT7_VALUE);
        return ba;
    }

    public static byte[] minInt7ByteVector(int size) {
        byte[] ba = new byte[size];
        Arrays.fill(ba, MIN_INT7_VALUE);
        return ba;
    }

    static void assertFloatArrayEquals(float[] expected, float[] actual, float delta) {
        assertThat(actual.length, equalTo(expected.length));
        for (int i = 0; i < expected.length; i++) {
            assertEquals("differed at element [" + i + "]", expected[i], actual[i], Math.abs(expected[i]) * delta + delta);
        }
    }

    static void assertFloatEquals(float expected, float actual, float delta) {
        assertEquals(expected, actual, Math.abs(expected) * delta + delta);
    }

    static RandomVectorScorerSupplier luceneScoreSupplier(QuantizedByteVectorValues values, VectorSimilarityFunction sim)
        throws IOException {
        return new Lucene104ScalarQuantizedVectorScorer(null).getRandomVectorScorerSupplier(sim, values);
    }
}
