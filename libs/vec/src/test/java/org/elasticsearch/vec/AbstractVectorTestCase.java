/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.vec;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.startsWith;

public abstract class AbstractVectorTestCase extends ESTestCase {

    protected AbstractVectorTestCase() {
        logger.info(platformMsg());
    }

    static boolean supported() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if (jdkVersion >= 21 && arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux"))) {
            assertNotNull(VectorScorerProvider.getInstanceOrNull());
            return true;
        } else {
            assertNull(VectorScorerProvider.getInstanceOrNull());
            assertThat(osName, either(startsWith("Mac")).or(startsWith("Linux")));
            return false;
        }
    }

    static String notSupportedMsg() {
        return "Not supported on [" + platformMsg() + "]";
    }

    static String platformMsg() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");
        return "JDK=" + jdkVersion + ", os=" + osName + ", arch=" + arch;
    }

    /** Computes the scalar quantized dot product of the given vectors a and b. */
    static float scalarQuantizedDotProductScore(byte[] a, byte[] b, float correction, float aOffsetValue, float bOffsetValue) {
        int dotProduct = dotProductScalar(a, b);
        float adjustedDistance = dotProduct * correction + aOffsetValue + bOffsetValue;
        return (1 + adjustedDistance) / 2;
    }

    /** Computes the dot product of the given vectors a and b. */
    static int dotProductScalar(byte[] a, byte[] b) {
        int res = 0;
        for (int i = 0; i < a.length; i++) {
            res += a[i] * b[i];
        }
        return res;
    }

    static float scaleMaxInnerProductScore(float rawSimilarity) {
        if (rawSimilarity < 0) {
            return 1 / (1 + -1 * rawSimilarity);
        }
        return rawSimilarity + 1;
    }

    /** Converts a float value to a byte array. */
    static byte[] floatToByteArray(float value) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(value).array();
    }

    /** Concatenates byte arrays. */
    static byte[] concat(byte[]... arrays) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
            for (var ba : arrays) {
                baos.write(ba);
            }
            return baos.toByteArray();
        }
    }
}
