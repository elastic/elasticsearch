/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.IntFunction;

import static org.elasticsearch.test.hamcrest.OptionalMatchers.isPresent;
import static org.hamcrest.Matchers.not;

public abstract class AbstractVectorTestCase extends ESTestCase {

    static Optional<org.elasticsearch.simdvec.VectorScorerFactory> factory;

    protected static final float DELTA = 1e-6f;

    /**
     * Use a slightly larger delta for bulk scoring to account for floating point precision
     * issues: applying the corrections in even a slightly different order can impact the score.
     */
    protected static final float BULK_DELTA = 2e-5f;

    // Support for passing on-heap arrays/segments to native
    protected static boolean SUPPORTS_HEAP_SEGMENTS = Runtime.version().feature() >= 22;

    @BeforeClass
    public static void getVectorScorerFactory() {
        factory = org.elasticsearch.simdvec.VectorScorerFactory.instance();
    }

    protected AbstractVectorTestCase() {
        logger.info(platformMsg());
    }

    public static boolean supported() {
        var jdkVersion = Runtime.version().feature();
        var arch = System.getProperty("os.arch");
        var osName = System.getProperty("os.name");

        if (jdkVersion >= 21
            && (arch.equals("aarch64") && (osName.startsWith("Mac") || osName.equals("Linux"))
                || arch.equals("amd64") && osName.equals("Linux"))) {
            assertThat(factory, isPresent());
            return true;
        } else {
            assertThat(factory, not(isPresent()));
            return false;
        }
    }

    public static String notSupportedMsg() {
        return "Not supported on [" + platformMsg() + "]";
    }

    public static String platformMsg() {
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

    static IntFunction<float[]> FLOAT_ARRAY_RANDOM_FUNC = size -> {
        float[] fa = new float[size];
        for (int i = 0; i < size; i++) {
            fa[i] = randomFloat();
        }
        return fa;
    };

    static IntFunction<float[]> FLOAT_ARRAY_MAX_FUNC = size -> {
        float[] fa = new float[size];
        Arrays.fill(fa, Float.MAX_VALUE);
        return fa;
    };
}
