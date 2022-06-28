/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class ByteUtilsTests extends ESTestCase {

    public void testZigZag(long l) {
        assertEquals(l, ByteUtils.zigZagDecode(ByteUtils.zigZagEncode(l)));
    }

    public void testZigZag() {
        testZigZag(0);
        testZigZag(1);
        testZigZag(-1);
        testZigZag(Long.MAX_VALUE);
        testZigZag(Long.MIN_VALUE);
        for (int i = 0; i < 1000; ++i) {
            testZigZag(randomLong());
            assertTrue(ByteUtils.zigZagEncode(randomInt(1000)) >= 0);
            assertTrue(ByteUtils.zigZagEncode(-randomInt(1000)) >= 0);
        }
    }

    public void testFloat() throws IOException {
        final float[] data = new float[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 4];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomFloat();
            ByteUtils.writeFloatLE(data[i], encoded, i * 4);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readFloatLE(encoded, i * 4), Float.MIN_VALUE);
        }
    }

    public void testDouble() throws IOException {
        final double[] data = new double[scaledRandomIntBetween(1000, 10000)];
        final byte[] encoded = new byte[data.length * 8];
        for (int i = 0; i < data.length; ++i) {
            data[i] = randomDouble();
            ByteUtils.writeDoubleLE(data[i], encoded, i * 8);
        }
        for (int i = 0; i < data.length; ++i) {
            assertEquals(data[i], ByteUtils.readDoubleLE(encoded, i * 8), Double.MIN_VALUE);
        }
    }

}
