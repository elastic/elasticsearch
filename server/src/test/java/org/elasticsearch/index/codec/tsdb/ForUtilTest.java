/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import junit.framework.TestCase;

import org.apache.lucene.store.ByteArrayDataInput;
import org.apache.lucene.store.ByteArrayDataOutput;
import org.apache.lucene.store.DataInput;
import org.apache.lucene.store.DataOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

public class ForUtilTest extends TestCase {

    private static final ForUtil forUtil = new ForUtil();

    public void testEncodeDecodeBitsPerValue() throws IOException {
        int size = 128;
        byte[] dataOutputBuffer = new byte[16 * 1024];
        byte[] dataInputBuffer = new byte[16 * 1024];
        long[] decodeBuffer = new long[size];

        for (int bitsPerValue = 1; bitsPerValue < 32; bitsPerValue++) {
            long[] values = randomValues(bitsPerValue, size);

            // Encode
            DataOutput dataOutput = new ByteArrayDataOutput(dataOutputBuffer);
            long[] encodeBuffer = Arrays.copyOf(values, values.length);
            forUtil.encode(encodeBuffer, bitsPerValue, dataOutput);

            // Prepare for decoding
            DataInput dataInput = new ByteArrayDataInput(dataInputBuffer);
            System.arraycopy(dataOutputBuffer, 0, dataInputBuffer, 0, dataOutputBuffer.length);

            // Decode
            forUtil.decode(bitsPerValue, dataInput, decodeBuffer);

            assertArrayEquals(decodeBuffer, values);
        }
    }

    private long[] randomValues(int bitsPerValue, int size) {
        final Random random = new Random(0);
        long[] values = new long[size];
        long upperBound = 1L << bitsPerValue;
        for (int i = 0; i < size; i++) {
            values[i] = random.nextLong(upperBound);
        }
        return values;
    }

}
