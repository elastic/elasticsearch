/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.io.stream;

import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.RamUsageEstimator;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

public class BytesRefStreamOutputTests extends ESTestCase {
    public void testEmpty() throws Exception {
        BytesRefStreamOutput out = new BytesRefStreamOutput();

        assertNotNull(out.get());
        assertEquals(0, out.get().length);

        out.close();
    }

    public void testSingleByte() throws Exception {
        BytesRefStreamOutput out = new BytesRefStreamOutput();

        byte b = randomByte();
        out.writeByte(b);

        assertEquals(1, out.get().length);
        assertEquals(1, out.position());
        assertEquals(b, out.get().bytes[0]);

        // check reset
        out.reset();
        assertEquals(0, out.get().length);
        assertEquals(0, out.position());
        out.close();
    }

    public void testBytes() throws Exception {
        BytesRefStreamOutput out = new BytesRefStreamOutput();

        int size = randomIntBetween(10, 100);
        byte[] b = randomByteArrayOfLength(size);
        int offset = randomIntBetween(0, size - 10);
        int length = randomIntBetween(1, size - offset);

        out.writeBytes(b, offset, length);

        assertEquals(length, out.get().length);
        assertEquals(length, out.position());

        byte[] sliceExpected = Arrays.copyOfRange(b, offset, offset + length);
        byte[] sliceActual = Arrays.copyOfRange(out.get().bytes, 0, out.get().length);
        assertArrayEquals(sliceExpected, sliceActual);

        long expectedSize = RamUsageEstimator.shallowSizeOfInstance(BytesRefStreamOutput.class) + RamUsageEstimator.shallowSizeOfInstance(
            BytesRefBuilder.class
        ) + out.bytes().length;
        assertEquals(expectedSize, out.ramBytesUsed());

        // check reset
        out.reset();
        assertEquals(0, out.get().length);
        assertEquals(0, out.position());

        // a reset doesn't free memory
        assertEquals(expectedSize, out.ramBytesUsed());

        out.close();
    }

}
