/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.io.stream;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;

public class SlicedOutputStreamTests extends ESTestCase {

    public void testBulkWrite() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var backing = new ByteArrayOutputStream();
        var out = new SlicedOutputStream(backing, offset, length);
        out.write(data, 0, data.length);
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), backing.toByteArray());
    }

    public void testSingleByteWrites() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var backing = new ByteArrayOutputStream();
        var out = new SlicedOutputStream(backing, offset, length);
        for (byte b : data) {
            out.write(b);
        }
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), backing.toByteArray());
    }

    public void testChunkedWrites() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var backing = new ByteArrayOutputStream();
        var out = new SlicedOutputStream(backing, offset, length);
        int written = 0;
        while (written < data.length) {
            int chunkSize = randomIntBetween(1, data.length - written);
            out.write(data, written, chunkSize);
            written += chunkSize;
        }
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), backing.toByteArray());
    }

    public void testLongMaxValueLengthCapturesFromOffsetToEnd() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        var backing = new ByteArrayOutputStream();
        var out = new SlicedOutputStream(backing, offset, Long.MAX_VALUE);
        out.write(data, 0, data.length);
        assertArrayEquals(Arrays.copyOfRange(data, offset, data.length), backing.toByteArray());
    }

    public void testOffsetBeyondInput() throws IOException {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 100));
        int offset = randomIntBetween(data.length, data.length + 100);
        var backing = new ByteArrayOutputStream();
        var out = new SlicedOutputStream(backing, offset, randomIntBetween(1, 10));
        out.write(data, 0, data.length);
        assertEquals(0, backing.toByteArray().length);
    }
}
