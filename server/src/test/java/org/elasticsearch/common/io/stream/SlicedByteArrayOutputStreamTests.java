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

import java.util.Arrays;

public class SlicedByteArrayOutputStreamTests extends ESTestCase {

    public void testBulkWrite() {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var out = new SlicedByteArrayOutputStream(offset, length);
        out.write(data, 0, data.length);
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), out.toByteArray());
    }

    public void testSingleByteWrites() {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var out = new SlicedByteArrayOutputStream(offset, length);
        for (byte b : data) {
            out.write(b);
        }
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), out.toByteArray());
    }

    public void testChunkedWrites() {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var out = new SlicedByteArrayOutputStream(offset, length);
        int written = 0;
        while (written < data.length) {
            int chunkSize = randomIntBetween(1, data.length - written);
            out.write(data, written, chunkSize);
            written += chunkSize;
        }
        assertArrayEquals(Arrays.copyOfRange(data, offset, offset + length), out.toByteArray());
    }

    public void testOffsetBeyondInput() {
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 100));
        int offset = randomIntBetween(data.length, data.length + 100);
        var out = new SlicedByteArrayOutputStream(offset, randomIntBetween(1, 10));
        out.write(data, 0, data.length);
        assertEquals(0, out.toByteArray().length);
    }

    public void testWriteEndingExactlyAtOffsetCollectsNothing() {
        // writeEnd == start: condition is writeEnd > start (strict), so nothing collected
        byte[] data = randomByteArrayOfLength(randomIntBetween(2, 200));
        int offset = randomIntBetween(1, data.length);
        var out = new SlicedByteArrayOutputStream(offset, randomIntBetween(1, 10));
        out.write(data, 0, offset);
        assertEquals(0, out.toByteArray().length);
    }

    public void testWriteStartingExactlyAtEndCollectsNothing() {
        // bytesConsumed == end: condition is bytesConsumed < end (strict), so nothing collected
        byte[] data = randomByteArrayOfLength(randomIntBetween(2, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var out = new SlicedByteArrayOutputStream(offset, length);
        out.write(data, 0, offset + length);
        int collectedSoFar = out.toByteArray().length;
        out.write(data, 0, randomIntBetween(1, 10));
        assertArrayEquals(out.toByteArray(), Arrays.copyOf(out.toByteArray(), collectedSoFar));
    }

    public void testFirstAndLastByteOfSliceAreIncluded() {
        // Both boundary bytes (at offset and at offset+length-1) must be collected
        byte[] data = randomByteArrayOfLength(randomIntBetween(1, 200));
        int offset = randomIntBetween(0, data.length - 1);
        int length = randomIntBetween(1, data.length - offset);
        var out = new SlicedByteArrayOutputStream(offset, length);
        for (byte b : data) {
            out.write(b);
        }
        byte[] result = out.toByteArray();
        assertEquals(length, result.length);
        assertEquals(data[offset], result[0]);
        assertEquals(data[offset + length - 1], result[length - 1]);
    }
}
