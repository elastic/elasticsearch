/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.hash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.util.ByteUtils;
import org.elasticsearch.test.ESTestCase;

public class BufferedMurmur3HasherTests extends ESTestCase {

    private final BufferedMurmur3Hasher bufferedHasher = new BufferedMurmur3Hasher(0);
    private final Murmur3Hasher hasher = new Murmur3Hasher(0);

    public void testAddString() {
        String testString = randomUnicodeOfLengthBetween(10, 100);
        bufferedHasher.addString(testString);

        BytesRef bytesRef = new BytesRef(testString);
        hasher.update(bytesRef.bytes, bytesRef.offset, bytesRef.length);
        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testConstructorWithInvalidBufferSize() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> new BufferedMurmur3Hasher(0, 31));
        assertEquals("Buffer size must be at least 32 bytes", exception.getMessage());
    }

    public void testAddLong() {
        long value = randomLong();
        bufferedHasher.addLong(value);

        hasher.update(toBytes(value), 0, Long.BYTES);

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testAddTwoLongs() {
        long value1 = randomLong();
        long value2 = randomLong();

        bufferedHasher.addLongs(value1, value2);

        hasher.update(toBytes(value1), 0, Long.BYTES);
        hasher.update(toBytes(value2), 0, Long.BYTES);

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testAddFourLongs() {
        long value1 = randomLong();
        long value2 = randomLong();
        long value3 = randomLong();
        long value4 = randomLong();

        bufferedHasher.addLongs(value1, value2, value3, value4);

        hasher.update(toBytes(value1), 0, Long.BYTES);
        hasher.update(toBytes(value2), 0, Long.BYTES);
        hasher.update(toBytes(value3), 0, Long.BYTES);
        hasher.update(toBytes(value4), 0, Long.BYTES);

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    private static byte[] toBytes(int value) {
        byte[] bytes = new byte[4];
        ByteUtils.writeIntLE(value, bytes, 0);
        return bytes;
    }

    private byte[] toBytes(long value) {
        byte[] bytes = new byte[Long.BYTES];
        ByteUtils.writeLongLE(value, bytes, 0);
        return bytes;
    }
}
