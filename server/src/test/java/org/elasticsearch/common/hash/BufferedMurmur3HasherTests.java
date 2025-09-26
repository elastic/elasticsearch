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

    private final BufferedMurmur3Hasher bufferedHasher = new BufferedMurmur3Hasher(0, randomIntBetween(32, 128));
    private final Murmur3Hasher hasher = new Murmur3Hasher(0);

    public void testAddString() {
        String testString = randomUnicodeOfLengthBetween(0, 1024);
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

    public void testAddLongs() {
        long value1 = randomLong();
        long value2 = randomLong();
        long value3 = randomLong();
        long value4 = randomLong();
        bufferedHasher.addLong(value1);
        bufferedHasher.addLongs(value1, value2);
        bufferedHasher.addLongs(value1, value2, value3, value4);

        hasher.update(toBytes(value1));

        hasher.update(toBytes(value1));
        hasher.update(toBytes(value2));

        hasher.update(toBytes(value1));
        hasher.update(toBytes(value2));
        hasher.update(toBytes(value3));
        hasher.update(toBytes(value4));

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testAddTwoLongs() {
        long value1 = randomLong();
        long value2 = randomLong();

        bufferedHasher.addLongs(value1, value2);

        hasher.update(toBytes(value1));
        hasher.update(toBytes(value2));

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testAddFourLongs() {
        long value1 = randomLong();
        long value2 = randomLong();
        long value3 = randomLong();
        long value4 = randomLong();

        bufferedHasher.addLongs(value1, value2, value3, value4);

        hasher.update(toBytes(value1));
        hasher.update(toBytes(value2));
        hasher.update(toBytes(value3));
        hasher.update(toBytes(value4));

        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testRandomAdds() {
        int numAdds = randomIntBetween(128, 1024);
        for (int i = 0; i < numAdds; i++) {
            switch (randomIntBetween(0, 4)) {
                case 0 -> {
                    String randomString = randomUnicodeOfLengthBetween(0, 64);
                    bufferedHasher.addString(randomString);
                    BytesRef bytesRef = new BytesRef(randomString);
                    hasher.update(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                }
                case 1 -> {
                    String emptyString = "";
                    bufferedHasher.addString(emptyString);
                    BytesRef bytesRef = new BytesRef(emptyString);
                    hasher.update(bytesRef.bytes, bytesRef.offset, bytesRef.length);
                }
                case 2 -> {
                    long randomLong = randomLong();
                    bufferedHasher.addLong(randomLong);
                    hasher.update(toBytes(randomLong));
                }
                case 3 -> {
                    long randomLong1 = randomLong();
                    long randomLong2 = randomLong();
                    bufferedHasher.addLongs(randomLong1, randomLong2);
                    hasher.update(toBytes(randomLong1));
                    hasher.update(toBytes(randomLong2));
                }
                case 4 -> {
                    long randomLong1 = randomLong();
                    long randomLong2 = randomLong();
                    long randomLong3 = randomLong();
                    long randomLong4 = randomLong();
                    bufferedHasher.addLongs(randomLong1, randomLong2, randomLong3, randomLong4);
                    hasher.update(toBytes(randomLong1));
                    hasher.update(toBytes(randomLong2));
                    hasher.update(toBytes(randomLong3));
                    hasher.update(toBytes(randomLong4));
                }
            }
        }
        assertEquals(hasher.digestHash(), bufferedHasher.digestHash());
    }

    public void testReset() {
        bufferedHasher.addString(randomUnicodeOfLengthBetween(0, 1024));
        bufferedHasher.addLong(randomLong());
        bufferedHasher.addLongs(randomLong(), randomLong());
        bufferedHasher.addLongs(randomLong(), randomLong(), randomLong(), randomLong());
        bufferedHasher.reset();
        assertEquals(new MurmurHash3.Hash128(0, 0), bufferedHasher.digestHash());
    }

    private byte[] toBytes(long value) {
        byte[] bytes = new byte[Long.BYTES];
        ByteUtils.writeLongLE(value, bytes, 0);
        return bytes;
    }
}
