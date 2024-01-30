/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.zstd;

import org.elasticsearch.secure_sm.SecureSM;
import org.elasticsearch.secure_sm.ThreadPermission;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.nio.ByteBuffer;
import java.security.Permission;
import java.security.Policy;
import java.security.ProtectionDomain;

public class ZstdTests extends ESTestCase {

    public void testMaxCompressedLength() {
        assertThat(Zstd.getMaxCompressedLen(0), Matchers.greaterThanOrEqualTo(1));
        assertThat(Zstd.getMaxCompressedLen(100), Matchers.greaterThanOrEqualTo(100));
        expectThrows(IllegalArgumentException.class, () -> Zstd.getMaxCompressedLen(Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> Zstd.getMaxCompressedLen(-1));
        expectThrows(IllegalArgumentException.class, () -> Zstd.getMaxCompressedLen(-100));
        expectThrows(IllegalArgumentException.class, () -> Zstd.getMaxCompressedLen(Integer.MIN_VALUE));
    }

    public void testEmpty() {
        ByteBuffer uncompressed = ByteBuffer.allocate(0);
        ByteBuffer compressed = ByteBuffer.allocate(Zstd.getMaxCompressedLen(0));
        int compressedLen = Zstd.compress(compressed, uncompressed, randomIntBetween(0, 10));
        assertThat(compressedLen, Matchers.greaterThan(0));
        compressed.limit(compressedLen);
        ByteBuffer decompressed = ByteBuffer.allocate(0);
        assertEquals(0, Zstd.decompress(decompressed, compressed));
    }

    public void testOneByte() {
        ByteBuffer uncompressed = ByteBuffer.wrap(new byte[] { 'z' });
        ByteBuffer compressed = ByteBuffer.allocate(Zstd.getMaxCompressedLen(1));
        int compressedLen = Zstd.compress(compressed, uncompressed, randomIntBetween(0, 10));
        assertThat(compressedLen, Matchers.greaterThan(0));
        compressed.limit(compressedLen);
        ByteBuffer decompressed = ByteBuffer.allocate(1);
        assertEquals(1, Zstd.decompress(decompressed, compressed));
        assertEquals(uncompressed, decompressed);
    }

    public void testBufferOverflowOnCompression() {
        ByteBuffer uncompressed = ByteBuffer.wrap(new byte[] { 'z' });
        ByteBuffer compressed = ByteBuffer.allocate(0);
        expectThrows(IllegalArgumentException.class, () -> Zstd.compress(compressed, uncompressed, randomIntBetween(0, 10)));
    }

    public void testBufferOverflowOnDecompression() {
        ByteBuffer uncompressed = ByteBuffer.wrap(new byte[] { 'z' });
        ByteBuffer compressed = ByteBuffer.allocate(Zstd.getMaxCompressedLen(1));
        int compressedLen = Zstd.compress(compressed, uncompressed, randomIntBetween(0, 10));
        assertThat(compressedLen, Matchers.greaterThan(0));
        compressed.limit(compressedLen);
        ByteBuffer decompressed = ByteBuffer.allocate(0);
        expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(decompressed, compressed));
    }

    public void testNonZeroOffsets() {
        byte[] b = new byte[100];
        b[60] = 'z';
        ByteBuffer uncompressed = ByteBuffer.wrap(b, 60, 1);
        ByteBuffer compressed = ByteBuffer.allocate(42 + Zstd.getMaxCompressedLen(uncompressed.remaining()));
        compressed.position(42);
        int compressedLen = Zstd.compress(compressed, uncompressed, randomIntBetween(0, 10));
        compressed.limit(compressed.position() + compressedLen);
        for (int i = 0; i < compressed.capacity(); ++i) {
            if (i < compressed.position() || i > compressed.limit()) {
                // Bytes outside of the range have not been updated
                assertEquals(0, compressed.array()[i]);
            }
        }
        ByteBuffer decompressed = ByteBuffer.allocate(80);
        decompressed.position(10);
        assertEquals(1, Zstd.decompress(decompressed, compressed));
        decompressed.limit(decompressed.position() + 1);
        assertEquals(uncompressed, decompressed);
        for (int i = 0; i < decompressed.capacity(); ++i) {
            if (i < compressed.position() || i > compressed.limit()) {
                // Bytes outside of the range have not been updated
                assertEquals(0, compressed.array()[i]);
            }
        }
    }
}
