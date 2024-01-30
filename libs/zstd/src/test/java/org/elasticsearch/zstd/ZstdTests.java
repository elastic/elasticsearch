/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.zstd;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class ZstdTests extends ESTestCase {

    public void testCompressBound() {
        assertThat(Zstd.compressBound(0), Matchers.greaterThanOrEqualTo(1));
        assertThat(Zstd.compressBound(100), Matchers.greaterThanOrEqualTo(100));
        expectThrows(IllegalArgumentException.class, () -> Zstd.compressBound(Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> Zstd.compressBound(-1));
        expectThrows(IllegalArgumentException.class, () -> Zstd.compressBound(-100));
        expectThrows(IllegalArgumentException.class, () -> Zstd.compressBound(Integer.MIN_VALUE));
    }

    public void testCompressValidation() {
        assertEquals(
            "Null destination buffer",
            expectThrows(NullPointerException.class, () -> Zstd.compress(null, ByteBuffer.allocate(1000), 0)).getMessage()
        );
        assertEquals(
            "Null source buffer",
            expectThrows(NullPointerException.class, () -> Zstd.compress(ByteBuffer.allocate(1000), null, 0)).getMessage()
        );
        // dst capacity too low
        byte[] toCompress = new byte[1000];
        for (int i = 0; i < toCompress.length; ++i) {
            toCompress[i] = randomByte();
        }
        assertEquals(
            "Destination buffer is too small",
            expectThrows(IllegalArgumentException.class, () -> Zstd.compress(ByteBuffer.allocate(500), ByteBuffer.wrap(toCompress), 0))
                .getMessage()
        );
    }

    public void testDecompressValidation() {
        assertEquals(
            "Null destination buffer",
            expectThrows(NullPointerException.class, () -> Zstd.decompress(null, ByteBuffer.allocate(1000))).getMessage()
        );
        assertEquals(
            "Null source buffer",
            expectThrows(NullPointerException.class, () -> Zstd.decompress(ByteBuffer.allocate(1000), null)).getMessage()
        );
        // Invalid compressed format
        byte[] toCompress = new byte[1000];
        for (int i = 0; i < toCompress.length; ++i) {
            toCompress[i] = (byte) i;
        }
        assertEquals(
            "Unknown frame descriptor",
            expectThrows(IllegalArgumentException.class, () -> Zstd.decompress(ByteBuffer.allocate(500), ByteBuffer.wrap(toCompress)))
                .getMessage()
        );
        final int compressedLength = Zstd.compress(ByteBuffer.wrap(toCompress), ByteBuffer.allocate(1000), 0);
        assertEquals(
            "Destination buffer is too small",
            expectThrows(
                IllegalArgumentException.class,
                () -> Zstd.decompress(ByteBuffer.allocate(500), ByteBuffer.wrap(toCompress, 0, compressedLength))
            ).getMessage()
        );
    }

    public void testEmpty() {
        doTestRoundtrip(new byte[0]);
    }

    public void testOneByte() {
        doTestRoundtrip(new byte[] { 'z' });
    }

    public void testConstant() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtrip(b);
    }

    public void testCycle() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtrip(b);
    }

    private void doTestRoundtrip(byte[] data) {
        {
            byte[] compressed = new byte[Zstd.compressBound(data.length)];
            final int compressedLength = Zstd.compress(ByteBuffer.wrap(compressed), ByteBuffer.wrap(data), randomIntBetween(-3, 9));
            compressed = Arrays.copyOf(compressed, compressedLength);
            byte[] restored = new byte[data.length];
            Zstd.decompress(ByteBuffer.wrap(restored), ByteBuffer.wrap(compressed, 0, compressed.length));
            assertArrayEquals(data, restored);
        }
        // Now with non-zero offsets
        {
            final int compressedOffset = randomIntBetween(1, 1000);
            final int decompressedOffset = randomIntBetween(1, 1000);
            byte[] dataCopy = new byte[decompressedOffset + data.length];
            System.arraycopy(data, 0, dataCopy, decompressedOffset, data.length);
            byte[] compressed = new byte[compressedOffset + Zstd.compressBound(data.length)];
            final int compressedLength = Zstd.compress(
                ByteBuffer.wrap(compressed, compressedOffset, compressed.length - compressedOffset),
                ByteBuffer.wrap(dataCopy, decompressedOffset, data.length),
                randomIntBetween(-3, 9)
            );
            byte[] restored = new byte[decompressedOffset + data.length];
            final int decompressedLen = Zstd.decompress(
                ByteBuffer.wrap(restored, decompressedOffset, data.length),
                ByteBuffer.wrap(compressed, compressedOffset, compressedLength)
            );
            assertEquals(data.length, decompressedLen);
            assertArrayEquals(data, Arrays.copyOfRange(restored, decompressedOffset, decompressedOffset + data.length));
        }
    }
}
