/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class ZstdTests extends ESTestCase {

    static NativeAccess nativeAccess;
    static Zstd zstd;

    @BeforeClass
    public static void getZstd() {
        nativeAccess = NativeAccess.instance();
        zstd = nativeAccess.getZstd();
    }

    public void testCompressBound() {
        assertThat(zstd.compressBound(0), Matchers.greaterThanOrEqualTo(1));
        assertThat(zstd.compressBound(100), Matchers.greaterThanOrEqualTo(100));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(-1));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(-100));
        expectThrows(IllegalArgumentException.class, () -> zstd.compressBound(Integer.MIN_VALUE));
    }

    public void testCompressValidation() {
        try (var src = nativeAccess.newConfinedBuffer(1000); var dst = nativeAccess.newConfinedBuffer(500)) {
            var srcBuf = src.buffer();
            var dstBuf = dst.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.compress(null, src, 0));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.compress(dst, null, 0));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // dst capacity too low
            for (int i = 0; i < srcBuf.remaining(); ++i) {
                srcBuf.put(i, randomByte());
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.compress(dst, src, 0));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));
        }
    }

    public void testDecompressValidation() {
        try (
            var original = nativeAccess.newConfinedBuffer(1000);
            var compressed = nativeAccess.newConfinedBuffer(500);
            var restored = nativeAccess.newConfinedBuffer(500)
        ) {
            var originalBuf = original.buffer();
            var compressedBuf = compressed.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, original));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(compressed, (CloseableByteBuffer) null));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // Invalid compressed format
            for (int i = 0; i < originalBuf.remaining(); ++i) {
                originalBuf.put(i, (byte) i);
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(compressed, original));
            assertThat(e.getMessage(), equalTo("Unknown frame descriptor"));

            int compressedLength = zstd.compress(compressed, original, 0);
            compressedBuf.limit(compressedLength);
            e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(restored, compressed));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));

        }
    }

    public void testDecompressDirectByteBufferValidation() {
        try (var dst = nativeAccess.newConfinedBuffer(500)) {
            var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, ByteBuffer.allocateDirect(1)));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(dst, (ByteBuffer) null));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            var heapBuf = ByteBuffer.allocate(100);
            var iae = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(dst, heapBuf));
            assertThat(iae.getMessage(), equalTo("Source buffer must be direct"));
        }
    }

    public void testOneByte() {
        doTestRoundtrip(new byte[] { 'z' });
    }

    public void testOneByteDirectByteBuffer() {
        doTestRoundtripWithDirectByteBuffer(new byte[] { 'z' });
    }

    public void testConstant() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtrip(b);
    }

    public void testConstantDirectByteBuffer() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        Arrays.fill(b, randomByte());
        doTestRoundtripWithDirectByteBuffer(b);
    }

    public void testCycle() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtrip(b);
    }

    public void testCycleDirectByteBuffer() {
        byte[] b = new byte[randomIntBetween(100, 1000)];
        for (int i = 0; i < b.length; ++i) {
            b[i] = (byte) (i & 0x0F);
        }
        doTestRoundtripWithDirectByteBuffer(b);
    }

    /**
     * Compress with CloseableByteBuffer, then decompress using a direct ByteBuffer source
     * to exercise the {@link Zstd#decompress(CloseableByteBuffer, ByteBuffer)} overload.
     */
    private void doTestRoundtripWithDirectByteBuffer(byte[] data) {
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(data.length)
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));

            ByteBuffer directSrc = ByteBuffer.allocateDirect(compressedLength);
            for (int i = 0; i < compressedLength; i++) {
                directSrc.put(i, compressed.buffer().get(i));
            }

            int decompressedLength = zstd.decompress(restored, directSrc);
            assertThat(restored.buffer(), equalTo(original.buffer()));
            assertThat(decompressedLength, equalTo(data.length));
        }
    }

    private void doTestRoundtrip(byte[] data) {
        try (
            var original = nativeAccess.newConfinedBuffer(data.length);
            var compressed = nativeAccess.newConfinedBuffer(zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(data.length)
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedLength);
            int decompressedLength = zstd.decompress(restored, compressed);
            assertThat(restored.buffer(), equalTo(original.buffer()));
            assertThat(decompressedLength, equalTo(data.length));
        }

        // Now with non-zero offsets
        final int compressedOffset = randomIntBetween(1, 1000);
        final int decompressedOffset = randomIntBetween(1, 1000);
        try (
            var original = nativeAccess.newConfinedBuffer(decompressedOffset + data.length);
            var compressed = nativeAccess.newConfinedBuffer(compressedOffset + zstd.compressBound(data.length));
            var restored = nativeAccess.newConfinedBuffer(decompressedOffset + data.length)
        ) {
            original.buffer().put(decompressedOffset, data);
            original.buffer().position(decompressedOffset);
            compressed.buffer().position(compressedOffset);
            int compressedLength = zstd.compress(compressed, original, randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedOffset + compressedLength);
            restored.buffer().position(decompressedOffset);
            int decompressedLength = zstd.decompress(restored, compressed);
            assertThat(decompressedLength, equalTo(data.length));
            assertThat(
                restored.buffer().slice(decompressedOffset, data.length),
                equalTo(original.buffer().slice(decompressedOffset, data.length))
            );
        }
    }
}
