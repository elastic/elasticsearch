/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.nativeaccess;

import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;
import org.junit.BeforeClass;

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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/106347")
    public void testCompressValidation() {
        try (var src = nativeAccess.newBuffer(1000); var dst = nativeAccess.newBuffer(500)) {
            var srcBuf = src.buffer();
            var dstBuf = dst.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.compress(null, srcBuf, 0));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.compress(dstBuf, null, 0));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // dst capacity too low
            for (int i = 0; i < srcBuf.remaining(); ++i) {
                srcBuf.put(i, randomByte());
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.compress(dstBuf, srcBuf, 0));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/106347")
    public void testDecompressValidation() {
        try (
            var original = nativeAccess.newBuffer(1000);
            var compressed = nativeAccess.newBuffer(500);
            var restored = nativeAccess.newBuffer(500)
        ) {
            var originalBuf = original.buffer();
            var compressedBuf = compressed.buffer();

            var npe1 = expectThrows(NullPointerException.class, () -> zstd.decompress(null, originalBuf));
            assertThat(npe1.getMessage(), equalTo("Null destination buffer"));
            var npe2 = expectThrows(NullPointerException.class, () -> zstd.decompress(compressedBuf, null));
            assertThat(npe2.getMessage(), equalTo("Null source buffer"));

            // Invalid compressed format
            for (int i = 0; i < originalBuf.remaining(); ++i) {
                originalBuf.put(i, (byte) i);
            }
            var e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(compressedBuf, originalBuf));
            assertThat(e.getMessage(), equalTo("Unknown frame descriptor"));

            int compressedLength = zstd.compress(compressedBuf, originalBuf, 0);
            compressedBuf.limit(compressedLength);
            e = expectThrows(IllegalArgumentException.class, () -> zstd.decompress(restored.buffer(), compressedBuf));
            assertThat(e.getMessage(), equalTo("Destination buffer is too small"));

        }
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
        try (
            var original = nativeAccess.newBuffer(data.length);
            var compressed = nativeAccess.newBuffer(zstd.compressBound(data.length));
            var restored = nativeAccess.newBuffer(data.length)
        ) {
            original.buffer().put(0, data);
            int compressedLength = zstd.compress(compressed.buffer(), original.buffer(), randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedLength);
            int decompressedLength = zstd.decompress(restored.buffer(), compressed.buffer());
            assertThat(restored.buffer(), equalTo(original.buffer()));
            assertThat(decompressedLength, equalTo(data.length));
        }

        // Now with non-zero offsets
        final int compressedOffset = randomIntBetween(1, 1000);
        final int decompressedOffset = randomIntBetween(1, 1000);
        try (
            var original = nativeAccess.newBuffer(decompressedOffset + data.length);
            var compressed = nativeAccess.newBuffer(compressedOffset + zstd.compressBound(data.length));
            var restored = nativeAccess.newBuffer(decompressedOffset + data.length)
        ) {
            original.buffer().put(decompressedOffset, data);
            original.buffer().position(decompressedOffset);
            compressed.buffer().position(compressedOffset);
            int compressedLength = zstd.compress(compressed.buffer(), original.buffer(), randomIntBetween(-3, 9));
            compressed.buffer().limit(compressedOffset + compressedLength);
            restored.buffer().position(decompressedOffset);
            int decompressedLength = zstd.decompress(restored.buffer(), compressed.buffer());
            assertThat(
                restored.buffer().slice(decompressedOffset, data.length),
                equalTo(original.buffer().slice(decompressedOffset, data.length))
            );
            assertThat(decompressedLength, equalTo(data.length));
        }
    }
}
