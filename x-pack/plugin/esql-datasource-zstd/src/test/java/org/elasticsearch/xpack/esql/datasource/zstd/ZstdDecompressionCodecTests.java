/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.zstd;

import com.github.luben.zstd.ZstdOutputStream;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link ZstdDecompressionCodec}.
 */
public class ZstdDecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();
        assertEquals("zstd", codec.name());
        assertTrue(codec.extensions().contains(".zst"));
        assertTrue(codec.extensions().contains(".zstd"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = zstd(original.getBytes(StandardCharsets.UTF_8));

        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidZstdThrows() throws IOException {
        byte[] invalidZstd = new byte[] { 0x00, 0x01, 0x02 };
        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidZstd))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = zstd(new byte[0]);
        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    /**
     * Pin issue #811's scope: the streaming codec path must not pull in any
     * {@code com.github.luben.zstd.*} class. The parent compression-libs plugin still ships
     * zstd-jni on the runtime classpath (for the Parquet cold {@code byte[]} path), so an
     * accidental {@code import com.github.luben.zstd.ZstdInputStream} in
     * {@link ZstdDecompressionCodec} would compile and run silently — undoing the whole point of
     * this PR. This test fails fast if that ever regresses.
     *
     * <p>We assert on class names as strings rather than {@code instanceof PanamaZstdInputStream}
     * because the parent compression-libs project isn't on this module's test runtime classpath.
     * String matching is also exactly the right granularity for a "boundary" guard — it doesn't
     * care which Panama wrapper class is used, only that the chain holds no zstd-jni.
     */
    public void testStreamingPathDoesNotLoadZstdJni() throws IOException {
        byte[] compressed = zstd("hi".getBytes(StandardCharsets.UTF_8));
        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            String topClass = decompressed.getClass().getName();
            assertFalse(
                "Streaming codec returned a zstd-jni-backed stream [" + topClass + "]; #811 scope regressed",
                topClass.startsWith("com.github.luben.zstd.")
            );
            // The expected stream is the Panama wrapper; pin its FQN so the test also catches a
            // future "I'll just swap in another zstd-jni-backed wrapper" change.
            assertEquals(
                "Streaming codec must return the Panama FFI wrapper",
                "org.elasticsearch.xpack.esql.datasource.compress.PanamaZstdInputStream",
                topClass
            );
        }
    }

    /**
     * Decompresses a ~5 MB payload end-to-end to exercise multi-chunk refill behavior of the
     * Panama streaming wrapper through the public codec path. Sized to span ≥ 30 of libzstd's
     * 128 KB recommended input chunks, ensuring the wrapper's refill/needRead loop runs many times.
     */
    public void testLargeRoundTrip() throws IOException {
        byte[] data = new byte[5 * 1024 * 1024];
        for (int i = 0; i < data.length; i++) {
            // Mildly compressible payload (cycling pattern with the high bit flipped every 256
            // bytes) so the decoder produces a non-trivial frame structure rather than a single
            // RLE block — exercises the internal buffering paths the wrapper has to drain across
            // multiple read() calls.
            data[i] = (byte) (((i * 31) ^ (i >>> 8)) & 0xFF);
        }
        byte[] compressed = zstd(data);

        ZstdDecompressionCodec codec = new ZstdDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertArrayEquals(data, result);
        }
    }

    private static byte[] zstd(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(baos)) {
            zstdOut.write(input);
        }
        return baos.toByteArray();
    }
}
