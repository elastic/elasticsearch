/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.lz4;

import net.jpountz.lz4.LZ4FrameOutputStream;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Unit tests for {@link Lz4DecompressionCodec}.
 */
public class Lz4DecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        Lz4DecompressionCodec codec = new Lz4DecompressionCodec();
        assertEquals("lz4", codec.name());
        assertEquals(1, codec.extensions().size());
        assertTrue(codec.extensions().contains(".lz4"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = lz4(original.getBytes(StandardCharsets.UTF_8));

        Lz4DecompressionCodec codec = new Lz4DecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidLz4Throws() {
        byte[] invalidLz4 = new byte[] { 0x00, 0x01, 0x02 };
        Lz4DecompressionCodec codec = new Lz4DecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidLz4))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = lz4(new byte[0]);
        Lz4DecompressionCodec codec = new Lz4DecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testTruncatedStreamThrows() throws IOException {
        String original = "hello,world\n1,2\nbar,baz\nsome,more,data,here";
        byte[] compressed = lz4(original.getBytes(StandardCharsets.UTF_8));
        // Truncate to half the compressed data
        byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

        Lz4DecompressionCodec codec = new Lz4DecompressionCodec();
        expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(truncated))) {
                decompressed.readAllBytes();
            }
        });
    }

    private static byte[] lz4(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (LZ4FrameOutputStream lz4Out = new LZ4FrameOutputStream(baos)) {
            lz4Out.write(input);
        }
        return baos.toByteArray();
    }
}
