/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.snappy;

import org.elasticsearch.test.ESTestCase;
import org.xerial.snappy.SnappyFramedOutputStream;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Unit tests for {@link SnappyDecompressionCodec}.
 */
public class SnappyDecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        assertEquals("snappy", codec.name());
        assertEquals(1, codec.extensions().size());
        assertTrue(codec.extensions().contains(".snappy"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = snappyFrame(original.getBytes(StandardCharsets.UTF_8));

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidSnappyThrows() {
        byte[] invalidSnappy = new byte[] { 0x00, 0x01, 0x02 };
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidSnappy))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = snappyFrame(new byte[0]);
        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testTruncatedStreamThrows() throws IOException {
        String original = "hello,world\n1,2\nbar,baz\nsome,more,data,here";
        byte[] compressed = snappyFrame(original.getBytes(StandardCharsets.UTF_8));
        // Truncate to half the compressed data
        byte[] truncated = Arrays.copyOf(compressed, compressed.length / 2);

        SnappyDecompressionCodec codec = new SnappyDecompressionCodec();
        expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(truncated))) {
                decompressed.readAllBytes();
            }
        });
    }

    private static byte[] snappyFrame(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (SnappyFramedOutputStream snappyOut = new SnappyFramedOutputStream(baos)) {
            snappyOut.write(input);
        }
        return baos.toByteArray();
    }
}
