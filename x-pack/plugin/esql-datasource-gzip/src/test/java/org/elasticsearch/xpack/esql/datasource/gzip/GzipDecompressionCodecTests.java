/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.gzip;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.zip.GZIPOutputStream;

/**
 * Unit tests for {@link GzipDecompressionCodec}.
 */
public class GzipDecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        assertEquals("gzip", codec.name());
        assertTrue(codec.extensions().contains(".gz"));
        assertTrue(codec.extensions().contains(".gzip"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = gzip(original.getBytes(StandardCharsets.UTF_8));

        GzipDecompressionCodec codec = new GzipDecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidGzipThrows() throws IOException {
        byte[] invalidGzip = new byte[] { 0x00, 0x01, 0x02 };
        GzipDecompressionCodec codec = new GzipDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidGzip))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = gzip(new byte[0]);
        GzipDecompressionCodec codec = new GzipDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    private static byte[] gzip(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzipOut = new GZIPOutputStream(baos)) {
            gzipOut.write(input);
        }
        return baos.toByteArray();
    }
}
