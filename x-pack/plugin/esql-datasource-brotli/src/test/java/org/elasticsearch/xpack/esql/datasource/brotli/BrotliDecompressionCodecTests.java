/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.brotli;

import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Base64;

/**
 * Unit tests for {@link BrotliDecompressionCodec}.
 *
 * <p>Since {@code org.brotli:dec} is decode-only, test data is pre-compressed
 * and embedded as Base64-encoded constants.
 */
public class BrotliDecompressionCodecTests extends ESTestCase {

    // "hello,world\n1,2\nbar,baz" compressed with Brotli (brotli CLI, quality 1)
    // Generated via: printf 'hello,world\n1,2\nbar,baz' | brotli -1 | base64
    private static final String HELLO_CSV_BR_BASE64 = "DwuAaGVsbG8sd29ybGQKMSwyCmJhcixiYXoD";

    // Empty input compressed with Brotli
    // Generated via: printf '' | brotli | base64
    private static final String EMPTY_BR_BASE64 = "Pw==";

    public void testNameAndExtensions() {
        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();
        assertEquals("brotli", codec.name());
        assertEquals(1, codec.extensions().size());
        assertTrue(codec.extensions().contains(".br"));
    }

    public void testDecompression() throws IOException {
        byte[] compressed = Base64.getDecoder().decode(HELLO_CSV_BR_BASE64);
        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals("hello,world\n1,2\nbar,baz", new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidBrotliThrows() {
        byte[] invalidBrotli = new byte[] { 0x00, 0x01, 0x02, 0x03 };
        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidBrotli))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = Base64.getDecoder().decode(EMPTY_BR_BASE64);
        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    public void testTruncatedStreamThrows() {
        byte[] compressed = Base64.getDecoder().decode(HELLO_CSV_BR_BASE64);
        // Truncate to just 3 bytes — enough to start parsing but not enough to finish
        byte[] truncated = new byte[] { compressed[0], compressed[1], compressed[2] };

        BrotliDecompressionCodec codec = new BrotliDecompressionCodec();
        expectThrows(IOException.class, () -> {
            try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(truncated))) {
                decompressed.readAllBytes();
            }
        });
    }
}
