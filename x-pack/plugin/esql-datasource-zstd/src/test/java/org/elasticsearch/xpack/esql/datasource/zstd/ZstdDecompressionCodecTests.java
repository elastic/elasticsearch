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

    private static byte[] zstd(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ZstdOutputStream zstdOut = new ZstdOutputStream(baos)) {
            zstdOut.write(input);
        }
        return baos.toByteArray();
    }
}
