/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

/**
 * Unit tests for {@link Bzip2DecompressionCodec}.
 */
public class Bzip2DecompressionCodecTests extends ESTestCase {

    public void testNameAndExtensions() {
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
        assertEquals("bzip2", codec.name());
        assertTrue(codec.extensions().contains(".bz2"));
        assertTrue(codec.extensions().contains(".bz"));
    }

    public void testRoundTripDecompression() throws IOException {
        String original = "hello,world\n1,2\nbar,baz";
        byte[] compressed = bzip2(original.getBytes(StandardCharsets.UTF_8));

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testInvalidBzip2Throws() throws IOException {
        byte[] invalidBzip2 = new byte[] { 0x00, 0x01, 0x02 };
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();

        IOException e = expectThrows(IOException.class, () -> {
            try (InputStream ignored = codec.decompress(new ByteArrayInputStream(invalidBzip2))) {
                ignored.readAllBytes();
            }
        });
        assertNotNull(e.getMessage());
    }

    public void testEmptyInput() throws IOException {
        byte[] compressed = bzip2(new byte[0]);
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();

        try (InputStream decompressed = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = decompressed.readAllBytes();
            assertEquals(0, result.length);
        }
    }

    private static byte[] bzip2(byte[] input) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream bzip2Out = new BZip2CompressorOutputStream(baos)) {
            bzip2Out.write(input);
        }
        return baos.toByteArray();
    }
}
