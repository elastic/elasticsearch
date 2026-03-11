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
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.readBlockSizeDigit;
import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.scanBlockOffsets;

/**
 * Unit tests for {@link Bzip2BlockScanner}.
 */
public class Bzip2BlockScannerTests extends ESTestCase {

    public void testScanFindsBlockInSingleBlockFile() throws IOException {
        byte[] data = "hello world".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        // Skip the 4-byte header, scan the rest
        byte[] payload = new byte[compressed.length - 4];
        System.arraycopy(compressed, 4, payload, 0, payload.length);

        long[] offsets = scanBlockOffsets(new ByteArrayInputStream(payload), payload.length);
        assertTrue("Should find at least one block", offsets.length >= 1);
        assertEquals("First block should start at offset 0", 0, offsets[0]);
    }

    public void testScanFindsMultipleBlocks() throws IOException {
        // Generate enough data to create multiple blocks with the smallest block size (100k)
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb.append("Line ").append(i).append(": This is a test line with some data to fill up the bzip2 block.\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        // Skip the 4-byte header
        byte[] payload = new byte[compressed.length - 4];
        System.arraycopy(compressed, 4, payload, 0, payload.length);

        long[] offsets = scanBlockOffsets(new ByteArrayInputStream(payload), payload.length);
        assertTrue("Should find multiple blocks for large data with small block size", offsets.length > 1);

        // Offsets should be sorted
        for (int i = 1; i < offsets.length; i++) {
            assertTrue("Offsets should be sorted", offsets[i] > offsets[i - 1]);
        }
    }

    public void testScanEmptyRange() throws IOException {
        long[] offsets = scanBlockOffsets(new ByteArrayInputStream(new byte[0]), 0);
        assertEquals(0, offsets.length);
    }

    public void testReadBlockSizeDigitValid() {
        byte[] header = new byte[] { 'B', 'Z', 'h', '9' };
        assertEquals('9', readBlockSizeDigit(header));

        header[3] = '1';
        assertEquals('1', readBlockSizeDigit(header));
    }

    public void testReadBlockSizeDigitInvalidMagic() {
        byte[] header = new byte[] { 'X', 'Z', 'h', '9' };
        expectThrows(IllegalArgumentException.class, () -> readBlockSizeDigit(header));
    }

    public void testReadBlockSizeDigitInvalidDigit() {
        byte[] header = new byte[] { 'B', 'Z', 'h', '0' };
        expectThrows(IllegalArgumentException.class, () -> readBlockSizeDigit(header));
    }

    public void testReadBlockSizeDigitTooShort() {
        byte[] header = new byte[] { 'B', 'Z' };
        expectThrows(IllegalArgumentException.class, () -> readBlockSizeDigit(header));
    }

    public void testScanWithLimitedRange() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 5000; i++) {
            sb.append("Line ").append(i).append(": padding data to fill blocks.\n");
        }
        byte[] data = sb.toString().getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        // Scan only the first half of the payload
        byte[] payload = new byte[compressed.length - 4];
        System.arraycopy(compressed, 4, payload, 0, payload.length);

        long halfLength = payload.length / 2;
        long[] fullOffsets = scanBlockOffsets(new ByteArrayInputStream(payload), payload.length);
        long[] halfOffsets = scanBlockOffsets(new ByteArrayInputStream(payload), halfLength);

        assertTrue("Half scan should find fewer or equal blocks", halfOffsets.length <= fullOffsets.length);
    }

    private static byte[] bzip2(byte[] input, int blockSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(baos, blockSize)) {
            out.write(input);
        }
        return baos.toByteArray();
    }
}
