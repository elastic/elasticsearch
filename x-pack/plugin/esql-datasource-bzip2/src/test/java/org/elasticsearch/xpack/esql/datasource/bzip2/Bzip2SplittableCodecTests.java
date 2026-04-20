/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.bzip2;

import org.apache.commons.compress.compressors.bzip2.BZip2CompressorOutputStream;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;

import static org.elasticsearch.xpack.esql.datasource.bzip2.Bzip2BlockScanner.scanBlockOffsets;

/**
 * Tests for the bzip2 splittable decompression codec, verifying both stream-only
 * and block-aligned split decompression with NDJSON and CSV data.
 */
public class Bzip2SplittableCodecTests extends ESTestCase {

    public void testDecompressStream() throws IOException {
        String original = "hello world, this is a test";
        byte[] data = original.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = stream.readAllBytes();
            assertEquals(original, new String(result, StandardCharsets.UTF_8));
        }
    }

    public void testDecompressLargeData() throws IOException {
        byte[] data = generateNdJsonData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            byte[] result = stream.readAllBytes();
            assertArrayEquals(data, result);
        }
    }

    public void testCodecMetadata() {
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        assertEquals("bzip2", codec.name());
        assertTrue(codec.extensions().contains(".bz2"));
        assertTrue(codec.extensions().contains(".bz"));
    }

    public void testFindBlockBoundaries() throws IOException {
        byte[] data = generateNdJsonData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);

        assertTrue("Should find at least one block boundary", boundaries.length >= 1);
        for (int i = 1; i < boundaries.length; i++) {
            assertTrue("Boundaries must be sorted", boundaries[i] > boundaries[i - 1]);
        }
        for (long boundary : boundaries) {
            assertTrue("Boundary must be within file", boundary >= 0 && boundary < compressed.length);
        }
    }

    /**
     * When the compressed file is large enough, {@link Bzip2DecompressionCodec#findBlockBoundaries}
     * uses overlapped parallel scans; results must match a single full-stream scan.
     */
    public void testParallelFindBlockBoundariesMatchesFullScan() throws IOException {
        // Incompressible-ish payload; compressed size must exceed {@link Bzip2DecompressionCodec#MIN_PARALLEL_SCAN_BYTES}
        // (currently multi-megabyte), so raw length is sized with margin above that threshold.
        int rawLen = (int) Math.min(Integer.MAX_VALUE - 8, Bzip2DecompressionCodec.MIN_PARALLEL_SCAN_BYTES + 2 * 1024 * 1024);
        byte[] uncompressed = randomByteArrayOfLength(rawLen);
        byte[] compressed = bzip2(uncompressed, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        assertTrue(
            "Compressed fixture must exceed parallel threshold: " + compressed.length,
            compressed.length >= Bzip2DecompressionCodec.MIN_PARALLEL_SCAN_BYTES
        );

        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);
        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] parallelBoundaries = codec.findBlockBoundaries(object, 0, compressed.length);

        long[] expected;
        try (InputStream in = new ByteArrayInputStream(compressed)) {
            expected = scanBlockOffsets(in, compressed.length);
        }
        assertArrayEquals(expected, parallelBoundaries);
    }

    public void testDecompressRangeUnionEqualsOriginalNdJson() throws IOException {
        byte[] data = generateNdJsonData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);
        assertTrue("Need multiple blocks for this test", boundaries.length >= 2);

        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                reassembled.write(stream.readAllBytes());
            }
        }

        String result = reassembled.toString(StandardCharsets.UTF_8);
        String original = new String(data, StandardCharsets.UTF_8);
        assertEquals("Reassembled NDJSON should match original", original, result);
    }

    public void testDecompressRangeUnionEqualsOriginalCsv() throws IOException {
        byte[] data = generateCsvData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);
        assertTrue("Need multiple blocks for this test", boundaries.length >= 2);

        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                reassembled.write(stream.readAllBytes());
            }
        }

        String result = reassembled.toString(StandardCharsets.UTF_8);
        String original = new String(data, StandardCharsets.UTF_8);
        assertEquals("Reassembled CSV should match original", original, result);
    }

    public void testDecompressRangeSingleBlock() throws IOException {
        String small = "hello world\n";
        byte[] data = small.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);
        assertEquals("Small data should have exactly one block", 1, boundaries.length);

        try (InputStream stream = codec.decompressRange(object, boundaries[0], compressed.length)) {
            String result = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(small, result);
        }
    }

    public void testDecompressRangeWithVariousBlockSizes() throws IOException {
        byte[] data = generateNdJsonData(3000);

        for (int blockSize = BZip2CompressorOutputStream.MIN_BLOCKSIZE; blockSize <= 3; blockSize++) {
            byte[] compressed = bzip2(data, blockSize);
            ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

            Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
            long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);

            ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
            for (int i = 0; i < boundaries.length; i++) {
                long start = boundaries[i];
                long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
                try (InputStream stream = codec.decompressRange(object, start, end)) {
                    reassembled.write(stream.readAllBytes());
                }
            }

            assertArrayEquals("Block size " + blockSize + " should reassemble correctly", data, reassembled.toByteArray());
        }
    }

    public void testFindBlockBoundariesEmptyRange() throws IOException {
        byte[] data = generateNdJsonData(100);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        long[] boundaries = codec.findBlockBoundaries(object, 10, 10);
        assertEquals("Empty range should return no boundaries", 0, boundaries.length);
    }

    public void testDecompressRangeInvalidArguments() throws IOException {
        byte[] data = "test".getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        expectThrows(IllegalArgumentException.class, () -> codec.decompressRange(object, 10, 5));
    }

    private static byte[] generateNdJsonData(int lines) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < lines; i++) {
            sb.append("{\"id\":").append(i).append(",\"name\":\"record_").append(i).append("\",\"value\":").append(i * 1.5).append("}\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] generateCsvData(int rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("id,name,value\n");
        for (int i = 0; i < rows; i++) {
            sb.append(i).append(",item_").append(i).append(",").append(i * 2.5).append("\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static byte[] bzip2(byte[] input, int blockSize) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (BZip2CompressorOutputStream out = new BZip2CompressorOutputStream(baos, blockSize)) {
            out.write(input);
        }
        return baos.toByteArray();
    }
}
