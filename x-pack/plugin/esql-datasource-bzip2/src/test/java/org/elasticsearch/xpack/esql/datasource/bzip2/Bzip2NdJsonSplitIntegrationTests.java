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
import java.util.ArrayList;
import java.util.List;

/**
 * Integration tests for bzip2 split decompression with NDJSON and CSV data,
 * verifying that block-by-block decompression produces identical output to
 * full-stream decompression.
 */
public class Bzip2NdJsonSplitIntegrationTests extends ESTestCase {

    public void testDecompressNdJson() throws IOException {
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 1000; i++) {
            records.add("{\"id\":" + i + ",\"name\":\"record_" + i + "\",\"value\":" + (i * 1.5) + "}");
        }
        String ndjson = String.join("\n", records) + "\n";
        byte[] data = ndjson.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            String result = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
            assertEquals("Decompressed data should match original", ndjson, result);

            String[] lines = result.split("\n");
            assertEquals(1000, lines.length);
            for (int i = 0; i < lines.length; i++) {
                assertTrue("Line " + i + " should start with {", lines[i].startsWith("{"));
                assertTrue("Line " + i + " should contain id", lines[i].contains("\"id\":" + i));
            }
        }
    }

    public void testFullDecompressionMatchesOriginal() throws IOException {
        String data = "line1\nline2\nline3\n";
        byte[] compressed = bzip2(data.getBytes(StandardCharsets.UTF_8), BZip2CompressorOutputStream.MIN_BLOCKSIZE);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
        String codecResult;
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            codecResult = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }

        assertEquals("Codec decompression should match original data", data, codecResult);
    }

    public void testSplitDecompressionMatchesFullDecompression() throws IOException {
        byte[] data = generateNdJsonData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();

        // Full decompression
        String fullResult;
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            fullResult = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }

        // Split decompression
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);
        assertTrue("Need multiple blocks", boundaries.length >= 2);

        ByteArrayOutputStream splitResult = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                splitResult.write(stream.readAllBytes());
            }
        }

        assertEquals("Split decompression must match full decompression", fullResult, splitResult.toString(StandardCharsets.UTF_8));
    }

    public void testSplitDecompressionCsvMatchesFullDecompression() throws IOException {
        byte[] data = generateCsvData(5000);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();

        String fullResult;
        try (InputStream stream = codec.decompress(new ByteArrayInputStream(compressed))) {
            fullResult = new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }

        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);
        assertTrue("Need multiple blocks for CSV", boundaries.length >= 2);

        ByteArrayOutputStream splitResult = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                splitResult.write(stream.readAllBytes());
            }
        }

        assertEquals("Split CSV decompression must match full decompression", fullResult, splitResult.toString(StandardCharsets.UTF_8));
    }

    public void testSplitDecompressionPreservesAllNdJsonRecords() throws IOException {
        int recordCount = 3000;
        byte[] data = generateNdJsonData(recordCount);
        byte[] compressed = bzip2(data, BZip2CompressorOutputStream.MIN_BLOCKSIZE);
        ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

        Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
        long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);

        ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
        for (int i = 0; i < boundaries.length; i++) {
            long start = boundaries[i];
            long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
            try (InputStream stream = codec.decompressRange(object, start, end)) {
                reassembled.write(stream.readAllBytes());
            }
        }

        String result = reassembled.toString(StandardCharsets.UTF_8);
        String[] lines = result.split("\n");
        assertEquals("All NDJSON records must be present", recordCount, lines.length);
        for (int i = 0; i < lines.length; i++) {
            assertTrue("Line " + i + " should contain its id", lines[i].contains("\"id\":" + i));
        }
    }

    public void testDecompressLargeNdJsonWithVariousBlockSizes() throws IOException {
        List<String> records = new ArrayList<>();
        for (int i = 0; i < 2000; i++) {
            records.add("{\"id\":" + i + ",\"data\":\"" + "x".repeat(50) + "\"}");
        }
        String ndjson = String.join("\n", records) + "\n";
        byte[] data = ndjson.getBytes(StandardCharsets.UTF_8);

        for (int blockSize = BZip2CompressorOutputStream.MIN_BLOCKSIZE; blockSize <= 3; blockSize++) {
            byte[] compressed = bzip2(data, blockSize);
            ByteArrayStorageObject object = new ByteArrayStorageObject(compressed);

            Bzip2DecompressionCodec codec = new Bzip2DecompressionCodec();
            long[] boundaries = codec.findBlockBoundaries(object, 0, compressed.length);

            ByteArrayOutputStream reassembled = new ByteArrayOutputStream();
            for (int i = 0; i < boundaries.length; i++) {
                long start = boundaries[i];
                long end = (i + 1 < boundaries.length) ? boundaries[i + 1] : compressed.length;
                try (InputStream stream = codec.decompressRange(object, start, end)) {
                    reassembled.write(stream.readAllBytes());
                }
            }

            assertEquals(
                "Block size " + blockSize + " split decompression should match",
                ndjson,
                reassembled.toString(StandardCharsets.UTF_8)
            );
        }
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
