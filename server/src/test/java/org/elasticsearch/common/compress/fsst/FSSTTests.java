/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.compress.fsst;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class FSSTTests extends ESTestCase {

    private FSST.SymbolTable buildTable(String... lines) {
        List<byte[]> sample = new ArrayList<>();
        for (String line : lines) {
            sample.add(line.getBytes(StandardCharsets.UTF_8));
        }
        return FSST.SymbolTable.buildSymbolTable(sample);
    }

    private byte[] compressOne(FSST.SymbolTable table, byte[] input) {
        int[] inOffsets = new int[] { 0, input.length };
        byte[] outBuf = new byte[input.length * 2 + 8];
        int[] outOffsets = new int[2];
        table.compressBulk(1, input, inOffsets, outBuf, outOffsets);
        return Arrays.copyOfRange(outBuf, outOffsets[0], outOffsets[1]);
    }

    private byte[] decompressOne(byte[] compressed, FSST.Decoder decoder, int maxOutputLen) throws IOException {
        byte[] output = new byte[maxOutputLen + 7];
        int len = FSST.decompress(compressed, 0, compressed.length, decoder, output);
        return Arrays.copyOf(output, len);
    }

    public void testCompressDecompressSingleString() throws IOException {
        String input = "hello world";
        FSST.SymbolTable table = buildTable(input);
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        byte[] compressed = compressOne(table, inputBytes);

        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());
        byte[] decompressed = decompressOne(compressed, decoder, inputBytes.length);

        assertArrayEquals(inputBytes, decompressed);
    }

    public void testCompressDecompressEmptyString() throws IOException {
        FSST.SymbolTable table = buildTable("some training data");
        byte[] empty = new byte[0];

        byte[] compressed = compressOne(table, empty);
        assertEquals(0, compressed.length);

        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());
        byte[] decompressed = decompressOne(compressed, decoder, 0);
        assertEquals(0, decompressed.length);
    }

    public void testCompressAchievesCompressionWithRepetitiveData() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("metric.cpu.usage.host-").append(String.format("%03d", i % 50)).append(".example.com\n");
        }
        String input = sb.toString();
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);

        FSST.SymbolTable table = buildTable(input);
        byte[] compressed = compressOne(table, inputBytes);

        assertTrue(
            "compressed size " + compressed.length + " should be smaller than original " + inputBytes.length,
            compressed.length < inputBytes.length
        );

        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());
        byte[] decompressed = decompressOne(compressed, decoder, inputBytes.length);
        assertArrayEquals(inputBytes, decompressed);
    }

    public void testBulkCompressMultipleStrings() throws IOException {
        String[] lines = { "alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel" };

        FSST.SymbolTable table = buildTable(lines);
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        byte[][] inputArrays = new byte[lines.length][];
        int totalLen = 0;
        for (int i = 0; i < lines.length; i++) {
            inputArrays[i] = lines[i].getBytes(StandardCharsets.UTF_8);
            totalLen += inputArrays[i].length;
        }

        // Build contiguous input buffer with offsets
        byte[] allData = new byte[totalLen];
        int[] inOffsets = new int[lines.length + 1];
        int pos = 0;
        for (int i = 0; i < lines.length; i++) {
            inOffsets[i] = pos;
            System.arraycopy(inputArrays[i], 0, allData, pos, inputArrays[i].length);
            pos += inputArrays[i].length;
        }
        inOffsets[lines.length] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * lines.length];
        int[] outOffsets = new int[lines.length + 1];

        long linesCompressed = table.compressBulk(lines.length, allData, inOffsets, outBuf, outOffsets);
        assertEquals(lines.length, linesCompressed);

        // Decompress each string individually and verify
        for (int i = 0; i < lines.length; i++) {
            int compStart = outOffsets[i];
            int compLen = outOffsets[i + 1] - compStart;
            byte[] output = new byte[inputArrays[i].length + 7];
            int decompLen = FSST.decompress(outBuf, compStart, compLen, decoder, output);
            byte[] decompressed = Arrays.copyOf(output, decompLen);
            assertArrayEquals("Mismatch at string " + i + " (\"" + lines[i] + "\")", inputArrays[i], decompressed);
        }
    }

    public void testBulkCompressLargeBatch() throws IOException {
        int numStrings = 500;
        String[] lines = new String[numStrings];
        for (int i = 0; i < numStrings; i++) {
            lines[i] = String.format("service-%05d.dc-%02d.example.com", i, i % 10);
        }

        FSST.SymbolTable table = buildTable(lines);
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        byte[][] inputArrays = new byte[numStrings][];
        int totalLen = 0;
        for (int i = 0; i < numStrings; i++) {
            inputArrays[i] = lines[i].getBytes(StandardCharsets.UTF_8);
            totalLen += inputArrays[i].length;
        }

        byte[] allData = new byte[totalLen];
        int[] inOffsets = new int[numStrings + 1];
        int pos = 0;
        for (int i = 0; i < numStrings; i++) {
            inOffsets[i] = pos;
            System.arraycopy(inputArrays[i], 0, allData, pos, inputArrays[i].length);
            pos += inputArrays[i].length;
        }
        inOffsets[numStrings] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * numStrings];
        int[] outOffsets = new int[numStrings + 1];

        long linesCompressed = table.compressBulk(numStrings, allData, inOffsets, outBuf, outOffsets);
        assertEquals(numStrings, linesCompressed);

        for (int i = 0; i < numStrings; i++) {
            int compStart = outOffsets[i];
            int compLen = outOffsets[i + 1] - compStart;
            byte[] output = new byte[inputArrays[i].length + 7];
            int decompLen = FSST.decompress(outBuf, compStart, compLen, decoder, output);
            byte[] decompressed = Arrays.copyOf(output, decompLen);
            assertArrayEquals("Mismatch at string " + i, inputArrays[i], decompressed);
        }
    }

    public void testBulkCompressSingleString() throws IOException {
        String line = "a single string compressed in bulk";
        FSST.SymbolTable table = buildTable(line);
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        byte[] input = line.getBytes(StandardCharsets.UTF_8);
        int[] inOffsets = { 0, input.length };
        byte[] outBuf = new byte[input.length * 2 + 8];
        int[] outOffsets = new int[2];

        long count = table.compressBulk(1, input, inOffsets, outBuf, outOffsets);
        assertEquals(1, count);

        int compLen = outOffsets[1] - outOffsets[0];
        byte[] output = new byte[input.length + 7];
        int decompLen = FSST.decompress(outBuf, outOffsets[0], compLen, decoder, output);
        assertArrayEquals(input, Arrays.copyOf(output, decompLen));
    }

    public void testBulkCompressWithEmptyStrings() throws IOException {
        String[] lines = { "", "hello", "", "world", "" };
        FSST.SymbolTable table = buildTable("hello", "world");
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        byte[][] inputArrays = new byte[lines.length][];
        int totalLen = 0;
        for (int i = 0; i < lines.length; i++) {
            inputArrays[i] = lines[i].getBytes(StandardCharsets.UTF_8);
            totalLen += inputArrays[i].length;
        }

        byte[] allData = new byte[Math.max(totalLen, 1)];
        int[] inOffsets = new int[lines.length + 1];
        int pos = 0;
        for (int i = 0; i < lines.length; i++) {
            inOffsets[i] = pos;
            System.arraycopy(inputArrays[i], 0, allData, pos, inputArrays[i].length);
            pos += inputArrays[i].length;
        }
        inOffsets[lines.length] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * lines.length + 8];
        int[] outOffsets = new int[lines.length + 1];

        long count = table.compressBulk(lines.length, allData, inOffsets, outBuf, outOffsets);
        assertEquals(lines.length, count);

        for (int i = 0; i < lines.length; i++) {
            int compStart = outOffsets[i];
            int compLen = outOffsets[i + 1] - compStart;
            if (inputArrays[i].length == 0) {
                assertEquals("Empty string should have zero compressed length", 0, compLen);
            } else {
                byte[] output = new byte[inputArrays[i].length + 7];
                int decompLen = FSST.decompress(outBuf, compStart, compLen, decoder, output);
                assertArrayEquals("Mismatch at string " + i, inputArrays[i], Arrays.copyOf(output, decompLen));
            }
        }
    }

    public void testSymbolTableExportImportRoundTrip() throws IOException {
        String[] training = new String[200];
        for (int i = 0; i < training.length; i++) {
            training[i] = String.format("host-%03d.example.com", i);
        }

        FSST.SymbolTable table = buildTable(training);
        byte[] exported = table.exportToBytes();

        FSST.Decoder decoder = FSST.Decoder.readFrom(exported);
        assertNotNull(decoder);
        assertNotNull(decoder.getLens());
        assertNotNull(decoder.getSymbols());

        // Compress with original table, decompress with re-imported decoder
        for (String s : training) {
            byte[] input = s.getBytes(StandardCharsets.UTF_8);
            byte[] compressed = compressOne(table, input);
            byte[] decompressed = decompressOne(compressed, decoder, input.length);
            assertArrayEquals(input, decompressed);
        }
    }

    public void testDecompressWithOffset() throws IOException {
        String input = "test with output offset";
        FSST.SymbolTable table = buildTable(input);
        byte[] inputBytes = input.getBytes(StandardCharsets.UTF_8);
        byte[] compressed = compressOne(table, inputBytes);

        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        int outOffset = 10;
        byte[] output = new byte[outOffset + inputBytes.length + 7];
        int len = FSST.decompress(compressed, 0, compressed.length, decoder, output, outOffset);

        assertEquals(inputBytes.length, len);
        byte[] decompressed = Arrays.copyOfRange(output, outOffset, outOffset + len);
        assertArrayEquals(inputBytes, decompressed);
    }

    public void testRandomStrings() throws IOException {
        int numStrings = randomIntBetween(10, 200);
        String[] lines = new String[numStrings];
        for (int i = 0; i < numStrings; i++) {
            lines[i] = randomAlphaOfLengthBetween(1, 100);
        }

        FSST.SymbolTable table = buildTable(lines);
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        byte[][] inputArrays = new byte[numStrings][];
        int totalLen = 0;
        for (int i = 0; i < numStrings; i++) {
            inputArrays[i] = lines[i].getBytes(StandardCharsets.UTF_8);
            totalLen += inputArrays[i].length;
        }

        byte[] allData = new byte[totalLen];
        int[] inOffsets = new int[numStrings + 1];
        int pos = 0;
        for (int i = 0; i < numStrings; i++) {
            inOffsets[i] = pos;
            System.arraycopy(inputArrays[i], 0, allData, pos, inputArrays[i].length);
            pos += inputArrays[i].length;
        }
        inOffsets[numStrings] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * numStrings];
        int[] outOffsets = new int[numStrings + 1];

        long count = table.compressBulk(numStrings, allData, inOffsets, outBuf, outOffsets);
        assertEquals(numStrings, count);

        for (int i = 0; i < numStrings; i++) {
            int compStart = outOffsets[i];
            int compLen = outOffsets[i + 1] - compStart;
            byte[] output = new byte[inputArrays[i].length + 7];
            int decompLen = FSST.decompress(outBuf, compStart, compLen, decoder, output);
            assertArrayEquals("Mismatch at string " + i, inputArrays[i], Arrays.copyOf(output, decompLen));
        }
    }

    public void testBinaryData() throws IOException {
        byte[][] binaryStrings = new byte[50][];
        for (int i = 0; i < binaryStrings.length; i++) {
            binaryStrings[i] = new byte[randomIntBetween(1, 200)];
            random().nextBytes(binaryStrings[i]);
        }

        List<byte[]> sample = new ArrayList<>(Arrays.asList(binaryStrings));
        FSST.SymbolTable table = FSST.SymbolTable.buildSymbolTable(sample);
        FSST.Decoder decoder = FSST.Decoder.readFrom(table.exportToBytes());

        int totalLen = 0;
        for (byte[] bs : binaryStrings) {
            totalLen += bs.length;
        }

        byte[] allData = new byte[totalLen];
        int[] inOffsets = new int[binaryStrings.length + 1];
        int pos = 0;
        for (int i = 0; i < binaryStrings.length; i++) {
            inOffsets[i] = pos;
            System.arraycopy(binaryStrings[i], 0, allData, pos, binaryStrings[i].length);
            pos += binaryStrings[i].length;
        }
        inOffsets[binaryStrings.length] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * binaryStrings.length];
        int[] outOffsets = new int[binaryStrings.length + 1];

        table.compressBulk(binaryStrings.length, allData, inOffsets, outBuf, outOffsets);

        for (int i = 0; i < binaryStrings.length; i++) {
            int compStart = outOffsets[i];
            int compLen = outOffsets[i + 1] - compStart;
            byte[] output = new byte[binaryStrings[i].length + 7];
            int decompLen = FSST.decompress(outBuf, compStart, compLen, decoder, output);
            assertArrayEquals("Mismatch at binary string " + i, binaryStrings[i], Arrays.copyOf(output, decompLen));
        }
    }

    public void testPerStringOffsetsAreMonotonic() throws IOException {
        int numStrings = 100;
        String[] lines = new String[numStrings];
        for (int i = 0; i < numStrings; i++) {
            lines[i] = String.format("entry-%05d-data", i);
        }

        FSST.SymbolTable table = buildTable(lines);

        byte[][] inputArrays = new byte[numStrings][];
        int totalLen = 0;
        for (int i = 0; i < numStrings; i++) {
            inputArrays[i] = lines[i].getBytes(StandardCharsets.UTF_8);
            totalLen += inputArrays[i].length;
        }

        byte[] allData = new byte[totalLen];
        int[] inOffsets = new int[numStrings + 1];
        int pos = 0;
        for (int i = 0; i < numStrings; i++) {
            inOffsets[i] = pos;
            System.arraycopy(inputArrays[i], 0, allData, pos, inputArrays[i].length);
            pos += inputArrays[i].length;
        }
        inOffsets[numStrings] = pos;

        byte[] outBuf = new byte[totalLen * 2 + 8 * numStrings];
        int[] outOffsets = new int[numStrings + 1];

        table.compressBulk(numStrings, allData, inOffsets, outBuf, outOffsets);

        for (int i = 0; i < numStrings; i++) {
            assertTrue(
                "outOffsets[" + i + "]=" + outOffsets[i] + " should be <= outOffsets[" + (i + 1) + "]=" + outOffsets[i + 1],
                outOffsets[i] <= outOffsets[i + 1]
            );
        }
    }
}
