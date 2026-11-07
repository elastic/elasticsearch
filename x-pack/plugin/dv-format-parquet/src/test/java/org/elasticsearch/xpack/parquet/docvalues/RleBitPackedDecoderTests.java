/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet.docvalues;

import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Arrays;

public class RleBitPackedDecoderTests extends ESTestCase {

    public void testRepeatedRun() throws IOException {
        // header 10 << 1 = 20: ten copies of the value 5, which takes one byte at bit width 3
        byte[] input = { 20, 5 };
        int[] output = new int[10];
        RleBitPackedDecoder.decode(input, 0, input.length, 3, output, 10);
        assertArrayEquals(new int[] { 5, 5, 5, 5, 5, 5, 5, 5, 5, 5 }, output);
    }

    public void testBitPackedGroup() throws IOException {
        // one group of 8 values 0..7 at bit width 3, packed from the lowest bit: header (1 << 1) | 1 = 3
        // bits: 000 001 010 011 100 101 110 111 -> bytes 0b10001000 0b11000110 0b11111010
        byte[] input = { 3, (byte) 0b10001000, (byte) 0b11000110, (byte) 0b11111010 };
        int[] output = new int[8];
        RleBitPackedDecoder.decode(input, 0, input.length, 3, output, 8);
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4, 5, 6, 7 }, output);
    }

    public void testStopsAtTheCountInsideAGroupAndMixesRuns() throws IOException {
        // a bit-packed group of 8 where only 5 values are wanted, then nothing: padding of the last group is ignored
        byte[] input = { 3, (byte) 0b10001000, (byte) 0b11000110, (byte) 0b11111010 };
        int[] output = new int[5];
        RleBitPackedDecoder.decode(input, 0, input.length, 3, output, 5);
        assertArrayEquals(new int[] { 0, 1, 2, 3, 4 }, output);

        // a repeated run of 3 then a bit-packed group of 8
        byte[] mixed = { 6, 7, 3, (byte) 0b10001000, (byte) 0b11000110, (byte) 0b11111010 };
        int[] mixedOutput = new int[11];
        RleBitPackedDecoder.decode(mixed, 0, mixed.length, 3, mixedOutput, 11);
        assertArrayEquals(new int[] { 7, 7, 7, 0, 1, 2, 3, 4, 5, 6, 7 }, mixedOutput);
    }

    public void testBitWidthZeroIsAllZeros() throws IOException {
        // one repeated run of 100 values at bit width 0: header 200 as a varint is { 0xC8, 0x01 }, and no value bytes follow
        byte[] input = { (byte) 0xC8, 0x01 };
        int[] output = new int[100];
        Arrays.fill(output, -1);
        RleBitPackedDecoder.decode(input, 0, input.length, 0, output, 100);
        assertArrayEquals(new int[100], output);
    }

    public void testWideValuesAndOffsets() throws IOException {
        // a repeated run of 2 values of 0x01020304 at bit width 32, after 2 bytes that are not part of the data
        byte[] input = { 9, 9, 4, 4, 3, 2, 1 };
        int[] output = new int[2];
        RleBitPackedDecoder.decode(input, 2, input.length, 32, output, 2);
        assertArrayEquals(new int[] { 0x01020304, 0x01020304 }, output);
    }

    public void testRejectsInputThatIsCutOffOrInvalid() {
        expectThrows(IOException.class, () -> RleBitPackedDecoder.decode(new byte[] { 20 }, 0, 1, 3, new int[10], 10));
        expectThrows(IOException.class, () -> RleBitPackedDecoder.decode(new byte[] { 3, 1 }, 0, 2, 3, new int[8], 8));
        expectThrows(IOException.class, () -> RleBitPackedDecoder.decode(new byte[] { 20, 5 }, 0, 2, 3, new int[20], 20));
        expectThrows(IOException.class, () -> RleBitPackedDecoder.decode(new byte[0], 0, 0, 3, new int[1], 1));
        expectThrows(IOException.class, () -> RleBitPackedDecoder.decode(new byte[] { 20, 5 }, 0, 2, 33, new int[1], 1));
    }
}
