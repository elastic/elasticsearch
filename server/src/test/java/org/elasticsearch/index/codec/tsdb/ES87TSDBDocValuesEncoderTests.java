/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.codec.tsdb;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.util.NumericUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

public class ES87TSDBDocValuesEncoderTests extends LuceneTestCase {

    private final ES87TSDBDocValuesEncoder encoder;
    private final int blockSize = ES87TSDBDocValuesFormat.NUMERIC_BLOCK_SIZE;

    public ES87TSDBDocValuesEncoderTests() {
        this.encoder = new ES87TSDBDocValuesEncoder();
    }

    public void testRandomValues() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = random().nextLong();
        }
        arr[4] = Long.MAX_VALUE / 2;
        arr[100] = Long.MIN_VALUE / 2 - 1;
        final long expectedNumBytes = 2 // token
            + (blockSize * 64) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testAllEqual() throws IOException {
        long[] arr = new long[blockSize];
        Arrays.fill(arr, 3);
        final long expectedNumBytes = 2; // token + min value
        doTest(arr, expectedNumBytes);
    }

    public void testSmallPositiveValues() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = (i + 2) & 0x03; // 2 bits per value
        }
        final long expectedNumBytes = 1 // token
            + (blockSize * 2) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testSmallPositiveValuesWithOffset() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = 1000 + ((i + 2) & 0x03); // 2 bits per value
        }
        final long expectedNumBytes = 3 // token + min value (1000 -> 2 bytes)
            + (blockSize * 2) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testSmallPositiveValuesWithNegativeOffset() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = -1000 + ((i + 2) & 0x03); // 2 bits per value
        }
        final long expectedNumBytes = 3 // token + min value (-1000 -> 2 bytes)
            + (blockSize * 2) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    /**
     * Integers as doubles, GCD compression should help here given high numbers of trailing zeroes.
     */
    public void testIntegersAsDoubles() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = NumericUtils.doubleToSortableLong((i + 2) & 0x03); // 0, 1 or 2
        }
        final long expectedNumBytes = 9 // token + GCD (8 bytes)
            + blockSize * 12 / Byte.SIZE; // 12 bits per value -> 26 longs
        doTest(arr, expectedNumBytes);
    }

    public void testDecreasingValues() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = 1000 - 30 * i;
        }
        final long expectedNumBytes = 4; // token + min value + delta
        doTest(arr, expectedNumBytes);
    }

    public void testTwoValues() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = i % 3 == 1 ? 42 : 100;
        }
        final long expectedNumBytes = 3 // token + min value (1 byte) + GCD (1 byte)
            + (blockSize * 1) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    /** Monotonic timestamps: delta coding + GCD compression. */
    public void testMonotonicTimestamps() throws IOException {
        long offset = 2 * 60 * 60 * 1000; // 2h offset = timezone
        // a timestamp near Jan 1st 2021
        long first = (2021 - 1970) * 365L * 24 * 60 * 60 * 1000 + offset;
        long granularity = 24L * 60 * 60 * 1000; // 1 day

        long[] arr = new long[blockSize];
        arr[0] = first;
        for (int i = 1; i < blockSize; ++i) {
            arr[i] = arr[i - 1] + (5 + i % 4) * granularity; // increment by a multiple of granularity
        }
        final long expectedNumBytes = 16 // token + min delta + GCD + first
            + (blockSize * 2) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testZeroOrMinValue() throws IOException {
        long[] arr = new long[blockSize];
        for (int i = 0; i < blockSize; ++i) {
            arr[i] = i % 3 == 1 ? Long.MIN_VALUE : 0;
        }
        final long expectedNumBytes = 10 // token + GCD (9 byte)
            + (blockSize * 1) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testFloatingPointValues() throws IOException {
        long[] arr = new long[blockSize];
        // NOTE: these values are crafted in such a way that after applying GCD encoding we get values represented using 36 bits per value.
        for (int i = 0; i < blockSize; ++i) {
            double value = (i % 2 == 1) ? (i * 1956.0) : (i * 356923.5);
            arr[i] = Double.doubleToLongBits(value);
        }
        // NOTE: 36 bits per value strictly required, but we round to 40 bits per value to write exactly 5 bytes per value
        final long expectedNumBytes = 6 // token (2 bytes) + GCD (4 bytes)
            + (blockSize * 40) / Byte.SIZE; // data
        doTest(arr, expectedNumBytes);
    }

    public void testBitsPerValueFullRange() throws IOException {
        final Random random = new Random(17);
        long[] arr = new long[blockSize];
        long constant = 1;
        for (int bitsPerValue = 0; bitsPerValue <= 64; bitsPerValue++) {
            for (int i = 0; i < blockSize; ++i) {
                if (bitsPerValue == 0) {
                    arr[i] = constant;
                } else {
                    arr[i] = random.nextLong(0, bitsPerValue <= 62 ? 1L << bitsPerValue : Long.MAX_VALUE);
                }
            }
            long actualBitsPerValue = DocValuesForUtil.roundBits(bitsPerValue);
            int actualTokenBytes = bitsPerValue < 16 ? 1 : 2;
            final long expectedNumBytes = bitsPerValue == 0
                ? 2
                : actualTokenBytes // token
                    + (blockSize * actualBitsPerValue) / Byte.SIZE; // data
            doTest(arr, expectedNumBytes);
        }
    }

    private void doTest(long[] arr, long expectedNumBytes) throws IOException {
        final long[] expected = arr.clone();
        try (Directory dir = newDirectory()) {
            try (IndexOutput out = dir.createOutput("tests.bin", IOContext.DEFAULT)) {
                encoder.encode(arr, out);
                assertEquals(expectedNumBytes, out.getFilePointer());
            }
            try (IndexInput in = dir.openInput("tests.bin", IOContext.DEFAULT)) {
                long[] decoded = new long[blockSize];
                for (int i = 0; i < decoded.length; ++i) {
                    decoded[i] = random().nextLong();
                }
                encoder.decode(in, decoded);
                assertEquals(in.length(), in.getFilePointer());
                assertArrayEquals(expected, decoded);
            }
        }
    }
}
