/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.csv;

import org.elasticsearch.test.ESTestCase;

/**
 * Randomised property test: on every input, {@link CsvBoundaryScanner#PREFIX_XOR} and
 * {@link CsvBoundaryScanner#REFERENCE} must produce identical {@code findLastRealTerminator} and
 * {@code countRealTerminators} results. Sits separately from {@link AbstractCsvBoundaryScannerTests}
 * because it structurally compares both implementations rather than running each in isolation.
 */
public class CsvBoundaryScannerEquivalenceTests extends ESTestCase {

    private static final byte QUOTE = (byte) '"';

    public void testRandomizedEquivalence() {
        int iterations = 5000;
        for (int it = 0; it < iterations; it++) {
            int len = randomIntBetween(0, 400);
            byte[] data = generateRandomCsvBytes(len);

            int xorBoundary = CsvBoundaryScanner.PREFIX_XOR.findLastRealTerminator(data, 0, data.length, QUOTE);
            int refBoundary = CsvBoundaryScanner.REFERENCE.findLastRealTerminator(data, 0, data.length, QUOTE);
            if (xorBoundary != refBoundary) {
                fail(
                    "findLastRealTerminator mismatch at it="
                        + it
                        + " prefixXor="
                        + xorBoundary
                        + " reference="
                        + refBoundary
                        + " for input: "
                        + bytesToReadable(data)
                );
            }

            long[] xorCarry = new long[1];
            long[] refCarry = new long[1];
            long xorCount = CsvBoundaryScanner.PREFIX_XOR.countRealTerminators(data, 0, data.length, QUOTE, 0L, xorCarry);
            long refCount = CsvBoundaryScanner.REFERENCE.countRealTerminators(data, 0, data.length, QUOTE, 0L, refCarry);
            if (xorCount != refCount || xorCarry[0] != refCarry[0]) {
                fail(
                    "countRealTerminators mismatch at it="
                        + it
                        + " prefixXor=(count="
                        + xorCount
                        + ",carry="
                        + xorCarry[0]
                        + ") reference=(count="
                        + refCount
                        + ",carry="
                        + refCarry[0]
                        + ") for input: "
                        + bytesToReadable(data)
                );
            }
        }
    }

    /**
     * Same as above but threads a non-zero carry-in, exercising the cross-buffer state transition
     * path that {@code countRealTerminators} uses for streaming reads. The randomised input simulates
     * the bytes between two reads of the same logical file.
     */
    public void testRandomizedEquivalenceWithCarryIn() {
        int iterations = 5000;
        for (int it = 0; it < iterations; it++) {
            int len = randomIntBetween(0, 400);
            byte[] data = generateRandomCsvBytes(len);
            long carryIn = randomBoolean() ? -1L : 0L;

            long[] xorCarry = new long[1];
            long[] refCarry = new long[1];
            long xorCount = CsvBoundaryScanner.PREFIX_XOR.countRealTerminators(data, 0, data.length, QUOTE, carryIn, xorCarry);
            long refCount = CsvBoundaryScanner.REFERENCE.countRealTerminators(data, 0, data.length, QUOTE, carryIn, refCarry);
            if (xorCount != refCount || xorCarry[0] != refCarry[0]) {
                fail(
                    "countRealTerminators(carry="
                        + carryIn
                        + ") mismatch at it="
                        + it
                        + " prefixXor=(count="
                        + xorCount
                        + ",carry="
                        + xorCarry[0]
                        + ") reference=(count="
                        + refCount
                        + ",carry="
                        + refCarry[0]
                        + ") for input: "
                        + bytesToReadable(data)
                );
            }
        }
    }

    private byte[] generateRandomCsvBytes(int len) {
        byte[] out = new byte[len];
        for (int i = 0; i < len; i++) {
            int roll = randomIntBetween(0, 99);
            if (roll < 5) {
                out[i] = (byte) '"';
            } else if (roll < 15) {
                out[i] = (byte) '\n';
            } else if (roll < 25) {
                out[i] = (byte) ',';
            } else if (roll < 28) {
                out[i] = (byte) '\r';
            } else {
                out[i] = (byte) ('a' + randomIntBetween(0, 25));
            }
        }
        return out;
    }

    private static String bytesToReadable(byte[] data) {
        StringBuilder sb = new StringBuilder("[" + data.length + "]");
        for (byte b : data) {
            if (b == '\n') sb.append("\\n");
            else if (b == '\r') sb.append("\\r");
            else if (b == '"') sb.append("\\\"");
            else sb.append((char) (b & 0xff));
        }
        return sb.toString();
    }
}
