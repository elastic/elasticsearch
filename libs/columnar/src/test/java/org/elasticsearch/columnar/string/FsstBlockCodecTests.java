/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.columnar.string;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class FsstBlockCodecTests extends ESTestCase {

    public void testRoundTripKnownBlock() {
        final String[] values = {
            "host-eu-west-1.internal",
            "host-eu-west-2.internal",
            "host-us-east-1.internal",
            "host-us-east-2.internal" };
        final int count = values.length;
        final int[] lengths = new int[count];
        int totalLen = 0;
        for (int i = 0; i < count; i++) {
            lengths[i] = values[i].length();
            totalLen += lengths[i];
        }
        final byte[] data = new byte[totalLen];
        int at = 0;
        for (int i = 0; i < count; i++) {
            final byte[] b = values[i].getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(b, 0, data, at, b.length);
            at += b.length;
        }

        final FsstBlockCodec codec = FsstBlockCodec.of(FsstSymbolTable.MAX_SYMBOL_LENGTH);
        final FsstSymbolTable fsst = codec.buildTable(data, totalLen, lengths, count);
        final int maxSize = FsstBlockCodec.maxEncodedSize(fsst, count, totalLen);
        final byte[] encoded = new byte[maxSize];
        final int written = codec.encodeBlock(data, totalLen, lengths, count, fsst, encoded, 0);
        assertTrue("encoded block must be non-empty", written > 0);

        final int[] starts = new int[count];
        final int[] decLengths = new int[count];
        final byte[] decoded = FsstBlockCodec.decodeBlock(encoded, 0, count, new byte[0], starts, decLengths);

        for (int i = 0; i < count; i++) {
            assertArrayEquals(
                "value " + i + " round-trips",
                values[i].getBytes(StandardCharsets.US_ASCII),
                Arrays.copyOfRange(decoded, starts[i], starts[i] + decLengths[i])
            );
        }
    }

    public void testSingleValue() {
        // Use a value with intra-string repetition so bigrams repeat and build() returns a table.
        final byte[] value = "abcabcabcabcabcabc".getBytes(StandardCharsets.US_ASCII);
        final FsstBlockCodec codec = FsstBlockCodec.of(FsstSymbolTable.MAX_SYMBOL_LENGTH);
        final FsstSymbolTable fsst = codec.buildTable(value, value.length, new int[] { value.length }, 1);
        final int maxSize = FsstBlockCodec.maxEncodedSize(fsst, 1, value.length);
        final byte[] encoded = new byte[maxSize];
        codec.encodeBlock(value, value.length, new int[] { value.length }, 1, fsst, encoded, 0);

        final int[] starts = new int[1];
        final int[] lengths = new int[1];
        final byte[] decoded = FsstBlockCodec.decodeBlock(encoded, 0, 1, new byte[0], starts, lengths);

        assertArrayEquals(value, Arrays.copyOfRange(decoded, starts[0], starts[0] + lengths[0]));
    }

    public void testBlockOfIdenticalValues() {
        final int count = 64;
        final byte[] single = "identical-value".getBytes(StandardCharsets.US_ASCII);
        final int[] lengths = new int[count];
        Arrays.fill(lengths, single.length);
        final int totalLen = single.length * count;
        final byte[] data = new byte[totalLen];
        for (int i = 0; i < count; i++) {
            System.arraycopy(single, 0, data, i * single.length, single.length);
        }

        final FsstBlockCodec codec = FsstBlockCodec.of(FsstSymbolTable.MAX_SYMBOL_LENGTH);
        final FsstSymbolTable fsst = codec.buildTable(data, totalLen, lengths, count);
        final int maxSize = FsstBlockCodec.maxEncodedSize(fsst, count, totalLen);
        final byte[] encoded = new byte[maxSize];
        codec.encodeBlock(data, totalLen, lengths, count, fsst, encoded, 0);

        final int[] starts = new int[count];
        final int[] decLengths = new int[count];
        final byte[] decoded = FsstBlockCodec.decodeBlock(encoded, 0, count, new byte[0], starts, decLengths);

        for (int i = 0; i < count; i++) {
            assertArrayEquals("value " + i + " round-trips", single, Arrays.copyOfRange(decoded, starts[i], starts[i] + decLengths[i]));
        }
    }

    // High-entropy data may produce a non-null symbol table with a few accidental bigram repeats.
    // The escape path encodes all non-matching bytes as escape + literal, so the block still
    // round-trips correctly regardless of how many symbols the table contains.
    public void testHighEntropyBlockRoundTrips() {
        final int count = 32;
        final int valueLen = 32;
        final int[] lengths = new int[count];
        Arrays.fill(lengths, valueLen);
        final int totalLen = count * valueLen;
        final byte[] data = randomByteArrayOfLength(totalLen);

        final FsstBlockCodec codec = FsstBlockCodec.of(FsstSymbolTable.MAX_SYMBOL_LENGTH);
        final FsstSymbolTable fsst = codec.buildTable(data, totalLen, lengths, count);
        // build() can return null for high-entropy data; use an empty table as a fallback so the
        // escape-path round-trip is exercised either way.
        final FsstSymbolTable tableOrEmpty = fsst != null ? fsst : new FsstSymbolTable(new byte[0][]);
        final int maxSize = FsstBlockCodec.maxEncodedSize(tableOrEmpty, count, totalLen);
        final byte[] encoded = new byte[maxSize];
        codec.encodeBlock(data, totalLen, lengths, count, tableOrEmpty, encoded, 0);

        final int[] starts = new int[count];
        final int[] decLengths = new int[count];
        final byte[] decoded = FsstBlockCodec.decodeBlock(encoded, 0, count, new byte[0], starts, decLengths);

        for (int i = 0; i < count; i++) {
            assertArrayEquals(
                "value " + i + " round-trips",
                Arrays.copyOfRange(data, i * valueLen, (i + 1) * valueLen),
                Arrays.copyOfRange(decoded, starts[i], starts[i] + decLengths[i])
            );
        }
    }

    public void testBlockWithEmptyValues() {
        final int count = 4;
        final byte[] data = "hello".getBytes(StandardCharsets.US_ASCII);
        final int[] lengths = new int[] { 5, 0, 5, 0 };
        final byte[] twoValues = new byte[10];
        System.arraycopy(data, 0, twoValues, 0, 5);
        System.arraycopy(data, 0, twoValues, 5, 5);

        final FsstBlockCodec codec = FsstBlockCodec.of(FsstSymbolTable.MAX_SYMBOL_LENGTH);
        final FsstSymbolTable fsst = codec.buildTable(twoValues, 10, lengths, count);
        final FsstSymbolTable tableOrEmpty = fsst != null ? fsst : new FsstSymbolTable(new byte[0][]);
        final int maxSize = FsstBlockCodec.maxEncodedSize(tableOrEmpty, count, 10);
        final byte[] encoded = new byte[maxSize];
        codec.encodeBlock(twoValues, 10, lengths, count, tableOrEmpty, encoded, 0);

        final int[] starts = new int[count];
        final int[] decLengths = new int[count];
        final byte[] decoded = FsstBlockCodec.decodeBlock(encoded, 0, count, new byte[0], starts, decLengths);

        assertArrayEquals(data, Arrays.copyOfRange(decoded, starts[0], starts[0] + decLengths[0]));
        assertEquals(0, decLengths[1]);
        assertArrayEquals(data, Arrays.copyOfRange(decoded, starts[2], starts[2] + decLengths[2]));
        assertEquals(0, decLengths[3]);
    }
}
