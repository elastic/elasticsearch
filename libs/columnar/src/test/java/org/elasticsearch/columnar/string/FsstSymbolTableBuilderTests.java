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

public class FsstSymbolTableBuilderTests extends ESTestCase {

    public void testDistinctByteCountAllSame() {
        final byte[] data = new byte[100];
        Arrays.fill(data, (byte) 'a');
        assertEquals(1, FsstSymbolTableBuilder.distinctByteCount(data, data.length));
    }

    public void testDistinctByteCountAllUnique() {
        final byte[] data = new byte[256];
        for (int i = 0; i < 256; i++) {
            data[i] = (byte) i;
        }
        assertEquals(256, FsstSymbolTableBuilder.distinctByteCount(data, data.length));
    }

    public void testDistinctByteCountPartialLength() {
        final byte[] data = new byte[] { 'a', 'b', 'c', 'd', 'e' };
        assertEquals(3, FsstSymbolTableBuilder.distinctByteCount(data, 3));
    }

    public void testDistinctByteCountEmpty() {
        assertEquals(0, FsstSymbolTableBuilder.distinctByteCount(new byte[0], 0));
    }

    public void testBuildProducesMultiByteSymbolForRepetitiveData() {
        final String prefix = "host-eu-west-1-";
        final int count = 128;
        final int[] lengths = new int[count];
        final byte[] data = new byte[count * prefix.length()];
        int at = 0;
        for (int i = 0; i < count; i++) {
            final byte[] bytes = prefix.getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(bytes, 0, data, at, bytes.length);
            lengths[i] = bytes.length;
            at += bytes.length;
        }
        final FsstSymbolTable table = new FsstSymbolTableBuilder().data(data, at).lengths(lengths, count).build();
        assertTrue("expected at least one symbol", table.numSymbols() > 0);
        boolean hasMultiByte = false;
        int srcAt = 0;
        for (int i = 0; i < count; i++) {
            final byte[] compressed = new byte[lengths[i] * 2];
            final int compLen = table.encode(data, srcAt, lengths[i], compressed, 0);
            final byte[] decoded = new byte[lengths[i] + 1];
            final int decLen = table.decode(compressed, 0, compLen, decoded, 0);
            assertEquals("decoded length at value " + i, lengths[i], decLen);
            assertArrayEquals(
                "value " + i + " round-trips",
                Arrays.copyOfRange(data, srcAt, srcAt + lengths[i]),
                Arrays.copyOf(decoded, decLen)
            );
            if (compLen < lengths[i]) {
                hasMultiByte = true;
            }
            srcAt += lengths[i];
        }
        assertTrue("at least one value should be shorter after FSST on repetitive data", hasMultiByte);
    }

    public void testBuildReturnsNullForUniformHexData() {
        final byte[] hexChars = "0123456789abcdef".getBytes(StandardCharsets.US_ASCII);
        // 256 values, one per (a,b) pair. Value(a*16+b) = [a, b, a, b]:
        // bigram(a,b) appears twice, bigram(b,a) once, so every bigram has freq = 3 in total.
        // nonZero = 256 > MAX_SYMBOLS (255), and topFreq * nonZero = 3*256 = 768 < 4*768 = 3072.
        final int count = 256;
        final int valueLen = 4;
        final int[] lengths = new int[count];
        final byte[] data = new byte[count * valueLen];
        int at = 0;
        for (int a = 0; a < 16; a++) {
            for (int b = 0; b < 16; b++) {
                data[at] = hexChars[a];
                data[at + 1] = hexChars[b];
                data[at + 2] = hexChars[a];
                data[at + 3] = hexChars[b];
                lengths[a * 16 + b] = valueLen;
                at += valueLen;
            }
        }
        assertNull(
            "uniform hex data should produce no useful symbol table",
            new FsstSymbolTableBuilder().data(data, count * valueLen).lengths(lengths, count).build()
        );
    }

    public void testBuildReturnsNullForUniformSmallAlphabetData() {
        final byte[] alphabet = "0123456789abcdef-".getBytes(StandardCharsets.US_ASCII);
        final int alpha = alphabet.length; // 17
        // alpha^2 = 289 values, one per (a,b) pair: nonZero = 289 > 255, uniform distribution.
        final int count = alpha * alpha;
        final int valueLen = 4;
        final int[] lengths = new int[count];
        final byte[] data = new byte[count * valueLen];
        int at = 0;
        for (int a = 0; a < alpha; a++) {
            for (int b = 0; b < alpha; b++) {
                data[at] = alphabet[a];
                data[at + 1] = alphabet[b];
                data[at + 2] = alphabet[a];
                data[at + 3] = alphabet[b];
                lengths[a * alpha + b] = valueLen;
                at += valueLen;
            }
        }
        assertNull(
            "uniformly distributed 17-char alphabet data should produce no useful symbol table",
            new FsstSymbolTableBuilder().data(data, count * valueLen).lengths(lengths, count).build()
        );
    }

    public void testBuildReturnsNullWhenNoBigramsRepeat() {
        final byte[] data = "abcdefghij".getBytes(StandardCharsets.US_ASCII);
        final int[] lengths = new int[] { data.length };
        assertNull(
            "no repeated bigrams should produce no useful symbol table",
            new FsstSymbolTableBuilder().data(data, data.length).lengths(lengths, 1).build()
        );
    }

    public void testBuilderReuseSingletonFreqIsCleared() {
        final byte[] block = "abcdefghij".getBytes(StandardCharsets.US_ASCII);
        final int[] lengths = new int[] { block.length };
        final FsstSymbolTableBuilder builder = new FsstSymbolTableBuilder();
        assertNull("first call with no repeated bigrams must return null", builder.data(block, block.length).lengths(lengths, 1).build());
        assertNull(
            "second call with the same no-repeat input must also return null",
            builder.data(block, block.length).lengths(lengths, 1).build()
        );
    }

    public void testBuilderReuseAfterNonNullBuild() {
        final String prefix = "host-eu-west-1-";
        final int count = 128;
        final int[] structuredLengths = new int[count];
        final byte[] structured = new byte[count * prefix.length()];
        int at = 0;
        for (int i = 0; i < count; i++) {
            final byte[] bytes = prefix.getBytes(StandardCharsets.US_ASCII);
            System.arraycopy(bytes, 0, structured, at, bytes.length);
            structuredLengths[i] = bytes.length;
            at += bytes.length;
        }

        final byte[] noRepeat = "abcdefghij".getBytes(StandardCharsets.US_ASCII);
        final int[] noRepeatLengths = new int[] { noRepeat.length };

        final FsstSymbolTableBuilder builder = new FsstSymbolTableBuilder();

        final FsstSymbolTable first = builder.data(structured, at).lengths(structuredLengths, count).build();
        assertNotNull("structured input must produce a non-null table", first);

        assertNull(
            "no-repeat input after a non-null build must return null (cleanup after success)",
            builder.data(noRepeat, noRepeat.length).lengths(noRepeatLengths, 1).build()
        );

        final FsstSymbolTable third = builder.data(structured, at).lengths(structuredLengths, count).build();
        assertNotNull("structured input after a null build must still produce a non-null table", third);
    }

    public void testBuildRoundTripsAllValues() {
        final String[] terms = {
            "2024-01-15T10:23:44.123Z",
            "GET /api/v1/health HTTP/1.1",
            "Host: elasticsearch.local",
            "Content-Type: application/json",
            "Authorization: Bearer token123" };
        final int repeat = 25;
        final int count = terms.length * repeat;
        final int[] lengths = new int[count];
        int totalLen = 0;
        for (final String t : terms) {
            totalLen += t.length() * repeat;
        }
        final byte[] data = new byte[totalLen];
        int at = 0;
        int vi = 0;
        for (final String t : terms) {
            final byte[] bytes = t.getBytes(StandardCharsets.US_ASCII);
            for (int r = 0; r < repeat; r++) {
                System.arraycopy(bytes, 0, data, at, bytes.length);
                lengths[vi++] = bytes.length;
                at += bytes.length;
            }
        }

        final FsstSymbolTable table = new FsstSymbolTableBuilder().data(data, at).lengths(lengths, count).build();

        int srcAt = 0;
        for (int i = 0; i < count; i++) {
            final byte[] compressed = new byte[lengths[i] * 2];
            final int compLen = table.encode(data, srcAt, lengths[i], compressed, 0);
            final byte[] decoded = new byte[lengths[i] + 4];
            final int decLen = table.decode(compressed, 0, compLen, decoded, 0);
            assertEquals("decoded length at value " + i, lengths[i], decLen);
            assertArrayEquals(
                "value " + i + " must round-trip",
                Arrays.copyOfRange(data, srcAt, srcAt + lengths[i]),
                Arrays.copyOf(decoded, decLen)
            );
            srcAt += lengths[i];
        }
    }
}
