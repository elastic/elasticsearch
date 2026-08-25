/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal;

import org.elasticsearch.test.ESTestCase;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static org.elasticsearch.simdjson.SimdJsonTestSupport.makeJsonString;

public class StringParserTests extends ESTestCase {

    private final StringParser parser = new StringParser();

    private String parse(String content) {
        byte[] buf = makeJsonString(content);
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        return new String(dst, 0, len, StandardCharsets.UTF_8);
    }

    private byte[] parseToBytes(String content) {
        byte[] buf = makeJsonString(content);
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        return Arrays.copyOf(dst, len);
    }

    public void testSimpleString() {
        assertEquals("hello", parse("hello"));
    }

    public void testEmptyString() {
        byte[] buf = makeJsonString("");
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        assertEquals(0, len);
    }

    public void testEscapedQuote() {
        assertEquals("say \"hi\"", parse("say \\\"hi\\\""));
    }

    public void testEscapedBackslash() {
        assertEquals("a\\b", parse("a\\\\b"));
    }

    public void testEscapedSlash() {
        assertEquals("a/b", parse("a\\/b"));
    }

    public void testEscapedNewline() {
        byte[] result = parseToBytes("a\\nb");
        assertEquals(3, result.length);
        assertEquals((byte) 'a', result[0]);
        assertEquals((byte) 0x0A, result[1]);
        assertEquals((byte) 'b', result[2]);
    }

    public void testEscapedTab() {
        byte[] result = parseToBytes("a\\tb");
        assertEquals(3, result.length);
        assertEquals((byte) 'a', result[0]);
        assertEquals((byte) 0x09, result[1]);
        assertEquals((byte) 'b', result[2]);
    }

    public void testEscapedCarriageReturn() {
        byte[] result = parseToBytes("a\\rb");
        assertEquals(3, result.length);
        assertEquals((byte) 'a', result[0]);
        assertEquals((byte) 0x0D, result[1]);
        assertEquals((byte) 'b', result[2]);
    }

    public void testEscapedBackspace() {
        byte[] result = parseToBytes("a\\bb");
        assertEquals(3, result.length);
        assertEquals((byte) 'a', result[0]);
        assertEquals((byte) 0x08, result[1]);
        assertEquals((byte) 'b', result[2]);
    }

    public void testEscapedFormFeed() {
        byte[] result = parseToBytes("a\\fb");
        assertEquals(3, result.length);
        assertEquals((byte) 'a', result[0]);
        assertEquals((byte) 0x0C, result[1]);
        assertEquals((byte) 'b', result[2]);
    }

    public void testUnicodeEscapeAscii() {
        assertEquals("A", parse("\\u0041"));
    }

    public void testUnicodeEscape2Byte() {
        byte[] result = parseToBytes("\\u00E9");
        assertEquals(2, result.length);
        assertEquals((byte) 0xC3, result[0]);
        assertEquals((byte) 0xA9, result[1]);
    }

    public void testUnicodeEscape3Byte() {
        byte[] result = parseToBytes("\\u4E16");
        assertEquals(3, result.length);
        assertEquals((byte) 0xE4, result[0]);
        assertEquals((byte) 0xB8, result[1]);
        assertEquals((byte) 0x96, result[2]);
    }

    public void testSurrogatePair() {
        byte[] result = parseToBytes("\\uD83D\\uDE00");
        assertEquals(4, result.length);
        assertEquals((byte) 0xF0, result[0]);
        assertEquals((byte) 0x9F, result[1]);
        assertEquals((byte) 0x98, result[2]);
        assertEquals((byte) 0x80, result[3]);
    }

    public void testStringAt16ByteBoundary() {
        String content = "a".repeat(15);
        assertEquals(content, parse(content));
    }

    public void testStringAt32ByteBoundary() {
        String content = "a".repeat(31);
        assertEquals(content, parse(content));
    }

    public void testStringAt64ByteBoundary() {
        String content = "a".repeat(63);
        assertEquals(content, parse(content));
    }

    public void testLongStringMultipleSIMDLanes() {
        String content = "x".repeat(200);
        assertEquals(content, parse(content));
    }

    public void testEscapeAtWordBoundary() {
        String content = "abcdefg\\n";
        byte[] result = parseToBytes(content);
        assertEquals(8, result.length);
        assertEquals((byte) 'g', result[6]);
        assertEquals((byte) 0x0A, result[7]);
    }

    public void testSingleCharString() {
        assertEquals("x", parse("x"));
    }

    // ---- SIMD boundary tests ----
    // These test strings whose total buffer length (opening quote + content + closing quote)
    // places the closing quote or escape sequences at or near the SIMD vector width boundaries
    // (16, 32, 64 bytes), exercising the transition from vectorized loop to scalar tail.

    // --- 16-byte (128-bit) boundary ---

    public void testEscapeAtEnd_16ByteBoundary() {
        // 14 chars of content + escape = content straddles the 16-byte boundary
        String content = "a".repeat(13) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(14, result.length);
        assertEquals((byte) 0x0A, result[13]);
    }

    public void testEscapeCrossing_16ByteBoundary() {
        // Backslash at position 15 (last byte of first 16-byte vector), escape char in next chunk
        String content = "a".repeat(14) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(15, result.length);
        assertEquals((byte) 0x0A, result[14]);
    }

    public void testUnicodeEscapeAtEnd_16ByteBoundary() {
        // Place \\uXXXX so it spans the 16-byte boundary
        String content = "a".repeat(10) + "\\u0041";
        assertEquals("a".repeat(10) + "A", parse(content));
    }

    public void testSurrogatePairAtEnd_16ByteBoundary() {
        String content = "a".repeat(4) + "\\uD83D\\uDE00";
        byte[] result = parseToBytes(content);
        assertEquals(8, result.length);
        assertEquals((byte) 0xF0, result[4]);
        assertEquals((byte) 0x9F, result[5]);
        assertEquals((byte) 0x98, result[6]);
        assertEquals((byte) 0x80, result[7]);
    }

    // --- 32-byte (256-bit) boundary ---

    public void testEscapeAtEnd_32ByteBoundary() {
        String content = "a".repeat(29) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(30, result.length);
        assertEquals((byte) 0x0A, result[29]);
    }

    public void testEscapeCrossing_32ByteBoundary() {
        String content = "a".repeat(30) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(31, result.length);
        assertEquals((byte) 0x0A, result[30]);
    }

    public void testUnicodeEscapeAtEnd_32ByteBoundary() {
        String content = "a".repeat(26) + "\\u0041";
        assertEquals("a".repeat(26) + "A", parse(content));
    }

    public void testSurrogatePairAtEnd_32ByteBoundary() {
        String content = "a".repeat(20) + "\\uD83D\\uDE00";
        byte[] result = parseToBytes(content);
        assertEquals(24, result.length);
        assertEquals((byte) 0xF0, result[20]);
    }

    // --- 64-byte (512-bit) boundary ---

    public void testEscapeAtEnd_64ByteBoundary() {
        String content = "a".repeat(61) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(62, result.length);
        assertEquals((byte) 0x0A, result[61]);
    }

    public void testEscapeCrossing_64ByteBoundary() {
        String content = "a".repeat(62) + "\\n";
        byte[] result = parseToBytes(content);
        assertEquals(63, result.length);
        assertEquals((byte) 0x0A, result[62]);
    }

    public void testUnicodeEscapeAtEnd_64ByteBoundary() {
        String content = "a".repeat(58) + "\\u0041";
        assertEquals("a".repeat(58) + "A", parse(content));
    }

    public void testSurrogatePairAtEnd_64ByteBoundary() {
        String content = "a".repeat(52) + "\\uD83D\\uDE00";
        byte[] result = parseToBytes(content);
        assertEquals(56, result.length);
        assertEquals((byte) 0xF0, result[52]);
    }

    // --- Scalar tail coverage ---
    // These ensure the scalar fallback handles all escape types when the string
    // content is positioned so that the escape lands entirely in the tail.

    public void testScalarTailSimpleEscape() {
        // Place the escape at the very end of the buffer with minimal padding
        String content = "a".repeat(200) + "\\t";
        byte[] result = parseToBytes(content);
        assertEquals(201, result.length);
        assertEquals((byte) 0x09, result[200]);
    }

    public void testScalarTailUnicodeEscape() {
        String content = "a".repeat(200) + "\\u00E9";
        byte[] result = parseToBytes(content);
        assertEquals(202, result.length);
        assertEquals((byte) 0xC3, result[200]);
        assertEquals((byte) 0xA9, result[201]);
    }

    public void testScalarTailSurrogatePair() {
        String content = "a".repeat(200) + "\\uD83D\\uDE00";
        byte[] result = parseToBytes(content);
        assertEquals(204, result.length);
        assertEquals((byte) 0xF0, result[200]);
        assertEquals((byte) 0x9F, result[201]);
        assertEquals((byte) 0x98, result[202]);
        assertEquals((byte) 0x80, result[203]);
    }

    public void testScalarTailMultipleEscapes() {
        String content = "a".repeat(200) + "\\n\\t\\r";
        byte[] result = parseToBytes(content);
        assertEquals(203, result.length);
        assertEquals((byte) 0x0A, result[200]);
        assertEquals((byte) 0x09, result[201]);
        assertEquals((byte) 0x0D, result[202]);
    }

    public void testScalarTailEscapedQuote() {
        String content = "a".repeat(200) + "\\\"end";
        assertEquals("a".repeat(200) + "\"end", parse(content));
    }

    public void testScalarTailEscapedBackslash() {
        String content = "a".repeat(200) + "\\\\end";
        assertEquals("a".repeat(200) + "\\end", parse(content));
    }

    public void testScalarTailPlainCharsOnly() {
        // Entire string content in the scalar tail (buffer barely fits one vector load)
        String content = "tiny";
        assertEquals("tiny", parse(content));
    }

    // --- Boundary sweep: escape at every position near each SIMD width ---

    public void testEscapeSweepAround16() {
        for (int prefix = 12; prefix <= 18; prefix++) {
            String content = "a".repeat(prefix) + "\\n" + "z";
            byte[] result = parseToBytes(content);
            assertEquals(prefix + 2, result.length);
            assertEquals((byte) 0x0A, result[prefix]);
            assertEquals((byte) 'z', result[prefix + 1]);
        }
    }

    public void testEscapeSweepAround32() {
        for (int prefix = 28; prefix <= 34; prefix++) {
            String content = "a".repeat(prefix) + "\\n" + "z";
            byte[] result = parseToBytes(content);
            assertEquals(prefix + 2, result.length);
            assertEquals((byte) 0x0A, result[prefix]);
            assertEquals((byte) 'z', result[prefix + 1]);
        }
    }

    public void testEscapeSweepAround64() {
        for (int prefix = 60; prefix <= 66; prefix++) {
            String content = "a".repeat(prefix) + "\\n" + "z";
            byte[] result = parseToBytes(content);
            assertEquals(prefix + 2, result.length);
            assertEquals((byte) 0x0A, result[prefix]);
            assertEquals((byte) 'z', result[prefix + 1]);
        }
    }

    public void testUnicodeEscapeSweepAround64() {
        for (int prefix = 58; prefix <= 66; prefix++) {
            String content = "a".repeat(prefix) + "\\u0042";
            assertEquals("a".repeat(prefix) + "B", parse(content));
        }
    }

    public void testSurrogatePairSweepAround64() {
        for (int prefix = 52; prefix <= 66; prefix++) {
            String content = "a".repeat(prefix) + "\\uD83D\\uDE00";
            byte[] result = parseToBytes(content);
            assertEquals(prefix + 4, result.length);
            assertEquals((byte) 0xF0, result[prefix]);
            assertEquals((byte) 0x9F, result[prefix + 1]);
            assertEquals((byte) 0x98, result[prefix + 2]);
            assertEquals((byte) 0x80, result[prefix + 3]);
        }
    }
}
