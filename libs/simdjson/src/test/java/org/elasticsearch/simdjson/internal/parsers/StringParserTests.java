/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.parsers;

import org.elasticsearch.simdjson.JsonParsingException;
import org.elasticsearch.simdjson.SimdJsonSupport;
import org.elasticsearch.test.ESTestCase;
import org.junit.BeforeClass;

import java.util.Arrays;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.simdjson.SimdJsonTestCase.makeJsonString;
import static org.elasticsearch.simdjson.SimdJsonTestCase.toBytesAtOffset;

// Unit tests for StringParser (JSON string unescaping with SIMD + scalar tail).
public class StringParserTests extends ESTestCase {

    @BeforeClass
    public static void requireVectorSupport() {
        assumeTrue("simdjson not supported on this platform", SimdJsonSupport.isSupported());
    }

    private final StringParser parser = new StringParser();

    private String parse(String content) {
        byte[] buf = makeJsonString(content);
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        return new String(dst, 0, len, UTF_8);
    }

    private byte[] parseToBytes(String content) {
        byte[] buf = makeJsonString(content);
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        return Arrays.copyOf(dst, len);
    }

    // ---- Basic unescaping ----

    public void testSimpleString() {
        assertEquals("hello", parse("hello"));
    }

    // Zero-length content between quotes yields len=0.
    public void testEmptyString() {
        byte[] buf = makeJsonString("");
        byte[] dst = new byte[buf.length];
        int len = parser.parseString(buf, 0, dst);
        assertEquals(0, len);
    }

    // ---- Standard JSON escapes ----

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

    // ---- Unicode and surrogate pairs ----

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

    // Invalid \\u hex digits are rejected (same as Jackson and upstream simdjson-java).
    public void testInvalidUnicodeEscapeThrows() {
        expectThrows(JsonParsingException.class, () -> parseToBytes("\\u00G0"));
        expectThrows(JsonParsingException.class, () -> parseToBytes("\\u000 "));
        expectThrows(JsonParsingException.class, () -> parse("prefix\\u00ZZsuffix"));
        // Too few hex digits before closing quote
        expectThrows(JsonParsingException.class, () -> parseToBytes("\\u00"));
        expectThrows(JsonParsingException.class, () -> parseToBytes("\\u0"));
        expectThrows(JsonParsingException.class, () -> parseToBytes("\\u"));
    }

    // ---- SIMD lane width boundaries (plain content, no escapes) ----

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

    // Escape sequence straddling a 128-bit vector boundary.
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

    // Sweep newline escape position around the 16-byte buffer boundary.
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

    // ---- scanUnescapedLength ----
    // Direct coverage for the vectorized quote/backslash scan the walker uses to size and copy
    // escape-free string values without a full parseString() call. It has its own loop bound
    // and its own combined-mask (quote-or-backslash) disambiguation logic, so it gets the same
    // kind of SIMD-boundary coverage as parseString above, rather than relying solely on the
    // indirect coverage exercised via SimdJsonDirectWalkerTests.

    private int scanLen(String content) {
        byte[] json = makeJsonString(content);
        if (randomBoolean()) {
            int offset = randomIntBetween(1, 128);
            return parser.scanUnescapedLength(toBytesAtOffset(new String(json, UTF_8), offset), offset);
        } else {
            return parser.scanUnescapedLength(json, 0);
        }
    }

    public void testScanUnescapedLength() {
        assertEquals(0, scanLen(""));
        assertEquals(1, scanLen("x"));
        assertEquals(5, scanLen("hello"));
        for (int len = 0; len <= 256; len++) {
            assertEquals("len=" + len, len, scanLen(randomAlphaOfLength(len)));
        }
    }

    public void testScanUnescapedLengthReturnsNegativeOneOnBackslash() {
        assertEquals(-1, scanLen("a\\nb"));
        assertEquals(-1, scanLen("ab\\ncd"));
        assertEquals(-1, scanLen("\\\"quoted\\\""));
        assertEquals(-1, scanLen("prefix\\uD83D\\uDE00suffix"));
        assertEquals(-1, scanLen("a".repeat(50) + "\\n" + "b".repeat(50)));
        for (int len = 0; len <= 256; len++) {
            assertEquals(-1, scanLen(randomAlphaOfLength(len) + "\\n" + "z"));
            assertEquals(-1, scanLen(randomAlphaOfLength(len) + "\\n" + randomAlphaOfLength(len)));
            assertEquals(-1, scanLen("a" + "\\n" + randomAlphaOfLength(len)));
        }
    }

    // For any escape-free string, scanUnescapedLength must agree with the parseString()
    public void testScanUnescapedLengthMatchesParseStringForEscapeFreeContent() {
        for (int len = 0; len <= 256; len++) {
            String content = randomAlphaOfLength(len);
            assertEquals("len=" + len, len, scanLen(content));
            assertEquals("len=" + len, len, parse(content).length());
        }
    }

    // ---- Raw (unescaped) multi-byte UTF-8 ----
    // scanUnescapedLength only ever compares bytes against '"' (0x22) and '\' (0x5C); every byte
    // of a valid UTF-8 multi-byte sequence has its high bit set, so it can never alias with
    // either, even split across a chunk boundary. These confirm that -- and, unlike the
    // escape-free sweep above, use content whose byte length differs from its char length, so
    // the expected value must come from getBytes(UTF_8).length, not String.length().

    public void testScanUnescapedLengthWithRawMultiByteUtf8() {
        String content = "café 世界 😀 done";
        assertEquals(content.getBytes(UTF_8).length, scanLen(content));
    }

    // Sweeps a 2/3/4-byte character across every prefix length so its lead/continuation bytes
    // land at every alignment relative to common SIMD chunk boundaries (16/32/64 bytes).
    public void testScanUnescapedLengthMultiByteCharacterSweep() {
        for (String ch : new String[] { "é", "世", "😀" }) {
            for (int prefix = 0; prefix <= 80; prefix++) {
                String content = "a".repeat(prefix) + ch + "a".repeat(prefix);
                assertEquals("char=" + ch + " prefix=" + prefix, content.getBytes(UTF_8).length, scanLen(content));
            }
        }
    }

    // A trailing backslash immediately follows the closing quote, as if this field were
    // immediately followed by another field's escaped string value.
    public void testScanUnescapedLengthQuoteBeforeBackslashInSameVectorChunk() {
        String filler = "x".repeat(80);
        for (int count = 0; count <= 256; count++) {
            byte[] buf = ("\"" + "a".repeat(count) + "\"" + "\\" + filler).getBytes(UTF_8);
            assertEquals(count, parser.scanUnescapedLength(buf, 0));
        }
    }
}
