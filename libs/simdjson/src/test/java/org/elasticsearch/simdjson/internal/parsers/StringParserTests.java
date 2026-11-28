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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.elasticsearch.simdjson.SimdJsonTestCase.appendRandomStringContent;
import static org.elasticsearch.simdjson.SimdJsonTestCase.copyMismatch;
import static org.elasticsearch.simdjson.SimdJsonTestCase.makeJsonString;

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

    // ---- Unpaired surrogates ----

    public void testUnpairedSurrogatesInValuesBecomeQuestionMarks() {
        assertEquals("?", parse("\\uD800"));
        assertEquals("?", parse("\\uDBFF"));
        assertEquals("?", parse("\\uDC00"));
        assertEquals("?", parse("\\uDFFF"));
        assertEquals("?x", parse("\\uD800x"));
        assertEquals("?A", parse("\\uD800\\u0041"));
        assertEquals("??", parse("\\uD800\\uD800"));
        assertEquals("??", parse("\\uDC00\\uD800"));
        assertEquals("?\n", parse("\\uD800\\n"));
        // The unpaired high surrogate does not consume the escape after it, which starts a valid pair.
        assertEquals("?\uD83D\uDE00", parse("\\uD800\\uD83D\\uDE00"));
    }

    public void testInvalidHexAfterUnpairedSurrogateIsRejected() {
        expectThrows(JsonParsingException.class, () -> parse("\\uD800\\uZZZZ"));
        expectThrows(JsonParsingException.class, () -> parse("a".repeat(200) + "\\uD800\\u00G0"));
    }

    public void testUnpairedSurrogatesInFieldNamesAreRejected() {
        for (String content : List.of(
            "\\uD800",
            "\\uDC00",
            "\\uD800x",
            "\\uD800\\u0041",
            "a".repeat(60) + "\\uD800",
            "a".repeat(200) + "\\uDFFF"
        )) {
            byte[] buf = makeJsonString(content);
            expectThrows(JsonParsingException.class, () -> parser.parseFieldName(buf, 0, new byte[buf.length]));
        }
    }

    public void testSurrogatePairInFieldName() {
        byte[] buf = makeJsonString("\\uD83D\\uDE00");
        byte[] dst = new byte[buf.length];
        assertEquals("\uD83D\uDE00", new String(dst, 0, parser.parseFieldName(buf, 0, dst), UTF_8));
    }

    // ---- Chunk positions, into exactly the room a copy asks for ----
    // The vector loop handles BYTES_PROCESSED (= DESTINATION_PADDING) bytes per chunk, so these sweep each special character
    // over every position in a chunk, into a destination with no room to spare.

    private static final int WIDTH = SimdJsonSupport.DESTINATION_PADDING;

    /** The spelling of each kind of escape, and what it unescapes to: a lone surrogate stays one, see {@link #check}. */
    private static final String[][] ESCAPES = {
        { "\\n", "\n" },
        { "\\\"", "\"" },
        { "\\\\", "\\" },
        { "\\u0041", "A" },
        { "\\u00e9", "é" },
        { "\\u65e5", "日" },
        { "\\uD83D\\uDE00", "\uD83D\uDE00" },
        { "\\uD800", "\uD800" },
        { "\\uDC00", "\uDC00" } };

    /** Each escape kind at every position, followed by another character or by the closing quote. */
    public void testEscapeAtEveryChunkPosition() {
        List<String> mismatches = new ArrayList<>();
        for (String[] escape : ESCAPES) {
            for (int prefix = 0; prefix <= 2 * WIDTH + 1; prefix++) {
                String plain = "a".repeat(prefix);
                for (String suffix : new String[] { "z", "" }) {
                    check(mismatches, plain + escape[1] + suffix, plain + escape[0] + suffix);
                }
            }
        }
        assertEquals(List.of(), mismatches);
    }

    /** Every ordered pair of escape kinds, and a run longer than a chunk, starting at every position. */
    public void testConsecutiveEscapesWithinAndAcrossChunks() {
        List<String> mismatches = new ArrayList<>();
        for (int prefix = 0; prefix <= WIDTH; prefix++) {
            String plain = "a".repeat(prefix);
            for (String[] first : ESCAPES) {
                for (String[] second : ESCAPES) {
                    check(mismatches, plain + first[1] + second[1], plain + first[0] + second[0]);
                }
            }
            StringBuilder expected = new StringBuilder(plain);
            StringBuilder json = new StringBuilder(plain);
            for (int i = 0; i <= WIDTH; i++) {
                String[] escape = ESCAPES[i % ESCAPES.length];
                json.append(escape[0]);
                expected.append(escape[1]);
            }
            check(mismatches, expected.toString(), json.toString());
        }
        assertEquals(List.of(), mismatches);
    }

    public void testClosingQuoteAtEveryChunkPosition() {
        List<String> mismatches = new ArrayList<>();
        for (int length = 0; length <= 2 * WIDTH + 1; length++) {
            String plain = "a".repeat(length);
            check(mismatches, plain, plain);
        }
        assertEquals(List.of(), mismatches);
    }

    /** The vector loop stops when fewer than a chunk of bytes remain in the buffer, and the scalar tail takes over. */
    public void testSwitchToScalarTailAtEveryPosition() {
        List<String> mismatches = new ArrayList<>();
        for (int prefix = 0; prefix <= WIDTH; prefix++) {
            String plain = "a".repeat(prefix);
            for (int after = 0; after <= WIDTH; after++) {
                check(mismatches, plain + "\n" + plain, plain + "\\n" + plain, ",".repeat(after));
            }
        }
        assertEquals(List.of(), mismatches);
    }

    public void testRandomContentAtRandomOffsets() {
        StringBuilder expected = new StringBuilder();
        StringBuilder json = new StringBuilder();
        appendRandomStringContent(expected, json);
        String after = "," + randomAlphaOfLength(between(0, 2 * WIDTH));
        assertNull(copyMismatch(expected.toString(), json.toString(), between(0, 128), after, between(0, 128), this::copy));
    }

    /** Checks {@code json} with the buffer ending right after the closing quote, and with a byte after it. */
    private void check(List<String> mismatches, String decoded, String json) {
        check(mismatches, decoded, json, "");
        check(mismatches, decoded, json, ",");
    }

    /**
     * Checks {@code json} with the string at the start of the buffer and after it, copied to offset 0 and past it. Each surrogate
     * in {@code decoded} that is not part of a pair is expected as {@code ?}.
     */
    private void check(List<String> mismatches, String decoded, String json, String after) {
        StringBuilder expected = new StringBuilder(decoded.length());
        for (int i = 0; i < decoded.length(); i++) {
            char c = decoded.charAt(i);
            if (Character.isHighSurrogate(c) && i + 1 < decoded.length() && Character.isLowSurrogate(decoded.charAt(i + 1))) {
                expected.append(c).append(decoded.charAt(++i));
            } else {
                expected.append(Character.isSurrogate(c) ? '?' : c);
            }
        }
        for (int srcOffset : new int[] { 0, 3 }) {
            for (int dstOffset : new int[] { 0, 5 }) {
                String mismatch = copyMismatch(expected.toString(), json, srcOffset, after, dstOffset, this::copy);
                if (mismatch != null) {
                    mismatches.add(mismatch);
                }
            }
        }
    }

    private int copy(byte[] buffer, int quoteIdx, int maxLength, byte[] dst, int dstOffset) {
        return parser.parseString(buffer, quoteIdx, dst, dstOffset);
    }

    // ---- Raw (unescaped) multi-byte UTF-8 ----
    // The vector loop only ever compares bytes against '"' (0x22) and '\' (0x5C); every byte
    // of a valid UTF-8 multi-byte sequence has its high bit set, so it can never alias with
    // either, even split across a chunk boundary.

    public void testRawMultiByteUtf8() {
        String content = "café 世界 😀 done";
        assertEquals(content, parse(content));
    }

    // Sweeps a 2/3/4-byte character across every prefix length so its lead/continuation bytes
    // land at every alignment relative to common SIMD chunk boundaries (16/32/64 bytes).
    public void testMultiByteCharacterSweep() {
        for (String ch : new String[] { "é", "世", "😀" }) {
            for (int prefix = 0; prefix <= 80; prefix++) {
                String content = "a".repeat(prefix) + ch + "a".repeat(prefix);
                assertEquals("char=" + ch + " prefix=" + prefix, content, parse(content));
            }
        }
    }

    // A backslash immediately follows the closing quote, as if this field were immediately
    // followed by another field's escaped string value.
    public void testQuoteBeforeBackslashInSameVectorChunk() {
        String filler = "x".repeat(80);
        for (int count = 0; count <= 256; count++) {
            byte[] buf = ("\"" + "a".repeat(count) + "\"" + "\\" + filler).getBytes(UTF_8);
            byte[] dst = new byte[buf.length];
            assertEquals(count, parser.parseString(buf, 0, dst));
        }
    }
}
