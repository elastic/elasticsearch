/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action.suggestions;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class CursorOffsetTests extends ESTestCase {

    // ASCII: all three units agree.
    public void testAsciiAllUnitsAgree() {
        String query = "FROM foo";
        int offset = 4; // right after "FROM"
        assertThat(CursorOffset.utf16(offset).resolve(query), equalTo(offset));
        assertThat(CursorOffset.codePoint(offset).resolve(query), equalTo(offset));
        assertThat(CursorOffset.utf8(offset).resolve(query), equalTo(offset));
    }

    // BMP non-ASCII (accented Latin / CJK): UTF-16 and code point counts still coincide.
    public void testBmpNonAsciiUtf16AndCodePointAgree() {
        String query = "FROM caf\u00e9 | KEEP a"; // "café", é is BMP
        int afterCafe = query.indexOf("caf\u00e9") + "caf\u00e9".length();
        assertThat(CursorOffset.utf16(afterCafe).resolve(query), equalTo(afterCafe));
        assertThat(CursorOffset.codePoint(afterCafe).resolve(query), equalTo(afterCafe));
    }

    // Supplementary-plane (emoji): UTF-16 uses two code units, code point uses one, UTF-8 uses four bytes.
    public void testSupplementaryPlaneCharacterAllThreeUnitsResolveToSamePosition() {
        String emoji = "\uD83D\uDE00"; // 😀, U+1F600, one code point, two UTF-16 units, four UTF-8 bytes
        String query = "FROM " + emoji + " | KEEP a";
        int afterEmojiUtf16 = ("FROM " + emoji).length();
        int afterEmojiCodePoint = "FROM ".length() + 1; // "FROM " (5 code points) + the emoji (1 code point)
        int afterEmojiUtf8 = "FROM ".getBytes(StandardCharsets.UTF_8).length + 4;

        assertThat(CursorOffset.utf16(afterEmojiUtf16).resolve(query), equalTo(afterEmojiUtf16));
        assertThat(CursorOffset.codePoint(afterEmojiCodePoint).resolve(query), equalTo(afterEmojiUtf16));
        assertThat(CursorOffset.utf8(afterEmojiUtf8).resolve(query), equalTo(afterEmojiUtf16));
    }

    public void testUtf8OffsetSplittingMultiByteSequenceIsRejected() {
        String emoji = "\uD83D\uDE00";
        String query = "FROM " + emoji;
        int prefixBytes = "FROM ".getBytes(StandardCharsets.UTF_8).length;
        // Splits the emoji's 4-byte UTF-8 sequence after its second byte.
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CursorOffset.utf8(prefixBytes + 2).resolve(query));
        assertThat(e.getMessage(), containsString("splits a multi-byte UTF-8 sequence"));
    }

    public void testUtf16OutOfRangeRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CursorOffset.utf16(999).resolve("FROM foo"));
        assertThat(e.getMessage(), containsString("[cursor.utf16] must be within"));
    }

    public void testCodePointOutOfRangeRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CursorOffset.codePoint(999).resolve("FROM foo"));
        assertThat(e.getMessage(), containsString("[cursor.codepoint] must be within"));
    }

    public void testUtf8OutOfRangeRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CursorOffset.utf8(999).resolve("FROM foo"));
        assertThat(e.getMessage(), containsString("[cursor.utf8] must be within"));
    }

    // Windows line endings: \r is a plain ASCII byte/unit/code-point on its own, not collapsed into \n.
    public void testCrlfCarriageReturnCountsAsItsOwnUnitInAllThreeSpaces() {
        String query = "FROM foo\r\n| WHERE a == \"x\"";
        int afterCr = "FROM foo\r".length();
        assertThat(CursorOffset.utf16(afterCr).resolve(query), equalTo(afterCr));
        assertThat(CursorOffset.codePoint(afterCr).resolve(query), equalTo(afterCr));
        assertThat(CursorOffset.utf8(afterCr).resolve(query), equalTo(afterCr));
    }

    public void testFromXContentAcceptsSingleUtf16Key() throws IOException {
        CursorOffset offset = parseCursor("{\"utf16\": 12}");
        assertThat(offset, equalTo(CursorOffset.utf16(12)));
    }

    public void testFromXContentAcceptsSingleUtf8Key() throws IOException {
        CursorOffset offset = parseCursor("{\"utf8\": 14}");
        assertThat(offset, equalTo(CursorOffset.utf8(14)));
    }

    public void testFromXContentAcceptsSingleCodePointKey() throws IOException {
        CursorOffset offset = parseCursor("{\"codepoint\": 11}");
        assertThat(offset, equalTo(CursorOffset.codePoint(11)));
    }

    public void testFromXContentRejectsZeroKeys() {
        XContentParseException e = expectThrows(XContentParseException.class, () -> parseCursor("{}"));
        assertThat(e.getMessage(), containsString("got no unit"));
    }

    public void testFromXContentRejectsMultipleKeys() {
        XContentParseException e = expectThrows(XContentParseException.class, () -> parseCursor("{\"utf8\": 1, \"utf16\": 2}"));
        assertThat(e.getMessage(), containsString("got: [utf8, utf16]"));
    }

    public void testFromXContentRejectsUnknownKey() {
        XContentParseException e = expectThrows(XContentParseException.class, () -> parseCursor("{\"byte\": 1}"));
        assertThat(e.getMessage(), containsString("unknown cursor unit [byte]"));
    }

    private CursorOffset parseCursor(String json) throws IOException {
        try (XContentParser parser = createParser(XContentType.JSON.xContent(), json)) {
            parser.nextToken();
            return CursorOffset.fromXContent(parser);
        }
    }
}
