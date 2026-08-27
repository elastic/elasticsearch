/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.pushdown;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.test.ESTestCase;

public class StringPrefixUtilsTests extends ESTestCase {

    public void testAsciiIncrement() {
        assertEquals(new BytesRef("abd"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("abc")));
    }

    public void testSingleCharAscii() {
        assertEquals(new BytesRef("{"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("z")));
    }

    public void testEmptyPrefix() {
        assertNull(StringPrefixUtils.nextPrefixUpperBound(new BytesRef("")));
    }

    public void testNullPrefix() {
        assertNull(StringPrefixUtils.nextPrefixUpperBound(null));
    }

    public void testCombiningAccent() {
        // U+0301 (combining acute accent) increments to U+0302
        assertEquals(new BytesRef("cafe\u0302"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("cafe\u0301")));
    }

    public void testLastBeforeSurrogateRange() {
        // U+D7FF is the last code point before the surrogate range; incrementing skips surrogates to U+E000
        assertEquals(new BytesRef("hello\uE000"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("hello\uD7FF")));
    }

    public void testTwoByteCharIncrement() {
        // U+00FF (2-byte in UTF-8) increments to U+0100 (also 2-byte)
        assertEquals(new BytesRef("\u0100"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("\u00FF")));
    }

    public void testFourByteEmojiIncrement() {
        // U+1F600 (grinning face) should increment to U+1F601
        String emoji = new String(Character.toChars(0x1F600));
        String next = new String(Character.toChars(0x1F601));
        assertEquals(new BytesRef(next), StringPrefixUtils.nextPrefixUpperBound(new BytesRef(emoji)));
    }

    public void testMaxCodePointTrimAndRetry() {
        // U+10FFFF is max; can't increment, so trim and increment preceding code point
        String maxCp = new String(Character.toChars(Character.MAX_CODE_POINT));
        assertEquals(new BytesRef("b"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("a" + maxCp)));
    }

    public void testAllMaxCodePointsReturnsNull() {
        String maxCp = new String(Character.toChars(Character.MAX_CODE_POINT));
        assertNull(StringPrefixUtils.nextPrefixUpperBound(new BytesRef(maxCp)));
        assertNull(StringPrefixUtils.nextPrefixUpperBound(new BytesRef(maxCp + maxCp)));
    }

    public void testMultiBytePrefix() {
        // "héllo" → last char 'o' increments to 'p' → "héllp"
        assertEquals(new BytesRef("h\u00e9llp"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("h\u00e9llo")));
    }

    public void testSingleAsciiA() {
        assertEquals(new BytesRef("b"), StringPrefixUtils.nextPrefixUpperBound(new BytesRef("a")));
    }
}
