/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

public class IgnoreAboveTests extends ESTestCase {

    private static final Mapper.IgnoreAbove IGNORE_ABOVE_DEFAULT = new Mapper.IgnoreAbove(null, IndexMode.STANDARD);
    private static final Mapper.IgnoreAbove IGNORE_ABOVE_DEFAULT_LOGS = new Mapper.IgnoreAbove(null, IndexMode.LOGSDB);

    public void test_ignore_above_with_value_and_index_mode_and_index_version() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(123, IndexMode.STANDARD);

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
        assertTrue(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_with_value_only() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(123);

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
        assertTrue(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_with_null_value_should_throw() {
        assertThrows(NullPointerException.class, () -> new Mapper.IgnoreAbove(null));
    }

    public void test_ignore_above_with_negative_value_should_throw() {
        assertThrows(IllegalArgumentException.class, () -> new Mapper.IgnoreAbove(-1));
        assertThrows(IllegalArgumentException.class, () -> new Mapper.IgnoreAbove(-1, IndexMode.STANDARD));
    }

    public void test_ignore_above_with_null_value() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(null, IndexMode.STANDARD);

        // when/then
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
        assertFalse(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_with_null_value_and_logsdb_index_mode() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(null, IndexMode.LOGSDB);

        // when/then
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
        assertTrue(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_with_null_everything() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(null, null, null);

        // when/then
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
        assertFalse(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_default_for_standard_indices() {
        // given
        Mapper.IgnoreAbove ignoreAbove = IGNORE_ABOVE_DEFAULT;

        // when/then
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
        assertFalse(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_ignore_above_default_for_logsdb_indices() {
        // given
        Mapper.IgnoreAbove ignoreAbove = IGNORE_ABOVE_DEFAULT_LOGS;

        // when/then
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
        assertTrue(ignoreAbove.valuesPotentiallyIgnored());
    }

    public void test_string_isIgnored() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);

        // when/then
        assertFalse(ignoreAbove.isIgnored("potato"));
        assertFalse(ignoreAbove.isIgnored("1234567890"));
        assertTrue(ignoreAbove.isIgnored("12345678901"));
        assertTrue(ignoreAbove.isIgnored("potato potato tomato tomato"));
    }

    public void test_XContentString_isIgnored() {
        // given
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);

        // when/then
        assertFalse(ignoreAbove.isIgnored(new Text("potato")));
        assertFalse(ignoreAbove.isIgnored(new Text("1234567890")));
        assertTrue(ignoreAbove.isIgnored(new Text("12345678901")));
        assertTrue(ignoreAbove.isIgnored(new Text("potato potato tomato tomato")));
    }

    public void test_Text_isIgnored_without_bytes() {
        // Text backed by a String — hasBytes() is false, falls through to stringLength().
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);

        assertFalse(ignoreAbove.isIgnored(new Text("potato")));
        assertFalse(ignoreAbove.isIgnored(new Text("1234567890")));
        assertTrue(ignoreAbove.isIgnored(new Text("12345678901")));
    }

    public void test_Text_isIgnored_with_bytes_fast_path() {
        // Text backed by UTF8Bytes — hasBytes() is true; ASCII strings use the byte-length fast path.
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);

        // Byte length <= ignore_above: fast path returns false without counting code points.
        Text withinLimit = new Text(new XContentString.UTF8Bytes("potato".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue(withinLimit.hasBytes());
        assertFalse(ignoreAbove.isIgnored(withinLimit));

        Text atLimit = new Text(new XContentString.UTF8Bytes("1234567890".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue(atLimit.hasBytes());
        assertFalse(ignoreAbove.isIgnored(atLimit));

        // Byte length > ignore_above: fast path does not apply; falls through to stringLength().
        Text overLimit = new Text(new XContentString.UTF8Bytes("12345678901".getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue(overLimit.hasBytes());
        assertTrue(ignoreAbove.isIgnored(overLimit));
    }

    public void test_Text_isIgnored_multibyte_not_short_circuited() {
        // A 6-char string of 2-byte UTF-8 code points has 12 bytes but only 6 code points.
        // Byte length (12) > ignore_above (10), so fast path does not apply; stringLength() is used
        // and correctly returns 6 — not ignored.
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);
        String sixChars = "éééééé"; // é×6, 2 bytes each = 12 bytes
        Text t = new Text(new XContentString.UTF8Bytes(sixChars.getBytes(java.nio.charset.StandardCharsets.UTF_8)));
        assertTrue(t.hasBytes());
        assertEquals(12, t.bytes().length());
        assertFalse(ignoreAbove.isIgnored(t));
    }

    public void test_BytesRef_isIgnored_null() {
        assertFalse(new Mapper.IgnoreAbove(10).isIgnored((BytesRef) null));
    }

    public void test_BytesRef_isIgnored_ascii_fast_path() {
        // ASCII: 1 byte per char, so byte length == code-point count. Fast path fires.
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);

        assertFalse(ignoreAbove.isIgnored(new BytesRef("potato")));        // 6 bytes, within limit
        assertFalse(ignoreAbove.isIgnored(new BytesRef("1234567890")));    // 10 bytes, at limit
        assertTrue(ignoreAbove.isIgnored(new BytesRef("12345678901")));    // 11 bytes, over limit
    }

    public void test_BytesRef_isIgnored_multibyte_bytes_exceed_but_codepoints_do_not() {
        // "éééééé" = 6 code points, 12 UTF-8 bytes. Byte count (12) > ignore_above (10),
        // so the fast path does not apply; stringLength() returns 6 — not ignored.
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);
        BytesRef ref = new BytesRef("éééééé");
        assertEquals(12, ref.length);
        assertFalse(ignoreAbove.isIgnored(ref));
    }

    public void test_BytesRef_isIgnored_multibyte_both_exceed() {
        // 11 × "é" = 11 code points, 22 bytes — both exceed ignore_above (10).
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(10);
        assertTrue(ignoreAbove.isIgnored(new BytesRef("ééééééééééé")));
    }

    public void test_BytesRef_isIgnored_respects_offset_and_length() {
        // Wrap a short string inside a larger backing array with a non-zero offset.
        // Only the slice "hello" (5 bytes) should be evaluated; the surrounding bytes are noise.
        Mapper.IgnoreAbove ignoreAbove = new Mapper.IgnoreAbove(4);
        byte[] backing = "XXhelloXX".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        BytesRef sliced = new BytesRef(backing, 2, 5); // "hello"
        assertTrue(ignoreAbove.isIgnored(sliced));  // 5 code points > 4

        BytesRef withinLimit = new BytesRef(backing, 2, 4); // "hell"
        assertFalse(ignoreAbove.isIgnored(withinLimit)); // 4 code points == 4
    }

    public void test_default_value() {
        assertEquals(
            Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE,
            Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(IndexMode.STANDARD, IndexVersion.current())
        );
        assertEquals(
            Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES,
            Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(IndexMode.LOGSDB, IndexVersion.current())
        );
        assertEquals(
            Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE,
            Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(IndexMode.LOGSDB, IndexVersions.ENABLE_IGNORE_MALFORMED_LOGSDB)
        );
        assertEquals(
            Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES,
            Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(IndexMode.LOGSDB_COLUMNAR, IndexVersion.current())
        );
        assertEquals(
            Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE,
            Mapper.IgnoreAbove.getIgnoreAboveDefaultValue(IndexMode.LOGSDB_COLUMNAR, IndexVersions.ENABLE_IGNORE_MALFORMED_LOGSDB)
        );
    }

}
