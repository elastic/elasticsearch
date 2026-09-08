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
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.xcontent.Text;
import org.elasticsearch.xcontent.XContentString;

import java.util.List;

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

    /**
     * Strictly columnar modes make {@code ignore_above} inert at or after
     * {@link IndexVersions#IGNORE_ABOVE_NO_OP_IN_COLUMNAR}, but actively enforce it on older indices.
     */
    public void test_is_no_op_in_columnar_modes_at_or_after_gate() {
        List<IndexMode> columnarModes = List.of(IndexMode.COLUMNAR, IndexMode.LOGSDB_COLUMNAR, IndexMode.VECTORDB_COLUMNAR);
        for (IndexMode mode : columnarModes) {
            IndexVersion preGate = IndexVersionUtils.getPreviousVersion(IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR);
            assertFalse("isNoOp should be false for " + mode + " pre-gate", Mapper.IgnoreAbove.isNoOp(mode, preGate));
            Mapper.IgnoreAbove preGateObj = new Mapper.IgnoreAbove(10, mode, preGate);
            assertEquals("get() pre-gate " + mode, 10, preGateObj.get());
            assertTrue("isSet() pre-gate " + mode, preGateObj.isSet());
            assertTrue("valuesPotentiallyIgnored() pre-gate " + mode, preGateObj.valuesPotentiallyIgnored());
            assertTrue("isIgnored(String) pre-gate " + mode, preGateObj.isIgnored("12345678901"));
            assertFalse("!isIgnored(String) pre-gate " + mode, preGateObj.isIgnored("1234567890"));
            assertTrue("isIgnored(XContentString) pre-gate " + mode, preGateObj.isIgnored(new Text("12345678901")));
            assertFalse("!isIgnored(XContentString) pre-gate " + mode, preGateObj.isIgnored(new Text("1234567890")));
            assertTrue("isIgnored(BytesRef) pre-gate " + mode, preGateObj.isIgnored(new BytesRef("12345678901")));
            assertFalse("!isIgnored(BytesRef) pre-gate " + mode, preGateObj.isIgnored(new BytesRef("1234567890")));

            assertTrue(
                "isNoOp should be true for " + mode + " at gate",
                Mapper.IgnoreAbove.isNoOp(mode, IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR)
            );
            Mapper.IgnoreAbove atGateObj = new Mapper.IgnoreAbove(10, mode, IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR);
            assertEquals("get() at gate " + mode, 10, atGateObj.get());
            assertTrue("isSet() at gate " + mode, atGateObj.isSet());
            assertFalse("valuesPotentiallyIgnored() at gate " + mode, atGateObj.valuesPotentiallyIgnored());
            assertFalse("isIgnored(String) at gate " + mode, atGateObj.isIgnored("12345678901"));
            assertFalse("isIgnored(XContentString) at gate " + mode, atGateObj.isIgnored(new Text("12345678901")));
            assertFalse("isIgnored(BytesRef) at gate " + mode, atGateObj.isIgnored(new BytesRef("12345678901")));

            assertTrue("isNoOp should be true for " + mode + " current", Mapper.IgnoreAbove.isNoOp(mode, IndexVersion.current()));
            Mapper.IgnoreAbove currentObj = new Mapper.IgnoreAbove(10, mode, IndexVersion.current());
            assertEquals("get() current " + mode, 10, currentObj.get());
            assertTrue("isSet() current " + mode, currentObj.isSet());
            assertFalse("valuesPotentiallyIgnored() current " + mode, currentObj.valuesPotentiallyIgnored());
            assertFalse("isIgnored(String) current " + mode, currentObj.isIgnored("12345678901"));
            assertFalse("isIgnored(XContentString) current " + mode, currentObj.isIgnored(new Text("12345678901")));
            assertFalse("isIgnored(BytesRef) current " + mode, currentObj.isIgnored(new BytesRef("12345678901")));
        }
    }

    /**
     * Non-columnar modes (STANDARD, LOGSDB) are never a no-op regardless of the index version.
     */
    public void test_is_not_no_op_in_non_columnar_modes() {
        List<IndexMode> nonColumnarModes = List.of(IndexMode.STANDARD, IndexMode.LOGSDB);
        List<IndexVersion> versions = List.of(
            IndexVersionUtils.getPreviousVersion(IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR),
            IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR,
            IndexVersion.current()
        );
        for (IndexMode mode : nonColumnarModes) {
            for (IndexVersion version : versions) {
                assertFalse("expected isNoOp=false for mode=" + mode + " version=" + version, Mapper.IgnoreAbove.isNoOp(mode, version));
                Mapper.IgnoreAbove obj = new Mapper.IgnoreAbove(10, mode, version);
                assertTrue("valuesPotentiallyIgnored() should be true for mode=" + mode, obj.valuesPotentiallyIgnored());
                assertTrue("isIgnored(String) should be true for mode=" + mode, obj.isIgnored("12345678901"));
            }
        }
    }

    /**
     * In LOGSDB_COLUMNAR the default is 8191. After the gate {@code get()} still reports 8191
     * (so {@code GET _mapping} round-trips correctly) but {@code isIgnored()} always returns false.
     */
    public void test_no_op_preserves_configured_value_in_logsdb_columnar() {
        Mapper.IgnoreAbove withDefault = new Mapper.IgnoreAbove(null, IndexMode.LOGSDB_COLUMNAR, IndexVersion.current());
        assertEquals(Mapper.IgnoreAbove.IGNORE_ABOVE_DEFAULT_VALUE_FOR_LOGSDB_INDICES, withDefault.get());
        assertFalse("isSet() should be false when using the default", withDefault.isSet());
        assertFalse("valuesPotentiallyIgnored() inert in logsdb_columnar", withDefault.valuesPotentiallyIgnored());
        assertFalse("isIgnored(String) must be false despite default 8191", withDefault.isIgnored("x".repeat(8192)));
        assertFalse("isIgnored(Text) must be false despite default 8191", withDefault.isIgnored(new Text("x".repeat(8192))));
        assertFalse("isIgnored(BytesRef) must be false despite default 8191", withDefault.isIgnored(new BytesRef("x".repeat(8192))));

        Mapper.IgnoreAbove withExplicit = new Mapper.IgnoreAbove(50, IndexMode.LOGSDB_COLUMNAR, IndexVersion.current());
        assertEquals(50, withExplicit.get());
        assertTrue(withExplicit.isSet());
        assertFalse("valuesPotentiallyIgnored() inert even with explicit value", withExplicit.valuesPotentiallyIgnored());
        assertFalse("isIgnored(String) inert with explicit 50", withExplicit.isIgnored("x".repeat(51)));
    }

    /**
     * limit() returns MAX_VALUE when the parameter is inert, and the actual limit otherwise.
     */
    public void test_limit_accessor() {
        assertEquals(100, new Mapper.IgnoreAbove(100, IndexMode.STANDARD, IndexVersion.current()).limit());
        assertEquals(Integer.MAX_VALUE, new Mapper.IgnoreAbove(100, IndexMode.COLUMNAR, IndexVersion.current()).limit());
        IndexVersion preGate = IndexVersionUtils.getPreviousVersion(IndexVersions.IGNORE_ABOVE_NO_OP_IN_COLUMNAR);
        assertEquals(100, new Mapper.IgnoreAbove(100, IndexMode.COLUMNAR, preGate).limit());
        assertEquals(Integer.MAX_VALUE, new Mapper.IgnoreAbove(null, IndexMode.STANDARD, IndexVersion.current()).limit());
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
