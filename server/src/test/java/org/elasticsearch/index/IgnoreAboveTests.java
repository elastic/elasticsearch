/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;

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
    }

}
