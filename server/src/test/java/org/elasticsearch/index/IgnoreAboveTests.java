/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.Text;

public class IgnoreAboveTests extends ESTestCase {

    public void test_ignore_above_with_value_and_index_mode_and_index_version() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(123, IndexMode.STANDARD, IndexVersion.current());

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_value_only() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(123);

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_null_value_should_throw() {
        assertThrows(NullPointerException.class, () -> new IgnoreAbove(null));
    }

    public void test_ignore_above_with_negative_value_should_throw() {
        assertThrows(IllegalArgumentException.class, () -> new IgnoreAbove(-1));
        assertThrows(IllegalArgumentException.class, () -> new IgnoreAbove(-1, IndexMode.STANDARD, IndexVersion.current()));
    }

    public void test_ignore_above_with_null_value() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(null, IndexMode.STANDARD, IndexVersion.current());

        // when/then
        assertEquals(IndexSettings.IGNORE_ABOVE_DEFAULT_STANDARD_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_null_value_and_logsdb_index_mode() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(null, IndexMode.LOGSDB, IndexVersion.current());

        // when/then
        assertEquals(IndexSettings.IGNORE_ABOVE_DEFAULT_LOGSDB_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_null_everything() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(null, null, null);

        // when/then
        assertEquals(IndexSettings.IGNORE_ABOVE_DEFAULT_STANDARD_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_default_for_standard_indices() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.IGNORE_ABOVE_STANDARD_INDICES;

        // when/then
        assertEquals(IndexSettings.IGNORE_ABOVE_DEFAULT_STANDARD_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_default_for_logsdb_indices() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.IGNORE_ABOVE_LOGSDB_INDICES;

        // when/then
        assertEquals(IndexSettings.IGNORE_ABOVE_DEFAULT_LOGSDB_INDICES, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_string_isIgnored() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(10);

        // when/then
        assertFalse(ignoreAbove.isIgnored("potato"));
        assertFalse(ignoreAbove.isIgnored("1234567890"));
        assertTrue(ignoreAbove.isIgnored("12345678901"));
        assertTrue(ignoreAbove.isIgnored("potato potato tomato tomato"));
    }

    public void test_XContentString_isIgnored() {
        // given
        IgnoreAbove ignoreAbove = new IgnoreAbove(10);

        // when/then
        assertFalse(ignoreAbove.isIgnored(new Text("potato")));
        assertFalse(ignoreAbove.isIgnored(new Text("1234567890")));
        assertTrue(ignoreAbove.isIgnored(new Text("12345678901")));
        assertTrue(ignoreAbove.isIgnored(new Text("potato potato tomato tomato")));
    }

}
