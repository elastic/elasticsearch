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

    public void test_ignore_above_with_value_and_default() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().value(123).defaultValue(456).build();

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_value_only() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().value(123).build();

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertTrue(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_default_only() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().defaultValue(456).build();

        // when/then
        assertEquals(456, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_same_value_and_default() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().value(123).defaultValue(123).build();

        // when/then
        assertEquals(123, ignoreAbove.get());
        assertFalse(ignoreAbove.isSet());
    }

    public void test_ignore_above_with_nothing_should_throw() {
        // given/when/then
        assertThrows(IllegalArgumentException.class, () -> IgnoreAbove.builder().build());
    }

    public void test_string_isIgnored() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().value(10).defaultValue(456).build();

        // when/then
        assertFalse(ignoreAbove.isIgnored("potato"));
        assertFalse(ignoreAbove.isIgnored("1234567890"));
        assertTrue(ignoreAbove.isIgnored("12345678901"));
        assertTrue(ignoreAbove.isIgnored("potato potato tomato tomato"));
    }

    public void test_XContentString_isIgnored() {
        // given
        IgnoreAbove ignoreAbove = IgnoreAbove.builder().value(10).defaultValue(456).build();

        // when/then
        assertFalse(ignoreAbove.isIgnored(new Text("potato")));
        assertFalse(ignoreAbove.isIgnored(new Text("1234567890")));
        assertTrue(ignoreAbove.isIgnored(new Text("12345678901")));
        assertTrue(ignoreAbove.isIgnored(new Text("potato potato tomato tomato")));
    }

}
