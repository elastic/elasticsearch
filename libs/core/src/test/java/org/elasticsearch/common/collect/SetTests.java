/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.Set;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;

public class SetTests extends ESTestCase {

    public void testStringSetOfZero() {
        final String[] strings = {};
        final java.util.Set<String> stringsSet = Set.of(strings);
        assertThat(stringsSet.size(), equalTo(strings.length));
        assertTrue(stringsSet.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsSet.add("foo"));
    }

    public void testStringSetOfOne() {
        final String[] strings = { "foo" };
        final java.util.Set<String> stringsSet = Set.of(strings);
        assertThat(stringsSet.size(), equalTo(strings.length));
        assertTrue(stringsSet.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsSet.add("foo"));
    }

    public void testStringSetOfTwo() {
        final String[] strings = { "foo", "bar" };
        final java.util.Set<String> stringsSet = Set.of(strings);
        assertThat(stringsSet.size(), equalTo(strings.length));
        assertTrue(stringsSet.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsSet.add("foo"));
    }

    public void testStringSetOfN() {
        final String[] strings = { "foo", "bar", "baz" };
        final java.util.Set<String> stringsSet = Set.of(strings);
        assertThat(stringsSet.size(), equalTo(strings.length));
        assertTrue(stringsSet.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsSet.add("foo"));
    }

    public void testCopyOf() {
        final Collection<String> coll = Arrays.asList("foo", "bar", "baz");
        final java.util.Set<String> copy = Set.copyOf(coll);
        assertThat(coll.size(), equalTo(copy.size()));
        assertTrue(copy.containsAll(coll));
        expectThrows(UnsupportedOperationException.class, () -> copy.add("foo"));
    }
}
