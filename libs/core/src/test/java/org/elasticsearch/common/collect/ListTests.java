/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.core.List;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;

import static org.hamcrest.CoreMatchers.equalTo;

public class ListTests extends ESTestCase {

    public void testStringListOfZero() {
        final String[] strings = {};
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfOne() {
        final String[] strings = { "foo" };
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfTwo() {
        final String[] strings = { "foo", "bar" };
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testStringListOfN() {
        final String[] strings = { "foo", "bar", "baz" };
        final java.util.List<String> stringsList = List.of(strings);
        assertThat(stringsList.size(), equalTo(strings.length));
        assertTrue(stringsList.containsAll(Arrays.asList(strings)));
        expectThrows(UnsupportedOperationException.class, () -> stringsList.add("foo"));
    }

    public void testCopyOf() {
        final Collection<String> coll = Arrays.asList("foo", "bar", "baz");
        final java.util.List<String> copy = List.copyOf(coll);
        assertThat(coll.size(), equalTo(copy.size()));
        assertTrue(copy.containsAll(coll));
        expectThrows(UnsupportedOperationException.class, () -> copy.add("foo"));
    }
}
