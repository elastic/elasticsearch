/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest;

import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RequestParamsTests extends ESTestCase {

    // -------------------------------------------------------------------------
    // Factory methods
    // -------------------------------------------------------------------------

    public void testEmpty() {
        var map = RequestParams.empty();
        assertThat(map.isEmpty(), is(true));
        assertThat(map.size(), equalTo(0));
        assertThat(map.get("x"), nullValue());
        assertThat(map.getAll("x"), equalTo(List.of()));
    }

    public void testOf() {
        var map = RequestParams.of(Map.of("a", List.of("1", "2"), "b", List.of("3")));
        assertThat(map.size(), equalTo(2));
        assertThat(map.getAll("a"), equalTo(List.of("1", "2")));
        assertThat(map.getAll("b"), equalTo(List.of("3")));
    }

    public void testFromSingleValues() {
        var map = RequestParams.fromSingleValues(Map.of("a", "1", "b", "2"));
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("a"), equalTo("1"));
        assertThat(map.get("b"), equalTo("2"));
        assertThat(map.getAll("a"), equalTo(List.of("1")));
        assertThat(map.getAll("b"), equalTo(List.of("2")));
    }

    // -------------------------------------------------------------------------
    // get / getAll
    // -------------------------------------------------------------------------

    public void testGetReturnsLastValue() {
        var map = RequestParams.of(Map.of("k", List.of("first", "second", "last")));
        assertThat(map.get("k"), equalTo("last"));
    }

    public void testGetReturnsNullForAbsentKey() {
        var map = RequestParams.of(Map.of("k", List.of("v")));
        assertThat(map.get("missing"), nullValue());
    }

    public void testGetAllReturnsAllValues() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b", "c")));
        assertThat(map.getAll("k"), equalTo(List.of("a", "b", "c")));
    }

    public void testGetAllReturnsEmptyListForAbsentKey() {
        var map = RequestParams.empty();
        assertThat(map.getAll("missing"), equalTo(List.of()));
    }

    // -------------------------------------------------------------------------
    // getSingle
    // -------------------------------------------------------------------------

    public void testGetSingleReturnsSingleValue() {
        var map = RequestParams.of(Map.of("k", List.of("only")));
        assertThat(map.getSingle("k"), equalTo("only"));
    }

    public void testGetSingleReturnsNullForAbsentKey() {
        var map = RequestParams.empty();
        assertThat(map.getSingle("missing"), nullValue());
    }

    public void testGetSingleThrowsOnMultipleValues() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b")));
        var ex = expectThrows(IllegalArgumentException.class, () -> map.getSingle("k"));
        assertThat(ex.getMessage(), equalTo("parameter [k] must have a single value, but found: [a, b]"));
    }

    // -------------------------------------------------------------------------
    // immutability
    // -------------------------------------------------------------------------

    public void testPutThrows() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b")));
        expectThrows(UnsupportedOperationException.class, () -> map.put("k", "new"));
    }

    public void testRemoveThrows() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b")));
        expectThrows(UnsupportedOperationException.class, () -> map.remove("k"));
    }

    public void testClearThrows() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        expectThrows(UnsupportedOperationException.class, map::clear);
    }

    // -------------------------------------------------------------------------
    // Map interface — containsKey, keySet, entrySet
    // -------------------------------------------------------------------------

    public void testContainsKey() {
        var map = RequestParams.of(Map.of("present", List.of("v")));
        assertThat(map.containsKey("present"), is(true));
        assertThat(map.containsKey("absent"), is(false));
    }

    public void testKeySet() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        assertThat(map.keySet(), equalTo(java.util.Set.of("a", "b")));
    }

    public void testEntrySetValuesAreLastValues() {
        var map = RequestParams.of(Map.of("k", List.of("first", "last")));
        var entry = map.entrySet().iterator().next();
        assertThat(entry.getKey(), equalTo("k"));
        assertThat(entry.getValue(), equalTo("last"));
    }

    public void testEntrySetIteratorRemoveThrows() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        var it = map.entrySet().iterator();
        it.next();
        expectThrows(UnsupportedOperationException.class, it::remove);
    }
}
