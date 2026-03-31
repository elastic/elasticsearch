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

import java.net.URI;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class RequestParamsTests extends ESTestCase {

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

    public void testOfEmptyListThrows() {
        var ex = expectThrows(IllegalArgumentException.class, () -> RequestParams.of(Map.of("k", List.of())));
        assertThat(ex.getMessage(), equalTo("value list for parameter [k] must not be empty"));
    }

    public void testFromSingleValues() {
        var map = RequestParams.fromSingleValues(Map.of("a", "1", "b", "2"));
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("a"), equalTo("1"));
        assertThat(map.get("b"), equalTo("2"));
        assertThat(map.getAll("a"), equalTo(List.of("1")));
        assertThat(map.getAll("b"), equalTo(List.of("2")));
    }

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

    public void testRequireSingleReturnsSingleValue() {
        var map = RequestParams.of(Map.of("k", List.of("only")));
        assertThat(map.requireSingle("k"), equalTo("only"));
    }

    public void testRequireSingleReturnsNullForAbsentKey() {
        var map = RequestParams.empty();
        assertThat(map.requireSingle("missing"), nullValue());
    }

    public void testRequireSingleThrowsOnMultipleValues() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b")));
        var ex = expectThrows(RestRequest.BadParameterException.class, () -> map.requireSingle("k"));
        assertThat(ex.getMessage(), containsString("parameter [k] must have a single value, but found: [a, b]"));
    }

    public void testPut() {
        var map = RequestParams.of(Map.of("k", List.of("a", "b")));
        assertThat(map.put("k", "new"), equalTo("b")); // returns previous last value
        assertThat(map.get("k"), equalTo("new"));
        assertThat(map.getAll("k"), equalTo(List.of("new")));
    }

    public void testPutNewKey() {
        var map = RequestParams.of(Map.of("k", List.of("v")));
        assertThat(map.put("new", "val"), nullValue());
        assertThat(map.get("new"), equalTo("val"));
    }

    public void testRemove() {
        var map = RequestParams.of(Map.of("a", List.of("1", "last"), "b", List.of("2")));
        assertThat(map.remove("a"), equalTo("last")); // returns previous last value
        assertThat(map.containsKey("a"), is(false));
        assertThat(map.size(), equalTo(1));
    }

    public void testRemoveAbsentKey() {
        var map = RequestParams.of(Map.of("k", List.of("v")));
        assertThat(map.remove("missing"), nullValue());
        assertThat(map.size(), equalTo(1));
    }

    public void testClear() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        map.clear();
        assertThat(map.isEmpty(), is(true));
    }

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
        var iterator = map.entrySet().iterator();
        assertThat(iterator.hasNext(), is(true));
        var entry = iterator.next();
        assertThat(entry.getKey(), equalTo("k"));
        assertThat(entry.getValue(), equalTo("last"));
        assertThat(iterator.hasNext(), is(false));
    }

    public void testEntrySetIteratorRemove() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        var it = map.entrySet().iterator();
        it.next();
        it.remove();
        assertThat(map.size(), equalTo(1));
    }

    public void testEntrySetClear() {
        var map = RequestParams.of(Map.of("a", List.of("1"), "b", List.of("2")));
        map.entrySet().clear();
        assertThat(map.isEmpty(), is(true));
    }

    public void testAddValue() {
        var map = RequestParams.empty();
        map.addValue("k", "first");
        assertThat(map.getAll("k"), equalTo(List.of("first")));
        map.addValue("k", "second");
        assertThat(map.getAll("k"), equalTo(List.of("first", "second")));
        assertThat(map.get("k"), equalTo("second"));
    }

    public void testFromUri() {
        var map = RequestParams.fromUri("something?a=1&b=2");
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("a"), equalTo("1"));
        assertThat(map.get("b"), equalTo("2"));
        assertThat(RequestParams.fromUri("something").isEmpty(), is(true));
    }

    public void testFrom() throws Exception {
        var map = RequestParams.from(new URI("http://example.com/path?a=1&b=2"));
        assertThat(map.size(), equalTo(2));
        assertThat(map.get("a"), equalTo("1"));
        assertThat(map.get("b"), equalTo("2"));
        assertThat(RequestParams.from(new URI("http://example.com/path")).isEmpty(), is(true));
    }
}
