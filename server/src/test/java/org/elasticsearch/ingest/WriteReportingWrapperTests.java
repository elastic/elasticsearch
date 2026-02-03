/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.ingest.WriteReportingWrapper.reportingWritesUsing;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

public class WriteReportingWrapperTests extends ESTestCase {

    public void testWrap_topLevel() {
        Map<String, Integer> original = Map.of("one", 1, "two", 2, "three", 3);
        List<String> reports = new ArrayList<>();
        Map<String, Integer> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.put("three", 33);
        assertThat(wrapped, is(Map.of("one", 1, "two", 2, "three", 33)));
        assertThat(reports, contains("someMap.put(K, V)"));
    }

    public void testWrap_nestedMaps() {
        Map<String, Map<String, Map<String, Integer>>> original = Map.of("outer", Map.of("mid", Map.of("inner", 123)));
        List<String> reports = new ArrayList<>();
        Map<String, Map<String, Map<String, Integer>>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").get("mid").put("added", 456);
        assertThat(wrapped, is(Map.of("outer", Map.of("mid", Map.of("inner", 123, "added", 456)))));
        assertThat(reports, contains("someMap[outer][mid].put(K, V)"));
    }

    public void testWrap_listInMap() {
        Map<String, List<Integer>> original = Map.of("outer", List.of(123));
        List<String> reports = new ArrayList<>();
        Map<String, List<Integer>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").add(456);
        assertThat(wrapped, is(Map.of("outer", List.of(123, 456))));
        assertThat(reports, contains("someMap[outer].add(E)"));
    }

    public void testWrap_listInListInMap() {
        Map<String, List<List<Integer>>> original = Map.of("outer", List.of(List.of(123)));
        List<String> reports = new ArrayList<>();
        Map<String, List<List<Integer>>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").getFirst().add(456);
        assertThat(wrapped, is(Map.of("outer", List.of(List.of(123, 456)))));
        assertThat(reports, contains("someMap[outer][0].add(E)"));
    }

    public void testWrap_mapInListInMap() {
        Map<String, List<Map<String, Integer>>> original = Map.of("outer", List.of(Map.of("inner", 123)));
        List<String> reports = new ArrayList<>();
        Map<String, List<Map<String, Integer>>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").getFirst().put("added", 456);
        assertThat(wrapped, is(Map.of("outer", List.of(Map.of("inner", 123, "added", 456)))));
        assertThat(reports, contains("someMap[outer][0].put(K, V)"));
    }

    public void testWrap_setInMap() {
        Map<String, Set<Integer>> original = Map.of("outer", Set.of(123));
        List<String> reports = new ArrayList<>();
        Map<String, Set<Integer>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").add(456);
        assertThat(wrapped, is(Map.of("outer", Set.of(123, 456))));
        assertThat(reports, contains("someMap[outer].add(E)"));
    }

    public void testWrap_setInListInMap() {
        Map<String, List<Set<Integer>>> original = Map.of("outer", List.of(Set.of(123)));
        List<String> reports = new ArrayList<>();
        Map<String, List<Set<Integer>>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("outer").getFirst().add(456);
        assertThat(wrapped, is(Map.of("outer", List.of(Set.of(123, 456)))));
        assertThat(reports, contains("someMap[outer][0].add(E)"));
    }

    public void testWrap_setInSetInMap_reportsEvenThroughBreaksHashSet() {
        Map<String, Set<Set<Integer>>> original = Map.of("outer", Set.of(Set.of(123)));
        List<String> reports = new ArrayList<>();
        Map<String, Set<Set<Integer>>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        // This is a naughty thing to do. We are mutating a value (the inner Set) stored in a Set (the outer one).
        // The behaviour os Set is undefined in this case...
        wrapped.get("outer").iterator().next().add(456);
        // ...So we cannot make any assertions about the value in the wrapped map at this point, only about its keys.
        assertThat(wrapped.keySet(), contains("outer"));
        // However, we can assert that the write operation was reported correctly.
        assertThat(reports, contains("someMap[outer][0].add(E)"));
    }

    public void testWrap_arrayListInMap_throws() {
        Map<String, ArrayList<Integer>> original = Map.of("key", new ArrayList<>());
        List<String> reports = new ArrayList<>();
        Map<String, ArrayList<Integer>> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        // Expect ClassCastException when assigning the value to an ArrayList, as warned in the javadoc.
        assertThrows(ClassCastException.class, () -> { ArrayList<Integer> arrayList = wrapped.get("key"); });
        List<Integer> plainList = wrapped.get("key");
        assertThat(plainList, empty());
        assertThat(reports, empty());
    }

    public void testWrap_arrayInMap_notReported() {
        Map<String, String[]> original = Map.of("key", new String[] { "foo", "bar", "baz" });
        List<String> reports = new ArrayList<>();
        Map<String, String[]> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("key")[1] = "changed";
        assertThat(original.get("key"), arrayContaining("foo", "changed", "baz"));
        assertThat(reports, empty()); // does not report on mutations via arrays
    }

    public void testWrap_mutableDateType_notReported() {
        long beforeMillis = 1739297834000L;
        long afterMillis = beforeMillis + 1000L;
        Map<String, Date> original = Map.of("key", new Date(beforeMillis));
        List<String> reports = new ArrayList<>();
        Map<String, Date> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        wrapped.get("key").setTime(afterMillis);
        assertThat(original.get("key"), is(new Date(afterMillis)));
        assertThat(reports, empty()); // does not report on mutations via other mutable types such as java.util.Date
    }

    public void testWrap_nullValue() {
        Map<String, String> original = new HashMap<>();
        original.put("key", null);
        List<String> reports = new ArrayList<>();
        Map<String, String> wrapped = reportingWritesUsing(reports::add).wrap(original, "someMap");
        assertThat(wrapped.get("key"), nullValue());
        assertThat(reports, empty());
    }
}
