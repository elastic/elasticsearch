/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class ImmutableOpenMapTests extends ESTestCase {

    ImmutableOpenMap<String, String> regionCurrencySymbols = ImmutableOpenMap.<String, String>builder()
        .fPut("Japan", "¥")
        .fPut("USA", "$")
        .fPut("EU", "€")
        .fPut("UK", "£")
        .fPut("Korea", "₩")
        .build();

    ImmutableOpenMap<String, Integer> countryPopulations = ImmutableOpenMap.<String, Integer>builder()
        .fPut("Poland", 37_846_611)
        .fPut("France", 65_273_511)
        .fPut("Spain", 46_754_778)
        .fPut("Germany", 83_783_942)
        .fPut("Italy", 60_461_826)
        .build();

    public void testStreamOperationsAreSupported() {
        assertThat(
            regionCurrencySymbols.stream().filter(e -> e.getKey().startsWith("U")).map(Map.Entry::getValue).collect(Collectors.toSet()),
            equalTo(Stream.of("£", "$").collect(Collectors.toSet()))
        );
    }

    public void testSortedStream() {
        assertThat(
            regionCurrencySymbols.stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList()),
            equalTo(Stream.of("€", "¥", "₩", "£", "$").collect(Collectors.toList()))
        );
    }

    public void testStreamOperationsOnRandomMap() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        Map<Long, List<String>> collectedViaStreams = map.stream()
            .filter(e -> e.getKey() > 0)
            .sorted(Map.Entry.comparingByKey())
            .limit(limit)
            .collect(Collectors.groupingBy(e -> e.getKey() % 2, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

        Map<Long, String> sortedMap = new TreeMap<>();
        for (ObjectObjectCursor<Long, String> cursor : map) {
            if (cursor.key > 0) {
                sortedMap.put(cursor.key, cursor.value);
            }
        }
        int i = 0;
        Map<Long, List<String>> collectedIteratively = new HashMap<>();
        for (Map.Entry<Long, String> e : sortedMap.entrySet()) {
            if (i++ >= limit) {
                break;
            }
            collectedIteratively.computeIfAbsent(e.getKey() % 2, k -> new ArrayList<>()).add(e.getValue());
        }

        assertThat(collectedViaStreams, equalTo(collectedIteratively));
    }

    public void testEmptyStreamWorks() {
        assertThat(ImmutableOpenMap.of().stream().count(), equalTo(0L));
    }

    public void testKeySetStreamOperationsAreSupported() {
        assertThat(
            regionCurrencySymbols.keySet().stream().filter(e -> e.startsWith("U") == false).collect(Collectors.toSet()),
            equalTo(Stream.of("Japan", "EU", "Korea").collect(Collectors.toSet()))
        );
    }

    public void testSortedKeysSet() {
        assertThat(regionCurrencySymbols.keySet(), equalTo(Stream.of("EU", "Japan", "Korea", "UK", "USA").collect(Collectors.toSet())));
    }

    public void testStreamOperationsOnRandomMapKeys() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        List<Long> collectedViaStream = map.keySet().stream().filter(e -> e > 0).sorted().limit(limit).collect(Collectors.toList());

        SortedSet<Long> positiveNumbers = new TreeSet<>();
        for (ObjectObjectCursor<Long, String> cursor : map) {
            if (cursor.key > 0) {
                positiveNumbers.add(cursor.key);
            }
        }
        int i = 0;
        List<Long> collectedIteratively = new ArrayList<>();
        for (Long l : positiveNumbers) {
            if (i++ >= limit) {
                break;
            }
            collectedIteratively.add(l);
        }
        assertThat(collectedViaStream, equalTo(collectedIteratively));
    }

    public void testEmptyKeySetWorks() {
        assertThat(ImmutableOpenMap.of().keySet().size(), equalTo(0));
    }

    public void testEmptyValuesIsCollection() {
        assertThat(ImmutableOpenMap.of().values(), empty());
    }

    public void testValuesIsCollection() {
        assertThat(countryPopulations.values(), containsInAnyOrder(37_846_611, 46_754_778, 60_461_826, 65_273_511, 83_783_942));
    }

    public void testValuesToArray() {
        Integer[] populations = countryPopulations.values().toArray(new Integer[0]);

        assertThat(populations, arrayContainingInAnyOrder(37_846_611, 46_754_778, 60_461_826, 65_273_511, 83_783_942));
    }

    public void testStreamOperationOnValues() {
        assertThat(
            countryPopulations.values()
                .stream()
                .filter(e -> e > 60_000_000)
                .sorted(Comparator.reverseOrder())
                .limit(2)
                .collect(Collectors.toList()),
            equalTo(org.elasticsearch.core.List.of(83_783_942, 65_273_511))
        );
    }

    public void testStreamOperationsOnRandomMapValues() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        List<String> collectedViaStream = map.values()
            .stream()
            .filter(e -> (e.contains("ab") || e.contains("cd") || e.contains("ef")) == false)
            .sorted()
            .limit(limit)
            .collect(Collectors.toList());

        SortedSet<String> filteredSortedStrings = new TreeSet<>();
        for (ObjectObjectCursor<Long, String> cursor : map) {
            if ((cursor.value.contains("ab") || cursor.value.contains("cd") || cursor.value.contains("ef")) == false) {
                filteredSortedStrings.add(cursor.value);
            }
        }
        int i = 0;
        List<String> collectedIteratively = new ArrayList<>();
        for (String s : filteredSortedStrings) {
            if (i++ >= limit) {
                break;
            }
            collectedIteratively.add(s);
        }
        assertThat(collectedViaStream, equalTo(collectedIteratively));
    }

    private static ImmutableOpenMap<Long, String> randomImmutableOpenMap() {
        return Randomness.get()
            .longs(randomIntBetween(1, 1000))
            .mapToObj(e -> Tuple.tuple(e, randomAlphaOfLength(8)))
            .collect(
                ImmutableOpenMap::<Long, String>builder,
                (builder, t) -> builder.fPut(t.v1(), t.v2()),
                ImmutableOpenMap.Builder::putAll
            )
            .build();
    }
}
