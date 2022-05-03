/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.collect;

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

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
            regionCurrencySymbols.entrySet()
                .stream()
                .filter(e -> e.getKey().startsWith("U"))
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet()),
            equalTo(Set.of("£", "$"))
        );
    }

    public void testSortedStream() {
        assertThat(
            regionCurrencySymbols.entrySet().stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).toList(),
            equalTo(List.of("€", "¥", "₩", "£", "$"))
        );
    }

    public void testStreamOperationsOnRandomMap() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        Map<Long, List<String>> collectedViaStreams = map.entrySet()
            .stream()
            .filter(e -> e.getKey() > 0)
            .sorted(Map.Entry.comparingByKey())
            .limit(limit)
            .collect(Collectors.groupingBy(e -> e.getKey() % 2, Collectors.mapping(Map.Entry::getValue, Collectors.toList())));

        Map<Long, String> sortedMap = new TreeMap<>();
        for (var entry : map.entrySet()) {
            if (entry.getKey() > 0) {
                sortedMap.put(entry.getKey(), entry.getValue());
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
        assertThat(ImmutableOpenMap.of().entrySet().stream().count(), equalTo(0L));
    }

    public void testKeySetStreamOperationsAreSupported() {
        assertThat(
            regionCurrencySymbols.keySet().stream().filter(e -> e.startsWith("U") == false).collect(Collectors.toSet()),
            equalTo(Set.of("Japan", "EU", "Korea"))
        );
    }

    public void testIntMapKeySet() {
        ImmutableOpenIntMap<String> map = ImmutableOpenIntMap.<String>builder().fPut(1, "foo").fPut(2, "bar").build();
        Set<Integer> expectedKeys = Set.of(1, 2);
        Set<Integer> actualKeys = map.keySet();
        assertThat(actualKeys.contains(1), is(true));
        assertThat(actualKeys.contains(2), is(true));
        assertThat(actualKeys, equalTo(expectedKeys));
        assertThat(expectedKeys, equalTo(actualKeys));
        assertThat(expectedKeys.stream().filter(actualKeys::contains).count(), equalTo(2L));
        assertThat(actualKeys.stream().filter(expectedKeys::contains).count(), equalTo(2L));
    }

    public void testSortedKeysSet() {
        assertThat(regionCurrencySymbols.keySet(), equalTo(Set.of("EU", "Japan", "Korea", "UK", "USA")));
    }

    public void testStreamOperationsOnRandomMapKeys() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        List<Long> collectedViaStream = map.keySet().stream().filter(e -> e > 0).sorted().limit(limit).toList();

        SortedSet<Long> positiveNumbers = new TreeSet<>();
        for (var key : map.keySet()) {
            if (key > 0) {
                positiveNumbers.add(key);
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

    public void testEntrySet() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        ImmutableOpenMap.Builder<Long, String> builder1 = ImmutableOpenMap.builder(map.size());
        map.entrySet().forEach(entry -> builder1.put(entry.getKey(), entry.getValue()));

        ImmutableOpenMap.Builder<Long, String> builder2 = ImmutableOpenMap.builder(map.size());
        map.entrySet().stream().forEach(entry -> builder2.put(entry.getKey(), entry.getValue()));

        Map<Long, String> hMap = Maps.newMapWithExpectedSize(map.size());
        map.entrySet().forEach(entry -> hMap.put(entry.getKey(), entry.getValue()));

        ImmutableOpenMap.Builder<Long, String> builder3 = ImmutableOpenMap.builder(map.size());
        builder3.putAllFromMap(hMap);

        assertThat("forEach should match", map, equalTo(builder1.build()));
        assertThat("forEach on a stream should match", map, equalTo(builder2.build()));
        assertThat("hashmap should match", map, equalTo(builder3.build()));
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
        Integer[] populations = countryPopulations.values().toArray(Integer[]::new);

        assertThat(populations, arrayContainingInAnyOrder(37_846_611, 46_754_778, 60_461_826, 65_273_511, 83_783_942));
    }

    public void testStreamOperationOnValues() {
        assertThat(
            countryPopulations.values().stream().filter(e -> e > 60_000_000).sorted(Comparator.reverseOrder()).limit(2).toList(),
            equalTo(List.of(83_783_942, 65_273_511))
        );
    }

    public void testStreamOperationsOnRandomMapValues() {
        ImmutableOpenMap<Long, String> map = randomImmutableOpenMap();

        int limit = randomIntBetween(0, map.size());
        List<String> collectedViaStream = map.values()
            .stream()
            .filter(Predicate.not(e -> e.contains("ab") || e.contains("cd") || e.contains("ef")))
            .sorted()
            .limit(limit)
            .toList();

        SortedSet<String> filteredSortedStrings = new TreeSet<>();
        for (var value : map.values()) {
            if ((value.contains("ab") || value.contains("cd") || value.contains("ef")) == false) {
                filteredSortedStrings.add(value);
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

    public void testEntrySetContains() {
        ImmutableOpenMap<String, Integer> map;

        map = ImmutableOpenMap.<String, Integer>builder().fPut("foo", 1).build();
        assertTrue(map.containsKey("foo"));
        assertTrue(map.entrySet().contains(entry("foo", 1)));
        assertFalse(map.entrySet().contains(entry("foo", 17)));

        // Try with a null value
        map = ImmutableOpenMap.<String, Integer>builder().fPut("foo", null).build();
        assertTrue(map.containsKey("foo"));
        assertTrue(map.entrySet().contains(entry("foo", null)));
        assertFalse(map.containsKey("bar"));
        assertFalse(map.entrySet().contains(entry("bar", null)));
    }

    public void testIntMapEntrySetContains() {
        ImmutableOpenIntMap<String> map;

        map = ImmutableOpenIntMap.<String>builder().fPut(1, "foo").build();
        assertTrue(map.containsKey(1));
        assertTrue(map.entrySet().contains(entry(1, "foo")));
        assertFalse(map.entrySet().contains(entry(1, "bar")));

        // Try with a null value
        map = ImmutableOpenIntMap.<String>builder().fPut(1, null).build();
        assertTrue(map.containsKey(1));
        assertTrue(map.entrySet().contains(entry(1, null)));
        assertFalse(map.containsKey(2));
        assertFalse(map.entrySet().contains(entry(2, null)));
    }

    public void testContainsValue() {
        assertTrue(countryPopulations.containsValue(37_846_611));
    }

    public void testIntMapContainsValue() {
        ImmutableOpenIntMap<String> map = ImmutableOpenIntMap.<String>builder().fPut(1, "foo").fPut(2, "bar").build();
        assertTrue(map.containsValue("bar"));
    }

    public void testBuilderUseAfterBuild() {
        ImmutableOpenMap.Builder<String, Integer> builder = ImmutableOpenMap.<String, Integer>builder().fPut("foo", 1);
        assertTrue(builder.build().get("foo") == 1);
        expectThrows(NullPointerException.class, () -> builder.build());
        expectThrows(NullPointerException.class, () -> builder.put("bar", 2));
    }

    public void testBuilderNoopReferencesUnchanged() {
        ImmutableOpenMap<String, Integer> map = ImmutableOpenMap.<String, Integer>builder().fPut("foo", 1).build();
        assertTrue(map == ImmutableOpenMap.builder(map).build());
        assertFalse(map == ImmutableOpenMap.builder(map).fPut("bar", 2).build());
    }

    private static <KType, VType> Map.Entry<KType, VType> entry(KType key, VType value) {
        Map<KType, VType> map = Maps.newMapWithExpectedSize(1);
        map.put(key, value);
        return map.entrySet().iterator().next();
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
