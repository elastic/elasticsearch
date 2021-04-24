/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Map.entry;
import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class MapsTests extends ESTestCase {

    public void testConcatenateEntryToImmutableMap() {
        final var numberOfEntries = randomIntBetween(0, 32);
        final var entries = new ArrayList<Map.Entry<String, String>>(numberOfEntries);
        final var keys = new HashSet<String>();
        for (int i = 0; i < numberOfEntries; i++) {
            final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
            keys.add(key);
            entries.add(entry(key, randomAlphaOfLength(16)));
        }
        final Map<String, String> map = Maps.ofEntries(entries);
        final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
        final String value = randomAlphaOfLength(16);
        final Map<String, String> concatenation = Maps.copyMapWithAddedEntry(map, key, value);
        assertMapEntriesAndImmutability(
                concatenation,
                Stream.concat(entries.stream(), Stream.of(entry(key, value))).collect(Collectors.toUnmodifiableList()));
    }

    public void testAddEntryInImmutableMap() {
        final var numberOfEntries = randomIntBetween(0, 32);
        final var entries = new ArrayList<Map.Entry<String, String>>(numberOfEntries);
        final var keys = new HashSet<String>();
        for (int i = 0; i < numberOfEntries; i++) {
            final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
            keys.add(key);
            entries.add(entry(key, randomAlphaOfLength(16)));
        }
        final Map<String, String> map = Maps.ofEntries(entries);
        final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
        final String value = randomAlphaOfLength(16);
        final Map<String, String> add = Maps.copyMapWithAddedOrReplacedEntry(map, key, value);
        assertMapEntriesAndImmutability(
                add,
                Stream.concat(entries.stream(), Stream.of(entry(key, value))).collect(Collectors.toUnmodifiableList()));
    }

    public void testReplaceEntryInImmutableMap() {
        final var numberOfEntries = randomIntBetween(1, 32);
        final var entries = new ArrayList<Map.Entry<String, String>>(numberOfEntries);
        final var keys = new HashSet<String>();
        for (int i = 0; i < numberOfEntries; i++) {
            final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
            keys.add(key);
            entries.add(entry(key, randomAlphaOfLength(16)));
        }
        final Map<String, String> map = Maps.ofEntries(entries);
        final String key = randomFrom(map.keySet());
        final String value = randomAlphaOfLength(16);
        final Map<String, String> replaced = Maps.copyMapWithAddedOrReplacedEntry(map, key, value);
        assertMapEntriesAndImmutability(
                replaced,
                Stream.concat(
                        entries.stream().filter(e -> key.equals(e.getKey()) == false),
                        Stream.of(entry(key, value))).collect(Collectors.toUnmodifiableList()));
    }

    public void testOfEntries() {
        final var numberOfEntries = randomIntBetween(0, 32);
        final var entries = new ArrayList<Map.Entry<String, String>>(numberOfEntries);
        final var keys = new HashSet<String>();
        for (int i = 0; i < numberOfEntries; i++) {
            final String key = randomValueOtherThanMany(keys::contains, () -> randomAlphaOfLength(16));
            keys.add(key);
            entries.add(entry(key, randomAlphaOfLength(16)));
        }
        final Map<String, String> map = Maps.ofEntries(entries);
        assertMapEntriesAndImmutability(map, entries);
    }

    public void testDeepEquals() {
        final Supplier<String> keyGenerator = () -> randomAlphaOfLengthBetween(1, 5);
        final Supplier<int[]> arrayValueGenerator = () -> random().ints(randomInt(5)).toArray();
        final Map<String, int[]> map = randomMap(randomInt(5), keyGenerator, arrayValueGenerator);
        final Map<String, int[]> mapCopy = map.entrySet().stream()
                .collect(toMap(Map.Entry::getKey, e -> Arrays.copyOf(e.getValue(), e.getValue().length)));

        assertTrue(Maps.deepEquals(map, mapCopy));

        final Map<String, int[]> mapModified = mapCopy;
        if (mapModified.isEmpty()) {
            mapModified.put(keyGenerator.get(), arrayValueGenerator.get());
        } else {
            if (randomBoolean()) {
                final String randomKey = mapModified.keySet().toArray(new String[0])[randomInt(mapModified.size() - 1)];
                final int[] value = mapModified.get(randomKey);
                mapModified.put(randomKey, randomValueOtherThanMany((v) -> Arrays.equals(v, value), arrayValueGenerator));
            } else {
                mapModified.put(randomValueOtherThanMany(mapModified::containsKey, keyGenerator), arrayValueGenerator.get());
            }
        }

        assertFalse(Maps.deepEquals(map, mapModified));
    }

    public void testFlatten() {
        Map<String, Object> map = randomNestedMap(10);
        Map<String, Object> flatten = Maps.flatten(map, true, true);
        assertThat(flatten.size(), equalTo(deepCount(map.values())));
        for (Map.Entry<String, Object> entry : flatten.entrySet()) {
            assertThat(entry.getKey(), entry.getValue(), equalTo(deepGet(entry.getKey(), map)));
        }
    }

    @SuppressWarnings("unchecked")
    private static Object deepGet(String path, Object obj) {
        Object cur = obj;
        String[] keys = path.split("\\.");
        for (String key : keys) {
            if (Character.isDigit(key.charAt(0))) {
                List<Object> list = (List<Object>) cur;
                cur = list.get(Integer.parseInt(key));
             } else {
                Map<String, Object> map = (Map<String, Object>) cur;
                cur = map.get(key);
            }
        }
        return cur;
    }

    @SuppressWarnings("unchecked")
    private int deepCount(Collection<Object> map) {
        int sum = 0;
        for (Object val : map) {
            if (val instanceof Map) {
                sum += deepCount(((Map<String, Object>) val).values());
            } else if (val instanceof List) {
                sum += deepCount((List<Object>) val);
            } else {
                sum ++;
            }
        }
        return sum;
    }

    private Map<String, Object> randomNestedMap(int level) {
        final Supplier<String> keyGenerator = () -> randomAlphaOfLengthBetween(1, 5);
        final Supplier<Object> arrayValueGenerator = () -> random().ints(randomInt(5))
            .boxed()
            .map(s -> (Object) s)
            .collect(Collectors.toList());

        final Supplier<Object> mapSupplier;
        if (level > 0) {
            mapSupplier = () -> randomNestedMap(level - 1);
        } else {
            mapSupplier = ESTestCase::randomLong;
        }
        final Supplier<Supplier<Object>> valueSupplier = () -> randomFrom(
            ESTestCase::randomBoolean,
            ESTestCase::randomDouble,
            ESTestCase::randomLong,
            arrayValueGenerator,
            mapSupplier
        );
        final Supplier<Object> valueGenerator = () -> valueSupplier.get().get();
        return randomMap(randomInt(5), keyGenerator, valueGenerator);
    }

    private void assertMapEntries(final Map<String, String> map, final Collection<Map.Entry<String, String>> entries) {
        for (var entry : entries) {
            assertThat("map [" + map + "] does not contain key [" + entry.getKey() + "]", map.keySet(), hasItem(entry.getKey()));
            assertThat(
                    "key [" + entry.getKey() + "] should be mapped to value [" + entry.getValue() + "]",
                    map.get(entry.getKey()),
                    equalTo(entry.getValue()));
        }
    }

    private void assertMapImmutability(final Map<String, String> map) {
        final String nonPresentKey = randomValueOtherThanMany(map.keySet()::contains, () -> randomAlphaOfLength(16));
        assertUnsupported("putting a map entry should be unsupported", () -> map.put(nonPresentKey, randomAlphaOfLength(16)));
        assertUnsupported("putting map entries should be unsupported", () -> map.putAll(Map.of(nonPresentKey, randomAlphaOfLength(16))));
        assertUnsupported(
            "computing a new map association should be unsupported",
            () -> map.compute(nonPresentKey, (k, v) -> randomAlphaOfLength(16)));
        assertUnsupported(
            "computing a new map association should be unsupported",
            () -> map.computeIfAbsent(nonPresentKey, k -> randomAlphaOfLength(16)));
        assertUnsupported("map merge should be unsupported", () -> map.merge(nonPresentKey, randomAlphaOfLength(16), (k, v) -> v));
        if (map.isEmpty()) {
            return;
        }

        final String presentKey = randomFrom(map.keySet());
        final String valueNotAssociatedWithPresentKey = randomValueOtherThan(map.get(presentKey), () -> randomAlphaOfLength(16));
        assertUnsupported("clearing map should be unsupported", map::clear);
        assertUnsupported("replacing a map entry should be unsupported", () -> map.replace(presentKey, valueNotAssociatedWithPresentKey));
        assertUnsupported(
                "replacing a map entry should be unsupported",
                () -> map.replace(presentKey, map.get(presentKey), valueNotAssociatedWithPresentKey));
        assertUnsupported("replacing map entries should be unsupported", () -> map.replaceAll((k, v) -> v + v));
        assertUnsupported("removing a map entry should be unsupported", () -> map.remove(presentKey));
        assertUnsupported("removing a map entry should be unsupported", () -> map.remove(presentKey, map.get(presentKey)));
        assertUnsupported(
                "computing a new map association should be unsupported",
                () -> map.compute(presentKey, (k, v) -> randomBoolean() ? null : v + v));
        assertUnsupported(
                "computing a new map association should be unsupported",
                () -> map.computeIfPresent(presentKey, (k, v) -> randomBoolean() ? null : v + v));
        assertUnsupported(
                "map merge should be unsupported",
                () -> map.merge(presentKey, map.get(presentKey), (k, v) -> randomBoolean() ? null : v + v));
    }

    private void assertUnsupported(final String message, final ThrowingRunnable runnable) {
        expectThrows(UnsupportedOperationException.class, message, runnable);
    }

    private void assertMapEntriesAndImmutability(
            final Map<String, String> map,
            final Collection<Map.Entry<String, String>> entries) {
        assertMapEntries(map, entries);
        assertMapImmutability(map);
    }

    private static <K, V> Map<K, V> randomMap(int size, Supplier<K> keyGenerator, Supplier<V> valueGenerator) {
        final Map<K, V> map = new HashMap<>();
        IntStream.range(0, size).forEach(i -> map.put(keyGenerator.get(), valueGenerator.get()));
        return map;
    }

}
