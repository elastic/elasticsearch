/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;
import static org.hamcrest.Matchers.equalTo;


public class MapsTests extends ESTestCase {

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

    private static <K, V> Map<K, V> randomMap(int size, Supplier<K> keyGenerator, Supplier<V> valueGenerator) {
        final Map<K, V> map = new HashMap<>();
        IntStream.range(0, size).forEach(i -> map.put(keyGenerator.get(), valueGenerator.get()));
        return map;
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
}
