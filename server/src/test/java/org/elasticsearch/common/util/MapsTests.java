/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.util;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toMap;

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
}
