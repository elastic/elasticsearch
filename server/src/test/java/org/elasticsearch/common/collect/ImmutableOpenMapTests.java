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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;

public class ImmutableOpenMapTests extends ESTestCase {

    ImmutableOpenMap<String, String> regionCurrencySymbols = ImmutableOpenMap.<String, String>builder()
        .fPut("Japan", "¥")
        .fPut("USA", "$")
        .fPut("EU", "€")
        .fPut("UK", "£")
        .fPut("Korea", "₩")
        .build();

    public void testStreamOperationsAreSupported() {
        assertThat(regionCurrencySymbols.stream().filter(e -> e.getKey().startsWith("U")).map(Map.Entry::getValue)
                .collect(Collectors.toSet()), equalTo(Set.of("£", "$")));
    }

    public void testSortedStream() {
        assertThat(regionCurrencySymbols.stream().sorted(Map.Entry.comparingByKey()).map(Map.Entry::getValue).collect(Collectors.toList()),
            equalTo(List.of("€", "¥", "₩", "£", "$")));
    }

    public void testStreamOperationsOnRandomMap() {
        ImmutableOpenMap<Long, String> map = Randomness.get().longs(randomIntBetween(1, 1000))
            .mapToObj(e -> Tuple.tuple(e, randomAlphaOfLength(8)))
            .collect(() -> ImmutableOpenMap.<Long, String>builder(), (builder, t) -> builder.fPut(t.v1(), t.v2()),
                ImmutableOpenMap.Builder::putAll)
            .build();

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
}
