/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class FieldMemoryStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats stats = randomFieldMemoryStats();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldMemoryStats read = new FieldMemoryStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats, read);
    }

    public void testHashCodeEquals() {
        FieldMemoryStats stats = randomFieldMemoryStats();
        assertEquals(stats, stats);
        assertEquals(stats.hashCode(), stats.hashCode());
        Map<String, Long> map1 = new HashMap<>();
        map1.put("bar", 1L);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        Map<String, Long> map2 = new HashMap<>();
        map2.put("foo", 2L);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        Map<String, Long> map3 = new HashMap<>();
        map3.put("foo", 2L);
        map3.put("bar", 1L);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);

        Map<String, Long> map4 = new HashMap<>();
        map4.put("foo", 2L);
        map4.put("bar", 1L);
        FieldMemoryStats stats4 = new FieldMemoryStats(map4);

        assertNotEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertNotEquals(stats2, stats3);
        assertEquals(stats4, stats3);

        stats1.add(stats2);
        assertEquals(stats1, stats3);
        assertEquals(stats1, stats4);
        assertEquals(stats1.hashCode(), stats3.hashCode());
    }

    public void testAdd() {
        Map<String, Long> map1 = new HashMap<>();
        map1.put("bar", 1L);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        Map<String, Long> map2 = new HashMap<>();
        map2.put("foo", 2L);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        Map<String, Long> map3 = new HashMap<>();
        map3.put("bar", 1L);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);
        stats3.add(stats1);

        Map<String, Long> map4 = new HashMap<>();
        map4.put("foo", 2L);
        map4.put("bar", 2L);
        FieldMemoryStats stats4 = new FieldMemoryStats(map4);
        assertNotEquals(stats3, stats4);
        stats3.add(stats2);
        assertEquals(stats3, stats4);
    }

    public static FieldMemoryStats randomFieldMemoryStats() {
        Map<String, Long> map = new HashMap<>();
        int keys = randomIntBetween(1, 1000);
        for (int i = 0; i < keys; i++) {
            map.put(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), randomNonNegativeLong());
        }
        return new FieldMemoryStats(map);
    }
}
