/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.common;

import com.carrotsearch.hppc.ObjectLongHashMap;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

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
        ObjectLongHashMap<String> map1 = new ObjectLongHashMap<>();
        map1.put("bar", 1);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        ObjectLongHashMap<String> map2 = new ObjectLongHashMap<>();
        map2.put("foo", 2);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        ObjectLongHashMap<String> map3 = new ObjectLongHashMap<>();
        map3.put("foo", 2);
        map3.put("bar", 1);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);

        ObjectLongHashMap<String> map4 = new ObjectLongHashMap<>();
        map4.put("foo", 2);
        map4.put("bar", 1);
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
        ObjectLongHashMap<String> map1 = new ObjectLongHashMap<>();
        map1.put("bar", 1);
        FieldMemoryStats stats1 = new FieldMemoryStats(map1);
        ObjectLongHashMap<String> map2 = new ObjectLongHashMap<>();
        map2.put("foo", 2);
        FieldMemoryStats stats2 = new FieldMemoryStats(map2);

        ObjectLongHashMap<String> map3 = new ObjectLongHashMap<>();
        map3.put("bar", 1);
        FieldMemoryStats stats3 = new FieldMemoryStats(map3);
        stats3.add(stats1);

        ObjectLongHashMap<String> map4 = new ObjectLongHashMap<>();
        map4.put("foo", 2);
        map4.put("bar", 2);
        FieldMemoryStats stats4 = new FieldMemoryStats(map4);
        assertNotEquals(stats3, stats4);
        stats3.add(stats2);
        assertEquals(stats3, stats4);
    }

    public static FieldMemoryStats randomFieldMemoryStats() {
        ObjectLongHashMap<String> map = new ObjectLongHashMap<>();
        int keys = randomIntBetween(1, 1000);
        for (int i = 0; i < keys; i++) {
            map.put(randomRealisticUnicodeOfCodepointLengthBetween(1, 10), randomNonNegativeLong());
        }
        return new FieldMemoryStats(map);
    }
}
