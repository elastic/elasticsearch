/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.fielddata;

import org.elasticsearch.common.FieldMemoryStats;
import org.elasticsearch.common.FieldMemoryStatsTests;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FieldDataStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        FieldMemoryStats map = randomBoolean() ? null : FieldMemoryStatsTests.randomFieldMemoryStats();
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> fieldOrdinalStats = new HashMap<>();
        fieldOrdinalStats.put(
            randomAlphaOfLength(4),
            new FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats(randomNonNegativeLong(), randomNonNegativeLong())
        );
        FieldDataStats.GlobalOrdinalsStats glob = new FieldDataStats.GlobalOrdinalsStats(randomNonNegativeLong(), fieldOrdinalStats);
        FieldDataStats stats = new FieldDataStats(randomNonNegativeLong(), randomNonNegativeLong(), map, glob);
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldDataStats read = new FieldDataStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getEvictions(), read.getEvictions());
        assertEquals(stats.getMemorySize(), read.getMemorySize());
        assertEquals(stats.getFields(), read.getFields());
    }

    public void testAdd() {
        FieldDataStats fieldDataStats = createInstance(1L, 1L, 1L, List.of());
        fieldDataStats.add(createInstance(1L, 1L, 1L, List.of()));
        assertEquals(fieldDataStats.getMemorySizeInBytes(), 2L);
        assertEquals(fieldDataStats.getEvictions(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getBuildTimeMillis(), 2L);
        assertNull(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats());

        fieldDataStats = createInstance(2L, 2L, 2L, List.of(Map.entry("field1", new long[] { 2L, 2L })));
        fieldDataStats.add(createInstance(2L, 2L, 2L, List.of(Map.entry("field1", new long[] { 2L, 2L }))));
        assertEquals(fieldDataStats.getMemorySizeInBytes(), 4L);
        assertEquals(fieldDataStats.getEvictions(), 4L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getBuildTimeMillis(), 4L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().size(), 1);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").valueCount(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").totalBuildingTime(), 4L);

        fieldDataStats = createInstance(2L, 2L, 2L, List.of(Map.entry("field1", new long[] { 2L, 2L })));
        fieldDataStats.add(
            createInstance(2L, 2L, 4L, List.of(Map.entry("field1", new long[] { 2L, 2L }), Map.entry("field2", new long[] { 2L, 2L })))
        );
        assertEquals(fieldDataStats.getMemorySizeInBytes(), 4L);
        assertEquals(fieldDataStats.getEvictions(), 4L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getBuildTimeMillis(), 6L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().size(), 2);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").valueCount(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").totalBuildingTime(), 4L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().size(), 2);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field2").valueCount(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field2").totalBuildingTime(), 2L);

        fieldDataStats = createInstance(0L, 0L, 0L, List.of());
        fieldDataStats.add(createInstance(2L, 2L, 2L, List.of(Map.entry("field1", new long[] { 2L, 2L }))));
        assertEquals(fieldDataStats.getMemorySizeInBytes(), 2L);
        assertEquals(fieldDataStats.getEvictions(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getBuildTimeMillis(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().size(), 1);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").valueCount(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").totalBuildingTime(), 2L);

        fieldDataStats = createInstance(2L, 2L, 2L, List.of(Map.entry("field1", new long[] { 2L, 2L })));
        fieldDataStats.add(createInstance(0L, 0L, 0L, List.of()));
        assertEquals(fieldDataStats.getMemorySizeInBytes(), 2L);
        assertEquals(fieldDataStats.getEvictions(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getBuildTimeMillis(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().size(), 1);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").valueCount(), 2L);
        assertEquals(fieldDataStats.getGlobalOrdinalsStats().getFieldGlobalOrdinalsStats().get("field1").totalBuildingTime(), 2L);
    }

    private static FieldDataStats createInstance(
        long memoryInSize,
        long evictions,
        long buildTime,
        List<Map.Entry<String, long[]>> entries
    ) {
        Map<String, FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats> f = entries.isEmpty() ? null : new HashMap<>();
        for (Map.Entry<String, long[]> entry : entries) {
            f.put(entry.getKey(), new FieldDataStats.GlobalOrdinalsStats.GlobalOrdinalFieldStats(entry.getValue()[0], entry.getValue()[1]));
        }
        return new FieldDataStats(memoryInSize, evictions, null, new FieldDataStats.GlobalOrdinalsStats(buildTime, f));
    }
}
