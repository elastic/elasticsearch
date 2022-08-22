/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class FieldMappingStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        FieldMappingStats stats = randomFieldMappingStats();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        FieldMappingStats read = new FieldMappingStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getTotalCount(), read.getTotalCount());
        assertEquals(stats.getTotalEstimatedOverhead(), read.getTotalEstimatedOverhead());
    }

    public void testEqualityAndHashCode() {
        FieldMappingStats stats = randomFieldMappingStats();
        assertEquals(stats, stats);
        assertEquals(stats.hashCode(), stats.hashCode());

        FieldMappingStats stats1 = new FieldMappingStats(1L, 2L);
        FieldMappingStats stats2 = new FieldMappingStats(3L, 5L);
        FieldMappingStats stats3 = new FieldMappingStats(3L, 5L);

        assertNotEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats2, stats3);
    }

    public void testAdd() {
        FieldMappingStats stats1 = new FieldMappingStats(1L, 2L);
        FieldMappingStats stats2 = new FieldMappingStats(2L, 3L);
        FieldMappingStats stats3 = new FieldMappingStats(3L, 5L);

        stats1.add(stats2);
        assertEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats3.hashCode());
    }

    private static FieldMappingStats randomFieldMappingStats() {
        long totalCount = randomIntBetween(1, 100);
        long estimatedOverhead = totalCount * 1024;
        return new FieldMappingStats(totalCount, estimatedOverhead);
    }
}
