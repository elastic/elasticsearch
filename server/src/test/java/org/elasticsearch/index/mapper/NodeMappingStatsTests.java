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

public class NodeMappingStatsTests extends ESTestCase {

    public void testSerialize() throws IOException {
        NodeMappingStats stats = randomNodeMappingStats();
        BytesStreamOutput out = new BytesStreamOutput();
        stats.writeTo(out);
        StreamInput input = out.bytes().streamInput();
        NodeMappingStats read = new NodeMappingStats(input);
        assertEquals(-1, input.read());
        assertEquals(stats.getTotalCount(), read.getTotalCount());
        assertEquals(stats.getTotalEstimatedOverhead(), read.getTotalEstimatedOverhead());
    }

    public void testEqualityAndHashCode() {
        NodeMappingStats stats = randomNodeMappingStats();
        assertEquals(stats, stats);
        assertEquals(stats.hashCode(), stats.hashCode());

        NodeMappingStats stats1 = new NodeMappingStats(1L, 2L);
        NodeMappingStats stats2 = new NodeMappingStats(3L, 5L);
        NodeMappingStats stats3 = new NodeMappingStats(3L, 5L);

        assertNotEquals(stats1, stats2);
        assertNotEquals(stats1, stats3);
        assertEquals(stats2, stats3);
    }

    public void testAdd() {
        NodeMappingStats stats1 = new NodeMappingStats(1L, 2L);
        NodeMappingStats stats2 = new NodeMappingStats(2L, 3L);
        NodeMappingStats stats3 = new NodeMappingStats(3L, 5L);

        stats1.add(stats2);
        assertEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats3.hashCode());
    }

    private static NodeMappingStats randomNodeMappingStats() {
        long totalCount = randomIntBetween(1, 100);
        long estimatedOverhead = totalCount * 1024;
        return new NodeMappingStats(totalCount, estimatedOverhead);
    }
}
