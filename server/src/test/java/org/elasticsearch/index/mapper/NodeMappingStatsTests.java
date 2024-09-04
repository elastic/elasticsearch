/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.index.mapper;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

public class NodeMappingStatsTests extends AbstractWireSerializingTestCase<NodeMappingStats> {

    @Override
    protected Writeable.Reader<NodeMappingStats> instanceReader() {
        return NodeMappingStats::new;
    }

    @Override
    protected NodeMappingStats createTestInstance() {
        return new NodeMappingStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
    }

    @Override
    protected NodeMappingStats mutateInstance(NodeMappingStats in) throws IOException {
        return switch (between(0, 3)) {
            case 0 -> new NodeMappingStats(
                randomValueOtherThan(in.getTotalCount(), ESTestCase::randomNonNegativeLong),
                in.getTotalEstimatedOverhead().getBytes(),
                in.getTotalSegments(),
                in.getTotalSegmentFields()
            );
            case 1 -> new NodeMappingStats(
                in.getTotalCount(),
                randomValueOtherThan(in.getTotalCount(), ESTestCase::randomNonNegativeLong),
                in.getTotalSegments(),
                in.getTotalSegmentFields()
            );
            case 2 -> new NodeMappingStats(
                in.getTotalCount(),
                in.getTotalEstimatedOverhead().getBytes(),
                randomValueOtherThan(in.getTotalSegments(), ESTestCase::randomNonNegativeLong),
                in.getTotalSegmentFields()
            );
            case 3 -> new NodeMappingStats(
                in.getTotalCount(),
                in.getTotalEstimatedOverhead().getBytes(),
                in.getTotalSegments(),
                randomValueOtherThan(in.getTotalSegmentFields(), ESTestCase::randomNonNegativeLong)
            );
            default -> throw new AssertionError("invalid option");
        };
    }

    public void testAdd() {
        NodeMappingStats stats1 = new NodeMappingStats(1L, 2L, 4L, 6L);
        NodeMappingStats stats2 = new NodeMappingStats(2L, 3L, 10L, 20L);
        NodeMappingStats stats3 = new NodeMappingStats(3L, 5L, 14L, 26L);

        stats1.add(stats2);
        assertEquals(stats1, stats3);
        assertEquals(stats1.hashCode(), stats3.hashCode());
    }
}
