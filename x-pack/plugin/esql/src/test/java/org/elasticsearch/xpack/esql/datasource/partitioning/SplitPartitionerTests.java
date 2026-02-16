/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.partitioning;

import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.datasource.DataSourcePartition;
import org.elasticsearch.xpack.esql.datasource.DataSourcePlan;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.hasSize;

/**
 * Tests for {@link SplitPartitioner}.
 */
public class SplitPartitionerTests extends ESTestCase {

    public void testEmptyDiscoveryReturnsEmpty() {
        var partitioner = new SplitPartitioner<TestSplit>(plan -> List.of(), (plan, splits) -> stubPartition(splits.size()));
        List<DataSourcePartition> partitions = partitioner.planPartitions(null, new DistributionHints(4));
        assertThat(partitions, hasSize(0));
    }

    public void testDiscoverGroupWrapPipeline() {
        var splits = List.of(new TestSplit("a", OptionalLong.of(100)), new TestSplit("b", OptionalLong.of(200)));
        var partitioner = new SplitPartitioner<TestSplit>(plan -> splits, (plan, group) -> stubPartition(group.size()));
        List<DataSourcePartition> partitions = partitioner.planPartitions(null, new DistributionHints(4));
        // 2 splits, 4 targets → each split gets its own partition
        assertThat(partitions, hasSize(2));
    }

    public void testCustomGroupingFunctionIsCalled() {
        var splits = List.of(
            new TestSplit("a", OptionalLong.of(100)),
            new TestSplit("b", OptionalLong.of(100)),
            new TestSplit("c", OptionalLong.of(100)),
            new TestSplit("d", OptionalLong.of(100))
        );

        AtomicBoolean customGroupCalled = new AtomicBoolean(false);

        // Custom grouping: always return everything in one group
        var partitioner = new SplitPartitioner<TestSplit>(plan -> splits, (s, hints) -> {
            customGroupCalled.set(true);
            return List.of(s); // all in one group
        }, (plan, group) -> stubPartition(group.size()));

        List<DataSourcePartition> partitions = partitioner.planPartitions(null, new DistributionHints(4));
        assertTrue("Custom grouping should have been called", customGroupCalled.get());
        assertThat(partitions, hasSize(1));
        assertEquals(4, ((StubPartition) partitions.get(0)).splitCount);
    }

    // =========================================================================
    // TEST HELPERS
    // =========================================================================

    record TestSplit(String name, OptionalLong estimatedBytes) implements DataSourceSplit {}

    private static StubPartition stubPartition(int splitCount) {
        return new StubPartition(null, splitCount);
    }

    /** Minimal DataSourcePartition stub that tracks split count. */
    record StubPartition(DataSourcePlan plan, int splitCount) implements DataSourcePartition {
        @Override
        public String getWriteableName() {
            return "test";
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {}
    }
}
