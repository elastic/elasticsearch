/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.operator;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.aggregation.blockhash.BlockHash;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.ElementType;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link PartitionedHashAggregationOperator#canPartition},
 * {@link AbstractPartitionedHashAggregationOperator#sortPositionsByPartition}, and
 * {@link DriverContext#withBlockFactory}.
 */
public class PartitionedHashAggregationOperatorCanPartitionAndSortTests extends ESTestCase {

    // ---- canPartition ----

    public void testCanPartitionSingleColumnSupportedTypes() {
        for (ElementType type : List.of(
            ElementType.LONG,
            ElementType.INT,
            ElementType.DOUBLE,
            ElementType.BYTES_REF,
            ElementType.BOOLEAN
        )) {
            assertTrue(
                "single-column " + type + " should be partitionable",
                PartitionedHashAggregationOperator.canPartition(List.of(new BlockHash.GroupSpec(0, type)))
            );
        }
    }

    public void testCanPartitionMultiColumnAllSupported() {
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, ElementType.INT),
            new BlockHash.GroupSpec(2, ElementType.DOUBLE),
            new BlockHash.GroupSpec(3, ElementType.BYTES_REF),
            new BlockHash.GroupSpec(4, ElementType.BOOLEAN)
        );
        assertTrue(PartitionedHashAggregationOperator.canPartition(specs));
    }

    public void testCanPartitionMultiColumnUnsupportedType() {
        List<BlockHash.GroupSpec> specs = List.of(
            new BlockHash.GroupSpec(0, ElementType.LONG),
            new BlockHash.GroupSpec(1, ElementType.NULL)
        );
        assertFalse(PartitionedHashAggregationOperator.canPartition(specs));
    }

    public void testCanPartitionSingleColumnUnsupportedType() {
        // size <= 1 short-circuits to true regardless of type
        assertTrue(PartitionedHashAggregationOperator.canPartition(List.of(new BlockHash.GroupSpec(0, ElementType.NULL))));
    }

    public void testCanPartitionEmptyListReturnTrue() {
        assertTrue(PartitionedHashAggregationOperator.canPartition(List.of()));
    }

    // ---- sortPositionsByPartition ----

    public void testSortPositionsByPartitionAllInOnePartition() {
        int[] partitionOf = { 0, 0, 0, 0 };
        int[] counts = { 4, 0 };
        AbstractPartitionedHashAggregationOperator.BucketSort result = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            2
        );

        assertThat(result.sortedPositions(), equalTo(new int[] { 0, 1, 2, 3 }));
        assertThat(result.offsets()[0], equalTo(0));
        assertThat(result.offsets()[1], equalTo(4));
        assertThat(result.offsets()[2], equalTo(4));
    }

    public void testSortPositionsByPartitionUniformDistribution() {
        // rows 0,2 → partition 0; rows 1,3 → partition 1
        int[] partitionOf = { 0, 1, 0, 1 };
        int[] counts = { 2, 2 };
        AbstractPartitionedHashAggregationOperator.BucketSort result = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            2
        );

        int[] sorted = result.sortedPositions();
        assertThat(sorted.length, equalTo(4));
        // partition 0 slice: rows 0 and 2, in original order
        int p0Start = result.offsets()[0];
        assertThat(sorted[p0Start], equalTo(0));
        assertThat(sorted[p0Start + 1], equalTo(2));
        // partition 1 slice: rows 1 and 3, in original order
        int p1Start = result.offsets()[1];
        assertThat(sorted[p1Start], equalTo(1));
        assertThat(sorted[p1Start + 1], equalTo(3));
    }

    public void testSortPositionsByPartitionSingleRow() {
        int[] partitionOf = { 0 };
        int[] counts = { 1 };
        AbstractPartitionedHashAggregationOperator.BucketSort result = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            1
        );

        assertThat(result.sortedPositions(), equalTo(new int[] { 0 }));
        assertThat(result.offsets()[0], equalTo(0));
        assertThat(result.offsets()[1], equalTo(1));
    }

    public void testSortPositionsByPartitionStableSort() {
        // rows assigned to 3 partitions in a mixed pattern; within each partition, original row order must be preserved
        int[] partitionOf = { 2, 0, 1, 0, 2, 1 };
        int[] counts = { 2, 2, 2 };
        AbstractPartitionedHashAggregationOperator.BucketSort result = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            3
        );

        int[] sorted = result.sortedPositions();
        // partition 0: rows 1, 3 (in that order)
        int p0 = result.offsets()[0];
        assertThat(sorted[p0], equalTo(1));
        assertThat(sorted[p0 + 1], equalTo(3));
        // partition 1: rows 2, 5 (in that order)
        int p1 = result.offsets()[1];
        assertThat(sorted[p1], equalTo(2));
        assertThat(sorted[p1 + 1], equalTo(5));
        // partition 2: rows 0, 4 (in that order)
        int p2 = result.offsets()[2];
        assertThat(sorted[p2], equalTo(0));
        assertThat(sorted[p2 + 1], equalTo(4));
    }

    public void testSortPositionsByPartitionOffsetsCorrect() {
        int nPartitions = 4;
        int[] counts = { 3, 1, 0, 5 };
        int[] partitionOf = new int[9];
        // 3 rows for p0, 1 for p1, 0 for p2, 5 for p3
        partitionOf[0] = 0;
        partitionOf[1] = 0;
        partitionOf[2] = 0;
        partitionOf[3] = 1;
        partitionOf[4] = 3;
        partitionOf[5] = 3;
        partitionOf[6] = 3;
        partitionOf[7] = 3;
        partitionOf[8] = 3;
        AbstractPartitionedHashAggregationOperator.BucketSort result = AbstractPartitionedHashAggregationOperator.sortPositionsByPartition(
            partitionOf,
            counts,
            nPartitions
        );

        int[] offsets = result.offsets();
        assertThat(offsets.length, equalTo(nPartitions + 1));
        for (int p = 0; p < nPartitions; p++) {
            assertThat(
                "offsets[" + (p + 1) + "] - offsets[" + p + "] must equal counts[" + p + "]",
                offsets[p + 1] - offsets[p],
                equalTo(counts[p])
            );
        }
        // All rows appear exactly once
        int[] allRows = Arrays.copyOf(result.sortedPositions(), result.sortedPositions().length);
        Arrays.sort(allRows);
        for (int i = 0; i < allRows.length; i++) {
            assertThat(allRows[i], equalTo(i));
        }
    }

    // ---- DriverContext.withBlockFactory ----

    public void testWithBlockFactorySharedBigArrays() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.bigArrays(), sameInstance(parent.bigArrays()));
    }

    public void testWithBlockFactoryUsesNewBlockFactory() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.blockFactory(), sameInstance(childFactory));
        assertThat(child.blockFactory(), not(sameInstance(parent.blockFactory())));
    }

    public void testWithBlockFactorySharedWarningsMode() {
        BlockFactory parentFactory = new BlockFactory(new NoopCircuitBreaker("parent"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext parent = new DriverContext(BigArrays.NON_RECYCLING_INSTANCE, parentFactory, null);
        BlockFactory childFactory = new BlockFactory(new NoopCircuitBreaker("child"), BigArrays.NON_RECYCLING_INSTANCE);
        DriverContext child = parent.withBlockFactory(childFactory);
        assertThat(child.warningsMode(), equalTo(parent.warningsMode()));
    }
}
