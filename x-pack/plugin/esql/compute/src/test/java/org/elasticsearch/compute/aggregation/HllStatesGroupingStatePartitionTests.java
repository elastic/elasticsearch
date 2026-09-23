/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction.PartitionedState;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.operator.DriverContext;
import org.elasticsearch.compute.test.ComputeTestCase;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests the partition splitter/state round-trip for {@link HllStates.GroupingState}.
 * Analogous to {@link BytesRefArrayStatePartitionTests} for {@link BytesRefArrayState}.
 */
public class HllStatesGroupingStatePartitionTests extends ComputeTestCase {

    public void testRoundTrip() {
        BlockFactory blockFactory = blockFactory();
        var driverContext = new DriverContext(blockFactory.bigArrays(), blockFactory, null);
        var partitionBreaker = new NoopCircuitBreaker("partition");

        int numGroups = between(1, 50_000);

        try (var state = new HllStates.GroupingState(driverContext, 40_000)) {
            long[] expectedCardinalities = new long[numGroups];
            for (int g = 0; g < numGroups; g++) {
                int numValues = between(0, 32);
                for (int v = 0; v < numValues; v++) {
                    state.collect(g, randomLong());
                }
                expectedCardinalities[g] = state.cardinality(g);
            }

            var splitter = state.createPartitioningSplitter(partitionBreaker);
            int batchTotal = NUM_PARTITIONS * PARTITION_WRITE_BATCH;
            int[] cumulativeCounts = new int[NUM_PARTITIONS];

            for (int batchStart = 0; batchStart < numGroups; batchStart += batchTotal) {
                int batchEnd = Math.min(batchStart + batchTotal, numGroups);
                int batchSize = batchEnd - batchStart;

                int[] batchPartitionCounts = new int[NUM_PARTITIONS];
                short[] shiftedIds = new short[NUM_PARTITIONS * PARTITION_WRITE_BATCH];

                for (int i = 0; i < batchSize; i++) {
                    int p = (batchStart + i) % NUM_PARTITIONS;
                    shiftedIds[p * PARTITION_WRITE_BATCH + batchPartitionCounts[p]] = (short) i;
                    batchPartitionCounts[p]++;
                }

                splitter.split(batchStart, shiftedIds, batchSize, batchPartitionCounts, cumulativeCounts.clone());

                for (int p = 0; p < NUM_PARTITIONS; p++) {
                    cumulativeCounts[p] += batchPartitionCounts[p];
                }
            }

            PartitionedState partitioned = splitter.finish();

            BytesRef scratch = new BytesRef();
            for (int p = 0; p < NUM_PARTITIONS; p++) {
                // HLL has no null groups, so seen is always null
                assertThat("seen must be null for HLL", state.partitionSeen(partitioned, p), nullValue());
                assertThat("hasAllValues must be true for HLL", partitioned.hasAllValues(p), equalTo(true));

                BytesRefSequence values = state.partitionValues(partitioned, p);

                // Groups assigned to partition p: p, p + NUM_PARTITIONS, p + 2*NUM_PARTITIONS, ...
                int k = 0;
                for (int g = p; g < numGroups; g += NUM_PARTITIONS) {
                    // Merge the serialized sketch into a fresh single-group state and verify cardinality matches
                    try (var check = new HllStates.GroupingState(driverContext, 40_000)) {
                        check.merge(0, values.get(k, scratch), 0);
                        assertThat(
                            "cardinality mismatch for group " + g + " in partition " + p + " slot " + k,
                            check.cardinality(0),
                            equalTo(expectedCardinalities[g])
                        );
                    }
                    k++;
                }

                partitioned.releasePartition(partitionBreaker, p);
            }
        }
    }
}
