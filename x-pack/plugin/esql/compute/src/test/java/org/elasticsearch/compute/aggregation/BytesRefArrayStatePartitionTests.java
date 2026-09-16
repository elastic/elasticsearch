/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.aggregation;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.MockBigArrays;
import org.elasticsearch.common.util.PageCacheRecycler;
import org.elasticsearch.compute.aggregation.GroupingAggregatorFunction.PartitionedState;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.elasticsearch.common.util.PartitionedHashTable.NUM_PARTITIONS;
import static org.elasticsearch.common.util.PartitionedHashTable.PARTITION_WRITE_BATCH;
import static org.hamcrest.Matchers.equalTo;

public class BytesRefArrayStatePartitionTests extends ESTestCase {

    public void testFlatRoundTrip() {
        runTest(BytesRefArrayState.PAGED_PARTITION_THRESHOLD_BYTES);
    }

    public void testPagedRoundTrip() {
        runTest(0L);
    }

    private void runTest(long pagedThreshold) {
        var bigArrays = new MockBigArrays(PageCacheRecycler.NON_RECYCLING_INSTANCE, ByteSizeValue.ofMb(200)).withCircuitBreaking();
        var breaker = bigArrays.breakerService().getBreaker(CircuitBreaker.REQUEST);
        var partitionBreaker = new NoopCircuitBreaker("partition");

        int numGroups = between(1, 50000);
        boolean withNulls = randomBoolean();
        BytesRef[] expected = new BytesRef[numGroups];

        var state = new BytesRefArrayState(bigArrays, breaker, "test", pagedThreshold);
        try {
            if (withNulls) {
                state.enableGroupIdTracking(new SeenGroupIds.Empty());
            }

            for (int g = 0; g < numGroups; g++) {
                if (withNulls == false || randomBoolean()) {
                    byte[] bytes = randomByteArrayOfLength(randomIntBetween(0, 32));
                    expected[g] = new BytesRef(Arrays.copyOf(bytes, bytes.length));
                    state.set(g, expected[g]);
                }
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

            for (int p = 0; p < NUM_PARTITIONS; p++) {
                BytesRef[] values = state.partitionValues(partitioned, p);
                boolean[] seen = state.partitionSeen(partitioned, p);

                assertThat("seen==null iff no null-tracking", seen == null, equalTo(withNulls == false));

                int k = 0;
                for (int g = p; g < numGroups; g += NUM_PARTITIONS) {
                    BytesRef expectedValue = expected[g];
                    if (withNulls == false) {
                        assertNotNull("group " + g + " should have a value", expectedValue);
                        assertEquals("group " + g, expectedValue, values[k]);
                    } else {
                        if (expectedValue != null) {
                            assertTrue("seen[" + k + "] for group " + g, seen[k]);
                            assertEquals("group " + g, expectedValue, values[k]);
                        } else {
                            assertFalse("seen[" + k + "] for group " + g, seen[k]);
                        }
                    }
                    k++;
                }

                partitioned.releasePartition(partitionBreaker, p);
            }
        } finally {
            state.close();
        }
        assertThat(breaker.getUsed(), equalTo(0L));
    }
}
