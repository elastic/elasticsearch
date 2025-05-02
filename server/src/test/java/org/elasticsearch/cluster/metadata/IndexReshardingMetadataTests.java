/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.test.ESTestCase;

public class IndexReshardingMetadataTests extends ESTestCase {
    // test that we can drive a split through all valid state transitions in random order and terminate
    public void testSplit() {
        final var numShards = randomIntBetween(1, 10);
        final var multiple = randomIntBetween(2, 5);

        final var metadata = IndexReshardingMetadata.newSplitByMultiple(numShards, multiple);
        var split = metadata.getSplit();

        // starting state is as expected
        assertEquals(numShards, metadata.shardCountBefore());
        assertEquals(numShards * multiple, metadata.shardCountAfter());
        for (int i = 0; i < numShards; i++) {
            assertSame(IndexReshardingState.Split.SourceShardState.SOURCE, split.getSourceShardState(i));
            assertFalse(split.isTargetShard(i));
        }
        for (int i = numShards; i < numShards * multiple; i++) {
            assertSame(IndexReshardingState.Split.TargetShardState.CLONE, split.getTargetShardState(i));
            assertTrue(split.isTargetShard(i));
        }

        // advance split state randomly and expect to terminate
        while (split.inProgress()) {
            var splitBuilder = split.builder();
            // pick a shard at random and see if we can advance it
            int idx = randomIntBetween(0, numShards * multiple - 1);
            if (idx < numShards) {
                // can we advance source?
                var sourceState = split.getSourceShardState(idx);
                var nextState = randomFrom(IndexReshardingState.Split.SourceShardState.values());
                if (nextState.ordinal() == sourceState.ordinal() + 1) {
                    if (split.targetsDone(idx)) {
                        splitBuilder.setSourceShardState(idx, nextState);
                    } else {
                        assertThrows(AssertionError.class, () -> splitBuilder.setSourceShardState(idx, nextState));
                    }
                } else {
                    assertThrows(AssertionError.class, () -> splitBuilder.setSourceShardState(idx, nextState));
                }
            } else {
                // can we advance target?
                var targetState = split.getTargetShardState(idx);
                var nextState = randomFrom(IndexReshardingState.Split.TargetShardState.values());
                if (nextState.ordinal() == targetState.ordinal() + 1) {
                    splitBuilder.setTargetShardState(idx, nextState);
                } else {
                    assertThrows(AssertionError.class, () -> splitBuilder.setTargetShardState(idx, nextState));
                }
            }
            split = splitBuilder.build();
        }

        for (int i = 0; i < numShards; i++) {
            assertSame(IndexReshardingState.Split.SourceShardState.DONE, split.getSourceShardState(i));
            assertFalse(split.isTargetShard(i));
        }
        for (int i = numShards; i < numShards * multiple; i++) {
            assertSame(IndexReshardingState.Split.TargetShardState.DONE, split.getTargetShardState(i));
            assertTrue(split.isTargetShard(i));
        }
    }
}
