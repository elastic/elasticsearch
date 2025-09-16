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
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

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
        while (true) {
            var splitBuilder = split.builder();
            // pick a shard at random and see if we can advance it
            int idx = randomIntBetween(0, numShards * multiple - 1);
            if (idx < numShards) {
                // can we advance source?
                var sourceState = split.getSourceShardState(idx);
                var nextState = randomFrom(IndexReshardingState.Split.SourceShardState.values());
                if (nextState.ordinal() == sourceState.ordinal() + 1) {
                    if (split.targetsDone(idx)) {
                        long ongoingSourceShards = split.sourceStates()
                            .filter(state -> state != IndexReshardingState.Split.SourceShardState.DONE)
                            .count();
                        // all source shards done is an invalid state, terminate
                        if (ongoingSourceShards == 1) break;
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

        assertEquals(
            split.sourceStates().filter(state -> state == IndexReshardingState.Split.SourceShardState.DONE).count(),
            numShards - 1
        );
        for (int i = numShards; i < numShards * multiple; i++) {
            assertSame(IndexReshardingState.Split.TargetShardState.DONE, split.getTargetShardState(i));
            assertTrue(split.isTargetShard(i));
        }
    }

    // Test that the ReshardSplitShardCount is calculated correctly w.r.t the current state of resharding-split operation
    public void testReshardShardCountCalculation() {
        final var numShards = 1;
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();

        assertNull(reshardingMetadata);

        // Create IndexMetadata with 2 shards. This is to build the right value for numRoutingShards for an index with 2 shards.
        // There is no resharding yet.
        final var numSourceShards = 2;
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(numSourceShards).build();

        assertNull(reshardingMetadata);

        // When there is no resharding going on, the ReshardSplitShardCount is same as number of shards in the index
        for (int i = 0; i < numSourceShards; i++) {
            assertThat(
                indexMetadata.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.CLONE),
                equalTo(numSourceShards)
            );
            assertThat(
                indexMetadata.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.HANDOFF),
                equalTo(numSourceShards)
            );
            assertThat(
                indexMetadata.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.SPLIT),
                equalTo(numSourceShards)
            );
        }

        // Now reshard-split from 2 shards to 4 shards
        final int multiple = 2;
        var IndexMetadataAfterReshard = IndexMetadata.builder(indexMetadata)
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(numSourceShards, multiple))
            .reshardAddShards(numSourceShards * multiple)
            .build();

        reshardingMetadata = IndexMetadataAfterReshard.getReshardingMetadata();

        // starting state is as expected
        assertEquals(numSourceShards, reshardingMetadata.shardCountBefore());
        assertEquals(numSourceShards * multiple, reshardingMetadata.shardCountAfter());
        final int numTargetShards = reshardingMetadata.shardCountAfter();

        // All target shards in CLONE state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(reshardingMetadata.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.CLONE));
            assertThat(
                IndexMetadataAfterReshard.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.CLONE),
                equalTo(numTargetShards)
            );
            assertThat(
                IndexMetadataAfterReshard.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.HANDOFF),
                equalTo(numSourceShards)
            );
            assertThat(
                IndexMetadataAfterReshard.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.SPLIT),
                equalTo(numSourceShards)
            );
        }

        IndexReshardingState.Split.Builder builder = new IndexReshardingState.Split.Builder(reshardingMetadata.getSplit());
        for (int i = numSourceShards; i < numTargetShards; i++) {
            builder.setTargetShardState(i, IndexReshardingState.Split.TargetShardState.HANDOFF);
        }
        var indexReshardingMetadataHandoff = new IndexReshardingMetadata(builder.build());
        var indexMetadataHandoff = IndexMetadata.builder(IndexMetadataAfterReshard)
            .reshardingMetadata(indexReshardingMetadataHandoff)
            .build();

        // All target shards in HANDOFF state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(
                indexReshardingMetadataHandoff.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.HANDOFF)
            );
            assertThat(
                indexMetadataHandoff.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.CLONE),
                equalTo(numTargetShards)
            );
            assertThat(
                indexMetadataHandoff.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.HANDOFF),
                equalTo(numTargetShards)
            );
            assertThat(
                indexMetadataHandoff.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.SPLIT),
                equalTo(numSourceShards)
            );
        }

        builder = new IndexReshardingState.Split.Builder(indexReshardingMetadataHandoff.getSplit());
        for (int i = numSourceShards; i < numTargetShards; i++) {
            builder.setTargetShardState(i, IndexReshardingState.Split.TargetShardState.SPLIT);
        }
        var indexReshardingMetadataSplit = new IndexReshardingMetadata(builder.build());
        var indexMetadataSplit = IndexMetadata.builder(IndexMetadataAfterReshard).reshardingMetadata(indexReshardingMetadataSplit).build();

        // All target shards in SPLIT state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(
                indexReshardingMetadataSplit.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.SPLIT)
            );
            assertThat(
                indexMetadataSplit.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.CLONE),
                equalTo(numTargetShards)
            );
            assertThat(
                indexMetadataSplit.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.HANDOFF),
                equalTo(numTargetShards)
            );
            assertThat(
                indexMetadataSplit.getReshardSplitShardCount(i, IndexReshardingState.Split.TargetShardState.SPLIT),
                equalTo(numTargetShards)
            );
        }
    }
}
