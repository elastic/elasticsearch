/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingMetadata;
import org.elasticsearch.cluster.metadata.IndexReshardingState;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.index.IndexVersionUtils;

import java.util.Locale;

import static org.hamcrest.Matchers.equalTo;

public class SplitShardCountSummaryTests extends ESTestCase {
    // Test that the ReshardSplitShardCount is calculated correctly w.r.t the current state of resharding-split operation
    public void testReshardShardCountCalculation() {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        var settings = indexSettings(IndexVersionUtils.randomCompatibleVersion(random()), 1, 0).build();
        IndexMetadata indexMetadata = IndexMetadata.builder(indexName).settings(settings).build();
        IndexReshardingMetadata reshardingMetadata = indexMetadata.getReshardingMetadata();

        assertNull(reshardingMetadata);

        // Create IndexMetadata with 2 shards. This is to build the right value for numRoutingShards for an index with 2 shards.
        // There is no resharding yet.
        final var numSourceShards = 2;
        indexMetadata = IndexMetadata.builder(indexMetadata).reshardAddShards(numSourceShards).build();
        reshardingMetadata = indexMetadata.getReshardingMetadata();

        assertNull(reshardingMetadata);

        final var preSplitSummary = new SplitShardCountSummary(numSourceShards);
        // When there is no resharding going on, the ReshardSplitShardCount is same as number of shards in the index
        for (int i = 0; i < numSourceShards; i++) {
            assertThat(SplitShardCountSummary.forIndexing(indexMetadata, i), equalTo(preSplitSummary));
            assertThat(SplitShardCountSummary.forSearch(indexMetadata, i), equalTo(preSplitSummary));
        }

        // Now reshard-split from 2 shards to 4 shards
        final int multiple = 2;
        var indexMetadataAfterReshard = IndexMetadata.builder(indexMetadata)
            .reshardingMetadata(IndexReshardingMetadata.newSplitByMultiple(numSourceShards, multiple))
            .reshardAddShards(numSourceShards * multiple)
            .build();

        reshardingMetadata = indexMetadataAfterReshard.getReshardingMetadata();

        // starting state is as expected
        assertEquals(numSourceShards, reshardingMetadata.shardCountBefore());
        assertEquals(numSourceShards * multiple, reshardingMetadata.shardCountAfter());
        final int numShardsAfterReshard = reshardingMetadata.shardCountAfter();
        final var postSplitSummary = new SplitShardCountSummary(numShardsAfterReshard);

        // All target shards in CLONE state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(reshardingMetadata.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.CLONE));

            assertThat(SplitShardCountSummary.forIndexing(indexMetadataAfterReshard, i), equalTo(preSplitSummary));
            assertThat(SplitShardCountSummary.forSearch(indexMetadataAfterReshard, i), equalTo(preSplitSummary));
        }

        var indexReshardingMetadataHandoff = reshardingMetadata;
        for (int i = numSourceShards; i < numShardsAfterReshard; i++) {
            indexReshardingMetadataHandoff = indexReshardingMetadataHandoff.transitionSplitTargetToNewState(
                new ShardId("test", "na", i),
                IndexReshardingState.Split.TargetShardState.HANDOFF
            );
        }
        var indexMetadataHandoff = IndexMetadata.builder(indexMetadataAfterReshard)
            .reshardingMetadata(indexReshardingMetadataHandoff)
            .build();

        // All target shards in HANDOFF state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(
                indexReshardingMetadataHandoff.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.HANDOFF)
            );

            assertThat(SplitShardCountSummary.forIndexing(indexMetadataHandoff, i), equalTo(postSplitSummary));
            assertThat(SplitShardCountSummary.forSearch(indexMetadataHandoff, i), equalTo(preSplitSummary));
        }

        var indexReshardingMetadataSplit = indexReshardingMetadataHandoff;
        for (int i = numSourceShards; i < numShardsAfterReshard; i++) {
            indexReshardingMetadataSplit = indexReshardingMetadataSplit.transitionSplitTargetToNewState(
                new ShardId("test", "na", i),
                IndexReshardingState.Split.TargetShardState.SPLIT
            );
        }
        var indexMetadataSplit = IndexMetadata.builder(indexMetadataAfterReshard).reshardingMetadata(indexReshardingMetadataSplit).build();

        // All target shards in SPLIT state
        for (int i = 0; i < numSourceShards; i++) {
            assertTrue(
                indexReshardingMetadataSplit.getSplit().allTargetStatesAtLeast(i, IndexReshardingState.Split.TargetShardState.SPLIT)
            );

            assertThat(SplitShardCountSummary.forIndexing(indexMetadataSplit, i), equalTo(postSplitSummary));
            assertThat(SplitShardCountSummary.forSearch(indexMetadataSplit, i), equalTo(postSplitSummary));
        }
    }
}
