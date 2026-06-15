/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsResponse;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class TransportGetCheckpointActionTests extends ESTestCase {

    private static final String NODE_0 = "node-0";
    private static final String NODE_1 = "node-1";
    private static final String NODE_2 = "node-2";
    private static final Index INDEX_A = new Index("my-index-A", "A");
    private static final Index INDEX_B = new Index("my-index-B", "B");
    private static final Index INDEX_C = new Index("my-index-C", "C");
    private static final ShardId SHARD_A_0 = new ShardId(INDEX_A, 0);
    private static final ShardId SHARD_A_1 = new ShardId(INDEX_A, 1);
    private static final ShardId SHARD_B_0 = new ShardId(INDEX_B, 0);
    private static final ShardId SHARD_B_1 = new ShardId(INDEX_B, 1);

    private static final Map<String, Set<ShardId>> NODES_AND_SHARDS = Map.of(
        NODE_0,
        Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1),
        NODE_1,
        Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1),
        NODE_2,
        Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1)
    );

    // Empty nodesAndShards → empty result regardless of what search_shards returns.
    public void testFilterOutSkippedShards_EmptyNodesAndShards() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_1, NODE_2), false, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_2), true, SplitShardCountSummary.UNSET)
            ),
            0,
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            Map.of(),
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(anEmptyMap()));
    }

    // Empty search_shards response means no can-match skips → all routing-table shards are kept.
    // This ensures that index-resolution divergence (e.g. under CPS) never silently empties the
    // shard set and causes operations_behind to be reported as 0.
    public void testFilterOutSkippedShards_EmptySearchShardsResponse() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(Set.of(), 0, Set.of(), Map.of());
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(equalTo(NODES_AND_SHARDS)));
    }

    // Shards marked skipped=true are removed from the nodes they are allocated on; shards that are
    // absent from the response (neither skipped nor present) are kept.
    public void testFilterOutSkippedShards_SkippedShardsAreRemoved() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                // SHARD_A_0 skipped on NODE_0 and NODE_1; not returned for NODE_2 (absent → kept on NODE_2)
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1), true, SplitShardCountSummary.UNSET),
                // SHARD_B_0 skipped on NODE_1 and NODE_2
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_1, NODE_2), true, SplitShardCountSummary.UNSET)
            // SHARD_A_1 and SHARD_B_1 absent from response on all nodes → kept everywhere
            ),
            2,
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        Map<String, Set<ShardId>> expectedFilteredNodesAndShards = Map.of(
            NODE_0,
            Set.of(SHARD_A_1, SHARD_B_0, SHARD_B_1),          // A_0 skipped on NODE_0
            NODE_1,
            Set.of(SHARD_A_1, SHARD_B_1),                      // A_0 and B_0 skipped on NODE_1
            NODE_2,
            Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_1)            // B_0 skipped; A_0 absent → kept
        );
        assertThat(filteredNodesAndShards, is(equalTo(expectedFilteredNodesAndShards)));
    }

    // A node is removed from the result when all of its shards are flagged as skipped.
    public void testFilterOutSkippedShards_NodeRemovedWhenAllShardsSkipped() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_1), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_A_1, List.of(NODE_1), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_1), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_1), true, SplitShardCountSummary.UNSET)
            ),
            4,
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        // NODE_1 is removed; NODE_0 and NODE_2 keep all their shards (absent from skipped groups → kept)
        Map<String, Set<ShardId>> expectedFilteredNodesAndShards = Map.of(
            NODE_0,
            Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1),
            NODE_2,
            Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1)
        );
        assertThat(filteredNodesAndShards, is(equalTo(expectedFilteredNodesAndShards)));
    }

    // When every shard on every node is flagged as skipped, the result is empty.
    public void testFilterOutSkippedShards_AllShardsSkipped() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1, NODE_2), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_A_1, List.of(NODE_0, NODE_1, NODE_2), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_0, NODE_1, NODE_2), true, SplitShardCountSummary.UNSET),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_1, NODE_2), true, SplitShardCountSummary.UNSET)
            ),
            4,
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(anEmptyMap()));
    }
}
