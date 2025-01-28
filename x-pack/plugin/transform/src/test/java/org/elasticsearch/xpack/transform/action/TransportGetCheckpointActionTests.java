/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.action.search.SearchShardsGroup;
import org.elasticsearch.action.search.SearchShardsResponse;
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

    public void testFilterOutSkippedShards_EmptyNodesAndShards() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1), true),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_1, NODE_2), false),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_2), true)
            ),
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            Map.of(),
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(anEmptyMap()));
    }

    public void testFilterOutSkippedShards_EmptySearchShardsResponse() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(Set.of(), Set.of(), Map.of());
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(equalTo(NODES_AND_SHARDS)));
    }

    public void testFilterOutSkippedShards_SomeNodesEmptyAfterFiltering() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_2), true),
                new SearchShardsGroup(SHARD_A_1, List.of(NODE_0, NODE_2), true),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_0, NODE_2), true),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_2), true)
            ),
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        Map<String, Set<ShardId>> expectedFilteredNodesAndShards = Map.of(NODE_1, Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0, SHARD_B_1));
        assertThat(filteredNodesAndShards, is(equalTo(expectedFilteredNodesAndShards)));
    }

    public void testFilterOutSkippedShards_AllNodesEmptyAfterFiltering() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1, NODE_2), true),
                new SearchShardsGroup(SHARD_A_1, List.of(NODE_0, NODE_1, NODE_2), true),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_0, NODE_1, NODE_2), true),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_1, NODE_2), true)
            ),
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        assertThat(filteredNodesAndShards, is(equalTo(Map.of())));
    }

    public void testFilterOutSkippedShards() {
        SearchShardsResponse searchShardsResponse = new SearchShardsResponse(
            Set.of(
                new SearchShardsGroup(SHARD_A_0, List.of(NODE_0, NODE_1), true),
                new SearchShardsGroup(SHARD_B_0, List.of(NODE_1, NODE_2), false),
                new SearchShardsGroup(SHARD_B_1, List.of(NODE_0, NODE_2), true),
                new SearchShardsGroup(new ShardId(INDEX_C, 0), List.of(NODE_0, NODE_1, NODE_2), true)
            ),
            Set.of(),
            Map.of()
        );
        Map<String, Set<ShardId>> filteredNodesAndShards = TransportGetCheckpointAction.filterOutSkippedShards(
            NODES_AND_SHARDS,
            searchShardsResponse
        );
        Map<String, Set<ShardId>> expectedFilteredNodesAndShards = Map.of(
            NODE_0,
            Set.of(SHARD_A_1, SHARD_B_0),
            NODE_1,
            Set.of(SHARD_A_1, SHARD_B_0, SHARD_B_1),
            NODE_2,
            Set.of(SHARD_A_0, SHARD_A_1, SHARD_B_0)
        );
        assertThat(filteredNodesAndShards, is(equalTo(expectedFilteredNodesAndShards)));
    }
}
