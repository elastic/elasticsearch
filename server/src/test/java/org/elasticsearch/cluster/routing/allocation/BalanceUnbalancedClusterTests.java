/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.apache.lucene.tests.util.TestUtil;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.common.settings.Settings;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.routing.RoutingNodesHelper.shardsWithState;
import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;

/**
 * see issue #9023
 */
public class BalanceUnbalancedClusterTests extends CatAllocationTestCase {

    @Override
    protected Path getCatPath() throws IOException {
        Path tmp = createTempDir();
        try (InputStream stream = Files.newInputStream(getDataPath("/org/elasticsearch/cluster/routing/issue_9023.zip"))) {
            TestUtil.unzip(stream, tmp);
        }
        return tmp.resolve("issue_9023");
    }

    @Override
    protected ClusterState allocateNew(ClusterState state) {
        String index = "tweets-2014-12-29:00";
        AllocationService strategy = createAllocationService(Settings.builder().build());
        Metadata metadata = Metadata.builder(state.metadata())
            .put(IndexMetadata.builder(index).settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
            .build();

        RoutingTable initialRoutingTable = RoutingTable.builder(state.routingTable()).addAsNew(metadata.index(index)).build();

        ClusterState clusterState = ClusterState.builder(state).metadata(metadata).routingTable(initialRoutingTable).build();
        clusterState = strategy.reroute(clusterState, "reroute");
        while (shardsWithState(clusterState.getRoutingNodes(), INITIALIZING).isEmpty() == false) {
            clusterState = ESAllocationTestCase.startInitializingShardsAndReroute(strategy, clusterState);
        }
        Map<String, Integer> counts = new HashMap<>();
        final IndexRoutingTable indexRoutingTable = clusterState.routingTable().index(index);
        for (int shardId = 0; shardId < indexRoutingTable.size(); shardId++) {
            final IndexShardRoutingTable indexShardRoutingTable = indexRoutingTable.shard(shardId);
            for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
                String s = indexShardRoutingTable.shard(copy).currentNodeId();
                Integer count = counts.get(s);
                if (count == null) {
                    count = 0;
                }
                count++;
                counts.put(s, count);
            }
        }
        for (Map.Entry<String, Integer> count : counts.entrySet()) {
            // we have 10 shards and 4 nodes so 2 nodes have 3 shards and 2 nodes have 2 shards
            assertTrue("Node: " + count.getKey() + " has shard mismatch: " + count.getValue(), count.getValue() >= 2);
            assertTrue("Node: " + count.getKey() + " has shard mismatch: " + count.getValue(), count.getValue() <= 3);

        }
        return clusterState;
    }

}
