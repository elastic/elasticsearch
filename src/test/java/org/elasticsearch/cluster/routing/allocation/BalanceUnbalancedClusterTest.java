/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.cluster.routing.allocation;

import org.apache.lucene.util.TestUtil;
import org.apache.lucene.util.LuceneTestCase.Slow;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;

/**
 * see issue #9023
 */
@Slow
public class BalanceUnbalancedClusterTest extends CatAllocationTestBase {

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
        AllocationService strategy = createAllocationService(settingsBuilder()
                .build());
        MetaData metaData = MetaData.builder(state.metaData())
                .put(IndexMetaData.builder(index).settings(settings(Version.CURRENT)).numberOfShards(5).numberOfReplicas(1))
                .build();

        RoutingTable routingTable = RoutingTable.builder(state.routingTable())
                .addAsNew(metaData.index(index))
                .build();

        ClusterState clusterState = ClusterState.builder(state).metaData(metaData).routingTable(routingTable).build();
        routingTable = strategy.reroute(clusterState).routingTable();
        clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        while (true) {
            if (routingTable.shardsWithState(INITIALIZING).isEmpty()) {
                break;
            }
            routingTable = strategy.applyStartedShards(clusterState, routingTable.shardsWithState(INITIALIZING)).routingTable();
            clusterState = ClusterState.builder(clusterState).routingTable(routingTable).build();
        }
        Map<String, Integer> counts = new HashMap<>();
        for (IndexShardRoutingTable table : routingTable.index(index)) {
            for (ShardRouting r : table) {
                String s = r.currentNodeId();
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
