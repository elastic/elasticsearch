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

package org.elasticsearch.cluster.action.shard;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;


public class ShardStateActionTest extends ElasticsearchTestCase {

    public void testShardFiltering() {
        final IndexMetaData indexMetaData = IndexMetaData.builder("test")
                .settings(Settings.builder()
                        .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
                        .put(IndexMetaData.SETTING_UUID, "test_uuid"))
                .numberOfShards(2).numberOfReplicas(0)
                .build();
        ClusterState.Builder stateBuilder = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder()
                                .put(new DiscoveryNode("node1", DummyTransportAddress.INSTANCE, Version.CURRENT)).masterNodeId("node1")
                                .put(new DiscoveryNode("node2", DummyTransportAddress.INSTANCE, Version.CURRENT))
                )
                .metaData(MetaData.builder().put(indexMetaData, false));

        final ShardRouting initShard = TestShardRouting.newShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING, 1);
        final ShardRouting startedShard = TestShardRouting.newShardRouting("test", 1, "node2", randomBoolean(), ShardRoutingState.STARTED, 1);
        final ShardRouting relocatingShard = TestShardRouting.newShardRouting("test", 2, "node1", "node2", randomBoolean(), ShardRoutingState.RELOCATING, 1);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder("test")
                .addIndexShard(new IndexShardRoutingTable.Builder(initShard.shardId(), true).addShard(initShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(startedShard.shardId(), true).addShard(startedShard).build())
                .addIndexShard(new IndexShardRoutingTable.Builder(relocatingShard.shardId(), true).addShard(relocatingShard).build())));

        ClusterState state = stateBuilder.build();

        ArrayList<ShardStateAction.ShardRoutingEntry> listToFilter = new ArrayList<>();
        ArrayList<ShardStateAction.ShardRoutingEntry> expectedToBeApplied = new ArrayList<>();

        listToFilter.add(new ShardStateAction.ShardRoutingEntry(initShard, indexMetaData.uuid() + "_suffix", "wrong_uuid"));

        listToFilter.add(new ShardStateAction.ShardRoutingEntry(relocatingShard.buildTargetRelocatingShard(), indexMetaData.uuid(), "relocating_to_node"));
        expectedToBeApplied.add(listToFilter.get(listToFilter.size() - 1));

        listToFilter.add(new ShardStateAction.ShardRoutingEntry(startedShard, indexMetaData.uuid(), "started shard"));
        expectedToBeApplied.add(listToFilter.get(listToFilter.size() - 1));

        listToFilter.add(new ShardStateAction.ShardRoutingEntry(TestShardRouting.newShardRouting(initShard.index() + "_NA", initShard.id(),
                initShard.currentNodeId(), initShard.primary(), initShard.state(), initShard.version()), indexMetaData.uuid(), "wrong_uuid"));

        List<ShardStateAction.ShardRoutingEntry> toBeApplied = ShardStateAction.extractShardsToBeApplied(listToFilter, "for testing", state.metaData(), logger);
        if (toBeApplied.size() != expectedToBeApplied.size()) {
            fail("size mismatch.\n Got: \n [" + toBeApplied + "], \n expected: \n [" + expectedToBeApplied + "]");
        }
        for (int i = 0; i < toBeApplied.size(); i++) {
            final ShardStateAction.ShardRoutingEntry found = toBeApplied.get(i);
            final ShardStateAction.ShardRoutingEntry expected = expectedToBeApplied.get(i);
            assertThat(found, equalTo(expected));
        }
    }
}