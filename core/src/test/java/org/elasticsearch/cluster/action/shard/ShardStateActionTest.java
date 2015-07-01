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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.*;
import org.elasticsearch.cluster.routing.allocation.FailedRerouteAllocation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ElasticsearchTestCase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.contains;


public class ShardStateActionTest extends ElasticsearchTestCase {

    public void testStartedShardFiltering() {
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

        final ShardRouting initShard = new ShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING, 1);
        final ShardRouting startedShard = new ShardRouting("test", 1, "node1", randomBoolean(), ShardRoutingState.STARTED, 1);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder("test")
                .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 0), true)
                        .addShard(initShard)
                        .addShard(startedShard).build())));

        ClusterState state = stateBuilder.build();

        ArrayList<ShardStateAction.ShardRoutingEntry> listToFilter = new ArrayList<>();
        listToFilter.add(new ShardStateAction.ShardRoutingEntry(initShard, indexMetaData.uuid(), "the_droid_we_are_looking_for"));

        for (int i = randomIntBetween(5, 10); i > 0; i--) {
            switch (randomInt(3)) {
                case 0:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(initShard, indexMetaData.uuid() + "_suffix", "wrong_uuid"));
                    break;
                case 1:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(initShard.index(), initShard.id(),
                                    initShard.currentNodeId(), "some_node", initShard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "relocating_to_node"
                    ));
                    break;
                case 2:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(initShard.index(), initShard.id(),
                                    initShard.currentNodeId(), initShard.relocatingNodeId(), !initShard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "wrong_primary_flag"
                    ));
                    break;
                case 3:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(initShard.index(), initShard.id(), "some_node", initShard.currentNodeId(), initShard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "relocating_from_node"
                    ));
                    break;
                case 4:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(startedShard.index(), startedShard.id(), startedShard.currentNodeId(), startedShard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "already_started"
                    ));
                    break;
            }
        }

        Collections.shuffle(listToFilter);

        List<ShardRouting> toBeApplied = ShardStateAction.extractStartedRoutingToBeApplied(listToFilter, state.routingTable(), state.metaData(), logger);
        assertThat(toBeApplied, contains(initShard)); // using an array for error reporting
    }

    public void testFailedShardFiltering() {
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

        final ShardRouting shard = new ShardRouting("test", 0, "node1", randomBoolean(),
                randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.RELOCATING, ShardRoutingState.STARTED), 1);
        stateBuilder.routingTable(RoutingTable.builder().add(IndexRoutingTable.builder("test")
                .addIndexShard(new IndexShardRoutingTable.Builder(new ShardId("test", 0), true)
                        .addShard(shard).build())));

        ClusterState state = stateBuilder.build();

        ArrayList<ShardStateAction.ShardRoutingEntry> listToFilter = new ArrayList<>();
        ArrayList<ShardStateAction.ShardRoutingEntry> expectedToBeApplied = new ArrayList<>();

        // shard failures are super conservatives - it only checks index uuid. Some failures may be rejected later on
        // after a more fine grained control in ShardAllocation..
        for (int i = randomIntBetween(1, 4); i > 0; i--) {
            final boolean shouldBeApplied;
            switch (randomInt(5)) {
                case 0:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(shard, indexMetaData.uuid() + "_suffix", "wrong_uuid"));
                    shouldBeApplied = false;
                    break;
                case 1:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(shard.index(), shard.id(),
                                    shard.currentNodeId(), "some_node", shard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "relocating_to_node"
                    ));
                    shouldBeApplied = true;
                    break;
                case 2:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(shard.index(), shard.id(),
                                    shard.currentNodeId(), shard.relocatingNodeId(), !shard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "wrong_primary_flag"
                    ));
                    shouldBeApplied = true;
                    break;
                case 3:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(shard.index(), shard.id(), "some_node", shard.currentNodeId(), shard.primary(), ShardRoutingState.INITIALIZING, 1),
                            indexMetaData.uuid(),
                            "relocating_from_node"
                    ));
                    shouldBeApplied = true;
                    break;
                case 4:
                    ShardRoutingState newState = shard.state();
                    while (newState == shard.state()) {
                        newState = randomFrom(ShardRoutingState.INITIALIZING, ShardRoutingState.STARTED, ShardRoutingState.RELOCATING);
                    }

                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(
                            new ShardRouting(shard.index(), shard.id(), shard.currentNodeId(), shard.primary(), newState, 1),
                            indexMetaData.uuid(),
                            "wrong_state"
                    ));
                    shouldBeApplied = true;
                    break;
                case 5:
                    listToFilter.add(new ShardStateAction.ShardRoutingEntry(shard, indexMetaData.uuid(), "exact_match"));
                    shouldBeApplied = true;
                    break;
                default:
                    throw new ElasticsearchException("not enough failure modes");
            }
            if (shouldBeApplied) {
                expectedToBeApplied.add(listToFilter.get(listToFilter.size() - 1));
            }
        }

        List<FailedRerouteAllocation.FailedShard> toBeApplied = ShardStateAction.extractFailedShardsToBeApplied(listToFilter, state.metaData(), logger);
        if (toBeApplied.size() != expectedToBeApplied.size()) {
            fail("size mismatch.\n Got: \n [" + toBeApplied + "], \n expected: \n [" + expectedToBeApplied + "]");
        }
        for (int i = 0; i < toBeApplied.size(); i++) {
            final FailedRerouteAllocation.FailedShard found = toBeApplied.get(i);
            final ShardRouting expected = expectedToBeApplied.get(i).shardRouting;
            if (found.shard.equals(expected) == false) {
                fail("expected to find [" + expected + "], but had [" + found + "]");
            }
        }
    }
}