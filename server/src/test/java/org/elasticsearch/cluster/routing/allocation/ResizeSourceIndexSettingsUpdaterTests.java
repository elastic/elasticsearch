/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ESAllocationTestCase;
import org.elasticsearch.cluster.TestShardRoutingRoleStrategies;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;

import java.util.List;

import static org.elasticsearch.cluster.routing.ShardRoutingState.INITIALIZING;
import static org.elasticsearch.cluster.routing.ShardRoutingState.STARTED;
import static org.elasticsearch.cluster.routing.ShardRoutingState.UNASSIGNED;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class ResizeSourceIndexSettingsUpdaterTests extends ESAllocationTestCase {

    public void testResizeIndexSettingsRemovedAfterStart() {
        final DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(newNode("node1", "id1", MASTER_DATA_ROLES))
            .add(newNode("node2", "id2", MASTER_DATA_ROLES))
            .build();

        final DiscoveryNode resizeNode = randomFrom(discoveryNodes.getDataNodes().values());

        final String sourceIndex = "source";
        final String targetIndex = "target";

        final Metadata sourceMetadata = Metadata.builder()
            .put(
                IndexMetadata.builder(sourceIndex)
                    .settings(
                        settings(Version.CURRENT).put(
                            IndexMetadata.INDEX_ROUTING_REQUIRE_GROUP_SETTING.getKey() + "_name",
                            resizeNode.getName()
                        ).put("index.blocks.write", true)
                    )
                    .numberOfShards(2)
                    .numberOfReplicas(0)
                    .setRoutingNumShards(16)
            )
            .build();

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY).addAsNew(sourceMetadata.index(sourceIndex))
            )
            .metadata(sourceMetadata)
            .nodes(discoveryNodes)
            .build();

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable().index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(UNASSIGNED));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(UNASSIGNED));
        }

        final AllocationService allocationService = createAllocationService(
            Settings.builder()
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), 16)
                .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_INITIAL_PRIMARIES_RECOVERIES_SETTING.getKey(), 16)
                .build()
        );
        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable().index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(INITIALIZING));
            assertThat(sourceRoutingTable.shard(0).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
            assertThat(sourceRoutingTable.shard(1).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
        }

        clusterState = startInitializingShardsAndReroute(allocationService, clusterState);

        {
            IndexRoutingTable sourceRoutingTable = clusterState.routingTable().index(sourceIndex);
            assertThat(sourceRoutingTable.size(), equalTo(2));
            assertThat(sourceRoutingTable.shard(0).primaryShard().state(), equalTo(STARTED));
            assertThat(sourceRoutingTable.shard(1).primaryShard().state(), equalTo(STARTED));
            assertThat(sourceRoutingTable.shard(0).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
            assertThat(sourceRoutingTable.shard(1).primaryShard().currentNodeId(), equalTo(resizeNode.getId()));
        }

        final int targetNumShards = randomFrom(1, 2, 4, 8, 16);
        final int targetNumReplicas = randomInt(2);
        final Settings.Builder targetSettings = indexSettings(Version.CURRENT, targetNumShards, targetNumReplicas);
        targetSettings.put(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.getKey(), sourceIndex);
        targetSettings.put(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.getKey(), sourceMetadata.index(sourceIndex).getIndexUUID());
        final boolean isShrink = randomBoolean();
        if (isShrink) {
            targetSettings.put(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY, resizeNode.getId());
        }
        final boolean hasLifecyclePolicy = randomBoolean();
        if (hasLifecyclePolicy) {
            targetSettings.put(IndexMetadata.LIFECYCLE_NAME, "policy");
        }

        clusterState = ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .put(IndexMetadata.builder(targetIndex).settings(targetSettings).setRoutingNumShards(16))
            )
            .build();
        clusterState = ClusterState.builder(clusterState)
            .routingTable(
                RoutingTable.builder(TestShardRoutingRoleStrategies.DEFAULT_ROLE_ONLY, clusterState.routingTable())
                    .addAsNew(clusterState.metadata().index(targetIndex))
            )
            .build();

        {
            IndexRoutingTable targetRoutingTable = clusterState.routingTable().index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(UNASSIGNED));
            }
        }

        clusterState = allocationService.reroute(clusterState, "reroute", ActionListener.noop());

        {
            IndexMetadata targetIndexMetadata = clusterState.metadata().index(targetIndex);
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()), is(true));
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()), is(true));
            assertThat(targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY), is(isShrink));
            IndexRoutingTable targetRoutingTable = clusterState.routingTable().index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(INITIALIZING));
            }
        }

        while (true) {
            IndexRoutingTable targetIndexRoutingTable = clusterState.routingTable().index(targetIndex);
            List<ShardRouting> initializing = targetIndexRoutingTable.shardsWithState(INITIALIZING);
            if (initializing.isEmpty()) {
                break;
            }

            IndexMetadata targetIndexMetadata = clusterState.metadata().index(targetIndex);
            assertThat(
                IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()),
                is(hasLifecyclePolicy || (targetIndexRoutingTable.allPrimaryShardsActive() == false))
            );
            assertThat(
                IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()),
                is(targetIndexRoutingTable.allPrimaryShardsActive() == false)
            );
            assertThat(
                targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY),
                is(targetIndexRoutingTable.allPrimaryShardsActive() ? false : isShrink)
            );

            clusterState = startShardsAndReroute(allocationService, clusterState, randomNonEmptySubsetOf(initializing));
        }

        {
            IndexMetadata targetIndexMetadata = clusterState.metadata().index(targetIndex);
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_NAME.exists(targetIndexMetadata.getSettings()), is(hasLifecyclePolicy));
            assertThat(IndexMetadata.INDEX_RESIZE_SOURCE_UUID.exists(targetIndexMetadata.getSettings()), is(false));
            assertThat(targetIndexMetadata.getSettings().hasValue(IndexMetadata.INDEX_SHRINK_INITIAL_RECOVERY_KEY), is(false));
            IndexRoutingTable targetRoutingTable = clusterState.routingTable().index(targetIndex);
            assertThat(targetRoutingTable.size(), equalTo(targetNumShards));
            for (int i = 0; i < targetNumShards; i++) {
                ShardRouting shardRouting = targetRoutingTable.shard(i).primaryShard();
                assertThat(shardRouting.toString(), shardRouting.state(), equalTo(STARTED));
            }
        }
    }
}
