/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.routing;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.XContentTestUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;

public class ShardRoutingRoleIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin implements ClusterPlugin {
        @Override
        public ShardRoutingRoleStrategy getShardRoutingRoleStrategy() {
            return new ShardRoutingRoleStrategy() {
                @Override
                public ShardRouting.Role newReplicaRole() {
                    return ShardRouting.Role.SEARCH_ONLY;
                }

                @Override
                public ShardRouting.Role newEmptyRole(int copyIndex) {
                    return copyIndex <= 1 ? ShardRouting.Role.INDEX_ONLY : ShardRouting.Role.SEARCH_ONLY;
                }
            };
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    public void testShardRoutingRole() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(3);
        internalCluster().ensureAtMostNumDataNodes(3);

        createIndex(
            "test",
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 2).build()
        );
        ensureGreen("test");

        final var clusterStateAtStart = client().admin().cluster().prepareState().get().getState();

        assertRolesInRoutingTable(clusterStateAtStart);

        logger.info("--> {}", clusterStateAtStart);

        final ClusterStateListener stateListener = new ClusterStateListener() {
            private final AtomicLong lastVersionLogged = new AtomicLong();

            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                final var version = event.state().version();
                if (lastVersionLogged.getAndUpdate(l -> Math.max(version, l)) < version) {
                    logger.info("--> routing nodes for version [{}]\n{}", version, event.state().getRoutingNodes().toString());
                }
            }
        };

        for (ClusterService clusterService : internalCluster().getInstances(ClusterService.class)) {
            clusterService.addListener(stateListener);
        }

        internalCluster().restartRandomDataNode(new InternalTestCluster.RestartCallback() {
            @Override
            public Settings onNodeStopped(String nodeName) {
                ensureStableCluster(internalCluster().getNodeNames().length);
                logger.info("--> node [{}] stopped", nodeName);
                return Settings.EMPTY;
            }
        });
        ensureGreen("test");

        logger.info("--> after restart");

        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1))
        );

        for (RoutingNode routingNode : client().admin().cluster().prepareState().get().getState().getRoutingNodes()) {
            for (ShardRouting shardRouting : routingNode) {
                assertThat(shardRouting.getRole(), Matchers.oneOf(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.SEARCH_ONLY));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void assertRolesInRoutingTable(ClusterState state) throws IOException {
        var stateAsString = state.toString();
        for (var testRole : ShardRouting.Role.values()) {
            if (testRole != ShardRouting.Role.DEFAULT) {
                assertThat(stateAsString, containsString("[" + testRole + "]"));
            }
        }

        final var routingTable = (Map<String, Object>) XContentTestUtils.convertToMap(state).get("routing_table");
        final var routingTableIndices = (Map<String, Object>) routingTable.get("indices");
        final var routingTableIndex = (Map<String, Object>) routingTableIndices.get("test");
        final var routingTableShards = (Map<String, Object>) routingTableIndex.get("shards");
        for (final var routingTableShardValue : routingTableShards.values()) {
            for (Object routingTableShardCopy : (List<Object>) routingTableShardValue) {
                final var routingTableShard = (Map<String, String>) routingTableShardCopy;
                assertNotNull(ShardRouting.Role.valueOf(routingTableShard.get("role")));
            }
        }
    }

}
