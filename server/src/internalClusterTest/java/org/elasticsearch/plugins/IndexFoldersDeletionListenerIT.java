/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.plugins;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST)
public class IndexFoldersDeletionListenerIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(IndexFoldersDeletionListenerPlugin.class);
        return plugins;
    }

    public void testListenersInvokedWhenIndexIsDeleted() throws Exception {
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName);

        final NumShards numShards = getNumShards(indexName);
        ensureClusterSizeConsistency(); // wait for a stable cluster
        ensureGreen(indexName); // wait for no relocation

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        assertAcked(client().admin().indices().prepareDelete(indexName));
        ensureClusterStateConsistency();
        assertPendingDeletesProcessed();

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Listener should have been notified of deletion of index " + index + " on node " + nodeName,
                plugin.deletedIndices.contains(index));

            final List<ShardId> deletedShards = plugin.deletedShards.get(index);
            assertThat(deletedShards, notNullValue());
            assertFalse("Listener should have been notified of deletion of one or more shards on node " + nodeName,
                deletedShards.isEmpty());

            for (ShardRouting shardRouting : shardsByNode.getValue()) {
                final ShardId shardId = shardRouting.shardId();
                assertTrue("Listener should have been notified of deletion of shard " + shardId + " on node " + nodeName,
                    deletedShards.contains(shardId));
            }
        }
    }

    public void testListenersInvokedWhenIndexIsRelocated() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(4, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            .build());

        final NumShards numShards = getNumShards(indexName);
        ensureGreen(indexName);

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        final List<String> excludedNodes = randomSubsetOf(2, shardsByNodes.keySet());
        assertAcked(client().admin().indices().prepareUpdateSettings(indexName)
            .setSettings(Settings.builder()
                .put("index.routing.allocation.exclude._name", String.join(",", excludedNodes))
                .build()));
        ensureGreen(indexName);
        ensureClusterStateConsistency();
        assertPendingDeletesProcessed();

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);

            if (excludedNodes.contains(nodeName)) {
                assertTrue("Listener should have been notified of deletion of index " + index + " on node " + nodeName,
                    plugin.deletedIndices.contains(index));

                final List<ShardId> deletedShards = plugin.deletedShards.get(index);
                assertThat(deletedShards, notNullValue());
                assertFalse("Listener should have been notified of deletion of one or more shards on node " + nodeName,
                    deletedShards.isEmpty());

                for (ShardRouting shardRouting : shardsByNode.getValue()) {
                    final ShardId shardId = shardRouting.shardId();
                    assertTrue("Listener should have been notified of deletion of shard " + shardId + " on node " + nodeName,
                        deletedShards.contains(shardId));
                }
            } else {
                assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
                assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
            }
        }
    }

    public void testListenersInvokedWhenIndexIsDangling() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(4);
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(indexName, Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(4, 10))
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, between(0, 1))
            .build());

        final NumShards numShards = getNumShards(indexName);
        ensureGreen(indexName);

        final ClusterState clusterState = clusterService().state();
        final Index index = clusterState.metadata().index(indexName).getIndex();
        final Map<String, List<ShardRouting>> shardsByNodes = shardRoutingsByNodes(clusterState, index);
        assertThat(shardsByNodes.values().stream().mapToInt(List::size).sum(), equalTo(numShards.totalNumShards));

        for (Map.Entry<String, List<ShardRouting>> shardsByNode : shardsByNodes.entrySet()) {
            final String nodeName = shardsByNode.getKey();
            final IndexFoldersDeletionListenerPlugin plugin = plugin(nodeName);
            assertTrue("Expecting no indices deleted on node " + nodeName, plugin.deletedIndices.isEmpty());
            assertTrue("Expecting no shards deleted on node " + nodeName, plugin.deletedShards.isEmpty());
        }

        final String stoppedNode = randomFrom(shardsByNodes.keySet());
        final Settings stoppedNodeDataPathSettings = internalCluster().dataPathSettings(stoppedNode);
        internalCluster().stopRandomNode(InternalTestCluster.nameFilter(stoppedNode));

        assertAcked(client().admin().indices().prepareDelete(indexName));

        final String restartedNode = internalCluster().startNode(stoppedNodeDataPathSettings);
        ensureClusterSizeConsistency();
        ensureClusterStateConsistency();
        assertPendingDeletesProcessed();

        final IndexFoldersDeletionListenerPlugin plugin = plugin(restartedNode);

        assertTrue("Listener should have been notified of deletion of index " + index + " on node " + restartedNode,
            plugin.deletedIndices.contains(index));
    }

    private Map<String, List<ShardRouting>> shardRoutingsByNodes(ClusterState clusterState, Index index) {
        final Map<String, List<ShardRouting>> map = new HashMap<>();
        for (ShardRouting shardRouting : clusterState.routingTable().index(index).shardsWithState(ShardRoutingState.STARTED)) {
            final String nodeName = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
            map.computeIfAbsent(nodeName, name -> new ArrayList<>()).add(shardRouting);
        }
        return map;
    }

    public static class IndexFoldersDeletionListenerPlugin extends Plugin implements IndexStorePlugin {

        final Set<Index> deletedIndices = ConcurrentCollections.newConcurrentSet();
        final Map<Index, List<ShardId>> deletedShards = ConcurrentCollections.newConcurrentMap();

        @Override
        public List<IndexFoldersDeletionListener> getIndexFoldersDeletionListeners() {
            return List.of(new IndexFoldersDeletionListener() {
                @Override
                public void beforeIndexFoldersDeleted(Index index, IndexSettings indexSettings, List<Path> indexPaths) {
                    deletedIndices.add(index);
                }

                @Override
                public void beforeShardFoldersDeleted(ShardId shardId, IndexSettings indexSettings, List<Path> shardPaths) {
                    deletedShards.computeIfAbsent(shardId.getIndex(), i -> new ArrayList<>()).add(shardId);
                }
            });
        }

        @Override
        public Map<String, DirectoryFactory> getDirectoryFactories() {
            return Collections.emptyMap();
        }
    }

    private static IndexFoldersDeletionListenerPlugin plugin(String nodeId) {
        final PluginsService pluginsService = internalCluster().getInstance(PluginsService.class, nodeId);
        final List<IndexFoldersDeletionListenerPlugin> plugins = pluginsService.filterPlugins(IndexFoldersDeletionListenerPlugin.class);
        assertThat(plugins, hasSize(1));
        return plugins.get(0);
    }

    private static void assertPendingDeletesProcessed() throws Exception {
        assertBusy(() -> {
            final Iterable<IndicesService> services = internalCluster().getDataNodeInstances(IndicesService.class);
            services.forEach(indicesService -> assertFalse(indicesService.hasUncompletedPendingDeletes()));
        });
    }
}
