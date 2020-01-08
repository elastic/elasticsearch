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

package org.elasticsearch.cluster;

import com.carrotsearch.hppc.cursors.ObjectCursor;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.common.util.set.Sets.newHashSet;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin implements ActionPlugin {

        private final BlockingActionFilter blockingActionFilter;

        public TestPlugin() {
            blockingActionFilter = new BlockingActionFilter();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(blockingActionFilter);
        }
    }

    public static class BlockingActionFilter extends org.elasticsearch.action.support.ActionFilter.Simple {
        private Set<String> blockedActions = emptySet();

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener<?> listener) {
            if (blockedActions.contains(action)) {
                throw new ElasticsearchException("force exception on [" + action + "]");
            }
            return true;
        }

        @Override
        public int order() {
            return 0;
        }

        public void blockActions(String... actions) {
            blockedActions = unmodifiableSet(newHashSet(actions));
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(TestPlugin.class, MockTransportService.TestPlugin.class);
    }

    private void setClusterInfoTimeout(String timeValue) {
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(Settings.builder()
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), timeValue).build()));
    }

    public void testClusterInfoServiceCollectsInformation() {
        internalCluster().startNodes(2);
        assertAcked(prepareCreate("test").setSettings(Settings.builder()
            .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)
            .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE).build()));
        if (randomBoolean()) {
            assertAcked(client().admin().indices().prepareClose("test"));
        }
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster
            .getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        infoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        infoService.onMaster();
        ClusterInfo info = infoService.refresh();
        assertNotNull("info should not be null", info);
        ImmutableOpenMap<String, DiskUsage> leastUsages = info.getNodeLeastAvailableDiskUsages();
        ImmutableOpenMap<String, DiskUsage> mostUsages = info.getNodeMostAvailableDiskUsages();
        ImmutableOpenMap<String, Long> shardSizes = info.shardSizes;
        assertNotNull(leastUsages);
        assertNotNull(shardSizes);
        assertThat("some usages are populated", leastUsages.values().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (ObjectCursor<DiskUsage> usage : leastUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat("usage has be retrieved", usage.value.getFreeBytes(), greaterThan(0L));
        }
        for (ObjectCursor<DiskUsage> usage : mostUsages.values()) {
            logger.info("--> usage: {}", usage.value);
            assertThat("usage has be retrieved", usage.value.getFreeBytes(), greaterThan(0L));
        }
        for (ObjectCursor<Long> size : shardSizes.values()) {
            logger.info("--> shard size: {}", size.value);
            assertThat("shard size is greater than 0", size.value, greaterThanOrEqualTo(0L));
        }
        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getMasterName());
        ClusterState state = clusterService.state();
        for (ShardRouting shard : state.routingTable().allShards()) {
            String dataPath = info.getDataPath(shard);
            assertNotNull(dataPath);

            String nodeId = shard.currentNodeId();
            DiscoveryNode discoveryNode = state.getNodes().get(nodeId);
            IndicesService indicesService = internalTestCluster.getInstance(IndicesService.class, discoveryNode.getName());
            IndexService indexService = indicesService.indexService(shard.index());
            IndexShard indexShard = indexService.getShardOrNull(shard.id());
            assertEquals(indexShard.shardPath().getRootDataPath().toString(), dataPath);
        }
    }

    public void testClusterInfoServiceInformationClearOnError() {
        internalCluster().startNodes(2,
            // manually control publishing
            Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m").build());
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1)).get();
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster
            .getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        // get one healthy sample
        ClusterInfo info = infoService.refresh();
        assertNotNull("failed to collect info", info);
        assertThat("some usages are populated", info.getNodeLeastAvailableDiskUsages().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", info.shardSizes.size(), greaterThan(0));


        MockTransportService mockTransportService = (MockTransportService) internalCluster()
            .getInstance(TransportService.class, internalTestCluster.getMasterName());

        final AtomicBoolean timeout = new AtomicBoolean(false);
        final Set<String> blockedActions = newHashSet(NodesStatsAction.NAME, NodesStatsAction.NAME + "[n]",
            IndicesStatsAction.NAME, IndicesStatsAction.NAME + "[n]");
        // drop all outgoing stats requests to force a timeout.
        for (DiscoveryNode node : internalTestCluster.clusterService().state().getNodes()) {
            mockTransportService.addSendBehavior(internalTestCluster.getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (blockedActions.contains(action)) {
                        if (timeout.get()) {
                            logger.info("dropping [{}] to [{}]", action, node);
                            return;
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                });
        }

        setClusterInfoTimeout("1s");
        // timeouts shouldn't clear the info
        timeout.set(true);
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        // node info will time out both on the request level on the count down latch. this means
        // it is likely to update the node disk usage based on the one response that came be from local
        // node.
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), greaterThanOrEqualTo(1));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), greaterThanOrEqualTo(1));
        // indices is guaranteed to time out on the latch, not updating anything.
        assertThat(info.shardSizes.size(), greaterThan(1));

        // now we cause an exception
        timeout.set(false);
        ActionFilters actionFilters = internalTestCluster.getInstance(ActionFilters.class, internalTestCluster.getMasterName());
        BlockingActionFilter blockingActionFilter = null;
        for (ActionFilter filter : actionFilters.filters()) {
            if (filter instanceof BlockingActionFilter) {
                blockingActionFilter = (BlockingActionFilter) filter;
                break;
            }
        }

        assertNotNull("failed to find BlockingActionFilter", blockingActionFilter);
        blockingActionFilter.blockActions(blockedActions.toArray(Strings.EMPTY_ARRAY));
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), equalTo(0));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), equalTo(0));
        assertThat(info.shardSizes.size(), equalTo(0));

        // check we recover
        blockingActionFilter.blockActions();
        setClusterInfoTimeout("15s");
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.shardSizes.size(), greaterThan(0));

    }
}
