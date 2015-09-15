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

import com.google.common.collect.ImmutableSet;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionModule;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.RoutingNodes;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.common.settings.Settings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceIT extends ESIntegTestCase {

    public static class TestPlugin extends Plugin {

        @Override
        public String name() {
            return "ClusterInfoServiceIT";
        }

        @Override
        public String description() {
            return "ClusterInfoServiceIT";
        }

        public void onModule(ActionModule module) {
            module.registerFilter(BlockingActionFilter.class);
        }
    }

    public static class BlockingActionFilter extends org.elasticsearch.action.support.ActionFilter.Simple {

        ImmutableSet<String> blockedActions = ImmutableSet.of();

        @Inject
        public BlockingActionFilter(Settings settings) {
            super(settings);
        }

        @Override
        protected boolean apply(String action, ActionRequest request, ActionListener listener) {
            if (blockedActions.contains(action)) {
                throw new ElasticsearchException("force exception on [" + action + "]");
            }
            return true;
        }

        @Override
        protected boolean apply(String action, ActionResponse response, ActionListener listener) {
            return true;
        }

        @Override
        public int order() {
            return 0;
        }

        public void blockActions(String... actions) {
            blockedActions = ImmutableSet.copyOf(actions);
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                // manual collection or upon cluster forming.
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT, "1s")
                .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return pluginList(TestPlugin.class,
                MockTransportService.TestPlugin.class);
    }

    @Test
    public void testClusterInfoServiceCollectsInformation() throws Exception {
        internalCluster().startNodesAsync(2,
                Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "200ms").build())
                .get();
        assertAcked(prepareCreate("test").setSettings(settingsBuilder()
                .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL, 0)
                .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE, EnableAllocationDecider.Rebalance.NONE).build()));
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        ClusterInfo info = infoService.refresh();
        assertNotNull("info should not be null", info);
        final Map<String, DiskUsage> leastUsages = info.getNodeLeastAvailableDiskUsages();
        final Map<String, DiskUsage> mostUsages = info.getNodeMostAvailableDiskUsages();
        final Map<String, Long> shardSizes = info.shardSizes;
        assertNotNull(leastUsages);
        assertNotNull(shardSizes);
        assertThat("some usages are populated", leastUsages.values().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (DiskUsage usage : leastUsages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.getFreeBytes(), greaterThan(0L));
        }
        for (DiskUsage usage : mostUsages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.getFreeBytes(), greaterThanOrEqualTo(0L));
        }
        for (Long size : shardSizes.values()) {
            logger.info("--> shard size: {}", size);
            assertThat("shard size is greater than 0", size, greaterThan(0L));
        }
        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getMasterName());
        ClusterState state = clusterService.state();
        RoutingNodes routingNodes = state.getRoutingNodes();
        for (ShardRouting shard : routingNodes.getRoutingTable().allShards()) {
            String dataPath = info.getDataPath(shard);
            assertNotNull(dataPath);

            String nodeId = shard.currentNodeId();
            DiscoveryNode discoveryNode = state.getNodes().get(nodeId);
            IndicesService indicesService = internalTestCluster.getInstance(IndicesService.class, discoveryNode.getName());
            IndexService indexService = indicesService.indexService(shard.index());
            IndexShard indexShard = indexService.shard(shard.id());
            assertEquals(indexShard.shardPath().getRootDataPath().toString(), dataPath);
        }

    }

    @Test
    public void testClusterInfoServiceInformationClearOnError() throws InterruptedException, ExecutionException {
        internalCluster().startNodesAsync(2,
                // manually control publishing
                Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "60m").build())
                .get();
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        // get one healthy sample
        ClusterInfo info = infoService.refresh();
        assertNotNull("failed to collect info", info);
        assertThat("some usages are populated", info.getNodeLeastAvailableDiskUsages().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", info.shardSizes.size(), greaterThan(0));


        MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, internalTestCluster.getMasterName());

        final AtomicBoolean timeout = new AtomicBoolean(false);
        final Set<String> blockedActions = ImmutableSet.of(NodesStatsAction.NAME, NodesStatsAction.NAME + "[n]", IndicesStatsAction.NAME, IndicesStatsAction.NAME + "[n]");
        // drop all outgoing stats requests to force a timeout.
        for (DiscoveryNode node : internalTestCluster.clusterService().state().getNodes()) {
            mockTransportService.addDelegate(node, new MockTransportService.DelegateTransport(mockTransportService.original()) {
                @Override
                public void sendRequest(DiscoveryNode node, long requestId, String action, TransportRequest request,
                                        TransportRequestOptions options) throws IOException, TransportException {
                    if (blockedActions.contains(action)) {
                        if (timeout.get()) {
                            logger.info("dropping [{}] to [{}]", action, node);
                            return;
                        }
                    }
                    super.sendRequest(node, requestId, action, request, options);
                }
            });
        }

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
        info = infoService.refresh();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeLeastAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.getNodeMostAvailableDiskUsages().size(), equalTo(2));
        assertThat(info.shardSizes.size(), greaterThan(0));

    }
}
