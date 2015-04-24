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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.AbstractPlugin;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.*;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@ElasticsearchIntegrationTest.ClusterScope(scope = ElasticsearchIntegrationTest.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceTests extends ElasticsearchIntegrationTest {

    public static class Plugin extends AbstractPlugin {

        @Override
        public String name() {
            return "ClusterInfoServiceTests";
        }

        @Override
        public String description() {
            return "ClusterInfoServiceTests";
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

    static class InfoListener implements ClusterInfoService.Listener {
        final AtomicReference<CountDownLatch> collected = new AtomicReference<>(new CountDownLatch(1));
        volatile ClusterInfo lastInfo = null;

        @Override
        public void onNewInfo(ClusterInfo info) {
            lastInfo = info;
            CountDownLatch latch = collected.get();
            latch.countDown();
        }

        public void reset() {
            lastInfo = null;
            collected.set(new CountDownLatch(1));
        }

        public ClusterInfo get() throws InterruptedException {
            CountDownLatch latch = collected.get();
            if (!latch.await(10, TimeUnit.SECONDS)) {
                fail("failed to get a new cluster info");
            }
            return lastInfo;
        }
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return ImmutableSettings.builder()
                // manual collection or upon cluster forming.
                .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT, "1s")
                .put(TransportModule.TRANSPORT_SERVICE_TYPE_KEY, MockTransportService.class.getName())
                .put("plugin.types", Plugin.class.getName())
                .build();
    }

    @Test
    public void testClusterInfoServiceCollectsInformation() throws Exception {
        internalCluster().startNodesAsync(2,
                ImmutableSettings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "200ms").build())
                .get();
        assertAcked(prepareCreate("test").setSettings(settingsBuilder().put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL, 0).build()));
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        InfoListener listener = new InfoListener();
        infoService.addListener(listener);
        ClusterInfo info = listener.get();
        assertNotNull("info should not be null", info);
        Map<String, DiskUsage> usages = info.getNodeDiskUsages();
        Map<String, Long> shardSizes = info.getShardSizes();
        assertNotNull(usages);
        assertNotNull(shardSizes);
        assertThat("some usages are populated", usages.values().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (DiskUsage usage : usages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.getFreeBytes(), greaterThan(0L));
        }
        for (Long size : shardSizes.values()) {
            logger.info("--> shard size: {}", size);
            assertThat("shard size is greater than 0", size, greaterThan(0L));
        }
    }

    @Test
    public void testClusterInfoServiceInformationClearOnError() throws InterruptedException, ExecutionException {
        internalCluster().startNodesAsync(2,
                // manually control publishing
                ImmutableSettings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL, "60m").build())
                .get();
        prepareCreate("test").setSettings(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 1).get();
        ensureGreen("test");
        InternalTestCluster internalTestCluster = internalCluster();
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(ClusterInfoService.class, internalTestCluster.getMasterName());
        InfoListener listener = new InfoListener();
        infoService.addListener(listener);

        // get one healthy sample
        infoService.updateOnce();
        ClusterInfo info = listener.get();
        assertNotNull("failed to collect info", info);
        assertThat("some usages are populated", info.getNodeDiskUsages().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", info.getShardSizes().size(), greaterThan(0));


        MockTransportService mockTransportService = (MockTransportService) internalCluster().getInstance(TransportService.class, internalTestCluster.getMasterName());

        final AtomicBoolean timeout = new AtomicBoolean(false);
        final Set<String> blockedActions = ImmutableSet.of(NodesStatsAction.NAME, NodesStatsAction.NAME + "[n]", IndicesStatsAction.NAME, IndicesStatsAction.NAME + "[s]");
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
        listener.reset();
        infoService.updateOnce();
        info = listener.get();
        assertNotNull("info should not be null", info);
        // node info will time out both on the request level on the count down latch. this means
        // it is likely to update the node disk usage based on the one response that came be from local
        // node.
        assertThat(info.getNodeDiskUsages().size(), greaterThanOrEqualTo(1));
        // indices is guaranteed to time out on the latch, not updating anything.
        assertThat(info.getShardSizes().size(), greaterThan(1));

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
        listener.reset();
        infoService.updateOnce();
        info = listener.get();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeDiskUsages().size(), equalTo(0));
        assertThat(info.getShardSizes().size(), equalTo(0));

        // check we recover
        blockingActionFilter.blockActions();
        listener.reset();
        infoService.updateOnce();
        info = listener.get();
        assertNotNull("info should not be null", info);
        assertThat(info.getNodeDiskUsages().size(), equalTo(2));
        assertThat(info.getShardSizes().size(), greaterThan(0));

    }
}
