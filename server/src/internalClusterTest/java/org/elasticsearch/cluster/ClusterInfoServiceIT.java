/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SystemIndexPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration tests for the ClusterInfoService collecting information
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ClusterInfoServiceIT extends ESIntegTestCase {

    private static final String TEST_SYSTEM_INDEX_NAME = ".test-cluster-info-system-index";

    public static class TestPlugin extends Plugin implements ActionPlugin, SystemIndexPlugin {

        private final BlockingActionFilter blockingActionFilter;

        public TestPlugin() {
            blockingActionFilter = new BlockingActionFilter();
        }

        @Override
        public List<ActionFilter> getActionFilters() {
            return singletonList(blockingActionFilter);
        }

        @Override
        public Collection<SystemIndexDescriptor> getSystemIndexDescriptors(Settings settings) {
            return List.of(SystemIndexDescriptorUtils.createUnmanaged(TEST_SYSTEM_INDEX_NAME + "*", "Test system index"));
        }

        @Override
        public String getFeatureName() {
            return ClusterInfoServiceIT.class.getSimpleName();
        }

        @Override
        public String getFeatureDescription() {
            return "test plugin";
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
        updateClusterSettings(Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_TIMEOUT_SETTING.getKey(), timeValue));
    }

    public void testClusterInfoServiceCollectsInformation() {
        internalCluster().startNodes(2);

        final String indexName = randomBoolean() ? randomAlphaOfLength(5).toLowerCase(Locale.ROOT) : TEST_SYSTEM_INDEX_NAME;
        assertAcked(
            prepareCreate(indexName).setSettings(
                Settings.builder()
                    .put(Store.INDEX_STORE_STATS_REFRESH_INTERVAL_SETTING.getKey(), 0)
                    .put(EnableAllocationDecider.INDEX_ROUTING_REBALANCE_ENABLE_SETTING.getKey(), EnableAllocationDecider.Rebalance.NONE)
                    .put(IndexMetadata.SETTING_INDEX_HIDDEN, indexName.equals(TEST_SYSTEM_INDEX_NAME) || randomBoolean())
                    .build()
            )
        );
        if (randomBoolean()) {
            assertAcked(indicesAdmin().prepareClose(indexName));
        }
        ensureGreen(indexName);
        InternalTestCluster internalTestCluster = internalCluster();
        // Get the cluster info service on the master node
        final InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(
            ClusterInfoService.class,
            internalTestCluster.getMasterName()
        );
        infoService.setUpdateFrequency(TimeValue.timeValueMillis(200));
        ClusterInfo info = ClusterInfoServiceUtils.refresh(infoService);
        assertNotNull("info should not be null", info);
        Map<String, DiskUsage> leastUsages = info.getNodeLeastAvailableDiskUsages();
        Map<String, DiskUsage> mostUsages = info.getNodeMostAvailableDiskUsages();
        Map<String, Long> shardSizes = info.shardSizes;
        Map<ShardId, Long> shardDataSetSizes = info.shardDataSetSizes;
        assertNotNull(leastUsages);
        assertNotNull(shardSizes);
        assertNotNull(shardDataSetSizes);
        assertThat("some usages are populated", leastUsages.values().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", shardSizes.values().size(), greaterThan(0));
        for (DiskUsage usage : leastUsages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.freeBytes(), greaterThan(0L));
        }
        for (DiskUsage usage : mostUsages.values()) {
            logger.info("--> usage: {}", usage);
            assertThat("usage has be retrieved", usage.freeBytes(), greaterThan(0L));
        }
        for (Long size : shardSizes.values()) {
            logger.info("--> shard size: {}", size);
            assertThat("shard size is greater than 0", size, greaterThanOrEqualTo(0L));
        }
        for (Long size : shardDataSetSizes.values()) {
            assertThat("shard data set size is greater than 0", size, greaterThanOrEqualTo(0L));
        }

        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getMasterName());
        ClusterState state = clusterService.state();
        for (ShardRouting shard : state.routingTable().allShardsIterator()) {
            String dataPath = info.getDataPath(shard);
            assertNotNull(dataPath);

            String nodeId = shard.currentNodeId();
            DiscoveryNode discoveryNode = state.getNodes().get(nodeId);
            IndicesService indicesService = internalTestCluster.getInstance(IndicesService.class, discoveryNode.getName());
            IndexService indexService = indicesService.indexService(shard.index());
            IndexShard indexShard = indexService.getShardOrNull(shard.id());
            assertEquals(indexShard.shardPath().getRootDataPath().toString(), dataPath);

            assertTrue(info.getReservedSpace(nodeId, dataPath).containsShardId(shard.shardId()));
        }
    }

    public void testClusterInfoServiceInformationClearOnError() {
        internalCluster().startNodes(
            2,
            // manually control publishing
            Settings.builder().put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m").build()
        );
        prepareCreate("test").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)).get();
        ensureGreen("test");

        final IndexShardRoutingTable indexShardRoutingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .getRoutingTable()
            .index("test")
            .shard(0);
        final List<ShardRouting> shardRoutings = new ArrayList<>(indexShardRoutingTable.size());
        for (int copy = 0; copy < indexShardRoutingTable.size(); copy++) {
            shardRoutings.add(indexShardRoutingTable.shard(copy));
        }

        InternalTestCluster internalTestCluster = internalCluster();
        InternalClusterInfoService infoService = (InternalClusterInfoService) internalTestCluster.getInstance(
            ClusterInfoService.class,
            internalTestCluster.getMasterName()
        );

        // get one healthy sample
        ClusterInfo originalInfo = ClusterInfoServiceUtils.refresh(infoService);
        assertNotNull("failed to collect info", originalInfo);
        assertThat("some usages are populated", originalInfo.getNodeLeastAvailableDiskUsages().size(), Matchers.equalTo(2));
        assertThat("some shard sizes are populated", originalInfo.shardSizes.size(), greaterThan(0));
        assertThat("some shard data set sizes are populated", originalInfo.shardDataSetSizes.size(), greaterThan(0));
        for (ShardRouting shardRouting : shardRoutings) {
            assertThat("size for shard " + shardRouting + " found", originalInfo.getShardSize(shardRouting), notNullValue());
            assertThat(
                "data set size for shard " + shardRouting + " found",
                originalInfo.getShardDataSetSize(shardRouting.shardId()).isPresent(),
                is(true)
            );
        }

        final var masterTransportService = MockTransportService.getInstance(internalTestCluster.getMasterName());

        final AtomicBoolean timeout = new AtomicBoolean(false);
        final Set<String> blockedActions = newHashSet(
            TransportNodesStatsAction.TYPE.name(),
            TransportNodesStatsAction.TYPE.name() + "[n]",
            IndicesStatsAction.NAME,
            IndicesStatsAction.NAME + "[n]"
        );
        // drop all outgoing stats requests to force a timeout.
        for (DiscoveryNode node : internalTestCluster.clusterService().state().getNodes()) {
            masterTransportService.addSendBehavior(
                internalTestCluster.getInstance(TransportService.class, node.getName()),
                (connection, requestId, action, request, options) -> {
                    if (blockedActions.contains(action)) {
                        if (timeout.get()) {
                            logger.info("dropping [{}] to [{}]", action, node);
                            return;
                        }
                    }
                    connection.sendRequest(requestId, action, request, options);
                }
            );
        }

        setClusterInfoTimeout("1s");
        // timeouts shouldn't clear the info
        timeout.set(true);
        final ClusterInfo infoAfterTimeout = ClusterInfoServiceUtils.refresh(infoService);
        assertNotNull("info should not be null", infoAfterTimeout);
        // node stats from remote nodes will time out, but the local node will be included
        assertThat(infoAfterTimeout.getNodeLeastAvailableDiskUsages().size(), equalTo(1));
        assertThat(infoAfterTimeout.getNodeMostAvailableDiskUsages().size(), equalTo(1));
        // indices stats from remote nodes will time out, but the local node's shard will be included
        assertThat(infoAfterTimeout.shardSizes.size(), greaterThan(0));
        assertThat(infoAfterTimeout.shardDataSetSizes.size(), greaterThan(0));
        assertThat(shardRoutings.stream().filter(shardRouting -> infoAfterTimeout.getShardSize(shardRouting) != null).toList(), hasSize(1));
        assertThat(
            shardRoutings.stream()
                .map(ShardRouting::shardId)
                .distinct()
                .filter(shard -> infoAfterTimeout.getShardDataSetSize(shard).isPresent())
                .toList(),
            hasSize(1)
        );

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
        final ClusterInfo infoAfterException = ClusterInfoServiceUtils.refresh(infoService);
        assertNotNull("info should not be null", infoAfterException);
        assertThat(infoAfterException.getNodeLeastAvailableDiskUsages().size(), equalTo(0));
        assertThat(infoAfterException.getNodeMostAvailableDiskUsages().size(), equalTo(0));
        assertThat(infoAfterException.shardSizes.size(), equalTo(0));
        assertThat(infoAfterException.shardDataSetSizes.size(), equalTo(0));
        assertThat(infoAfterException.reservedSpace.size(), equalTo(0));

        // check we recover
        blockingActionFilter.blockActions();
        setClusterInfoTimeout("15s");
        final ClusterInfo infoAfterRecovery = ClusterInfoServiceUtils.refresh(infoService);
        assertNotNull("info should not be null", infoAfterRecovery);
        assertThat(infoAfterRecovery.getNodeLeastAvailableDiskUsages().size(), equalTo(2));
        assertThat(infoAfterRecovery.getNodeMostAvailableDiskUsages().size(), equalTo(2));
        assertThat(infoAfterRecovery.shardSizes.size(), greaterThan(0));
        assertThat(infoAfterRecovery.shardDataSetSizes.size(), greaterThan(0));
        for (ShardRouting shardRouting : shardRoutings) {
            assertThat("size for shard " + shardRouting + " found", originalInfo.getShardSize(shardRouting), notNullValue());
        }

        RoutingTable routingTable = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT)
            .clear()
            .setRoutingTable(true)
            .get()
            .getState()
            .routingTable();
        for (ShardRouting shard : routingTable.allShardsIterator()) {
            assertTrue(
                infoAfterRecovery.getReservedSpace(shard.currentNodeId(), infoAfterRecovery.getDataPath(shard))
                    .containsShardId(shard.shardId())
            );
        }
    }
}
