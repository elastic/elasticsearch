/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.admin.cluster.node.stats.TransportNodesStatsAction;
import org.elasticsearch.action.admin.cluster.node.usage.TransportNodeUsageStatsForThreadPoolsAction;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsAction;
import org.elasticsearch.action.support.ActionFilter;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.allocation.WriteLoadConstraintSettings;
import org.elasticsearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.seqno.GlobalCheckpointSyncAction;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RequestHandlerRegistry;
import org.elasticsearch.transport.TransportService;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableSet;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
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

    public void testClusterInfoIncludesNodeUsageStatsForThreadPools() {
        var settings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                randomBoolean()
                    ? WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
                    : WriteLoadConstraintSettings.WriteLoadDeciderStatus.LOW_THRESHOLD_ONLY
            )
            // Manually control cluster info refreshes
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m")
            .build();
        var masterName = internalCluster().startMasterOnlyNode(settings);
        var dataNodeName = internalCluster().startDataOnlyNode(settings);
        ensureStableCluster(2);
        assertEquals(internalCluster().getMasterName(), masterName);
        assertNotEquals(internalCluster().getMasterName(), dataNodeName);
        logger.info("---> master node: " + masterName + ", data node: " + dataNodeName);

        // Track when the data node receives a poll from the master for the write thread pool's stats.
        final MockTransportService dataNodeMockTransportService = MockTransportService.getInstance(dataNodeName);
        final CountDownLatch nodeThreadPoolStatsPolledByMaster = new CountDownLatch(1);
        dataNodeMockTransportService.addRequestHandlingBehavior(
            TransportNodeUsageStatsForThreadPoolsAction.NAME + "[n]",
            (handler, request, channel, task) -> {
                handler.messageReceived(request, channel, task);

                if (nodeThreadPoolStatsPolledByMaster.getCount() > 0) {
                    logger.info("---> Data node received a request for thread pool stats");
                }
                nodeThreadPoolStatsPolledByMaster.countDown();
            }
        );

        // Generate some writes to get some non-zero write thread pool stats.
        doALotOfDataNodeWrites();

        // Force a refresh of the ClusterInfo state to collect fresh info from the data nodes.
        final InternalClusterInfoService masterClusterInfoService = asInstanceOf(
            InternalClusterInfoService.class,
            internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class)
        );
        final ClusterInfo clusterInfo = ClusterInfoServiceUtils.refresh(masterClusterInfoService);

        // Verify that the data node received a request for thread pool stats.
        safeAwait(nodeThreadPoolStatsPolledByMaster);

        final Map<String, NodeUsageStatsForThreadPools> usageStatsForThreadPools = clusterInfo.getNodeUsageStatsForThreadPools();
        logger.info("---> Thread pool usage stats reported by data nodes to the master: " + usageStatsForThreadPools);
        assertThat(usageStatsForThreadPools.size(), equalTo(1)); // only stats from data nodes should be collected
        var dataNodeId = getNodeId(dataNodeName);
        var nodeUsageStatsForThreadPool = usageStatsForThreadPools.get(dataNodeId);
        assertNotNull(nodeUsageStatsForThreadPool);
        logger.info("---> Data node's thread pool stats: " + nodeUsageStatsForThreadPool);

        assertEquals(dataNodeId, nodeUsageStatsForThreadPool.nodeId());
        var writeThreadPoolStats = nodeUsageStatsForThreadPool.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
        assertNotNull("Expected to find stats for the WRITE thread pool", writeThreadPoolStats);
        assertThat(writeThreadPoolStats.totalThreadPoolThreads(), greaterThan(0));
        assertThat(writeThreadPoolStats.averageThreadPoolUtilization(), greaterThan(0f));
        assertThat(writeThreadPoolStats.maxThreadPoolQueueLatencyMillis(), greaterThanOrEqualTo(0L));
    }

    /**
     * The {@link TransportNodeUsageStatsForThreadPoolsAction} returns the max value of two kinds of queue latencies:
     * {@link TaskExecutionTimeTrackingEsThreadPoolExecutor#getMaxQueueLatencyMillisSinceLastPollAndReset()} and
     * {@link TaskExecutionTimeTrackingEsThreadPoolExecutor#peekMaxQueueLatencyInQueueMillis()}. The latter looks at currently queued tasks,
     * and the former tracks the queue latency of tasks when they are taken off of the queue to start execution.
     */
    public void testMaxQueueLatenciesInClusterInfo() throws Exception {
        var settings = Settings.builder()
            .put(
                WriteLoadConstraintSettings.WRITE_LOAD_DECIDER_ENABLED_SETTING.getKey(),
                WriteLoadConstraintSettings.WriteLoadDeciderStatus.ENABLED
            )
            // Manually control cluster info refreshes
            .put(InternalClusterInfoService.INTERNAL_CLUSTER_INFO_UPDATE_INTERVAL_SETTING.getKey(), "60m")
            .build();
        internalCluster().startMasterOnlyNode(settings);
        var dataNodeName = internalCluster().startDataOnlyNode(settings);
        ensureStableCluster(2);

        String indexName = randomIdentifier();
        final int numShards = randomIntBetween(1, 5);
        createIndex(indexName, Settings.builder().put(SETTING_NUMBER_OF_SHARDS, numShards).put(SETTING_NUMBER_OF_REPLICAS, 0).build());
        ensureGreen(indexName);

        // Global checkpoint sync actions are asynchronous. We cannot really tell exactly when they are completely off the
        // thread pool. To avoid busy waiting, we redirect them to the generic thread pool so that we have precise control
        // over the write thread pool for assertions.
        final MockTransportService mockTransportService = MockTransportService.getInstance(dataNodeName);
        final var originalRegistry = mockTransportService.transport()
            .getRequestHandlers()
            .getHandler(GlobalCheckpointSyncAction.ACTION_NAME + "[p]");
        mockTransportService.transport()
            .getRequestHandlers()
            .forceRegister(
                new RequestHandlerRegistry<>(
                    GlobalCheckpointSyncAction.ACTION_NAME + "[p]",
                    in -> null, // no need to deserialize the request since it's local
                    mockTransportService.getTaskManager(),
                    originalRegistry.getHandler(),
                    mockTransportService.getThreadPool().executor(ThreadPool.Names.GENERIC),
                    true,
                    true
                )
            );

        // Block indexing on the data node by submitting write thread pool tasks equal to the number of write threads.
        var barrier = blockDataNodeIndexing(dataNodeName);
        try {
            // Arbitrary number of tasks, which will queue because all the write threads are occupied already, greater than one: only
            // strictly need a single task to occupy the queue.
            int numberOfTasks = randomIntBetween(1, 5);
            Thread[] threadsToJoin = new Thread[numberOfTasks];
            for (int i = 0; i < numberOfTasks; ++i) {
                threadsToJoin[i] = startParallelSingleWrite(indexName);
            }

            // Reach into the data node's write thread pool to check that tasks have reached the queue.
            var dataNodeThreadPool = internalCluster().getInstance(ThreadPool.class, dataNodeName);
            var writeExecutor = dataNodeThreadPool.executor(ThreadPool.Names.WRITE);
            assert writeExecutor instanceof TaskExecutionTimeTrackingEsThreadPoolExecutor;
            var trackingWriteExecutor = (TaskExecutionTimeTrackingEsThreadPoolExecutor) writeExecutor;
            assertBusy(
                // Wait for the parallel threads' writes to get queued in the write thread pool.
                () -> assertThat(
                    "Write thread pool dump: " + trackingWriteExecutor,
                    trackingWriteExecutor.peekMaxQueueLatencyInQueueMillis(),
                    greaterThan(0L)
                )
            );

            // Force a refresh of the ClusterInfo state to collect fresh info from the data node.
            final ClusterInfo clusterInfo = refreshClusterInfo();
            assertNotNull(clusterInfo);

            // Since tasks are actively queued right now, #peekMaxQueueLatencyInQueue, which is called from the
            // TransportNodeUsageStatsForThreadPoolsAction that a ClusterInfoService refresh initiates, should return a max queue
            // latency > 0;
            {
                final Map<String, NodeUsageStatsForThreadPools> usageStatsForThreadPools = clusterInfo.getNodeUsageStatsForThreadPools();
                logger.info("---> Thread pool usage stats reported by data nodes to the master: " + usageStatsForThreadPools);
                assertThat(usageStatsForThreadPools.size(), equalTo(1)); // only stats from data node should be collected
                var dataNodeId = getNodeId(dataNodeName);
                var nodeUsageStatsForThreadPool = usageStatsForThreadPools.get(dataNodeId);
                assertNotNull(nodeUsageStatsForThreadPool);
                logger.info("---> Data node's thread pool stats: " + nodeUsageStatsForThreadPool);

                assertEquals(dataNodeId, nodeUsageStatsForThreadPool.nodeId());
                var writeThreadPoolStats = nodeUsageStatsForThreadPool.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
                assertNotNull("Expected to find stats for the WRITE thread pool", writeThreadPoolStats);
                assertThat(writeThreadPoolStats.totalThreadPoolThreads(), greaterThan(0));
                assertThat(writeThreadPoolStats.averageThreadPoolUtilization(), greaterThanOrEqualTo(0f));
                assertThat(writeThreadPoolStats.maxThreadPoolQueueLatencyMillis(), greaterThan(0L));
            }

            // Now release the data node's indexing, and drain the queued tasks. Max queue latency of executed (not queued) tasks is reset
            // by each TransportNodeUsageStatsForThreadPoolsAction call (#getMaxQueueLatencyMillisSinceLastPollAndReset), so the new queue
            // latencies will be present in the next call. There will be nothing in the queue to peek at now, so the result of the max
            // queue latency result in TransportNodeUsageStatsForThreadPoolsAction will reflect
            // #getMaxQueueLatencyMillisSinceLastPollAndReset and not #peekMaxQueueLatencyInQueue.
            barrier.await();
            for (int i = 0; i < numberOfTasks; ++i) {
                threadsToJoin[i].join();
            }
            Arrays.stream(threadsToJoin).forEach(thread -> assertFalse(thread.isAlive()));
            // Monitor the write executor on the data node to try and determine when the backlog of tasks
            // has been fully drained (and
            // {@link org.elasticsearch.common.util.concurrent.TaskExecutionTimeTrackingEsThreadPoolExecutor.afterExecute()}
            // has been called). This is probably not foolproof, so worth investigating if we see non-zero utilization numbers
            // after the next poll. See https://github.com/elastic/elasticsearch/issues/134500
            assertBusy(() -> assertThat(trackingWriteExecutor.getActiveCount(), equalTo(0)));

            assertThat(
                "Unexpectedly found a task queued for the write thread pool. Write thread pool dump: " + trackingWriteExecutor,
                trackingWriteExecutor.peekMaxQueueLatencyInQueueMillis(),
                equalTo(0L)
            );

            final ClusterInfo nextClusterInfo = refreshClusterInfo();
            {
                final Map<String, NodeUsageStatsForThreadPools> usageStatsForThreadPools = nextClusterInfo
                    .getNodeUsageStatsForThreadPools();
                logger.info("---> Thread pool usage stats reported by data nodes to the master: " + usageStatsForThreadPools);
                assertThat(usageStatsForThreadPools.size(), equalTo(1)); // only stats from data nodes should be collected
                var dataNodeId = getNodeId(dataNodeName);
                var nodeUsageStatsForThreadPool = usageStatsForThreadPools.get(dataNodeId);
                assertNotNull(nodeUsageStatsForThreadPool);
                logger.info("---> Data node's thread pool stats: " + nodeUsageStatsForThreadPool);

                assertEquals(dataNodeId, nodeUsageStatsForThreadPool.nodeId());
                var writeThreadPoolStats = nodeUsageStatsForThreadPool.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
                assertNotNull("Expected to find stats for the WRITE thread pool", writeThreadPoolStats);
                assertThat(writeThreadPoolStats.totalThreadPoolThreads(), greaterThan(0));
                assertThat(writeThreadPoolStats.averageThreadPoolUtilization(), greaterThan(0f));
                assertThat(writeThreadPoolStats.maxThreadPoolQueueLatencyMillis(), greaterThanOrEqualTo(0L));
            }
        } finally {
            // Ensure that the write threads have been released by signalling an interrupt on any callers waiting on the barrier. If the
            // callers have already all been successfully released, then there will be nothing left to interrupt.
            logger.info("---> Ensuring release of the barrier on write thread pool tasks");
            barrier.reset();
        }

        // Now that there's nothing in the queue, and no activity since the last ClusterInfo refresh, the max latency returned should be
        // zero. Verify this.
        final ClusterInfo clusterInfo = refreshClusterInfo();
        {
            final Map<String, NodeUsageStatsForThreadPools> usageStatsForThreadPools = clusterInfo.getNodeUsageStatsForThreadPools();
            logger.info("---> Thread pool usage stats reported by data nodes to the master: " + usageStatsForThreadPools);
            assertThat(usageStatsForThreadPools.size(), equalTo(1)); // only stats from data nodes should be collected
            var dataNodeId = getNodeId(dataNodeName);
            var nodeUsageStatsForThreadPool = usageStatsForThreadPools.get(dataNodeId);
            assertNotNull(nodeUsageStatsForThreadPool);
            logger.info("---> Data node's thread pool stats: " + nodeUsageStatsForThreadPool);

            assertEquals(dataNodeId, nodeUsageStatsForThreadPool.nodeId());
            var writeThreadPoolStats = nodeUsageStatsForThreadPool.threadPoolUsageStatsMap().get(ThreadPool.Names.WRITE);
            assertNotNull("Expected to find stats for the WRITE thread pool", writeThreadPoolStats);
            assertThat(writeThreadPoolStats.totalThreadPoolThreads(), greaterThan(0));
            assertThat(writeThreadPoolStats.averageThreadPoolUtilization(), equalTo(0f));
            assertThat(writeThreadPoolStats.maxThreadPoolQueueLatencyMillis(), equalTo(0L));
        }
    }

    /**
     * Do some writes to create some write thread pool activity.
     */
    private void doALotOfDataNodeWrites() {
        final String indexName = randomIdentifier();
        final int randomInt = randomIntBetween(500, 1000);
        for (int i = 0; i < randomInt; i++) {
            index(indexName, Integer.toString(i), Collections.singletonMap("foo", "bar"));
        }
    }

    /**
     * Starts a single index request on a parallel thread and returns the reference so {@link Thread#join()} can be called eventually.
     */
    private Thread startParallelSingleWrite(String indexName) {
        Thread running = new Thread(() -> doSingleWrite(indexName));
        running.start();
        return running;
    }

    private void doSingleWrite(String indexName) {
        final int randomId = randomIntBetween(500, 1000);
        index(indexName, Integer.toString(randomId), Collections.singletonMap("foo", "bar"));
    }
}
