/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.cluster.DiskUsageIntegTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.allocation.DiskThresholdSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.BeforeClass;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.stream.IntStream;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class MergeWithLowDiskSpaceIT extends DiskUsageIntegTestCase {
    protected static long MERGE_DISK_HIGH_WATERMARK_BYTES;

    @BeforeClass
    public static void setAvailableDiskSpaceBufferLimit() {
        // this has to be big in order to potentially accommodate the disk space for a few 100s of docs and a few merges,
        // because of the latency to process used disk space updates, and also because we cannot reliably separate indexing from merging
        // operations at this high abstraction level (merging is triggered more or less automatically in the background)
        MERGE_DISK_HIGH_WATERMARK_BYTES = randomLongBetween(10_000_000L, 20_000_000L);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        List<Class<? extends Plugin>> nodePluginsList = new ArrayList<>(super.nodePlugins());
        nodePluginsList.add(TestTelemetryPlugin.class);
        return nodePluginsList;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // only the threadpool-based merge scheduler has the capability to block merges when disk space is insufficient
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // the very short disk space polling interval ensures timely blocking of merges
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_CHECK_INTERVAL_SETTING.getKey(), "10ms")
            // merges pile up more easily when there's only a few threads executing them
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), randomIntBetween(1, 2))
            .put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), MERGE_DISK_HIGH_WATERMARK_BYTES + "b")
            // let's not worry about allocation watermarks (e.g. read-only shards) in this test suite
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "0b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "0b")
            .build();
    }

    public void testShardCloseWhenDiskSpaceInsufficient() throws Exception {
        String node = internalCluster().startNode();
        setTotalSpace(node, Long.MAX_VALUE);
        var indicesService = internalCluster().getInstance(IndicesService.class, node);
        ensureStableCluster(1);
        // create index
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0).put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).build()
        );
        // do some indexing
        indexRandom(
            false,
            false,
            false,
            false,
            IntStream.range(1, randomIntBetween(2, 10))
                .mapToObj(i -> prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                .toList()
        );
        // get current disk space usage
        IndicesStatsResponse stats = indicesAdmin().prepareStats().clear().setStore(true).get();
        long usedDiskSpaceAfterIndexing = stats.getTotal().getStore().sizeInBytes();
        // restrict the total disk space such that the next merge does not have sufficient disk space
        long insufficientTotalDiskSpace = usedDiskSpaceAfterIndexing + MERGE_DISK_HIGH_WATERMARK_BYTES - randomLongBetween(1L, 10L);
        setTotalSpace(node, insufficientTotalDiskSpace);
        // node stats' FS stats should report that there is insufficient disk space available
        assertBusy(() -> {
            NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setFs(true).get();
            assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));
            NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
            assertThat(nodeStats.getFs().getTotal().getTotal().getBytes(), equalTo(insufficientTotalDiskSpace));
            assertThat(nodeStats.getFs().getTotal().getAvailable().getBytes(), lessThan(MERGE_DISK_HIGH_WATERMARK_BYTES));
        });
        while (true) {
            // maybe trigger a merge (this still depends on the merge policy, i.e. it is not 100% guaranteed)
            assertNoFailures(indicesAdmin().prepareForceMerge(indexName).get());
            // keep indexing and ask for merging until node stats' threadpool stats reports enqueued merges,
            // and the merge executor says they're blocked due to insufficient disk space if (nodesStatsResponse.getNodes()
            NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setThreadPool(true).get();
            if (nodesStatsResponse.getNodes()
                .getFirst()
                .getThreadPool()
                .stats()
                .stream()
                .filter(s -> ThreadPool.Names.MERGE.equals(s.name()))
                .findAny()
                .get()
                .queue() > 0
                && indicesService.getThreadPoolMergeExecutorService().isMergingBlockedDueToInsufficientDiskSpace()) {
                break;
            }
            // more indexing
            indexRandom(
                false,
                false,
                false,
                false,
                IntStream.range(1, randomIntBetween(2, 10))
                    .mapToObj(i -> prepareIndex(indexName).setSource("another_field", randomAlphaOfLength(50)))
                    .toList()
            );
        }
        // now delete the index in this state, i.e. with merges enqueued and blocked
        assertAcked(indicesAdmin().prepareDelete(indexName).get());
        // index should now be gone
        assertBusy(() -> {
            expectThrows(
                IndexNotFoundException.class,
                () -> indicesAdmin().prepareGetIndex(TEST_REQUEST_TIMEOUT).setIndices(indexName).get()
            );
        });
        assertBusy(() -> {
            // merge thread pool should be done with the enqueue merge tasks
            NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setThreadPool(true).get();
            assertThat(
                nodesStatsResponse.getNodes()
                    .getFirst()
                    .getThreadPool()
                    .stats()
                    .stream()
                    .filter(s -> ThreadPool.Names.MERGE.equals(s.name()))
                    .findAny()
                    .get()
                    .queue(),
                equalTo(0)
            );
            // and the merge executor should also report that merging is done now
            assertFalse(indicesService.getThreadPoolMergeExecutorService().isMergingBlockedDueToInsufficientDiskSpace());
            assertTrue(indicesService.getThreadPoolMergeExecutorService().allDone());
        });
    }

    public void testForceMergeIsBlockedThenUnblocked() throws Exception {
        String node = internalCluster().startNode();
        ensureStableCluster(1);
        setTotalSpace(node, Long.MAX_VALUE);
        ThreadPoolMergeExecutorService threadPoolMergeExecutorService = internalCluster().getInstance(IndicesService.class, node)
            .getThreadPoolMergeExecutorService();
        TestTelemetryPlugin testTelemetryPlugin = getTelemetryPlugin(node);
        // create some index
        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                .build()
        );
        // get current disk space usage (for all indices on the node)
        IndicesStatsResponse stats = indicesAdmin().prepareStats().clear().setStore(true).get();
        long usedDiskSpaceAfterIndexing = stats.getTotal().getStore().sizeInBytes();
        // restrict the total disk space such that the next merge does not have sufficient disk space
        long insufficientTotalDiskSpace = usedDiskSpaceAfterIndexing + MERGE_DISK_HIGH_WATERMARK_BYTES - randomLongBetween(1L, 10L);
        setTotalSpace(node, insufficientTotalDiskSpace);
        // node stats' FS stats should report that there is insufficient disk space available
        assertBusy(() -> {
            NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats().setFs(true).get();
            assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));
            NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
            assertThat(nodeStats.getFs().getTotal().getTotal().getBytes(), equalTo(insufficientTotalDiskSpace));
            assertThat(nodeStats.getFs().getTotal().getAvailable().getBytes(), lessThan(MERGE_DISK_HIGH_WATERMARK_BYTES));
        });
        int indexingRounds = randomIntBetween(2, 5);
        while (indexingRounds-- > 0) {
            indexRandom(
                true,
                true,
                true,
                false,
                IntStream.range(1, randomIntBetween(2, 5))
                    .mapToObj(i -> prepareIndex(indexName).setSource("field", randomAlphaOfLength(50)))
                    .toList()
            );
        }
        // start force merging (which is blocking) on a separate thread
        Thread forceMergeThread = new Thread(
            // the max segments argument makes it a blocking call
            () -> assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).get())
        );
        forceMergeThread.start();
        assertBusy(() -> {
            // merge executor says merging is blocked due to insufficient disk space while there is a single merge task enqueued
            assertThat(threadPoolMergeExecutorService.getMergeTasksQueueLength(), equalTo(1));
            assertTrue(threadPoolMergeExecutorService.isMergingBlockedDueToInsufficientDiskSpace());
            // telemetry says that there are indeed some segments enqueued to be merged
            testTelemetryPlugin.collect();
            assertThat(
                testTelemetryPlugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_QUEUED_USAGE).getLast().getLong(),
                greaterThan(0L)
            );
            // but still no merges are currently running
            assertThat(
                testTelemetryPlugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_RUNNING_USAGE).getLast().getLong(),
                equalTo(0L)
            );
            // indices stats also says that no merge is currently running (blocked merges are NOT considered as "running")
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(indexName).setMerge(true).get();
            long currentMergeCount = indicesStatsResponse.getIndices().get(indexName).getPrimaries().merge.getCurrent();
            assertThat(currentMergeCount, equalTo(0L));
        });
        // the force merge call is still blocked
        assertTrue(forceMergeThread.isAlive());
        // merge executor still confirms merging is blocked due to insufficient disk space
        assertTrue(threadPoolMergeExecutorService.isMergingBlockedDueToInsufficientDiskSpace());
        // make disk space available in order to unblock the merge
        if (randomBoolean()) {
            setTotalSpace(node, Long.MAX_VALUE);
        } else {
            updateClusterSettings(
                Settings.builder().put(ThreadPoolMergeExecutorService.INDICES_MERGE_DISK_HIGH_WATERMARK_SETTING.getKey(), "0b")
            );
        }
        // assert that all the merges are now done and that the force-merge call returns
        assertBusy(() -> {
            IndicesStatsResponse indicesStatsResponse = client().admin().indices().prepareStats(indexName).setMerge(true).get();
            long currentMergeCount = indicesStatsResponse.getIndices().get(indexName).getPrimaries().merge.getCurrent();
            // NO merging is in progress
            assertThat(currentMergeCount, equalTo(0L));
            long totalMergeCount = indicesStatsResponse.getIndices().get(indexName).getPrimaries().merge.getTotal();
            assertThat(totalMergeCount, greaterThan(0L));
            // force merge call returned
            assertFalse(forceMergeThread.isAlive());
            // telemetry also says that some merging took place
            testTelemetryPlugin.collect();
            assertThat(testTelemetryPlugin.getLongCounterMeasurement(MergeMetrics.MERGE_DOCS_TOTAL).getLast().getLong(), greaterThan(0L));
            // and no further merging
            assertThat(
                testTelemetryPlugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_QUEUED_USAGE).getLast().getLong(),
                equalTo(0L)
            );
            assertThat(
                testTelemetryPlugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_RUNNING_USAGE).getLast().getLong(),
                equalTo(0L)
            );
        });
        // assert there's a single segment after the force merge
        List<ShardSegments> shardSegments = getShardSegments(indexName);
        assertThat(shardSegments.size(), equalTo(1));
        assertThat(shardSegments.get(0).getSegments().size(), equalTo(1));
        assertAcked(indicesAdmin().prepareDelete(indexName).get());
        forceMergeThread.join();
    }

    public void setTotalSpace(String dataNodeName, long totalSpace) {
        getTestFileStore(dataNodeName).setTotalSpace(totalSpace);
        refreshClusterInfo();
    }

    private TestTelemetryPlugin getTelemetryPlugin(String dataNodeName) {
        var plugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
                .filterPlugins(TestTelemetryPlugin.class)
                .findFirst()
                .orElseThrow();
        plugin.resetMeter();
        return plugin;
    }
}
