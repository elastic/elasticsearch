/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless;

import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.MergeMetrics;
import co.elastic.elasticsearch.stateless.engine.ThreadPoolMergeScheduler;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.xpack.shutdown.ShutdownPlugin;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.function.IntSupplier;

import static co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

public class StatelessMergeIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(TestTelemetryPlugin.class, ShutdownPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ThreadPoolMergeScheduler.MERGE_THREAD_POOL_SCHEDULER.getKey(), true)
            // Occasionally the abstract stateless test will set this low causing many flushes in these test. We want to control the flushes
            // in these tests.
            .put(StatelessCommitService.STATELESS_UPLOAD_MAX_AMOUNT_COMMITS.getKey(), 30);
    }

    public void testMergesUseTheMergeThreadPool() {
        String indexNode = startMasterAndIndexNode();
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());

        final int minMerges = randomIntBetween(1, 5);
        long totalDocs = 0;
        while (true) {
            int docs = randomIntBetween(100, 200);
            totalDocs += docs;
            indexDocs(indexName, docs);
            flush(indexName);

            var mergesResponse = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get();
            var primaries = mergesResponse.getIndices().get(indexName).getPrimaries();
            if (primaries.merge.getTotal() >= minMerges) {
                break;
            }
        }

        forceMerge();
        refresh(indexName);

        final long expectedTotalDocs = totalDocs;
        assertHitCount(prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true), expectedTotalDocs);

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        long mergeCount = indicesStats.getIndices().get(indexName).getPrimaries().merge.getTotal();
        NodesStatsResponse nodesStatsResponse = client().admin().cluster().prepareNodesStats(indexNode).setThreadPool(true).get();
        assertThat(nodesStatsResponse.getNodes().size(), equalTo(1));
        NodeStats nodeStats = nodesStatsResponse.getNodes().get(0);
        assertThat(
            mergeCount,
            equalTo(
                nodeStats.getThreadPool()
                    .stats()
                    .stream()
                    .filter(s -> Stateless.MERGE_THREAD_POOL.equals(s.name()))
                    .findAny()
                    .get()
                    .completed()
            )
        );
    }

    public void testOnlyForceMergesAreAllowedDuringRelocation() throws Exception {
        String indexNode = startMasterAndIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);

        var sourceShard = findIndexShard(resolveIndex(indexName), 0, indexNode);

        indexDocs(indexName, 1_000);
        flush(indexName);

        indexDocs(indexName, 1_000);
        flush(indexName);

        var newIndexNode = startIndexNode();

        final var resumeHandoff = new CountDownLatch(1);
        final var pauseHandoff = new CountDownLatch(1);

        MockTransportService.getInstance(newIndexNode).addSendBehavior(new StubbableTransport.SendRequestBehavior() {

            private void await() {
                pauseHandoff.countDown();
                logger.info("--> start relocation paused");
                safeAwait(resumeHandoff, TimeValue.timeValueSeconds(20));
                logger.info("--> start relocation resumed");
            }

            @Override
            public void sendRequest(
                Transport.Connection connection,
                long requestId,
                String action,
                TransportRequest request,
                TransportRequestOptions options
            ) throws IOException {
                if (action.equals(START_RELOCATION_ACTION_NAME)) {
                    await();
                }
                connection.sendRequest(requestId, action, request, options);
            }
        });

        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, indexNode, newIndexNode));

        assertBusy(() -> assertTrue(sourceShard.routingEntry().relocating()));

        pauseHandoff.await();

        var startingMerges = client().admin().indices().prepareStats(indexName).clear().setMerge(true).get().getPrimaries().merge
            .getTotal();

        // In tests, this many documents + commits tend to produce 3-6 merges
        int totalDocs = 0;
        int threshold = randomIntBetween(1500, 2000);
        while (totalDocs < threshold) {
            int docs = randomIntBetween(100, 200);
            totalDocs += docs;
            indexDocs(indexName, docs);
            indicesAdmin().prepareRefresh(indexName).get();
        }

        try {
            assertThat(
                client().admin().indices().prepareStats(indexName).clear().setMerge(true).get().getPrimaries().merge.getTotal(),
                equalTo(startingMerges)
            );

            logger.info("--> force merge call");
            client(indexNode).admin().indices().prepareForceMerge(indexName).setMaxNumSegments(1).get();
            logger.info("--> force merge call returned");

            assertThat(
                client().admin().indices().prepareStats(indexName).clear().setMerge(true).get().getPrimaries().merge.getTotal(),
                equalTo(startingMerges + 1)
            );

        } finally {
            logger.info("--> resume handoff countdown");
            resumeHandoff.countDown();
        }
    }

    public void testRefreshOnLargeMerge_true() throws Exception {
        refreshOnLargeMergeTest(true);
    }

    public void testRefreshOnLargeMerge_false() throws Exception {
        refreshOnLargeMergeTest(false);
    }

    // test that merges at or above a size threshold trigger an immediate refresh, and below that threshold do not.
    // Generates multiple commits, then forces a merge, then compares the index on the primary and search node to see if the search node
    // was refreshed or not.
    private void refreshOnLargeMergeTest(boolean forceRefresh) throws Exception {
        // A threshold too large to cross during testing
        final var refreshThreshold = forceRefresh ? ByteSizeValue.ZERO : ByteSizeValue.ofPb(1);
        // Two external refreshes occur during initial recovery. We'll expect to also have one for a merge if we force it.
        final long expectedRefreshes = forceRefresh ? 3L : 2L;

        startMasterAndIndexNode(
            Settings.builder().put(ThreadPoolMergeScheduler.MERGE_FORCE_REFRESH_SIZE.getKey(), refreshThreshold).build()
        );
        startSearchNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());

        IntSupplier addDocs = () -> {
            int numDocs = randomIntBetween(100, 200);
            indexDocs(indexName, numDocs);
            return numDocs;
        };

        long flushedDocs = 0;
        long unflushedDocs = 0;

        // Create two commits to merge.
        flushedDocs += addDocs.getAsInt();
        flush(indexName);
        logger.info("flushed {} docs", flushedDocs);
        flushedDocs += addDocs.getAsInt();
        flush(indexName);
        logger.info("flushed {} docs", flushedDocs);
        // then add some more docs and force a merge, which will refresh depending on forceRefresh
        unflushedDocs += addDocs.getAsInt();
        logger.info("indexed {} docs", flushedDocs);
        assertNoFailures(indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(false).get());

        final long expectedTotalDocs = forceRefresh ? flushedDocs + unflushedDocs : flushedDocs;
        assertBusy(
            () -> assertHitCount(
                prepareSearch(indexName).setQuery(QueryBuilders.matchAllQuery()).setTrackTotalHits(true),
                expectedTotalDocs
            )
        );

        IndicesStatsResponse indicesStats = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        var actualRefreshes = indicesStats.getIndices().get(indexName).getPrimaries().getRefresh().getExternalTotal();
        assertThat("unexpected number of refreshes", actualRefreshes, equalTo(expectedRefreshes));
    }

    public void testMergeMetricsPublication() throws Exception {
        String indexNode = startMasterAndIndexNode();

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());

        ensureGreen(indexName);

        int numDocs = randomIntBetween(100, 1000);
        indexDocs(indexName, numDocs);
        refresh(indexName);

        var plugin = internalCluster().getInstance(PluginsService.class, indexNode)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        var threadPoolExecutor = internalCluster().getInstance(ThreadPool.class, indexNode);

        for (int i = 0; i < 4; ++i) {
            indexDocs(indexName, randomIntBetween(100, 200));
            refresh(indexName);
        }

        CountDownLatch latch = new CountDownLatch(1);

        ActionFuture<BroadcastResponse> mergeFuture;
        try {
            blockMergePool(threadPoolExecutor, latch);
            mergeFuture = indicesAdmin().prepareForceMerge().setMaxNumSegments(1).execute();

            assertBusy(() -> {
                plugin.collect();
                List<Measurement> queuedBytes = plugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_QUEUED_USAGE);
                List<Measurement> runningBytes = plugin.getLongGaugeMeasurement(MergeMetrics.MERGE_SEGMENTS_RUNNING_USAGE);
                assertThat(queuedBytes.stream().mapToLong(Measurement::getLong).sum(), greaterThanOrEqualTo(1L));
                assertThat(runningBytes.stream().mapToLong(Measurement::getLong).sum(), equalTo(0L));
            });
        } finally {
            latch.countDown();
        }

        assertNoFailures(mergeFuture.actionGet());

        List<Measurement> docs = plugin.getLongCounterMeasurement(MergeMetrics.MERGE_DOCS_TOTAL);
        List<Measurement> bytes = plugin.getLongCounterMeasurement(MergeMetrics.MERGE_SEGMENTS_SIZE);
        List<Measurement> mergedSegmentBytes = plugin.getLongCounterMeasurement(MergeMetrics.MERGE_SEGMENTS_MERGED_SIZE);
        List<Measurement> time = plugin.getLongHistogramMeasurement(MergeMetrics.MERGE_TIME_IN_SECONDS);

        assertThat(bytes.size(), equalTo(docs.size()));
        assertThat(time.size(), equalTo(docs.size()));

        // Will likely have 1 merge, but could have more
        for (int i = 0; i < docs.size(); ++i) {
            var mergeDocs = docs.get(i);
            var mergeBytes = bytes.get(i);
            var mergeMergedSegmentBytes = mergedSegmentBytes.get(i);
            var mergeTime = time.get(i);

            assertThat(mergeDocs.getLong(), greaterThan(0L));
            assertThat(mergeBytes.getLong(), greaterThan(0L));
            assertThat(mergeMergedSegmentBytes.getLong(), greaterThan(0L));
            assertThat(mergeTime.getLong(), greaterThanOrEqualTo(0L));
        }

        long totalDocs = docs.stream().mapToLong(Measurement::getLong).sum();
        long totalBytes = bytes.stream().mapToLong(Measurement::getLong).sum();

        IndicesStatsResponse statsResponse = client().admin().indices().prepareStats(indexName).setMerge(true).get();
        MergeStats mergeStats = statsResponse.getIndex(indexName).getShards()[0].getStats().merge;
        assertThat(totalDocs, equalTo(mergeStats.getTotalNumDocs()));
        assertThat(totalBytes, equalTo(mergeStats.getTotalSize().getBytes()));
    }

    private static void blockMergePool(ThreadPool threadPool, CountDownLatch finishLatch) {
        final var threadCount = threadPool.info(Stateless.MERGE_THREAD_POOL).getMax();
        final var startBarrier = new CyclicBarrier(threadCount + 1);
        final var blockingTask = new AbstractRunnable() {
            @Override
            public void onFailure(Exception e) {
                fail(e);
            }

            @Override
            protected void doRun() {
                safeAwait(startBarrier);
                safeAwait(finishLatch);
            }

            @Override
            public boolean isForceExecution() {
                return true;
            }
        };
        for (int i = 0; i < threadCount; i++) {
            threadPool.executor(Stateless.MERGE_THREAD_POOL).execute(blockingTask);
        }
        safeAwait(startBarrier);
    }
}
