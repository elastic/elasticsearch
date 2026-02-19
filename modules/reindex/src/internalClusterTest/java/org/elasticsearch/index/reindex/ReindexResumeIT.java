/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;

@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class ReindexResumeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class);
    }

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(TransportReindexAction.REMOTE_CLUSTER_WHITELIST.getKey(), "*:*")
            .build();
    }

    public void testLocalResumeReindexFromScroll() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        // Manually initiate a scroll search to get a scroll ID
        String scrollId;
        int remainingDocs = totalDocs - batchSize;
        SearchResponse searchResponse = client().prepareSearch(sourceIndex).setScroll(DEFAULT_SCROLL_TIMEOUT).setSize(batchSize).get();
        try {
            scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);
            assertEquals((int) searchResponse.getHits().getTotalHits().value(), totalDocs);
            assertEquals(searchResponse.getHits().getHits().length, batchSize);
        } finally {
            searchResponse.decRef();
        }

        // Resume reindexing from the manual scroll search
        BulkByScrollTask.Status randomStats = randomStats();
        // random start time in the past to ensure that "took" is updated
        long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo(scrollId, startTime, randomStats, null), null));
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertStatus(randomStats, getTaskResponse.getTask(), totalDocs, batchSize, remainingDocs);
        // ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
        // ensure the scroll is cleared
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testRemoteResumeReindexFromScroll() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        // Manually initiate a scroll search to get a scroll ID
        String scrollId;
        int remainingDocs = totalDocs - batchSize;
        SearchResponse searchResponse = client().prepareSearch(sourceIndex).setScroll(DEFAULT_SCROLL_TIMEOUT).setSize(batchSize).get();
        try {
            scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);
            assertEquals((int) searchResponse.getHits().getTotalHits().value(), totalDocs);
            assertEquals(searchResponse.getHits().getHits().length, batchSize);
        } finally {
            searchResponse.decRef();
        }

        // Resume reindexing from the manual scroll with remote search
        BulkByScrollTask.Status randomStats = randomStats();
        // random start time in the past to ensure that "took" is updated
        long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setRemoteInfo(
                new RemoteInfo(
                    "http",
                    remoteAddress.getHostString(),
                    remoteAddress.getPort(),
                    null,
                    new BytesArray("{\"match_all\":{}}"),
                    null,
                    null,
                    Map.of(),
                    RemoteInfo.DEFAULT_SOCKET_TIMEOUT,
                    RemoteInfo.DEFAULT_CONNECT_TIMEOUT
                )
            )
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo(scrollId, startTime, randomStats, Version.CURRENT), null));
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertStatus(randomStats, getTaskResponse.getTask(), totalDocs, batchSize, remainingDocs);
        // ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
        // ensure the scroll is cleared
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testRejectWithoutResumeInfo() {
        ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices("source").setDestIndex("dest");

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(reindexRequest)).actionGet()
        );

        assertTrue(e.getMessage().contains("No resume information provided"));
    }

    public void testRejectShouldStoreResultFalse() {
        ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(false)
            .setEligibleForRelocationOnShutdown(true)
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo("ignored", 0L, randomStats(), null), null));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(reindexRequest)).actionGet()
        );

        assertTrue(e.getMessage().contains("Resumed task result should be stored"));
    }

    public void testRejectEligibleForRelocationOnShutdownFalse() {
        ReindexRequest reindexRequest = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(false)
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo("ignored", 0L, randomStats(), null), null));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(reindexRequest)).actionGet()
        );

        assertTrue(e.getMessage().contains("Resumed task should be eligible for relocation on shutdown"));
    }

    private BulkByScrollTask.Status randomStats() {
        return new BulkByScrollTask.Status(
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomTimeValue(),
            randomFloatBetween(1000, 10000, true),
            null,
            TimeValue.ZERO
        );
    }

    private long currentNumberOfScrollContexts() {
        final NodesStatsResponse stats = clusterAdmin().prepareNodesStats().clear().setIndices(true).get();
        long total = 0;
        for (var nodeStats : stats.getNodes()) {
            total += nodeStats.getIndices().getSearch().getTotal().getScrollCurrent();
        }
        return total;
    }

    private static void assertStatus(BulkByScrollTask.Status status, TaskResult task, long totalDocs, int batchSize, int remainingDocs) {
        assertTrue(task.isCompleted());
        Map<String, Object> response = task.getResponseAsMap();
        assertNotNull(response);

        // total should equal to total hits from the search
        assertEquals(totalDocs, longFromMap(response, "total"));
        // stats are updated
        assertEquals(remainingDocs + status.getCreated(), longFromMap(response, "created"));
        int remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
        assertEquals(remainingBatches + status.getBatches(), intFromMap(response, "batches"));
        assertTrue(longFromMap(response, "took") > TimeValue.ONE_HOUR.millis());
        // other stats should be retained
        assertEquals(status.getDeleted(), longFromMap(response, "deleted"));
        assertEquals(status.getUpdated(), longFromMap(response, "updated"));
        assertEquals(status.getVersionConflicts(), longFromMap(response, "version_conflicts"));
        assertEquals(status.getNoops(), longFromMap(response, "noops"));
        @SuppressWarnings("unchecked")
        Map<String, Object> retries = (Map<String, Object>) response.get("retries");
        assertNotNull(retries);
        assertEquals(status.getBulkRetries(), longFromMap(retries, "bulk"));
        assertEquals(status.getSearchRetries(), longFromMap(retries, "search"));
        assertEquals(status.getRequestsPerSecond(), floatFromMap(response, "requests_per_second"), 0);
    }

    private static long longFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).longValue();
    }

    private static int intFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).intValue();
    }

    private static float floatFromMap(Map<String, Object> map, String key) {
        return ((Number) map.get(key)).floatValue();
    }
}
