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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.SliceStatus;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESIntegTestCase;

import java.net.InetSocketAddress;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES;
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

        assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
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

        assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
        // ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
        // ensure the scroll is cleared
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testLocalResumeReindexFromScroll_slicedN() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);
        // the first manual search batch creates the scroll, and is not indexed into destination
        final long expectedDocsDest = totalDocs - numSlices * batchSize;

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        final long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();

        // Manually create scroll slices and pass their scroll IDs in resume info
        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                assertEquals(batchSize, searchResponse.getHits().getHits().length);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }

        // Resume reindexing from the manual scroll search slices
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(null, sliceStatus));
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, totalDocs, batchSize);
    }

    public void testLocalResumeReindexFromScroll_slicedN_partialCompleted() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);
        final int numCompletedSlices = randomIntBetween(1, numSlices - 1);
        final int numPendingSlices = numSlices - numCompletedSlices;
        // num docs in dest = manually completed slices + resumed slices (exclude first batch)
        final long expectedDocsDest = totalDocs - numPendingSlices * batchSize;

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        // Complete some slices with manual slicing and pass their results as completed slices in resume info
        for (int sliceId = 0; sliceId < numCompletedSlices; sliceId++) {
            ReindexRequest sliceRequest = new ReindexRequest().setSourceIndices(sourceIndex)
                .setDestIndex(destIndex)
                .setRefresh(true)
                .setSourceBatchSize(batchSize);
            sliceRequest.getSearchRequest()
                .source(new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)));
            BulkByScrollResponse sliceResponse = client().execute(ReindexAction.INSTANCE, sliceRequest).actionGet();
            assertTrue(sliceResponse.getCreated() > 0);
            sliceStatus.put(sliceId, new SliceStatus(sliceId, null, new WorkerResult(sliceResponse, null)));
        }

        // Manually create scroll slices for the remaining slices that are not completed, and pass their scroll IDs in resume info
        final long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();
        for (int sliceId = numCompletedSlices; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                assertEquals(batchSize, searchResponse.getHits().getHits().length);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }

        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(null, sliceStatus));

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, totalDocs, batchSize);
    }

    public void testLocalResumeReindexFromScroll_slicedAuto() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        // at least 2 shards to ensure auto-slicing creates multiple slices
        int numSourceShards = randomIntBetween(2, 10);
        // slice count differs from shard count to ensure slicing is from resume info
        int numSlices = numSourceShards + 1;
        // the first manual search batch creates the scroll, and is not indexed into destination
        final long expectedDocsDest = totalDocs - numSlices * batchSize;

        createIndex(sourceIndex, numSourceShards, 0);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        final long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();

        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                assertEquals(batchSize, searchResponse.getHits().getHits().length);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }

        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(AUTO_SLICES)
            .setResumeInfo(new ResumeInfo(null, sliceStatus));

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, totalDocs, batchSize);

        // response must have same number of slices as resume info, not auto-resolved from shard count
        Map<String, Object> response = getTaskResponse.getTask().getResponseAsMap();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) response.get("slices");
        assertNotNull(slices);
        assertEquals(numSlices, slices.size());
    }

    public void testLocalResumeReindexFromScroll_slicedManual() {
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        final int numSlices = randomIntBetween(2, 5);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        final long startTime = System.nanoTime() - randomTimeValue(2, 10, TimeUnit.HOURS).nanos();
        // the first manual search batch per slice creates the scroll and is not indexed; resume indexes the rest
        final long expectedDocsDest = totalDocs - numSlices * batchSize;

        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            final String scrollId;
            final BulkByScrollTask.Status sliceStats;
            final long totalHits;
            try {
                scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                assertEquals(batchSize, searchResponse.getHits().getHits().length);
                sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                totalHits = searchResponse.getHits().getTotalHits().value();
            } finally {
                searchResponse.decRef();
            }

            final long remainingDocs = totalHits - batchSize;

            ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
                .setShouldStoreResult(true)
                .setEligibleForRelocationOnShutdown(true)
                .setDestIndex(destIndex)
                .setSourceBatchSize(batchSize)
                .setRefresh(true)
                .setSlices(1)
                .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)));

            ResumeBulkByScrollResponse resumeResponse = client().execute(
                ResumeReindexAction.INSTANCE,
                new ResumeBulkByScrollRequest(request)
            ).actionGet();
            GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
                .setWaitForCompletion(true)
                .setTimeout(TimeValue.timeValueSeconds(30))
                .get();
            assertTrue(getTaskResponse.getTask().isCompleted());
            assertStatus(getTaskResponse.getTask(), sliceStats, totalHits, batchSize, (int) remainingDocs);
        }

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
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

    public void testRejectRemote_sliced() {
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remoteInfo = new RemoteInfo(
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
        );
        ReindexRequest request = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRemoteInfo(remoteInfo)
            .setSlices(randomIntBetween(2, 10))
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo("scrollId", 0L, randomStats(), Version.CURRENT), null));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request)).actionGet()
        );

        assertTrue(e.getMessage().contains("reindex from remote sources doesn't support slices > 1"));
    }

    public void testRejectRemote_slicedManual() {
        InetSocketAddress remoteAddress = randomFrom(cluster().httpAddresses());
        RemoteInfo remoteInfo = new RemoteInfo(
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
        );
        ReindexRequest request = new ReindexRequest().setSourceIndices("source")
            .setDestIndex("dest")
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setRemoteInfo(remoteInfo)
            .setSlices(1)
            .setResumeInfo(new ResumeInfo(new ScrollWorkerResumeInfo("scrollId", 0L, randomStats(), Version.CURRENT), null));
        request.getSearchRequest().source(new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, 0, 2)));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request)).actionGet()
        );

        assertTrue(e.getMessage().contains("reindex from remote sources doesn't support source.slice"));
    }

    private BulkByScrollTask.Status randomStats() {
        return randomStats(null, randomNonNegativeLong());
    }

    private BulkByScrollTask.Status randomStats(Integer sliceId, long total) {
        return new BulkByScrollTask.Status(
            sliceId,
            total,
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

    private static void assertSlicedResponse(TaskResult taskResult, Map<Integer, SliceStatus> resumeStatus, long totalDocs, int batchSize) {
        assertTrue(taskResult.isCompleted());
        Map<String, Object> response = taskResult.getResponseAsMap();
        assertNotNull(response);
        @SuppressWarnings("unchecked")
        var slices = (List<Map<String, Object>>) response.get("slices");
        assertNotNull(slices);
        assertEquals(resumeStatus.size(), slices.size());

        long slicesTotal = 0, slicesTotalCreated = 0, slicesTotalUpdated = 0, slicesTotalDeleted = 0, slicesTotalVersionConflicts = 0,
            sliceTotalNoops = 0;
        for (Map<String, Object> slice : slices) {
            int sliceId = intFromMap(slice, "slice_id");
            SliceStatus sliceStatus = resumeStatus.get(sliceId);
            assertNotNull(sliceStatus);
            BulkByScrollTask.Status status;
            if (sliceStatus.resumeInfo() != null) {
                status = sliceStatus.resumeInfo().status();
                // ensure each resumed slice's status is updated compared to the resume info
                assertSliceStatus(slice, status, batchSize);
            } else {
                assertNotNull(sliceStatus.result());
                status = sliceStatus.result().getResponse().orElseThrow().getStatus();
                // completed slice: response should match the result we passed in exactly
                assertEquals(status.getTotal(), longFromMap(slice, "total"));
                assertEquals(status.getCreated(), longFromMap(slice, "created"));
                assertEquals(status.getBatches(), intFromMap(slice, "batches"));
                assertEquals(status.getDeleted(), longFromMap(slice, "deleted"));
                assertEquals(status.getUpdated(), longFromMap(slice, "updated"));
                assertEquals(status.getVersionConflicts(), longFromMap(slice, "version_conflicts"));
                assertEquals(status.getNoops(), longFromMap(slice, "noops"));
                @SuppressWarnings("unchecked")
                Map<String, Object> retries = (Map<String, Object>) slice.get("retries");
                assertNotNull(retries);
                assertEquals(status.getBulkRetries(), longFromMap(retries, "bulk"));
                assertEquals(status.getSearchRetries(), longFromMap(retries, "search"));
            }

            slicesTotal += longFromMap(slice, "total");
            slicesTotalCreated += longFromMap(slice, "created");
            slicesTotalUpdated += longFromMap(slice, "updated");
            slicesTotalDeleted += longFromMap(slice, "deleted");
            slicesTotalVersionConflicts += longFromMap(slice, "version_conflicts");
            sliceTotalNoops += longFromMap(slice, "noops");
        }
        // ensure the stats from all slices can add up
        assertEquals(totalDocs, slicesTotal);
        assertEquals(slicesTotal, longFromMap(response, "total"));
        assertEquals(slicesTotalCreated, longFromMap(response, "created"));
        assertEquals(slicesTotalUpdated, longFromMap(response, "updated"));
        assertEquals(slicesTotalDeleted, longFromMap(response, "deleted"));
        assertEquals(slicesTotalVersionConflicts, longFromMap(response, "version_conflicts"));
        assertEquals(sliceTotalNoops, longFromMap(response, "noops"));
        assertTrue(longFromMap(response, "took") > TimeValue.ONE_HOUR.millis());
    }

    private static void assertStatus(
        TaskResult task,
        BulkByScrollTask.Status resumeStatus,
        long totalDocs,
        int batchSize,
        int remainingDocs
    ) {
        assertTrue(task.isCompleted());
        Map<String, Object> response = task.getResponseAsMap();
        assertNotNull(response);

        // total should equal to total hits from the search
        assertEquals(totalDocs, longFromMap(response, "total"));
        // stats are updated
        assertEquals(remainingDocs + resumeStatus.getCreated(), longFromMap(response, "created"));
        int remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
        assertEquals(remainingBatches + resumeStatus.getBatches(), intFromMap(response, "batches"));
        assertTrue(longFromMap(response, "took") > TimeValue.ONE_HOUR.millis());
        // other stats should be retained
        assertEquals(resumeStatus.getDeleted(), longFromMap(response, "deleted"));
        assertEquals(resumeStatus.getUpdated(), longFromMap(response, "updated"));
        assertEquals(resumeStatus.getVersionConflicts(), longFromMap(response, "version_conflicts"));
        assertEquals(resumeStatus.getNoops(), longFromMap(response, "noops"));
        @SuppressWarnings("unchecked")
        Map<String, Object> retries = (Map<String, Object>) response.get("retries");
        assertNotNull(retries);
        assertEquals(resumeStatus.getBulkRetries(), longFromMap(retries, "bulk"));
        assertEquals(resumeStatus.getSearchRetries(), longFromMap(retries, "search"));
        assertEquals(resumeStatus.getRequestsPerSecond(), floatFromMap(response, "requests_per_second"), 0);
    }

    private static void assertSliceStatus(Map<String, Object> response, BulkByScrollTask.Status resumeStatus, int batchSize) {
        assertEquals((int) resumeStatus.getSliceId(), intFromMap(response, "slice_id"));
        long remainingDocs = resumeStatus.getTotal() - batchSize;
        assertEquals(resumeStatus.getCreated() + remainingDocs, longFromMap(response, "created"));
        long remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
        assertEquals(resumeStatus.getBatches() + remainingBatches, intFromMap(response, "batches"));
        assertEquals(resumeStatus.getTotal(), longFromMap(response, "total"));
        assertEquals(resumeStatus.getDeleted(), longFromMap(response, "deleted"));
        assertEquals(resumeStatus.getUpdated(), longFromMap(response, "updated"));
        assertEquals(resumeStatus.getVersionConflicts(), longFromMap(response, "version_conflicts"));
        assertEquals(resumeStatus.getNoops(), longFromMap(response, "noops"));
        @SuppressWarnings("unchecked")
        Map<String, Object> retries = (Map<String, Object>) response.get("retries");
        assertNotNull(retries);
        assertEquals(resumeStatus.getBulkRetries(), longFromMap(retries, "bulk"));
        assertEquals(resumeStatus.getSearchRetries(), longFromMap(retries, "search"));
        assertEquals(resumeStatus.getRequestsPerSecond(), floatFromMap(response, "requests_per_second"), 0);
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
