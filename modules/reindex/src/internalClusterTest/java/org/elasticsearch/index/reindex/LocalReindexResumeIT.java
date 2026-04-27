/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.reindex;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.TransportOpenPointInTimeAction;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.reindex.ResumeInfo.PitWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.ScrollWorkerResumeInfo;
import org.elasticsearch.index.reindex.ResumeInfo.SliceStatus;
import org.elasticsearch.index.reindex.ResumeInfo.WorkerResult;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.reindex.ReindexMetrics;
import org.elasticsearch.reindex.ReindexPlugin;
import org.elasticsearch.reindex.TransportReindexAction;
import org.elasticsearch.rest.root.MainRestPlugin;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.slice.SliceBuilder;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.AUTO_SLICES;
import static org.elasticsearch.index.reindex.AbstractBulkByScrollRequest.DEFAULT_SCROLL_TIMEOUT;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests local reindexing tasks resumed after node relocation.
 * NB This test includes tests for both scroll and point-in-time search. The scroll-based tests can be removed once
 * {@link ReindexPlugin#REINDEX_PIT_SEARCH_ENABLED} is defaulted to true.
 */
@ESIntegTestCase.ClusterScope(numDataNodes = 1, numClientNodes = 0)
public class LocalReindexResumeIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ReindexPlugin.class, MainRestPlugin.class, TestTelemetryPlugin.class);
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

    public void testResumeReindexFromScroll() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
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
        long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo(scrollId, startTime, randomStats, null), null));
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

    public void testResumeReindexFromPit() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        int totalDocs = randomIntBetween(20, 100);
        int batchSize = randomIntBetween(1, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        OpenPointInTimeResponse openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
        ).actionGet();
        BytesReference pitId = openPitResponse.getPointInTimeId();

        // Manually initiate a pit search to get a pit ID
        SearchRequest firstPitSearch = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                .sort(SortBuilders.pitTiebreaker())
                .size(batchSize)
        );
        SearchResponse searchResponse = client().search(firstPitSearch).actionGet();
        Object[] searchAfterValues;
        try {
            assertEquals((int) searchResponse.getHits().getTotalHits().value(), totalDocs);
            assertEquals(searchResponse.getHits().getHits().length, batchSize);
            if (searchResponse.pointInTimeId() != null) {
                pitId = searchResponse.pointInTimeId();
            }
            SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
            searchAfterValues = lastHit.getSortValues();
            assertNotNull(searchAfterValues);
        } finally {
            searchResponse.decRef();
        }

        // Resume reindexing
        int remainingDocs = totalDocs - batchSize;
        BulkByScrollTask.Status randomStats = randomStats();
        // Random start time in the past to ensure that "took" is updated
        long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(
                new ResumeInfo(randomOrigin(), new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, randomStats, null), null)
            );
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
        // ReindexRequest by default enable scroll, but PIT and scroll are mutually exclusive
        request.getSearchRequest().scroll(null);
        // PIT searches must not set explicit indices on SearchRequest (see ReindexRequest#convertSearchRequestToUsePit)
        request.getSearchRequest().indices(Strings.EMPTY_ARRAY);
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertStatus(getTaskResponse.getTask(), randomStats, totalDocs, batchSize, remainingDocs);
        // Ensure remaining docs were indexed
        assertHitCount(prepareSearch(destIndex), remainingDocs);
        // Sanity check since point-in-time search shouldn't open any scroll contexts
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testResumeReindexFromScroll_slicedN() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

        // Manually create scroll slices and pass their scroll IDs in resume info
        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                // the actual search hits may be less than batch size if the slice has few docs, since doc are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }
        // the first manual search batch creates the scroll, and is not indexed into destination
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        // Resume reindexing from the manual scroll search slices
        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);
    }

    public void testResumeReindexFromPit_slicedN() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        OpenPointInTimeResponse openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
        ).actionGet();
        BytesReference pitId = openPitResponse.getPointInTimeId();

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

        // Manually create pit slices and pass their pit IDs in resume info
        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                // The actual search hits may be less than batch size if the slice has few docs, since docs are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                if (searchResponse.pointInTimeId() != null) {
                    pitId = searchResponse.pointInTimeId();
                }
                final Object[] searchAfterValues;
                if (firstBatchDocs > 0) {
                    SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
                    searchAfterValues = lastHit.getSortValues();
                    assertNotNull(searchAfterValues);
                } else {
                    searchAfterValues = new Object[0];
                }
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(
                    sliceId,
                    new SliceStatus(sliceId, new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, sliceStats, null), null)
                );
            } finally {
                searchResponse.decRef();
            }
        }
        // Each manual sliced PIT search prefetched a page of hits to build resume state. Those hits are not written to the
        // destination, so the resumed job only indexes the remaining documents across slices.
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        // Resume reindexing from the manual pit search slices
        ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
        request.getSearchRequest().scroll(null);
        request.getSearchRequest().indices(Strings.EMPTY_ARRAY);
        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testResumeReindexFromScroll_slicedN_partialCompleted() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);
        final int numCompletedSlices = randomIntBetween(1, numSlices - 1);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
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
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        for (int sliceId = numCompletedSlices; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                // the actual search hits may be less than batch size if the slice has few docs, since doc are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }
        // the first manual search batch creates the scroll, and is not indexed into destination
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);
    }

    public void testResumeReindexFromPit_slicedN_partialCompleted() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int numSlices = randomIntBetween(2, 5);
        final int batchSize = randomIntBetween(5, 10);
        final int numCompletedSlices = randomIntBetween(1, numSlices - 1);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
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

        OpenPointInTimeResponse openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
        ).actionGet();
        BytesReference pitId = openPitResponse.getPointInTimeId();

        // Manually create pit slices for the remaining slices that are not completed, and pass their pit IDs in resume info
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        for (int sliceId = numCompletedSlices; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                // The actual search hits may be less than batch size if the slice has few docs, since docs are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                if (searchResponse.pointInTimeId() != null) {
                    pitId = searchResponse.pointInTimeId();
                }
                final Object[] searchAfterValues;
                if (firstBatchDocs > 0) {
                    SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
                    searchAfterValues = lastHit.getSortValues();
                    assertNotNull(searchAfterValues);
                } else {
                    searchAfterValues = new Object[0];
                }
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(
                    sliceId,
                    new SliceStatus(sliceId, new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, sliceStats, null), null)
                );
            } finally {
                searchResponse.decRef();
            }
        }
        // Each manual sliced PIT search prefetched a page of hits to build resume state. Those hits are not written to the
        // destination, so the resumed job only indexes the remaining documents across slices.
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(numSlices)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
        request.getSearchRequest().scroll(null);
        request.getSearchRequest().indices(Strings.EMPTY_ARRAY);

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);
    }

    public void testResumeReindexFromScroll_slicedAuto() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        // at least 2 shards to ensure auto-slicing creates multiple slices
        int numSourceShards = randomIntBetween(2, 10);
        // slice count differs from shard count to ensure slicing is from resume info
        int numSlices = numSourceShards + 1;

        createIndex(sourceIndex, numSourceShards, 0);
        indexRandom(true, sourceIndex, totalDocs);

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest(sourceIndex).source(
                new SearchSourceBuilder().slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices)).size(batchSize)
            ).scroll(DEFAULT_SCROLL_TIMEOUT);
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                String scrollId = searchResponse.getScrollId();
                assertNotNull(scrollId);
                // the actual search hits may be less than batch size if the slice has few docs, since doc are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(sliceId, new SliceStatus(sliceId, new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
            } finally {
                searchResponse.decRef();
            }
        }
        // the first manual search batch per slice creates the scroll and is not indexed; resume indexes the rest
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(AUTO_SLICES)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);

        // response must have same number of slices as resume info, not auto-resolved from shard count
        Map<String, Object> response = getTaskResponse.getTask().getResponseAsMap();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) response.get("slices");
        assertNotNull(slices);
        assertEquals(numSlices, slices.size());
    }

    public void testResumeReindexFromPit_slicedAuto() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        // At least 2 shards to ensure auto-slicing creates multiple slices
        int numSourceShards = randomIntBetween(2, 10);
        // Slice count differs from shard count to ensure slicing is from resume info
        int numSlices = numSourceShards + 1;

        createIndex(sourceIndex, numSourceShards, 0);
        indexRandom(true, sourceIndex, totalDocs);

        OpenPointInTimeResponse openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
        ).actionGet();
        BytesReference pitId = openPitResponse.getPointInTimeId();

        Map<Integer, SliceStatus> sliceStatus = new HashMap<>();
        Map<Integer, Long> sliceFirstBatchDocs = new HashMap<>();
        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));

        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            try {
                // The actual search hits may be less than batch size if the slice has few docs, since docs are randomly sliced
                long firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                sliceFirstBatchDocs.put(sliceId, firstBatchDocs);
                if (searchResponse.pointInTimeId() != null) {
                    pitId = searchResponse.pointInTimeId();
                }
                final Object[] searchAfterValues;
                if (firstBatchDocs > 0) {
                    SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
                    searchAfterValues = lastHit.getSortValues();
                    assertNotNull(searchAfterValues);
                } else {
                    searchAfterValues = new Object[0];
                }
                BulkByScrollTask.Status sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                sliceStatus.put(
                    sliceId,
                    new SliceStatus(sliceId, new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, sliceStats, null), null)
                );
            } finally {
                searchResponse.decRef();
            }
        }
        // Each manual sliced PIT search prefetched a page of hits to build resume state. Those hits are not written to the
        // destination, so the resumed job only indexes the remaining documents across slices.
        final long expectedDocsDest = totalDocs - sliceFirstBatchDocs.values().stream().mapToLong(Long::longValue).sum();

        ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setSlices(AUTO_SLICES)
            .setResumeInfo(new ResumeInfo(randomOrigin(), null, sliceStatus));
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
        request.getSearchRequest().scroll(null);
        request.getSearchRequest().indices(Strings.EMPTY_ARRAY);

        ResumeBulkByScrollResponse resumeResponse = client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(request))
            .actionGet();
        GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
        assertSlicedResponse(getTaskResponse.getTask(), sliceStatus, sliceFirstBatchDocs, totalDocs, batchSize);

        // response must have same number of slices as resume info, not auto-resolved from shard count
        Map<String, Object> response = getTaskResponse.getTask().getResponseAsMap();
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) response.get("slices");
        assertNotNull(slices);
        assertEquals(numSlices, slices.size());
    }

    public void testResumeReindexFromScroll_slicedManual() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        final int numSlices = randomIntBetween(2, 5);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        long firstBatchDocsTotal = 0;

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
                // the actual search hits may be less than batch size if the slice has few docs, since doc are randomly sliced
                int firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                firstBatchDocsTotal += firstBatchDocs;
                sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                totalHits = searchResponse.getHits().getTotalHits().value();
            } finally {
                searchResponse.decRef();
            }

            final long remainingDocs = totalHits - Math.min(totalHits, batchSize);

            ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
                .setShouldStoreResult(true)
                .setEligibleForRelocationOnShutdown(true)
                .setDestIndex(destIndex)
                .setSourceBatchSize(batchSize)
                .setRefresh(true)
                .setSlices(1)
                .setResumeInfo(new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo(scrollId, startTime, sliceStats, null), null));
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
            assertStatus(getTaskResponse.getTask(), sliceStats, totalHits, batchSize, remainingDocs);
        }

        // the first manual search batch per slice creates the scroll and is not indexed; resume indexes the rest
        final long expectedDocsDest = totalDocs - firstBatchDocsTotal;
        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testResumeReindexFromPit_slicedManual() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(200, 300);
        final int batchSize = randomIntBetween(5, 10);
        final int numSlices = randomIntBetween(2, 5);

        createIndex(sourceIndex);
        indexRandom(true, sourceIndex, totalDocs);

        final long startTime = timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS));
        long firstBatchDocsTotal = 0;

        for (int sliceId = 0; sliceId < numSlices; sliceId++) {
            // A fresh PIT per slice: each ResumeReindexAction closes its PIT when the slice task completes (unlike scroll ids,
            // which stay valid until cleared). Mirrors testResumeReindexFromScroll_slicedManual opening a new scroll per slice.
            OpenPointInTimeResponse openPitResponse = client().execute(
                TransportOpenPointInTimeAction.TYPE,
                new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
            ).actionGet();
            BytesReference pitId = openPitResponse.getPointInTimeId();

            SearchRequest searchRequest = new SearchRequest().source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
            SearchResponse searchResponse = client().search(searchRequest).actionGet();
            final Object[] searchAfterValues;
            final BulkByScrollTask.Status sliceStats;
            final long totalHits;
            try {
                // The actual search hits may be less than batch size if the slice has few docs, since docs are randomly sliced
                int firstBatchDocs = searchResponse.getHits().getHits().length;
                assertTrue(firstBatchDocs <= batchSize);
                firstBatchDocsTotal += firstBatchDocs;
                if (searchResponse.pointInTimeId() != null) {
                    pitId = searchResponse.pointInTimeId();
                }
                if (firstBatchDocs > 0) {
                    SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
                    searchAfterValues = lastHit.getSortValues();
                    assertNotNull(searchAfterValues);
                } else {
                    searchAfterValues = new Object[0];
                }
                sliceStats = randomStats(sliceId, searchResponse.getHits().getTotalHits().value());
                totalHits = searchResponse.getHits().getTotalHits().value();
            } finally {
                searchResponse.decRef();
            }

            final long remainingDocs = totalHits - Math.min(totalHits, batchSize);

            ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
                .setEligibleForRelocationOnShutdown(true)
                .setDestIndex(destIndex)
                .setSourceBatchSize(batchSize)
                .setRefresh(true)
                .setSlices(1)
                .setResumeInfo(
                    new ResumeInfo(randomOrigin(), new PitWorkerResumeInfo(pitId, searchAfterValues, startTime, sliceStats, null), null)
                );
            request.getSearchRequest()
                .source(
                    new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                        .slice(new SliceBuilder(IdFieldMapper.NAME, sliceId, numSlices))
                        .sort(SortBuilders.pitTiebreaker())
                        .size(batchSize)
                );
            request.getSearchRequest().scroll(null);
            request.getSearchRequest().indices(Strings.EMPTY_ARRAY);

            ResumeBulkByScrollResponse resumeResponse = client().execute(
                ResumeReindexAction.INSTANCE,
                new ResumeBulkByScrollRequest(request)
            ).actionGet();
            GetTaskResponse getTaskResponse = clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
                .setWaitForCompletion(true)
                .setTimeout(TimeValue.timeValueSeconds(30))
                .get();
            assertTrue(getTaskResponse.getTask().isCompleted());
            assertStatus(getTaskResponse.getTask(), sliceStats, totalHits, batchSize, remainingDocs);
        }

        // Each manual sliced PIT search prefetched a page of hits to build resume state. Those hits are not written to the
        // destination, so the resumed job only indexes the remaining documents across slices.
        final long expectedDocsDest = totalDocs - firstBatchDocsTotal;
        assertHitCount(expectedDocsDest, prepareSearch(destIndex));
        assertEquals(0, currentNumberOfScrollContexts());
    }

    public void testResumeReindexMetricsRecordsDurationFromRelocationOrigin() {
        assumeFalse("reindex with point-in-time search must not be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assertThat(internalCluster().numDataNodes(), equalTo(1));
        final String dataNodeName = internalCluster().getRandomDataNodeName();
        final TestTelemetryPlugin telemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        telemetryPlugin.resetMeter(); // reset previous test metrics

        final String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(20, 100);
        final int batchSize = randomIntBetween(1, 10);

        indexRandom(true, sourceIndex, totalDocs);

        final String scrollId;
        final SearchResponse searchResponse = client().prepareSearch(sourceIndex)
            .setScroll(DEFAULT_SCROLL_TIMEOUT)
            .setSize(batchSize)
            .get();
        try {
            scrollId = searchResponse.getScrollId();
            assertNotNull(scrollId);
        } finally {
            searchResponse.decRef();
        }

        final int originalStartAgoSeconds = randomIntBetween(2, 10);
        final long originStartTimeMillis = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(originalStartAgoSeconds);
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new TaskId(randomAlphanumericOfLength(10), randomNonNegativeLong()),
            originStartTimeMillis
        );

        final ReindexRequest request = new ReindexRequest().setSourceIndices(sourceIndex)
            .setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(
                new ResumeInfo(
                    origin,
                    new ScrollWorkerResumeInfo(scrollId, timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS)), randomStats(), null),
                    null
                )
            );
        // execute resume on data node because that's where we'll check the metrics
        final ResumeBulkByScrollResponse resumeResponse = client(dataNodeName).execute(
            ResumeReindexAction.INSTANCE,
            new ResumeBulkByScrollRequest(request)
        ).actionGet();
        clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        telemetryPlugin.collect();
        final List<Measurement> histograms = telemetryPlugin.getLongHistogramMeasurement(ReindexMetrics.REINDEX_TIME_HISTOGRAM);
        assertThat(histograms.size(), equalTo(1));
        assertThat(histograms.getFirst().getLong(), greaterThanOrEqualTo((long) originalStartAgoSeconds));
        assertThat(
            histograms.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SOURCE),
            equalTo(ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL)
        );
        assertThat(histograms.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
        final List<Measurement> completions = telemetryPlugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_COMPLETION_COUNTER);
        assertThat(completions.size(), equalTo(1));
        assertNull(completions.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE));
    }

    public void testResumeReindexFromPit_metricsRecordsDurationFromRelocationOrigin() {
        assumeTrue("reindex with point-in-time search must be enabled", ReindexPlugin.REINDEX_PIT_SEARCH_ENABLED);
        assertThat(internalCluster().numDataNodes(), equalTo(1));
        final String dataNodeName = internalCluster().getRandomDataNodeName();
        final TestTelemetryPlugin telemetryPlugin = internalCluster().getInstance(PluginsService.class, dataNodeName)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();
        telemetryPlugin.resetMeter(); // reset previous test metrics

        final String sourceIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final String destIndex = randomAlphanumericOfLength(10).toLowerCase(Locale.ROOT);
        final int totalDocs = randomIntBetween(20, 100);
        final int batchSize = randomIntBetween(1, 10);

        indexRandom(true, sourceIndex, totalDocs);

        OpenPointInTimeResponse openPitResponse = client().execute(
            TransportOpenPointInTimeAction.TYPE,
            new OpenPointInTimeRequest(sourceIndex).keepAlive(DEFAULT_SCROLL_TIMEOUT)
        ).actionGet();
        BytesReference pitId = openPitResponse.getPointInTimeId();
        SearchRequest firstPitSearch = new SearchRequest().source(
            new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                .sort(SortBuilders.pitTiebreaker())
                .size(batchSize)
        );
        SearchResponse searchResponse = client().search(firstPitSearch).actionGet();
        final Object[] searchAfterValues;
        try {
            if (searchResponse.pointInTimeId() != null) {
                pitId = searchResponse.pointInTimeId();
            }
            SearchHit lastHit = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1];
            searchAfterValues = lastHit.getSortValues();
            assertNotNull(searchAfterValues);
        } finally {
            searchResponse.decRef();
        }

        final int originalStartAgoSeconds = randomIntBetween(2, 10);
        final long originStartTimeMillis = System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(originalStartAgoSeconds);
        final ResumeInfo.RelocationOrigin origin = new ResumeInfo.RelocationOrigin(
            new TaskId(randomAlphanumericOfLength(10), randomNonNegativeLong()),
            originStartTimeMillis
        );

        final ReindexRequest request = new ReindexRequest().setShouldStoreResult(true)
            .setEligibleForRelocationOnShutdown(true)
            .setDestIndex(destIndex)
            .setSourceBatchSize(batchSize)
            .setRefresh(true)
            .setResumeInfo(
                new ResumeInfo(
                    origin,
                    new PitWorkerResumeInfo(pitId, searchAfterValues, timeAgo(randomTimeValue(2, 10, TimeUnit.HOURS)), randomStats(), null),
                    null
                )
            );
        request.getSearchRequest()
            .source(
                new SearchSourceBuilder().pointInTimeBuilder(new PointInTimeBuilder(pitId).setKeepAlive(DEFAULT_SCROLL_TIMEOUT))
                    .sort(SortBuilders.pitTiebreaker())
                    .size(batchSize)
            );
        request.getSearchRequest().scroll(null);
        request.getSearchRequest().indices(Strings.EMPTY_ARRAY);

        // Execute resume on data node because that's where we'll check the metrics
        final ResumeBulkByScrollResponse resumeResponse = client(dataNodeName).execute(
            ResumeReindexAction.INSTANCE,
            new ResumeBulkByScrollRequest(request)
        ).actionGet();
        clusterAdmin().prepareGetTask(resumeResponse.getTaskId())
            .setWaitForCompletion(true)
            .setTimeout(TimeValue.timeValueSeconds(30))
            .get();

        telemetryPlugin.collect();
        final List<Measurement> histograms = telemetryPlugin.getLongHistogramMeasurement(ReindexMetrics.REINDEX_TIME_HISTOGRAM);
        assertThat(histograms.size(), equalTo(1));
        assertThat(histograms.getFirst().getLong(), greaterThanOrEqualTo((long) originalStartAgoSeconds));
        assertThat(
            histograms.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SOURCE),
            equalTo(ReindexMetrics.ATTRIBUTE_VALUE_SOURCE_LOCAL)
        );
        assertThat(histograms.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_SLICING_MODE), equalTo("none"));
        final List<Measurement> completions = telemetryPlugin.getLongCounterMeasurement(ReindexMetrics.REINDEX_COMPLETION_COUNTER);
        assertThat(completions.size(), equalTo(1));
        assertNull(completions.getFirst().attributes().get(ReindexMetrics.ATTRIBUTE_NAME_ERROR_TYPE));
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
            .setResumeInfo(new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo("ignored", 0L, randomStats(), null), null));

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
            .setResumeInfo(new ResumeInfo(randomOrigin(), new ScrollWorkerResumeInfo("ignored", 0L, randomStats(), null), null));

        ActionRequestValidationException e = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(ResumeReindexAction.INSTANCE, new ResumeBulkByScrollRequest(reindexRequest)).actionGet()
        );

        assertTrue(e.getMessage().contains("Resumed task should be eligible for relocation on shutdown"));
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

    private static void assertSlicedResponse(
        TaskResult taskResult,
        Map<Integer, SliceStatus> resumeStatus,
        Map<Integer, Long> firstBatchDocsBySlice,
        long totalDocs,
        int batchSize
    ) {
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
                Long firstBatchDocs = firstBatchDocsBySlice.get(sliceId);
                assertNotNull(firstBatchDocs);
                assertSliceStatus(slice, status, batchSize, firstBatchDocs);
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
        long remainingDocs
    ) {
        assertTrue(task.isCompleted());
        Map<String, Object> response = task.getResponseAsMap();
        assertNotNull(response);

        // total should equal to total hits from the search
        assertEquals(totalDocs, longFromMap(response, "total"));
        // stats are updated
        assertEquals(remainingDocs + resumeStatus.getCreated(), longFromMap(response, "created"));
        long remainingBatches = remainingDocs / batchSize + (remainingDocs % batchSize == 0 ? 0 : 1);
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

    private static void assertSliceStatus(
        Map<String, Object> response,
        BulkByScrollTask.Status resumeStatus,
        int batchSize,
        long firstBatchDocs
    ) {
        assertEquals((int) resumeStatus.getSliceId(), intFromMap(response, "slice_id"));
        long remainingDocs = resumeStatus.getTotal() - Math.min(resumeStatus.getTotal(), firstBatchDocs);
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

    private static long timeAgo(TimeValue period) {
        return System.currentTimeMillis() - period.millis();
    }

    private static ResumeInfo.RelocationOrigin randomOrigin() {
        return new ResumeInfo.RelocationOrigin(
            randomBoolean() ? TaskId.EMPTY_TASK_ID : new TaskId(randomAlphanumericOfLength(10), randomNonNegativeLong()),
            randomNonNegativeLong()
        );
    }
}
