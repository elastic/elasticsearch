/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.breaker.CircuitBreakerService;
import org.elasticsearch.indices.breaker.HierarchyCircuitBreakerService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.async.GetAsyncStatusRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.AsyncStatusResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.GetAsyncStatusAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;

import static org.elasticsearch.search.SearchService.MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.core.XPackPlugin.ASYNC_RESULTS_INDEX;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Tests how an async search whose response is too large to store fails. Hits that are projected to be over the limit fail the search as
 * they arrive, and a response that gets past that check is rejected by the store. A response that isn't stored isn't checked.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class AsyncSearchStoreFailureIT extends AsyncSearchIntegTestCase {

    private static final String INDEX = "test";
    private static final int NUM_DOCS = 200;
    private static final TimeValue LONG_WAIT = TimeValue.timeValueMinutes(1);
    private static final String CHECK_FAILURE = "Can't store an async search response larger than [102400] bytes, and the hits of this "
        + "search";

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // The test cluster randomly makes the request breaker a noop one.
            .put(HierarchyCircuitBreakerService.REQUEST_CIRCUIT_BREAKER_TYPE_SETTING.getKey(), "memory")
            .build();
    }

    /**
     * The hits are under the limit, so the fetch phase lets them through, and the aggregation makes the response too large to store. The
     * store saves a failure in its place.
     */
    public void testStoreSavesAFailureInPlaceOfAResponseOverTheLimit() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        int numDocs = 400;
        assertAcked(prepareCreate(INDEX).setSettings(indexSettings(2, 0)).setMapping("k", "type=keyword"));
        indexDocs(numDocs, () -> randomAlphaOfLength(2048));
        updateClusterSettings(Settings.builder().put(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey(), "200kb"));
        try {
            String id = submitAndGetId(
                searchNode,
                new SearchSourceBuilder().size(40).aggregation(AggregationBuilders.terms("k").field("k").size(numDocs)),
                true
            );
            ensureTaskCompletion(id);

            AsyncSearchResponse response = client(searchNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
            try {
                assertFalse(response.isRunning());
                assertNull(response.getSearchResponse());
                assertThat(
                    response.getFailure().getMessage(),
                    containsString("Can't store an async search response larger than [204800] bytes.")
                );
            } finally {
                response.decRef();
            }
            assertCompletionStatus(searchNode, id, RestStatus.BAD_REQUEST);
            deleteAsyncSearch(id);
            assertBreakersReleased();
        } finally {
            updateClusterSettings(Settings.builder().putNull(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()));
        }
    }

    /**
     * The search runs past a zero wait_for_completion_timeout, which is all that marks the response as stored. Chunks are small
     * enough that each shard sends more than one.
     */
    public void testChunkedFetchFailsHitsProjectedOverTheLimit() throws Exception {
        assertHitsProjectedOverTheLimitFailTheSearch(true);
    }

    /**
     * Like {@link #testChunkedFetchFailsHitsProjectedOverTheLimit}, with each shard sending its hits at once.
     */
    public void testFetchFailsHitsProjectedOverTheLimit() throws Exception {
        assertHitsProjectedOverTheLimitFailTheSearch(false);
    }

    /**
     * The search completes within wait_for_completion_timeout, so only keep_on_completion marks the response as stored.
     */
    public void testHitsOfAResponseKeptOnCompletionAreChecked() throws Exception {
        String searchNode = startNodeWithIncompressibleDocs();
        try {
            AsyncSearchResponse response = submit(searchNode, new SearchSourceBuilder().size(NUM_DOCS), LONG_WAIT, true);
            String id;
            try {
                assertFalse(response.isRunning());
                id = response.getId();
                assertNotNull(id);
                assertThat(rootCause(response.getFailure()).getMessage(), containsString(CHECK_FAILURE));
            } finally {
                response.decRef();
            }
            assertCompletionStatus(searchNode, id, RestStatus.BAD_REQUEST);
            deleteAsyncSearch(id);
            assertBreakersReleased();
        } finally {
            resetSettings();
        }
    }

    /**
     * The search completes within wait_for_completion_timeout and isn't kept, so nothing is stored and the response comes back in full.
     */
    public void testHitsOfAResponseThatIsNotStoredAreNotChecked() throws Exception {
        String searchNode = startNodeWithIncompressibleDocs();
        try {
            AsyncSearchResponse response = submit(searchNode, new SearchSourceBuilder().size(NUM_DOCS), LONG_WAIT, false);
            try {
                assertFalse(response.isRunning());
                assertNull(response.getId());
                assertNull(response.getFailure());
                assertThat(response.getSearchResponse().getHits().getHits().length, equalTo(NUM_DOCS));
            } finally {
                response.decRef();
            }
            assertBreakersReleased();
        } finally {
            resetSettings();
        }
    }

    /**
     * Hits that repeat one letter take many times the limit on the heap but compress to a fraction of it, so they are stored.
     */
    public void testCompressibleHitsOverTheLimitAreStored() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        int numDocs = 20;
        createIndex(INDEX, indexSettings(2, 0).build());
        indexDocs(numDocs, () -> "a".repeat(4096));
        updateClusterSettings(Settings.builder().put(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey(), "10kb"));
        try {
            String id = submitAndGetId(searchNode, new SearchSourceBuilder().size(numDocs), true);
            ensureTaskCompletion(id);

            AsyncSearchResponse response = client(searchNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
            try {
                assertFalse(response.isRunning());
                assertNull(response.getFailure());
                assertThat(response.getSearchResponse().getHits().getHits().length, equalTo(numDocs));
            } finally {
                response.decRef();
            }
            deleteAsyncSearch(id);
            assertBreakersReleased();
        } finally {
            updateClusterSettings(Settings.builder().putNull(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey()));
        }
    }

    /**
     * What a sample projects for the hits of a response is close to the size the store writes for that response.
     */
    public void testProjectionIsCloseToTheStoredSize() throws Exception {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        int numDocs = 100;
        createIndex(INDEX, indexSettings(2, 0).build());
        indexDocs(numDocs, () -> randomAlphaOfLength(4096));

        String id = submitAndGetId(searchNode, new SearchSourceBuilder().size(numDocs), true);
        ensureTaskCompletion(id);
        GetResponse doc = client().prepareGet().setIndex(ASYNC_RESULTS_INDEX).setId(AsyncExecutionId.decode(id).getDocId()).get();
        long storedBytes = ((String) doc.getSource().get(AsyncTaskIndexService.RESULT_FIELD)).length();

        AsyncSearchResponse response = client(searchNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
        try {
            List<SearchHit> hits = Arrays.asList(response.getSearchResponse().getHits().getHits());
            assertThat(hits.size(), equalTo(numDocs));
            long heapBytes = 0L;
            for (SearchHit hit : hits) {
                heapBytes += hit.ramBytesUsed();
            }
            double projected = heapBytes * Math.min(
                StoredResponseSizeCheck.Sample.spread(hits, heapBytes).storedPerHeapByte(),
                StoredResponseSizeCheck.Sample.runs(hits, heapBytes).storedPerHeapByte()
            );
            assertThat("projected [" + projected + "], stored [" + storedBytes + "]", projected / storedBytes, closeTo(1.0, 0.05));
        } finally {
            response.decRef();
        }
        deleteAsyncSearch(id);
    }

    private void assertHitsProjectedOverTheLimitFailTheSearch(boolean chunked) throws Exception {
        String searchNode = startNodeWithIncompressibleDocs();
        updateClusterSettings(Settings.builder().put(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey(), chunked));
        try {
            String id = submitAndGetId(searchNode, new SearchSourceBuilder().size(NUM_DOCS), false);
            ensureTaskCompletion(id);

            AsyncSearchResponse response = client(searchNode).execute(GetAsyncSearchAction.INSTANCE, new GetAsyncResultRequest(id)).get();
            try {
                assertFalse(response.isRunning());
                // The search fails before its hits are merged, so it keeps at most the partial response of the query phase.
                SearchResponse partial = response.getSearchResponse();
                assertThat(partial == null ? 0 : partial.getHits().getHits().length, equalTo(0));
                assertThat(rootCause(response.getFailure()).getMessage(), containsString(CHECK_FAILURE));
            } finally {
                response.decRef();
            }
            assertCompletionStatus(searchNode, id, RestStatus.BAD_REQUEST);
            deleteAsyncSearch(id);
            assertBreakersReleased();
        } finally {
            resetSettings();
        }
    }

    /**
     * Indexes docs that take several times the 100 KB limit once stored, since random letters barely compress. Also sets that limit
     * and small chunks, and returns a coordinating-only node to search from.
     */
    private String startNodeWithIncompressibleDocs() {
        String searchNode = internalCluster().startCoordinatingOnlyNode(Settings.EMPTY);
        createIndex(INDEX, indexSettings(2, 0).build());
        indexDocs(NUM_DOCS, () -> randomAlphaOfLength(4096));
        updateClusterSettings(
            Settings.builder()
                .put(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey(), "100kb")
                .put(SearchService.FETCH_PHASE_CHUNKED_TARGET_CHUNK_BYTES.getKey(), "256kb")
        );
        return searchNode;
    }

    private void resetSettings() {
        updateClusterSettings(
            Settings.builder()
                .putNull(MAX_ASYNC_SEARCH_RESPONSE_SIZE_SETTING.getKey())
                .putNull(SearchService.FETCH_PHASE_CHUNKED_ENABLED.getKey())
                .putNull(SearchService.FETCH_PHASE_CHUNKED_TARGET_CHUNK_BYTES.getKey())
        );
    }

    private void indexDocs(int numDocs, Supplier<String> value) {
        List<IndexRequestBuilder> docs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            docs.add(prepareIndex(INDEX).setSource("k", value.get()));
        }
        indexRandom(true, docs);
    }

    private static AsyncSearchResponse submit(String node, SearchSourceBuilder source, TimeValue waitForCompletion, boolean keep)
        throws Exception {
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, INDEX).setWaitForCompletionTimeout(waitForCompletion)
            .setKeepOnCompletion(keep);
        return client(node).execute(SubmitAsyncSearchAction.INSTANCE, request).get();
    }

    /**
     * Submits {@code source} on {@code node} with a zero wait_for_completion_timeout, so the response is stored either way.
     */
    private static String submitAndGetId(String node, SearchSourceBuilder source, boolean keep) throws Exception {
        AsyncSearchResponse response = submit(node, source, TimeValue.ZERO, keep);
        try {
            assertNotNull(response.getId());
            return response.getId();
        } finally {
            response.decRef();
        }
    }

    private static void assertCompletionStatus(String node, String id, RestStatus expected) throws Exception {
        AsyncStatusResponse status = client(node).execute(GetAsyncStatusAction.INSTANCE, new GetAsyncStatusRequest(id)).get();
        assertThat(status.getCompletionStatus(), equalTo(expected));
    }

    private static Throwable rootCause(Throwable e) {
        while (e.getCause() != null) {
            e = e.getCause();
        }
        return e;
    }

    private void assertBreakersReleased() throws Exception {
        assertBusy(() -> {
            for (String node : internalCluster().getNodeNames()) {
                assertThat(node, requestBreaker(node).getUsed(), equalTo(0L));
            }
        });
    }

    private static CircuitBreaker requestBreaker(String node) {
        return internalCluster().getInstance(CircuitBreakerService.class, node).getBreaker(CircuitBreaker.REQUEST);
    }
}
