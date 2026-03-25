/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.async.AsyncTaskIndexService;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class AsyncSearchRefcountAndFallbackTests extends ESSingleNodeTestCase {

    private AsyncTaskIndexService<AsyncSearchResponse> store;
    public String index = ".async-search";

    private TestThreadPool threadPool;
    private NoOpClient client;

    final int totalShards = 1;
    final int skippedShards = 0;

    @Before
    public void setup() {
        this.threadPool = new TestThreadPool(getTestName());
        this.client = new NoOpClient(threadPool);

        ClusterService clusterService = getInstanceFromNode(ClusterService.class);
        TransportService transportService = getInstanceFromNode(TransportService.class);
        BigArrays bigArrays = getInstanceFromNode(BigArrays.class);
        this.store = new AsyncTaskIndexService<>(
            index,
            clusterService,
            transportService.getThreadPool().getThreadContext(),
            client(),
            "test_origin",
            AsyncSearchResponse::new,
            writableRegistry(),
            bigArrays
        );
    }

    @After
    public void cleanup() {
        terminate(threadPool);
    }

    /**
     * If another thread still holds a ref to the in-memory MutableSearchResponse,
     * building an AsyncSearchResponse succeeds (final SearchResponse is still live).
     */
    public void testBuildSucceedsIfAnotherThreadHoldsRef() {
        // Build a SearchResponse (sr refCount -> 1)
        SearchResponse searchResponse = createSearchResponse(totalShards, totalShards, skippedShards);

        // Take a ref - (msr refCount -> 1, sr refCount -> 2)
        MutableSearchResponse msr = new MutableSearchResponse(threadPool.getThreadContext());
        msr.updateShardsAndClusters(totalShards, skippedShards, null);
        msr.updateFinalResponse(searchResponse, false);

        searchResponse.decRef(); // sr refCount -> 1

        // Simulate another thread : take a resource (msr refCount -> 2)
        msr.incRef();
        // close resource (msr refCount -> 1) -> closeInternal not called yet
        msr.decRef();

        // Build a response
        AsyncSearchResponse resp = msr.toAsyncSearchResponse(createAsyncSearchTask(), System.currentTimeMillis() + 60_000, false, false);
        try {
            assertNotNull("Expect SearchResponse when a live ref prevents close", resp.getSearchResponse());
            assertNull("No failure expected while ref is held", resp.getFailure());
            assertFalse("Response should not be marked running", resp.isRunning());
        } finally {
            resp.decRef();
        }

        // Release msr (msr refCount -> 0, sr refCount -> 0) -> now calling closeInternal
        msr.decRef();
    }

    /**
     * When the in-memory MutableSearchResponse has been released (tryIncRef == false),
     * the task falls back to the async-search store and returns the persisted response (200 OK).
     */
    public void testFallbackToStoreWhenInMemoryResponseReleased() throws Exception {
        // Create an AsyncSearchTask
        AsyncSearchTask task = createAsyncSearchTask();
        assertNotNull(task);

        // Build a SearchResponse (ssr refCount -> 1) to be stored in the index
        SearchResponse storedSearchResponse = createSearchResponse(totalShards, totalShards, skippedShards);

        // Take a ref - (msr refCount -> 1, ssr refCount -> 2)
        MutableSearchResponse msr = task.getSearchResponse();
        msr.updateShardsAndClusters(totalShards, skippedShards, /*clusters*/ null);
        msr.updateFinalResponse(storedSearchResponse, /*ccsMinimizeRoundtrips*/ false);

        // Build the AsyncSearchResponse to persist in the store
        AsyncSearchResponse asyncSearchResponse = null;
        try {
            long now = System.currentTimeMillis();
            asyncSearchResponse = new AsyncSearchResponse(
                task.getExecutionId().getEncoded(),
                storedSearchResponse,
                /*failure*/ null,
                /*isPartial*/ false,
                /*isRunning*/ false,
                task.getStartTime(),
                now + TimeValue.timeValueMinutes(1).millis()
            );
        } finally {
            if (asyncSearchResponse != null) {
                // (ssr refCount -> 2, asr refCount -> 0)
                asyncSearchResponse.decRef();
            }
        }

        // Persist initial/final response to the store while we still hold a ref
        PlainActionFuture<DocWriteResponse> write = new PlainActionFuture<>();
        store.createResponse(task.getExecutionId().getDocId(), Map.of(), asyncSearchResponse, write);
        write.actionGet();

        // Release the in-memory objects so the task path must fall back to the store
        // - drop our extra ref to the SearchResponse that updateFinalResponse() took (sr -> 1)
        storedSearchResponse.decRef();
        // - drop the in-memory MutableSearchResponse so mutableSearchResponse.tryIncRef() == false
        // msr -> 0 (closeInternal runs, releasing its ssr) → tryIncRef() will now fail
        msr.decRef();

        PlainActionFuture<AsyncSearchResponse> future = new PlainActionFuture<>();
        task.getResponse(future);

        AsyncSearchResponse resp = future.actionGet();
        assertNotNull("Expected response loaded from store", resp);
        assertNull("No failure expected when loaded from store", resp.getFailure());
        assertNotNull("SearchResponse must be present", resp.getSearchResponse());
        assertFalse("Should not be running", resp.isRunning());
        assertFalse("Should not be partial", resp.isPartial());
        assertEquals(RestStatus.OK, resp.status());
    }

    /**
     * When both the in-memory MutableSearchResponse has been released AND the stored
     * document has been deleted or not found, the task returns GONE (410).
     */
    public void testGoneWhenInMemoryReleasedAndStoreMissing() throws Exception {
        AsyncSearchTask task = createAsyncSearchTask();

        SearchResponse searchResponse = createSearchResponse(totalShards, totalShards, skippedShards);
        MutableSearchResponse msr = task.getSearchResponse();
        msr.updateShardsAndClusters(totalShards, skippedShards, null);
        msr.updateFinalResponse(searchResponse, false);

        long now = System.currentTimeMillis();
        AsyncSearchResponse asr = new AsyncSearchResponse(
            task.getExecutionId().getEncoded(),
            searchResponse,
            null,
            false,
            false,
            task.getStartTime(),
            now + TimeValue.timeValueMinutes(1).millis()
        );
        asr.decRef();

        PlainActionFuture<DocWriteResponse> write = new PlainActionFuture<>();
        store.createResponse(task.getExecutionId().getDocId(), Map.of(), asr, write);
        write.actionGet();

        searchResponse.decRef();
        msr.decRef();

        // Delete the doc from store
        PlainActionFuture<DeleteResponse> del = new PlainActionFuture<>();
        store.deleteResponse(task.getExecutionId(), del);
        del.actionGet();

        // Now the task must surface GONE
        PlainActionFuture<AsyncSearchResponse> future = new PlainActionFuture<>();
        task.getResponse(future);

        Exception ex = expectThrows(Exception.class, future::actionGet);
        Throwable cause = ExceptionsHelper.unwrapCause(ex);
        assertThat(cause, instanceOf(ElasticsearchStatusException.class));
        assertThat(ExceptionsHelper.status(cause), is(RestStatus.GONE));
    }

    private AsyncSearchTask createAsyncSearchTask() {
        return new AsyncSearchTask(
            1L,
            "search",
            "indices:data/read/search",
            TaskId.EMPTY_TASK_ID,
            () -> "debug",
            TimeValue.timeValueMinutes(1),
            Map.of(),
            Map.of(),
            new AsyncExecutionId("debug", new TaskId("node", 1L)),
            client,
            threadPool,
            isCancelled -> () -> new AggregationReduceContext.ForFinal(null, null, null, null, null, PipelineAggregator.PipelineTree.EMPTY),
            store
        );
    }

    private SearchResponse createSearchResponse(int totalShards, int successfulShards, int skippedShards) {
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(1, 1, 0);
        return new SearchResponse(
            SearchHits.empty(Lucene.TOTAL_HITS_GREATER_OR_EQUAL_TO_ZERO, Float.NaN),
            null,
            null,
            false,
            false,
            null,
            0,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            1L,
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
    }
}
