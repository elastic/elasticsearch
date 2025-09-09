/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.search;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.xpack.core.async.AsyncExecutionId;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.junit.After;
import org.junit.Before;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class MutableSearchResponseRefCountingTests extends ESTestCase {

    private TestThreadPool threadPool;
    private NoOpClient client;

    @Before
    public void setup() {
        this.threadPool = new TestThreadPool(getTestName());
        this.client = new NoOpClient(threadPool);
    }

    @After
    public void cleanup() throws Exception {
        terminate(threadPool);
    }

    public void testBuildSucceedsIfAnotherThreadHoldsRef() {
        final int totalShards = 1;
        final int skippedShards = 0;

        // Build a SearchResponse (sr refCount -> 1)
        SearchResponse searchResponse = createSearchResponse(totalShards, totalShards, skippedShards);

        // Take a ref - (msr refCount -> 1, sr refCount -> 2)
        MutableSearchResponse msr = new MutableSearchResponse(threadPool.getThreadContext());
        msr.updateShardsAndClusters(totalShards, skippedShards, null);
        msr.updateFinalResponse(searchResponse, false);

        searchResponse.decRef(); // sr refCount -> 1

        //Simulate another thread : take a resource (msr refCount -> 2)
        msr.incRef();
        // close resource (msr refCount -> 1) -> closeInternal not called yet
        msr.decRef();

        // Build a response
        AsyncSearchResponse resp = msr.toAsyncSearchResponse(createAsyncSearchTask(),
            System.currentTimeMillis() + 60_000, /*restoreResponseHeaders*/
            false);
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


    public void testGetResponseAfterCloseReturnsGone    () throws Exception {
        final int totalShards = 1;
        final int skippedShards = 0;

        // Build a SearchResponse (sr refCount -> 1)
        SearchResponse searchResponse = createSearchResponse(totalShards, totalShards, skippedShards);

        // Create an AsyncSearchTask
        AsyncSearchTask task = createAsyncSearchTask();

        // Get response instance and method from task
        Field f = AsyncSearchTask.class.getDeclaredField("searchResponse");
        f.setAccessible(true);
        Method m = AsyncSearchTask.class.getDeclaredMethod("getResponseWithHeaders");
        m.setAccessible(true);

        // Take a ref - (msr refCount -> 1, sr refCount -> 2)
        MutableSearchResponse msr = (MutableSearchResponse) f.get(task);
        msr.updateShardsAndClusters(totalShards, skippedShards, null);
        msr.updateFinalResponse(searchResponse, false);

        searchResponse.decRef(); // sr ref -> 1
        msr.decRef();            // msr ref -> 0 -> closeInternal() -> sr ref -> 0

        // Invoke getResponseWithHeaders and expect GONE exception
        InvocationTargetException ite = expectThrows(InvocationTargetException.class, () -> {
            AsyncSearchResponse resp = (AsyncSearchResponse) m.invoke(task);
            if (resp != null)  {
                resp.decRef();
            }
        });

        Throwable cause = ExceptionsHelper.unwrapCause(ite.getCause());
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
            isCancelled -> () -> new AggregationReduceContext.ForFinal(null, null, null, null, null, PipelineAggregator.PipelineTree.EMPTY)
        );
    }

    private SearchResponse createSearchResponse(int totalShards, int successfulShards, int skippedShards) {
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
            null
        );
    }
}
