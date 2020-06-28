/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchPhaseExecutionException;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.core.search.action.AsyncSearchResponse;
import org.elasticsearch.xpack.core.search.action.GetAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchAction;
import org.elasticsearch.xpack.core.search.action.SubmitAsyncSearchRequest;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

@ESIntegTestCase.SuiteScopeTestCase
public class CompletionListenerFailuresIT extends AsyncSearchIntegTestCase {
    private static String indexName;
    private static int numShards;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        Collection<Class<? extends Plugin>> plugins = super.nodePlugins();
        ArrayList<Class<? extends Plugin>> list = new ArrayList<>(plugins);
        list.add(org.elasticsearch.xpack.search.FailReduceAggPlugin.class);
        return list;
    }

    @Override
    protected void setupSuiteScopeCluster() throws InterruptedException {
        indexName = "test-async";
        numShards = 5;
        int numDocs = randomIntBetween(100, 1000);
        createIndex(indexName, Settings.builder()
            .put("index.number_of_shards", numShards)
            .build());
        List<IndexRequestBuilder> reqs = new ArrayList<>();
        for (int i = 0; i < numDocs; i++) {
            reqs.add(client().prepareIndex(indexName).setSource("field", "value" + i));
        }
        indexRandom(true, true, reqs);
    }

    public void testSubmitFailureDuringReduction() {
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            new org.elasticsearch.xpack.search.FailReduceAggPlugin.FailReduceAggregationBuilder("fail"));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueSeconds(1));
        request.setBatchedReduceSize(2);
        //we configure the query to return results for 3 shards (enough for a partial reduction to happen) and to block after that.
        //This way we can test the behaviour when waitForCompletionTimeout expires when some results are available but before the search
        //has completed.
        BlockingQueryBuilder.QueryLatch queryLatch = BlockingQueryBuilder.blockAfterShards(3);
        request.getSearchRequest().source().query(new BlockingQueryBuilder(random().nextLong()));
        CircuitBreakingException circuitBreakingException = expectThrows(CircuitBreakingException.class,
            () -> client().execute(SubmitAsyncSearchAction.INSTANCE, request).get());
        assertEquals("boom", circuitBreakingException.getMessage());
        assertEquals(numShards - 3, queryLatch.getWaitingShards());

        //TODO how can we verify that the task gets cancelled?
    }

    public void testGetFailureDuringReduction() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            new org.elasticsearch.xpack.search.FailReduceAggPlugin.FailReduceAggregationBuilder("fail"));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(0L));
        request.setBatchedReduceSize(2);
        //we configure the query to return results for 3 shards (enough for a partial reduction to happen) and to block after that.
        //This way we can test the behaviour when waitForCompletionTimeout expires when some results are available but before the search
        //has completed.
        BlockingQueryBuilder.QueryLatch queryLatch = BlockingQueryBuilder.blockAfterShards(3);
        request.getSearchRequest().source().query(new BlockingQueryBuilder(random().nextLong()));
        //TODO I'm afraid that this submit may still rarely fail if a partial reduce happens before it has returned
        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(numShards));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));

        assertEquals(numShards - 3, queryLatch.getWaitingShards());

        GetAsyncResultRequest getAsyncResultRequest = new GetAsyncResultRequest(initial.getId());
        CircuitBreakingException circuitBreakingException = expectThrows(CircuitBreakingException.class,
            () -> client().execute(GetAsyncSearchAction.INSTANCE, getAsyncResultRequest).actionGet());
        assertEquals("boom", circuitBreakingException.getMessage());
        assertEquals(0, circuitBreakingException.getSuppressed().length);

        //TODO how can we verify that the task doesn't get cancelled?
    }

    public void testGetFailureDuringReductionNoPartialResults() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            new org.elasticsearch.xpack.search.FailReduceAggPlugin.FailReduceAggregationBuilder("fail"));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName);
        //This determines that no partial results will be available, hence no exception gets thrown
        //when the completion listener that we register for get async search is invoked
        request.setBatchedReduceSize(10);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(0L));
        request.getSearchRequest().source().query(new BlockingQueryBuilder(random().nextLong()));
        BlockingQueryBuilder.acquireQueryLatch(0);
        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(numShards));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));
        BlockingQueryBuilder.releaseQueryLatch();

        GetAsyncResultRequest getAsyncResultRequest = new GetAsyncResultRequest(initial.getId())
            .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(1));
        {
            AsyncSearchResponse response = client().execute(GetAsyncSearchAction.INSTANCE, getAsyncResultRequest).get();
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
            assertThat(response.getSearchResponse().getShardFailures().length, equalTo(0));
            assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
            assertThat(response.getFailure(), instanceOf(SearchPhaseExecutionException.class));
            assertThat(response.getFailure().getCause(), instanceOf(CircuitBreakingException.class));
            CircuitBreakingException circuitBreakingException = (CircuitBreakingException) response.getFailure().getCause();
            assertEquals("boom", circuitBreakingException.getMessage());
            assertEquals(0, response.getFailure().getSuppressed().length);
        }

        //wait until the task is unregistered, then the response will have been stored
        ensureTaskCompletion(initial.getId());

        {
            //get async search is now called after the final response has been stored in the index
            AsyncSearchResponse response = client().execute(GetAsyncSearchAction.INSTANCE, getAsyncResultRequest).get();
            assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
            assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
            assertThat(response.getSearchResponse().getShardFailures().length, equalTo(0));
            assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
            assertThat(response.getFailure(), instanceOf(SearchPhaseExecutionException.class));
            assertThat(response.getFailure().getCause(), instanceOf(CircuitBreakingException.class));
            CircuitBreakingException circuitBreakingException = (CircuitBreakingException) response.getFailure().getCause();
            assertEquals("boom", circuitBreakingException.getMessage());
            assertEquals(0, response.getFailure().getSuppressed().length);
        }
    }

    public void testGetFailureDuringReductionWithPartialResults() throws Exception {
        SearchSourceBuilder source = new SearchSourceBuilder().aggregation(
            new org.elasticsearch.xpack.search.FailReduceAggPlugin.FailReduceAggregationBuilder("fail"));
        SubmitAsyncSearchRequest request = new SubmitAsyncSearchRequest(source, indexName);
        request.setWaitForCompletionTimeout(TimeValue.timeValueMillis(0L));
        //This determines that partial results will be available: the search fails, onFailure gets invoked which triggers the completion
        // listeners, which will fail too when trying to reduce the partial results available from before the fatal failure.
        //when the completion listener that we register for get async search is invoked
        request.setBatchedReduceSize(2);
        request.getSearchRequest().source().query(new BlockingQueryBuilder(random().nextLong()));
        BlockingQueryBuilder.acquireQueryLatch(0);
        final AsyncSearchResponse initial = client().execute(SubmitAsyncSearchAction.INSTANCE, request).get();
        assertTrue(initial.isPartial());
        assertThat(initial.status(), equalTo(RestStatus.OK));
        assertThat(initial.getSearchResponse().getTotalShards(), equalTo(numShards));
        assertThat(initial.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(initial.getSearchResponse().getShardFailures().length, equalTo(0));
        BlockingQueryBuilder.releaseQueryLatch();
        GetAsyncResultRequest getAsyncResultRequest = new GetAsyncResultRequest(initial.getId())
            .setWaitForCompletionTimeout(TimeValue.timeValueSeconds(1));
        //get async search fails with the error thrown when reducing the partial results that were made available
        //before the search completed. Based on this failure, it is unknown whether search has completed or not.
        CircuitBreakingException circuitBreakingException = expectThrows(CircuitBreakingException.class,
            () -> client().execute(GetAsyncSearchAction.INSTANCE, getAsyncResultRequest).actionGet());
        assertEquals("boom", circuitBreakingException.getMessage());

        //wait until the task is unregistered, then the response will have been stored
        ensureTaskCompletion(initial.getId());

        //get async search is now called after the final response has been stored in the index
        AsyncSearchResponse response = client().execute(GetAsyncSearchAction.INSTANCE, getAsyncResultRequest).get();
        assertThat(response.getSearchResponse().getTotalShards(), equalTo(numShards));
        assertThat(response.getSearchResponse().getSuccessfulShards(), equalTo(0));
        assertThat(response.getSearchResponse().getShardFailures().length, equalTo(0));
        assertEquals(RestStatus.TOO_MANY_REQUESTS, response.status());
        assertThat(response.getFailure(), instanceOf(SearchPhaseExecutionException.class));
        assertThat(response.getFailure().getCause(), instanceOf(CircuitBreakingException.class));
        CircuitBreakingException cause = (CircuitBreakingException) response.getFailure().getCause();
        assertEquals("boom", cause.getMessage());
        assertEquals(1, response.getFailure().getSuppressed().length);
        Throwable suppressed = response.getFailure().getSuppressed()[0];
        assertThat(suppressed, instanceOf(CircuitBreakingException.class));
        assertEquals("boom", suppressed.getMessage());
    }
}
