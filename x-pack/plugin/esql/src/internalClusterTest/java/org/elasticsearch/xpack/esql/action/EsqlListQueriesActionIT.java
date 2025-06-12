/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.test.IntOrLongMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.jsonEntityToMap;
import static org.hamcrest.Matchers.is;

public class EsqlListQueriesActionIT extends AbstractPausableIntegTestCase {
    private static final String QUERY = "from test | stats sum(pause_me)";

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testNoRunningQueries() throws Exception {
        var request = new Request("GET", "/_query/queries");
        var response = getRestClient().performRequest(request);
        assertThat(jsonEntityToMap(response.getEntity()), is(Map.of("queries", Map.of())));
    }

    public void testRunningQueries() throws Exception {
        String id = null;
        try (var initialResponse = sendAsyncQuery()) {
            id = initialResponse.asyncExecutionId().get();

            assertRunningQueries();
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(1));
            client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).get().close();
        } finally {
            if (id != null) {
                // Finish the query.
                scriptPermits.release(numberOfDocs());
                var getResultsRequest = new GetAsyncResultRequest(id);
                getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(60));
                client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).get().close();
            }
            scriptPermits.drainPermits();
        }
    }

    public void testRunningQueriesSync() throws Exception {
        var future = sendSyncQueryAsyncly();
        try {
            scriptWaits.acquire();
            assertRunningQueries();
        } finally {
            scriptPermits.release(numberOfDocs());
            future.actionGet(timeValueSeconds(60)).close();
        }
    }

    private static void assertRunningQueries() throws IOException {
        Response listResponse = getRestClient().performRequest(new Request("GET", "/_query/queries"));
        @SuppressWarnings("unchecked")
        var listResult = (Map<String, Map<String, Object>>) EsqlTestUtils.singleValue(jsonEntityToMap(listResponse.getEntity()).values());
        String queryId = EsqlTestUtils.singleValue(listResult.keySet());
        MapMatcher basicMatcher = MapMatcher.matchesMap()
            .entry("query", is(QUERY))
            .entry("start_time_millis", IntOrLongMatcher.isIntOrLong())
            .entry("running_time_nanos", IntOrLongMatcher.isIntOrLong());
        MapMatcher.assertMap(EsqlTestUtils.singleValue(listResult.values()), basicMatcher);

        Response getQueryResponse = getRestClient().performRequest(new Request("GET", "/_query/queries/" + queryId));
        MapMatcher.assertMap(
            jsonEntityToMap(getQueryResponse.getEntity()),
            basicMatcher.entry("documents_found", IntOrLongMatcher.isIntOrLong()).entry("values_loaded", IntOrLongMatcher.isIntOrLong())
        );
    }

    private EsqlQueryResponse sendAsyncQuery() {
        scriptPermits.drainPermits();
        scriptPermits.release(between(1, 5));
        return EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client()).query(QUERY).execute().actionGet(60, TimeUnit.SECONDS);
    }

    private ActionFuture<EsqlQueryResponse> sendSyncQueryAsyncly() {
        scriptPermits.drainPermits();
        scriptPermits.release(between(1, 5));
        return EsqlQueryRequestBuilder.newSyncEsqlQueryRequestBuilder(client()).query(QUERY).execute();
    }
}
