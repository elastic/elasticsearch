/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.apache.http.HttpEntity;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.IntegerMatcher;
import org.elasticsearch.test.MapMatcher;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.EsqlTestUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;

/**
 * Individual tests for specific aspects of the async query API.
 */
public class EsqlListQuerriesActionIT extends AbstractPausableIntegTestCase {
    public static final String QUERY = "from test | stats sum(pause_me)";

    @Override
    protected boolean addMockHttpTransport() {
        return false;
    }

    public void testNoRunningQueries() throws Exception {
        var request = new Request("GET", "/_query/queries");
        var response = getRestClient().performRequest(request);
        assertThat(entityToMap(response.getEntity()), is(Map.of("queries", Map.of())));
    }

    public void testRunningQueries() throws Exception {
        String id = null;
        try (var initialResponse = sendAsyncQuery()) {
            id = initialResponse.asyncExecutionId().get();
            // FIXME(gal, NOCOMMIT) more copy paste
            var getResultsRequest = new GetAsyncResultRequest(id);
            getResultsRequest.setWaitForCompletionTimeout(timeValueSeconds(1));
            client().execute(EsqlAsyncGetResultAction.INSTANCE, getResultsRequest).get().close();
            var request = new Request("GET", "/_query/queries");
            var response = getRestClient().performRequest(request);
            @SuppressWarnings("unchecked")
            var listResult = (Map<String, Map<String, Object>>) EsqlTestUtils.singleValue(entityToMap(response.getEntity()).values());
            var taskId = new TaskId(EsqlTestUtils.singleValue(listResult.keySet()));
            MapMatcher basicMatcher = MapMatcher.matchesMap()
                .entry("node", is(taskId.getNodeId()))
                .entry("id", IntegerMatcher.matches(taskId.getId()))
                .entry("query", is(QUERY))
                .entry("start_time_millis", IntegerMatcher.isIntOrLong())
                .entry("running_time_nanos", IntegerMatcher.isIntOrLong());
            MapMatcher.assertMap(EsqlTestUtils.singleValue(listResult.values()), basicMatcher);

            request = new Request("GET", "/_query/queries/" + taskId + "1234");
            response = getRestClient().performRequest(request);
            MapMatcher.assertMap(
                entityToMap(response.getEntity()),
                basicMatcher.entry("coordinating_node", isA(String.class))
                    .entry("data_nodes", allOf(isA(List.class), everyItem(isA(String.class))))
            );
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

    // FIXME(gal, NOCOMMIT) copy paste
    private EsqlQueryResponse sendAsyncQuery() {
        scriptPermits.drainPermits();
        scriptPermits.release(between(1, 5));
        return EsqlQueryRequestBuilder.newAsyncEsqlQueryRequestBuilder(client()).query(QUERY).execute().actionGet(60, TimeUnit.SECONDS);
    }

    // FIXME(gal, NOCOMMIT) copy pasted from another place
    @SuppressWarnings("unchecked")
    private static <T> Map<String, T> entityToMap(HttpEntity entity) throws IOException {
        try (InputStream content = entity.getContent()) {
            XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
            assertEquals(XContentType.JSON, xContentType);
            var map = XContentHelper.convertToMap(xContentType.xContent(), content, false);
            return (Map<String, T>) map;
        }
    }
}
