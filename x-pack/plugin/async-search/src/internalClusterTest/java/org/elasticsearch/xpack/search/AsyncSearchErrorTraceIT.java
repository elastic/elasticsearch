/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.ErrorTraceHelper;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class AsyncSearchErrorTraceIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), AsyncSearch.class, MockTransportService.TestPlugin.class);
    }

    private BooleanSupplier transportMessageHasStackTrace;

    @Before
    public void setupMessageListener() {
        transportMessageHasStackTrace = ErrorTraceHelper.setupErrorTraceListener(internalCluster());
    }

    private void setupIndexWithDocs() {
        createIndex("test1", "test2");
        indexRandom(
            true,
            prepareIndex("test1").setId("1").setSource("field", "foo"),
            prepareIndex("test2").setId("10").setSource("field", 5)
        );
        refresh();
    }

    public void testAsyncSearchFailingQueryErrorTraceDefault() throws IOException, InterruptedException {
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrue() throws IOException, InterruptedException {
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("error_trace", "true");
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "true");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
        }
        // check that the stack trace was sent from the data node to the coordinating node
        assertTrue(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalse() throws IOException, InterruptedException {
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("error_trace", "false");
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "false");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalseOnSubmitAndTrueOnGet() throws IOException, InterruptedException {
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("error_trace", "false");
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "true");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrueOnSubmitAndFalseOnGet() throws IOException, InterruptedException {
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_async_search");
        searchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        searchRequest.addParameter("error_trace", "true");
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "false");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
        }
        // check that the stack trace was sent from the data node to the coordinating node
        assertTrue(transportMessageHasStackTrace.getAsBoolean());
    }

    private Map<String, Object> performRequestAndGetResponseEntityAfterDelay(Request r, TimeValue sleep) throws IOException,
        InterruptedException {
        Thread.sleep(sleep.millis());
        Response response = getRestClient().performRequest(r);
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        return XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(), false);
    }
}
