/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.ErrorTraceHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BooleanSupplier;

public class AsyncSearchErrorTraceIT extends ESIntegTestCase {
    private BooleanSupplier transportMessageHasStackTrace;

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), AsyncSearch.class, MockTransportService.TestPlugin.class);
    }

    @BeforeClass
    public static void setDebugLogLevel() {
        Configurator.setLevel(SearchService.class, Level.DEBUG);
    }

    @Before
    public void setupMessageListener() {
        transportMessageHasStackTrace = ErrorTraceHelper.setupErrorTraceListener(internalCluster());
    }

    private int setupIndexWithDocs() {
        int numShards = numberOfShards();
        createIndex("test1", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build());
        createIndex("test2", Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numShards).build());
        indexRandom(
            true,
            prepareIndex("test1").setId("1").setSource("field", "foo"),
            prepareIndex("test2").setId("10").setSource("field", 5)
        );
        refresh();
        return numShards;
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

    public void testLoggingInAsyncSearchFailingQueryErrorTraceTrue() throws IOException, InterruptedException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addUnseenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
            String asyncExecutionId = (String) responseEntity.get("id");
            Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
            request.addParameter("error_trace", "true");
            while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
                responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
            }

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLoggingInAsyncSearchFailingQueryErrorTraceFalse() throws IOException, InterruptedException {
        int numShards = setupIndexWithDocs();
        // error_trace defaults to false so we can test both cases with some randomization
        final boolean defineErrorTraceFalse = randomBoolean();

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
        if (defineErrorTraceFalse) {
            searchRequest.addParameter("error_trace", "false");
        }
        searchRequest.addParameter("keep_on_completion", "true");
        searchRequest.addParameter("wait_for_completion_timeout", "0ms");

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            Map<String, Object> responseEntity = performRequestAndGetResponseEntityAfterDelay(searchRequest, TimeValue.ZERO);
            String asyncExecutionId = (String) responseEntity.get("id");
            Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
            if (defineErrorTraceFalse) {
                request.addParameter("error_trace", "false");
            }
            while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
                responseEntity = performRequestAndGetResponseEntityAfterDelay(request, TimeValue.timeValueSeconds(1L));
            }

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
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
