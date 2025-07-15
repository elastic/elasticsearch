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
import org.junit.After;
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
        // TODO: make this test work with batched query execution by enhancing ErrorTraceHelper.setupErrorTraceListener
        updateClusterSettings(Settings.builder().put(SearchService.BATCHED_QUERY_PHASE.getKey(), false));
    }

    @After
    public void resetSettings() {
        updateClusterSettings(Settings.builder().putNull(SearchService.BATCHED_QUERY_PHASE.getKey()));
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

    public void testAsyncSearchFailingQueryErrorTraceDefault() throws Exception {
        setupIndexWithDocs();

        Request createAsyncRequest = new Request("POST", "/_async_search");
        createAsyncRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        createAsyncRequest.addParameter("keep_on_completion", "true");
        createAsyncRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(createAsyncRequest, TimeValue.ZERO);
        if (createAsyncResponseEntity.get("is_running").equals("true")) {
            String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
            Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
            awaitAsyncRequestDoneRunning(getAsyncRequest);
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrue() throws Exception {
        setupIndexWithDocs();

        Request createAsyncRequest = new Request("POST", "/_async_search");
        createAsyncRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        createAsyncRequest.addParameter("error_trace", "true");
        createAsyncRequest.addParameter("keep_on_completion", "true");
        createAsyncRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(createAsyncRequest, TimeValue.ZERO);
        if (createAsyncResponseEntity.get("is_running").equals("true")) {
            String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
            Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
            getAsyncRequest.addParameter("error_trace", "true");
            awaitAsyncRequestDoneRunning(getAsyncRequest);
        }
        // check that the stack trace was sent from the data node to the coordinating node
        assertTrue(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalse() throws Exception {
        setupIndexWithDocs();

        Request createAsyncRequest = new Request("POST", "/_async_search");
        createAsyncRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        createAsyncRequest.addParameter("error_trace", "false");
        createAsyncRequest.addParameter("keep_on_completion", "true");
        createAsyncRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(createAsyncRequest, TimeValue.ZERO);
        if (createAsyncResponseEntity.get("is_running").equals("true")) {
            String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
            Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
            getAsyncRequest.addParameter("error_trace", "false");
            awaitAsyncRequestDoneRunning(getAsyncRequest);
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testDataNodeLogsStackTrace() throws Exception {
        setupIndexWithDocs();

        Request createAsyncRequest = new Request("POST", "/_async_search");
        createAsyncRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);

        // No matter the value of error_trace (empty, true, or false) we should see stack traces logged
        int errorTraceValue = randomIntBetween(0, 2);
        if (errorTraceValue == 0) {
            createAsyncRequest.addParameter("error_trace", "true");
        } else if (errorTraceValue == 1) {
            createAsyncRequest.addParameter("error_trace", "false");
        } // else empty

        createAsyncRequest.addParameter("keep_on_completion", "true");
        createAsyncRequest.addParameter("wait_for_completion_timeout", "0ms");

        String errorTriggeringIndex = "test2";
        int numShards = getNumShards(errorTriggeringIndex).numPrimaries;
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);
            Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(
                createAsyncRequest,
                TimeValue.ZERO
            );
            if (createAsyncResponseEntity.get("is_running").equals("true")) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                // Use the same value of error_trace as the search request
                if (errorTraceValue == 0) {
                    getAsyncRequest.addParameter("error_trace", "true");
                } else if (errorTraceValue == 1) {
                    getAsyncRequest.addParameter("error_trace", "false");
                } // else empty
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }

            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testAsyncSearchFailingQueryErrorTraceFalseOnSubmitAndTrueOnGet() throws Exception {
        setupIndexWithDocs();

        Request createAsyncSearchRequest = new Request("POST", "/_async_search");
        createAsyncSearchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        createAsyncSearchRequest.addParameter("error_trace", "false");
        createAsyncSearchRequest.addParameter("keep_on_completion", "true");
        createAsyncSearchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(
            createAsyncSearchRequest,
            TimeValue.ZERO
        );
        if (createAsyncResponseEntity.get("is_running").equals("true")) {
            String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
            Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
            getAsyncRequest.addParameter("error_trace", "true");
            awaitAsyncRequestDoneRunning(getAsyncRequest);
        }
        // check that the stack trace was not sent from the data node to the coordinating node
        assertFalse(transportMessageHasStackTrace.getAsBoolean());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrueOnSubmitAndFalseOnGet() throws Exception {
        setupIndexWithDocs();

        Request createAsyncSearchRequest = new Request("POST", "/_async_search");
        createAsyncSearchRequest.setJsonEntity("""
            {
                "query": {
                    "simple_query_string" : {
                        "query": "foo",
                        "fields": ["field"]
                    }
                }
            }
            """);
        createAsyncSearchRequest.addParameter("error_trace", "true");
        createAsyncSearchRequest.addParameter("keep_on_completion", "true");
        createAsyncSearchRequest.addParameter("wait_for_completion_timeout", "0ms");
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(
            createAsyncSearchRequest,
            TimeValue.ZERO
        );
        if (createAsyncResponseEntity.get("is_running").equals("true")) {
            String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
            Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
            getAsyncRequest.addParameter("error_trace", "false");
            awaitAsyncRequestDoneRunning(getAsyncRequest);
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

    private void awaitAsyncRequestDoneRunning(Request getAsyncRequest) throws Exception {
        assertBusy(() -> {
            Map<String, Object> getAsyncResponseEntity = performRequestAndGetResponseEntityAfterDelay(
                getAsyncRequest,
                TimeValue.timeValueSeconds(1L)
            );
            assertFalse((Boolean) getAsyncResponseEntity.get("is_running"));
        });
    }
}
