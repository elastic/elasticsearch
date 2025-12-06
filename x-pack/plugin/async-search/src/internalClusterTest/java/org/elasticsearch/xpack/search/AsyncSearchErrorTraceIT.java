/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.apache.http.HttpEntity;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.ErrorTraceHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

@TestLogging(
    reason = "testing debug log output to identify race condition",
    value = "org.elasticsearch.xpack.search.MutableSearchResponse:DEBUG,org.elasticsearch.xpack.search.AsyncSearchTask:DEBUG"
)
public class AsyncSearchErrorTraceIT extends AsyncSearchIntegTestCase {

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
        ErrorTraceHelper.expectStackTraceCleared(internalCluster());
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncRequest);

        try {
            if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }
        } finally {
            deleteAsyncSearchIfPresent(createAsyncResponseEntity);
        }
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
        ErrorTraceHelper.expectStackTraceObserved(internalCluster());
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncRequest);

        try {
            if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                getAsyncRequest.addParameter("error_trace", "true");
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }
        } finally {
            deleteAsyncSearchIfPresent(createAsyncResponseEntity);
        }
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
        ErrorTraceHelper.expectStackTraceCleared(internalCluster());
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncRequest);

        try {
            if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                getAsyncRequest.addParameter("error_trace", "false");
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }
        } finally {
            deleteAsyncSearchIfPresent(createAsyncResponseEntity);
        }
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
            Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncRequest);

            try {
                if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
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
            } finally {
                deleteAsyncSearchIfPresent(createAsyncResponseEntity);
            }
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
        ErrorTraceHelper.expectStackTraceCleared(internalCluster());
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncSearchRequest);

        try {
            if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                getAsyncRequest.addParameter("error_trace", "true");
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }
        } finally {
            deleteAsyncSearchIfPresent(createAsyncResponseEntity);
        }
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
        ErrorTraceHelper.expectStackTraceObserved(internalCluster());
        Map<String, Object> createAsyncResponseEntity = performRequestAndGetResponseEntity(createAsyncSearchRequest);
        try {
            if (Boolean.TRUE.equals(createAsyncResponseEntity.get("is_running"))) {
                String asyncExecutionId = (String) createAsyncResponseEntity.get("id");
                Request getAsyncRequest = new Request("GET", "/_async_search/" + asyncExecutionId);
                getAsyncRequest.addParameter("error_trace", "false");
                awaitAsyncRequestDoneRunning(getAsyncRequest);
            }
        } finally {
            deleteAsyncSearchIfPresent(createAsyncResponseEntity);
        }
    }

    private Map<String, Object> performRequestAndGetResponseEntity(Request r) throws IOException {
        Response response = getRestClient().performRequest(r);
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        return XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(), false);
    }

    private void deleteAsyncSearchIfPresent(Map<String, Object> map) throws IOException {
        String id = (String) map.get("id");
        if (id == null) {
            return;
        }

        // Make sure the .async-search system index is green before deleting it
        try {
            ensureGreen(".async-search");
        } catch (Exception ignore) {
            // the index may not exist
        }

        Response response = getRestClient().performRequest(new Request("DELETE", "/_async_search/" + id));
        HttpEntity entity = response.getEntity();
        if (entity != null) {
            EntityUtils.consumeQuietly(entity);
        }
    }

    private void awaitAsyncRequestDoneRunning(Request getAsyncRequest) throws Exception {
        assertBusy(() -> {
            Map<String, Object> getAsyncResponseEntity = performRequestAndGetResponseEntity(getAsyncRequest);
            assertFalse((Boolean) getAsyncResponseEntity.get("is_running"));
        });
    }
}
