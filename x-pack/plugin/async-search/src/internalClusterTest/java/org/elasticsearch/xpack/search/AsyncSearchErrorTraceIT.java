/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

public class AsyncSearchErrorTraceIT extends ESIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // enable http
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(AsyncSearch.class);
    }

    private AtomicBoolean transportMessageHasStackTrace;

    @Before
    private void setupMessageListener() {
        internalCluster().getDataNodeInstances(TransportService.class).forEach(ts -> {
            ts.addMessageListener(new TransportMessageListener() {
                @Override
                public void onResponseSent(long requestId, String action, Exception error) {
                    TransportMessageListener.super.onResponseSent(requestId, action, error);
                    if (action.startsWith("indices:data/read/search")) {
                        Optional<Throwable> throwable = ExceptionsHelper.unwrapCausesAndSuppressed(
                            error,
                            t -> t.getStackTrace().length > 0
                        );
                        transportMessageHasStackTrace.set(throwable.isPresent());
                    }
                }
            });
        });
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

    public void testAsyncSearchFailingQueryErrorTraceDefault() throws IOException {
        transportMessageHasStackTrace = new AtomicBoolean();
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
        Map<String, Object> responseEntity = performRequestAndGetResponseEntity(searchRequest);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntity(request);
        }
        assertFalse(transportMessageHasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrue() throws IOException {
        transportMessageHasStackTrace = new AtomicBoolean();
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
        Map<String, Object> responseEntity = performRequestAndGetResponseEntity(searchRequest);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "true");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntity(request);
        }
        assertTrue(transportMessageHasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalse() throws IOException {
        transportMessageHasStackTrace = new AtomicBoolean();
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
        Map<String, Object> responseEntity = performRequestAndGetResponseEntity(searchRequest);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "false");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntity(request);
        }
        assertFalse(transportMessageHasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalseOnSubmitAndTrueOnGet() throws IOException {
        transportMessageHasStackTrace = new AtomicBoolean();
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
        Map<String, Object> responseEntity = performRequestAndGetResponseEntity(searchRequest);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "true");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntity(request);
        }
        assertFalse(transportMessageHasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrueOnSubmitAndFalseOnGet() throws IOException {
        transportMessageHasStackTrace = new AtomicBoolean();
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
        Map<String, Object> responseEntity = performRequestAndGetResponseEntity(searchRequest);
        String asyncExecutionId = (String) responseEntity.get("id");
        Request request = new Request("GET", "/_async_search/" + asyncExecutionId);
        request.addParameter("error_trace", "false");
        while (responseEntity.get("is_running") instanceof Boolean isRunning && isRunning) {
            responseEntity = performRequestAndGetResponseEntity(request);
        }
        assertTrue(transportMessageHasStackTrace.get());
    }

    private Map<String, Object> performRequestAndGetResponseEntity(Request r) throws IOException {
        Response response = getRestClient().performRequest(r);
        XContentType entityContentType = XContentType.fromMediaType(response.getEntity().getContentType().getValue());
        return XContentHelper.convertToMap(entityContentType.xContent(), response.getEntity().getContent(), false);
    }
}
