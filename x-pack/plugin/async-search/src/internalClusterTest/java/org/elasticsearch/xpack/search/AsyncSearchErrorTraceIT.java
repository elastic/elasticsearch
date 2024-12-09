/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.search;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Request;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
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

    private AtomicBoolean hasStackTrace;

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
                        hasStackTrace.set(throwable.isPresent());
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
        hasStackTrace = new AtomicBoolean();
        setupMessageListener();
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
        getRestClient().performRequest(searchRequest);
        assertFalse(hasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceTrue() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupMessageListener();
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
        getRestClient().performRequest(searchRequest);
        assertTrue(hasStackTrace.get());
    }

    public void testAsyncSearchFailingQueryErrorTraceFalse() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupMessageListener();
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
        getRestClient().performRequest(searchRequest);
        assertFalse(hasStackTrace.get());
    }
}
