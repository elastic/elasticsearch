/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.http;

import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.transport.TransportMessageListener;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;

public class SearchErrorTraceIT extends HttpSmokeTestCase {
    private AtomicBoolean hasStackTrace;

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

    public void testSearchFailingQueryErrorTraceDefault() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_search");
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

    public void testSearchFailingQueryErrorTraceTrue() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_search");
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

    public void testSearchFailingQueryErrorTraceFalse() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        Request searchRequest = new Request("POST", "/_search");
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

    public void testMultiSearchFailingQueryErrorTraceDefault() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(
            new SearchRequest("test*").source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo").field("field")))
        );
        Request searchRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        searchRequest.setEntity(
            new NByteArrayEntity(requestBody, ContentType.create(contentType.mediaTypeWithoutParameters(), (Charset) null))
        );
        getRestClient().performRequest(searchRequest);
        assertFalse(hasStackTrace.get());
    }

    public void testMultiSearchFailingQueryErrorTraceTrue() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(
            new SearchRequest("test*").source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo").field("field")))
        );
        Request searchRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        searchRequest.setEntity(
            new NByteArrayEntity(requestBody, ContentType.create(contentType.mediaTypeWithoutParameters(), (Charset) null))
        );
        searchRequest.addParameter("error_trace", "true");
        getRestClient().performRequest(searchRequest);
        assertTrue(hasStackTrace.get());
    }

    public void testMultiSearchFailingQueryErrorTraceFalse() throws IOException {
        hasStackTrace = new AtomicBoolean();
        setupIndexWithDocs();

        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(
            new SearchRequest("test*").source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo").field("field")))
        );
        Request searchRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        searchRequest.setEntity(
            new NByteArrayEntity(requestBody, ContentType.create(contentType.mediaTypeWithoutParameters(), (Charset) null))
        );
        searchRequest.addParameter("error_trace", "false");
        getRestClient().performRequest(searchRequest);

        assertFalse(hasStackTrace.get());
    }
}
