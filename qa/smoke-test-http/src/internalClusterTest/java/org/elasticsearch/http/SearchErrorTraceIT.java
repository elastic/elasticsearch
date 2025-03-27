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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.core.config.Configurator;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.ErrorTraceHelper;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xcontent.XContentType;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.function.BooleanSupplier;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;

public class SearchErrorTraceIT extends HttpSmokeTestCase {
    private BooleanSupplier hasStackTrace;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopyNoNullElements(super.nodePlugins(), MockTransportService.TestPlugin.class);
    }

    @BeforeClass
    public static void setDebugLogLevel() {
        Configurator.setLevel("org.elasticsearch.search.SearchService", Level.DEBUG);
    }

    @Before
    public void setupMessageListener() {
        hasStackTrace = ErrorTraceHelper.setupErrorTraceListener(internalCluster());
    }

    private int setupIndexWithDocs() {
        int numShards = between(DEFAULT_MIN_NUM_SHARDS, DEFAULT_MAX_NUM_SHARDS);
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

    public void testSearchFailingQueryErrorTraceDefault() throws IOException {
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
        assertFalse(hasStackTrace.getAsBoolean());
    }

    public void testSearchFailingQueryErrorTraceTrue() throws IOException {
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
        assertTrue(hasStackTrace.getAsBoolean());
    }

    public void testSearchFailingQueryErrorTraceFalse() throws IOException {
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
        assertFalse(hasStackTrace.getAsBoolean());
    }

    public void testLoggingInSearchFailingQueryErrorTraceDefault() throws IOException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testNoLoggingInSearchFailingQueryErrorTraceTrue() throws IOException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addUnseenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            searchRequest.addParameter("error_trace", "true");
            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLoggingInSearchFailingQueryErrorTraceFalse() throws IOException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            searchRequest.addParameter("error_trace", "false");
            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testMultiSearchFailingQueryErrorTraceDefault() throws IOException {
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
        assertFalse(hasStackTrace.getAsBoolean());
    }

    public void testMultiSearchFailingQueryErrorTraceTrue() throws IOException {
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
        assertTrue(hasStackTrace.getAsBoolean());
    }

    public void testMultiSearchFailingQueryErrorTraceFalse() throws IOException {
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

        assertFalse(hasStackTrace.getAsBoolean());
    }

    public void testLoggingInMultiSearchFailingQueryErrorTraceDefault() throws IOException {
        int numShards = setupIndexWithDocs();

        XContentType contentType = XContentType.JSON;
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest().add(
            new SearchRequest("test*").source(new SearchSourceBuilder().query(simpleQueryStringQuery("foo").field("field")))
        );
        Request searchRequest = new Request("POST", "/_msearch");
        byte[] requestBody = MultiSearchRequest.writeMultiLineFormat(multiSearchRequest, contentType.xContent());
        searchRequest.setEntity(
            new NByteArrayEntity(requestBody, ContentType.create(contentType.mediaTypeWithoutParameters(), (Charset) null))
        );

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLoggingInMultiSearchFailingQueryErrorTraceTrue() throws IOException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addUnseenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testLoggingInMultiSearchFailingQueryErrorTraceFalse() throws IOException {
        int numShards = setupIndexWithDocs();

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

        String errorTriggeringIndex = "test2";
        try (var mockLog = MockLog.capture(SearchService.class)) {
            ErrorTraceHelper.addSeenLoggingExpectations(numShards, mockLog, errorTriggeringIndex);

            getRestClient().performRequest(searchRequest);
            mockLog.assertAllExpectationsMatched();
        }
    }
}
