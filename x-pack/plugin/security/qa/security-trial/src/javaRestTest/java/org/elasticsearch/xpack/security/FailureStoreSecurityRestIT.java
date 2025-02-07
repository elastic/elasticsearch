/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class FailureStoreSecurityRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String DATA_ACCESS_USER = "data_access_user";
    private static final String FAILURE_STORE_ACCESS_USER = "failure_store_access_user";
    private static final String BOTH_ACCESS_USER = "both_access_user";
    private static final SecureString PASSWORD = new SecureString("elastic-password");

    @SuppressWarnings("unchecked")
    public void testFailureStoreAccess() throws IOException {
        String dataAccessRole = "data_access";
        String failureStoreAccessRole = "failure_store_access";
        String bothAccessRole = "both_access";

        createUser(DATA_ACCESS_USER, PASSWORD, List.of(dataAccessRole));
        createUser(FAILURE_STORE_ACCESS_USER, PASSWORD, List.of(failureStoreAccessRole));
        createUser(BOTH_ACCESS_USER, PASSWORD, List.of(bothAccessRole));

        upsertRole(Strings.format("""
            {
              "description": "Role with data access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read"]}]
            }"""), dataAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read_failures"]}]
            }"""), failureStoreAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read", "read_failures"]}]
            }"""), bothAccessRole);

        createTemplates();
        List<String> docIds = populateDataStreamWithBulkRequest();
        assertThat(docIds.size(), equalTo(2));
        assertThat(docIds, hasItem("1"));
        String successDocId = "1";
        String failedDocId = docIds.stream().filter(id -> false == id.equals(successDocId)).findFirst().get();

        Request dataStream = new Request("GET", "/_data_stream/test1");
        Response response = adminClient().performRequest(dataStream);
        Map<String, Object> dataStreams = entityAsMap(response);
        assertEquals(Collections.singletonList("test1"), XContentMapValues.extractValue("data_streams.name", dataStreams));
        List<String> dataIndexNames = (List<String>) XContentMapValues.extractValue("data_streams.indices.index_name", dataStreams);
        assertThat(dataIndexNames.size(), equalTo(1));
        List<String> failureIndexNames = (List<String>) XContentMapValues.extractValue(
            "data_streams.failure_store.indices.index_name",
            dataStreams
        );
        assertThat(failureIndexNames.size(), equalTo(1));

        String dataIndexName = dataIndexNames.get(0);
        String failureIndexName = failureIndexNames.get(0);

        // user with access to failures index
        assertContainsDocIds(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/.fs*/_search")), failedDocId);
        assertContainsDocIds(
            performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + failureIndexName + "/_search")),
            failedDocId
        );
        assertContainsDocIds(
            performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + failureIndexName + "/_search?ignore_unavailable=true")),
            failedDocId
        );

        expectThrows404(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2::failures/_search")));
        expectThrows404(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test12::*/_search")));

        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1::data/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2::data/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search")));

        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1::data/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2::data/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2/_search?ignore_unavailable=true")));
        assertEmpty(
            performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search?ignore_unavailable=true"))
        );

        // assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*1::data/_search")));
        // assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*1/_search")));
        // TODO is this correct?
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/.ds*/_search")));

        // user with access to data index
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/test1/_search")), successDocId);
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/test*/_search")), successDocId);
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/*1/_search")), successDocId);
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/*/_search")), successDocId);
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/.ds*/_search")), successDocId);
        assertContainsDocIds(performRequest(DATA_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search")), successDocId);
        assertContainsDocIds(
            performRequest(DATA_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search?ignore_unavailable=true")),
            successDocId
        );

        expectThrows404(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test12/_search")));
        expectThrows404(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test2/_search")));
        expectThrows404(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test12::*/_search")));

        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test1::failures/_search")));
        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test2::failures/_search")));
        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/" + failureIndexName + "/_search")));
        // TODO is this correct?
        assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/.fs*/_search")));
        // assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/*1::failures/_search")));

        // user with access to everything
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/.fs*/_search")), failedDocId);

        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test2::failures/_search")));

        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/.fs*/_search")), failedDocId);

        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test2::failures/_search")));

        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test*/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*1/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*/_search")), successDocId);

        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test12/_search")));
        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test2/_search")));
    }

    private static void expectThrows404(ThrowingRunnable get) {
        var ex = expectThrows(ResponseException.class, get);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
    }

    private static void expectThrows403(ThrowingRunnable get) {
        var ex = expectThrows(ResponseException.class, get);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(403));
    }

    @SuppressWarnings("unchecked")
    private static void assertContainsDocIds(Response response, String... docIds) throws IOException {
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(docIds.length));
            List<String> actualDocIds = Arrays.stream(hits).map(SearchHit::getId).toList();
            assertThat(actualDocIds, containsInAnyOrder(docIds));
        } finally {
            searchResponse.decRef();
        }
    }

    private static void assertEmpty(Response response) throws IOException {
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            SearchHit[] hits = searchResponse.getHits().getHits();
            assertThat(hits.length, equalTo(0));
        } finally {
            searchResponse.decRef();
        }
    }

    private void createTemplates() throws IOException {
        var componentTemplateRequest = new Request("PUT", "/_component_template/component1");
        componentTemplateRequest.setJsonEntity("""
            {
                "template": {
                    "mappings": {
                        "properties": {
                            "@timestamp": {
                                "type": "date"
                            },
                            "age": {
                                "type": "integer"
                            },
                            "email": {
                                "type": "keyword"
                            },
                            "name": {
                                "type": "text"
                            }
                        }
                    },
                    "data_stream_options": {
                      "failure_store": {
                        "enabled": true
                      }
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(componentTemplateRequest));

        var indexTemplateRequest = new Request("PUT", "/_index_template/template1");
        indexTemplateRequest.setJsonEntity("""
            {
                "index_patterns": ["test*"],
                "data_stream": {},
                "priority": 500,
                "composed_of": ["component1"]
            }
            """);
        assertOK(adminClient().performRequest(indexTemplateRequest));
    }

    @SuppressWarnings("unchecked")
    private List<String> populateDataStreamWithBulkRequest() throws IOException {
        var bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "create" : { "_index" : "test1", "_id" : "1" } }
            { "@timestamp": 1, "age" : 1, "name" : "jack", "email" : "jack@example.com" }
            { "create" : { "_index" : "test1", "_id" : "2" } }
            { "@timestamp": 2, "age" : "this should be an int", "name" : "jack", "email" : "jack@example.com" }
            """);
        Response response = adminClient().performRequest(bulkRequest);
        assertOK(response);
        // we need this dance because the ID for the failed document is random, **not** 2
        Map<String, Object> stringObjectMap = responseAsMap(response);
        List<Object> items = (List<Object>) stringObjectMap.get("items");
        List<String> ids = new ArrayList<>();
        for (Object item : items) {
            Map<String, Object> itemMap = (Map<String, Object>) item;
            Map<String, Object> create = (Map<String, Object>) itemMap.get("create");
            assertThat(create.get("status"), equalTo(201));
            ids.add((String) create.get("_id"));
        }
        return ids;
    }

    private Response performRequest(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build());
        return client().performRequest(request);
    }
}
