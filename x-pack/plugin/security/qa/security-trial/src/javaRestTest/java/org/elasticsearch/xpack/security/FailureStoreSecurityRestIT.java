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
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class FailureStoreSecurityRestIT extends SecurityOnTrialLicenseRestTestCase {

    private static final String USER = "user";
    private static final SecureString PASSWORD = new SecureString("elastic-password");

    public void testFailureStoreAccess() throws IOException {
        String failureStoreAccessRole = "failure_store_access";
        createUser(USER, PASSWORD, List.of(failureStoreAccessRole));

        upsertRole(Strings.format("""
            {
              "description": "Role with failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*::failures"], "privileges": ["read"]}]
            }"""), failureStoreAccessRole);

        createTemplates();
        List<String> ids = populateDataStreamWithBulkRequest();
        assertThat(ids.size(), equalTo(2));
        assertThat(ids, hasItem("1"));
        String successDocId = "1";
        String failedDocId = ids.stream().filter(id -> false == id.equals(successDocId)).findFirst().get();

        // user with access to failures index
        assertContainsDocIds(performRequestAsUser1(new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequestAsUser1(new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequestAsUser1(new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequestAsUser1(new Request("GET", "/*::failures/_search")), failedDocId);

        expectThrows404(() -> performRequestAsUser1(new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> performRequestAsUser1(new Request("GET", "/test2::failures/_search")));

        // user with access to everything
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*::failures/_search")), failedDocId);

        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test2::failures/_search")));
        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test12::failures/_search")));
    }

    private static void expectThrows404(ThrowingRunnable get) {
        var ex = expectThrows(ResponseException.class, get);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(404));
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

    private static void assert404(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(404));
    }

    private static void assert403(Response response) {
        assertThat(response.getStatusLine().getStatusCode(), equalTo(403));
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

    private Response performRequestAsUser1(Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASSWORD)).build());
        var response = client().performRequest(request);
        return response;
    }

}
