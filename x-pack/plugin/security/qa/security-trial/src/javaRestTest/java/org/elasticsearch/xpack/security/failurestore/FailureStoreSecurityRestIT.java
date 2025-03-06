/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.failurestore;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.FeatureFlag;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.SecurityOnTrialLicenseRestTestCase;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

public class FailureStoreSecurityRestIT extends ESRestTestCase {

    private TestSecurityClient securityClient;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .apply(SecurityOnTrialLicenseRestTestCase.commonTrialSecurityClusterConfig)
        .feature(FeatureFlag.FAILURE_STORE_ENABLED)
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private static final String DATA_ACCESS_USER = "data_access_user";
    private static final String STAR_READ_ONLY_USER = "star_read_only_user";
    private static final String FAILURE_STORE_ACCESS_USER = "failure_store_access_user";
    private static final String BOTH_ACCESS_USER = "both_access_user";
    private static final String WRITE_ACCESS_USER = "write_access_user";
    private static final String MANAGE_ACCESS_USER = "manage_access_user";
    private static final String MANAGE_FAILURE_STORE_ACCESS_USER = "manage_failure_store_access_user";
    private static final SecureString PASSWORD = new SecureString("elastic-password");

    public void testGetUserPrivileges() throws IOException {
        Request userRequest = new Request("PUT", "/_security/user/user");
        userRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles": ["role"]
            }
            """);
        assertOK(adminClient().performRequest(userRequest));

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [{
                  "names": ["*"],
                  "privileges": ["read"],
                  "allow_restricted_indices": false
                },
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");

        upsertRole("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"]
                }
              ]
            }
            """, "role");
        expectUserPrivilegesResponse("""
            {
              "cluster": ["all"],
              "global": [],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all", "read_failure_store"],
                  "allow_restricted_indices": false
                }],
              "applications": [],
              "run_as": []
            }""");
    }

    @SuppressWarnings("unchecked")
    public void testFailureStoreAccess() throws IOException {
        String dataAccessRole = "data_access";
        String starReadOnlyRole = "star_read_only_access";
        String failureStoreAccessRole = "failure_store_access";
        String bothAccessRole = "both_access";
        String writeAccessRole = "write_access";
        String manageAccessRole = "manage_access";
        String manageFailureStoreRole = "manage_failure_store_access";

        createUser(DATA_ACCESS_USER, PASSWORD, List.of(dataAccessRole));
        createUser(STAR_READ_ONLY_USER, PASSWORD, List.of(starReadOnlyRole));
        createUser(FAILURE_STORE_ACCESS_USER, PASSWORD, List.of(failureStoreAccessRole));
        createUser(BOTH_ACCESS_USER, PASSWORD, randomBoolean() ? List.of(bothAccessRole) : List.of(dataAccessRole, failureStoreAccessRole));
        createUser(WRITE_ACCESS_USER, PASSWORD, List.of(writeAccessRole));
        createUser(MANAGE_ACCESS_USER, PASSWORD, List.of(manageAccessRole));
        createUser(MANAGE_FAILURE_STORE_ACCESS_USER, PASSWORD, List.of(manageFailureStoreRole));

        upsertRole(Strings.format("""
            {
              "description": "Role with data access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read"]}]
            }"""), dataAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with data access",
              "cluster": ["all"],
              "indices": [{"names": ["*"], "privileges": ["read"]}]
            }"""), starReadOnlyRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read_failure_store"]}]
            }"""), failureStoreAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with both data and failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["read", "read_failure_store"]}]
            }"""), bothAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with regular write access without failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["write", "auto_configure"]}]
            }"""), writeAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with regular manage access without failure store access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage"]}]
            }"""), manageAccessRole);
        upsertRole(Strings.format("""
            {
              "description": "Role with failure store manage access",
              "cluster": ["all"],
              "indices": [{"names": ["test*"], "privileges": ["manage_failure_store"]}]
            }"""), manageFailureStoreRole);

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

        // `*` with read access user _can_ read concrete failure index with only read
        assertContainsDocIds(performRequest(STAR_READ_ONLY_USER, new Request("GET", "/" + failureIndexName + "/_search")), failedDocId);

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

        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1::data/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2::data/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search")));
        expectThrows403(() -> performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1,test1::failures/_search")));

        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1::data/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test1/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2::data/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/test2/_search?ignore_unavailable=true")));
        assertEmpty(
            performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/" + dataIndexName + "/_search?ignore_unavailable=true"))
        );
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/.ds*/_search")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*1::data/_search")));
        assertEmpty(performRequest(FAILURE_STORE_ACCESS_USER, new Request("GET", "/*1/_search")));

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

        assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/test1::failures/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/test2::failures/_search?ignore_unavailable=true")));

        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test1::failures/_search")));
        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/test2::failures/_search")));
        expectThrows403(() -> performRequest(DATA_ACCESS_USER, new Request("GET", "/" + failureIndexName + "/_search")));
        // TODO is this correct?
        assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/.fs*/_search")));
        assertEmpty(performRequest(DATA_ACCESS_USER, new Request("GET", "/*1::failures/_search")));

        // user with access to everything
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/*::failures/_search")), failedDocId);
        assertContainsDocIds(adminClient().performRequest(new Request("GET", "/.fs*/_search")), failedDocId);

        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> adminClient().performRequest(new Request("GET", "/test2::failures/_search")));

        assertEmpty(adminClient().performRequest(new Request("GET", "/test12::failures/_search?ignore_unavailable=true")));
        assertEmpty(adminClient().performRequest(new Request("GET", "/test2::failures/_search?ignore_unavailable=true")));

        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*1::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*::failures/_search")), failedDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/.fs*/_search")), failedDocId);

        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test12::failures/_search")));
        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test2::failures/_search")));

        assertEmpty(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test12::failures/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test2::failures/_search?ignore_unavailable=true")));

        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test*/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*1/_search")), successDocId);
        assertContainsDocIds(performRequest(BOTH_ACCESS_USER, new Request("GET", "/*/_search")), successDocId);

        assertEmpty(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test12/_search?ignore_unavailable=true")));
        assertEmpty(performRequest(BOTH_ACCESS_USER, new Request("GET", "/test2/_search?ignore_unavailable=true")));

        // TODO extra make sure manage_failure_store CANNOT delete the whole data stream

        // user with manage access to data stream does NOT get direct access to failure index
        expectThrows403(() -> deleteIndex(MANAGE_ACCESS_USER, failureIndexName));
        expectThrows(() -> deleteIndex(MANAGE_ACCESS_USER, dataIndexName), 400);
        // manage_failure_store user COULD delete failure index (not valid because it's a write index, but allow security-wise)
        expectThrows403(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS_USER, dataIndexName));
        expectThrows(() -> deleteIndex(MANAGE_FAILURE_STORE_ACCESS_USER, failureIndexName), 400);
        expectThrows403(() -> deleteDataStream(MANAGE_FAILURE_STORE_ACCESS_USER, dataIndexName));

        // manage user can delete data stream
        deleteDataStream(MANAGE_ACCESS_USER, "test1");

        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1/_search")));
        expectThrows404(() -> performRequest(BOTH_ACCESS_USER, new Request("GET", "/test1::failures/_search")));
    }

    private static void expectThrows404(ThrowingRunnable get) {
        expectThrows(get, 404);
    }

    private static void expectThrows403(ThrowingRunnable get) {
        expectThrows(get, 403);
    }

    private static void expectThrows(ThrowingRunnable get, int statusCode) {
        var ex = expectThrows(ResponseException.class, get);
        assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(statusCode));
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
        Response response = performRequest(WRITE_ACCESS_USER, bulkRequest);
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

    private void deleteDataStream(String user, String dataStreamName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/_data_stream/" + dataStreamName)));
    }

    private void deleteIndex(String user, String indexName) throws IOException {
        assertOK(performRequest(user, new Request("DELETE", "/" + indexName)));
    }

    private Response performRequest(String user, Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(user, PASSWORD)).build());
        return client().performRequest(request);
    }

    private static void expectUserPrivilegesResponse(String userPrivilegesResponse) throws IOException {
        Request request = new Request("GET", "/_security/user/_privileges");
        request.setOptions(
            request.getOptions()
                .toBuilder()
                .addHeader("Authorization", basicAuthHeaderValue("user", new SecureString("x-pack-test-password".toCharArray())))
        );
        Response response = client().performRequest(request);
        assertOK(response);
        assertThat(responseAsMap(response), equalTo(mapFromJson(userPrivilegesResponse)));
    }

    private static Map<String, Object> mapFromJson(String json) {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, json, false);
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = new TestSecurityClient(adminClient());
        }
        return securityClient;
    }

    protected void createUser(String username, SecureString password, List<String> roles) throws IOException {
        getSecurityClient().putUser(new User(username, roles.toArray(String[]::new)), password);
    }

    protected void upsertRole(String roleDescriptor, String roleName) throws IOException {
        Request createRoleRequest = roleRequest(roleDescriptor, roleName);
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);
    }

    protected Request roleRequest(String roleDescriptor, String roleName) {
        Request createRoleRequest;
        if (randomBoolean()) {
            createRoleRequest = new Request(randomFrom(HttpPut.METHOD_NAME, HttpPost.METHOD_NAME), "/_security/role/" + roleName);
            createRoleRequest.setJsonEntity(roleDescriptor);
        } else {
            createRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role");
            createRoleRequest.setJsonEntity(org.elasticsearch.core.Strings.format("""
                {"roles": {"%s": %s}}
                """, roleName, roleDescriptor));
        }
        return createRoleRequest;
    }
}
