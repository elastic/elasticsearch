/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster. The typical usage is to
 * conditionalize on whether the test is running against the fulfilling or the querying cluster.
 */
public class RemoteClusterSecuritySmokeIT extends ESRestTestCase {
    private static final String USER = "test_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    private boolean isFulfillingCluster() {
        return "fulfilling_cluster".equals(System.getProperty("tests.rest.suite"));
    }

    /**
     * This test really depends on the local build.gradle, which configures cross-cluster search using the `remote_cluster.*` settings.
     */
    public void testRemoteAccessPortFunctions() throws Exception {
        if (isFulfillingCluster()) {
            var createApiKeyRequest = new Request("POST", "/_security/api_key");
            createApiKeyRequest.setJsonEntity("""
                {
                  "name": "remote_access_key",
                  "role_descriptors": {
                    "role": {
                      "cluster": ["cluster:monitor/state"],
                      "index": [
                        {
                          "names": ["*idx"],
                          "privileges": ["read", "read_cross_cluster"]
                        }
                      ]
                    }
                  }
                }""");
            var createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
            assertOK(createApiKeyResponse);
            var apiKeyMap = responseAsMap(createApiKeyResponse);
            var encodedApiKey = (String) apiKeyMap.get("encoded");

            Request indexDocRequest = new Request("POST", "/test_idx/_doc");
            // Store API key credential so that QC can fetch and use it for authentication
            indexDocRequest.setJsonEntity("{\"key\": \"" + encodedApiKey + "\"}");
            Response response = adminClient().performRequest(indexDocRequest);
            assertOK(response);
        } else {
            try (var fulfillingClusterClient = buildClient(System.getProperty("tests.fulfilling_cluster_host"), Settings.EMPTY)) {
                var request = new Request("GET", "/test_idx/_search");
                request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
                var apiKeyResponse = SearchResponse.fromXContent(responseAsParser(fulfillingClusterClient.performRequest(request)));
                var encodedKey = (String) apiKeyResponse.getHits().getHits()[0].getSourceAsMap().get("key");
                updateClusterSettings(Settings.builder().put("cluster.remote.my_remote_cluster.authorization", encodedKey).build());

                var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
                putRoleRequest.setJsonEntity("""
                    {
                      "remote_indices": [
                        {
                          "names": ["test_idx"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["my_remote_cluster"]
                        }
                      ]
                    }""");
                assertOK(adminClient().performRequest(putRoleRequest));
                var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
                putUserRequest.setJsonEntity("""
                    {
                      "password": "x-pack-test-password",
                      "roles" : ["remote_search"]
                    }""");
                assertOK(adminClient().performRequest(putUserRequest));

                // Check that we can search the fulfilling cluster from the querying cluster
                var searchRequest = new Request("GET", "/my_remote_cluster:test_idx/_search");
                searchRequest.setOptions(
                    RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS))
                );
                Response response = client().performRequest(searchRequest);
                assertOK(response);
                ObjectPath responseObj = ObjectPath.createFromResponse(response);
                int totalHits = responseObj.evaluate("hits.total.value");
                assertThat(totalHits, equalTo(1));
            }
        }
    }

    private RestClient buildClient(final String url, final Settings settings) throws IOException {
        int portSeparator = url.lastIndexOf(':');
        HttpHost httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(settings, new HttpHost[] { httpHost });
    }
}
