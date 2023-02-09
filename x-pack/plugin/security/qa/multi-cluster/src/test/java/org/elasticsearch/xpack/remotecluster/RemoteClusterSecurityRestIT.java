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
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * This test suite will be run twice: Once against the fulfilling cluster, then again against the querying cluster. The typical usage is to
 * conditionalize on whether the test is running against the fulfilling or the querying cluster.
 */
public class RemoteClusterSecurityRestIT extends ESRestTestCase {
    private static final String USER = "test_user";
    private static final SecureString PASS = new SecureString("x-pack-test-password".toCharArray());
    private static final String REMOTE_SEARCH_USER = "remote_search_user";
    private static final String REMOTE_SEARCH_ROLE = "remote_search";

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    @Override
    protected Settings restClientSettings() {
        final String token = basicAuthHeaderValue(USER, PASS);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    private boolean isFulfillingCluster() {
        return "fulfilling_cluster".equals(System.getProperty("tests.rest.suite"));
    }

    /**
     * This test really depends on the local build.gradle, which configures cross-cluster search using the `remote_cluster.*` settings.
     */
    public void testRemoteAccessForCrossClusterSearch() throws Exception {
        if (isFulfillingCluster()) {
            final var createApiKeyRequest = new Request("POST", "/_security/api_key");
            createApiKeyRequest.setJsonEntity("""
                {
                  "name": "remote_access_key",
                  "role_descriptors": {
                    "role": {
                      "cluster": ["cluster:monitor/state"],
                      "index": [
                        {
                          "names": ["index*", "not_found_index"],
                          "privileges": ["read", "read_cross_cluster"]
                        }
                      ]
                    }
                  }
                }""");
            // Index API key so querying cluster can retrieve and add it to its cluster settings
            createAndIndexRemoteAccessApiKey(createApiKeyRequest);

            // Index some documents, so we can attempt to search them from the querying cluster
            final var indexDocRequest = new Request("POST", "/index1/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            final var indexDocRequest2 = new Request("POST", "/index2/_doc?refresh=true");
            indexDocRequest2.setJsonEntity("{\"bar\": \"foo\"}");
            assertOK(client().performRequest(indexDocRequest2));

            final var indexDocRequest3 = new Request("POST", "/prefixed_index/_doc?refresh=true");
            indexDocRequest3.setJsonEntity("{\"baz\": \"fee\"}");
            assertOK(client().performRequest(indexDocRequest3));
        } else {
            getRemoteAccessApiKeyAndStoreInSettings();

            // Index some documents, to use them in a mixed-cluster search
            final var indexDocRequest = new Request("POST", "/local_index/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"local_foo\": \"local_bar\"}");
            assertOK(client().performRequest(indexDocRequest));

            // Create user role with privileges for remote and local indices
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["local_index"],
                      "privileges": ["read"]
                    }
                  ],
                  "remote_indices": [
                    {
                      "names": ["index1", "not_found_index", "prefixed_index"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));
            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Check that we can search the fulfilling cluster from the querying cluster
            final boolean alsoSearchLocally = randomBoolean();
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s%s:%s/_search?ccs_minimize_roundtrips=%s",
                    alsoSearchLocally ? "local_index," : "",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    randomFrom("index1", "*"),
                    randomBoolean()
                )
            );
            final Response response = performRequestWithRemoteAccessUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            if (alsoSearchLocally) {
                assertThat(actualIndices, containsInAnyOrder("index1", "local_index"));
            } else {
                assertThat(actualIndices, containsInAnyOrder("index1"));
            }

            // Check that access is denied because of user privileges
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteAccessUser(new Request("GET", "/my_remote_cluster:index2/_search"))
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception.getMessage(),
                containsString("action [indices:data/read/search] is unauthorized for user [remote_search_user] on indices [index2]")
            );

            // Check that access is denied because of API key privileges
            final ResponseException exception2 = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteAccessUser(new Request("GET", "/my_remote_cluster:prefixed_index/_search"))
            );
            assertThat(exception2.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception2.getMessage(),
                containsString(
                    "action [indices:data/read/search] is unauthorized for user [remote_search_user] on indices [prefixed_index]"
                )
            );

            // Check that authentication fails if we use a non-existent API key
            updateClusterSettings(Settings.builder().put("cluster.remote.my_remote_cluster.authorization", randomEncodedApiKey()).build());
            final ResponseException exception3 = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteAccessUser(new Request("GET", "/my_remote_cluster:index1/_search"))
            );
            assertThat(exception3.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(exception3.getMessage(), containsString("unable to authenticate user"));
            assertThat(exception3.getMessage(), containsString("unable to find apikey"));
        }
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private void getRemoteAccessApiKeyAndStoreInSettings() throws IOException {
        try (var fulfillingClusterClient = buildClient(System.getProperty("tests.fulfilling_cluster_host"))) {
            final var request = new Request("GET", "/apikey/_search");
            request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
            final SearchResponse apiKeyResponse = SearchResponse.fromXContent(
                responseAsParser(fulfillingClusterClient.performRequest(request))
            );
            final String encodedKey = (String) apiKeyResponse.getHits().getHits()[0].getSourceAsMap().get("apikey");
            updateClusterSettings(Settings.builder().put("cluster.remote.my_remote_cluster.authorization", encodedKey).build());
        }
    }

    private void createAndIndexRemoteAccessApiKey(Request createApiKeyRequest) throws IOException {
        final Response createApiKeyResponse = adminClient().performRequest(createApiKeyRequest);
        assertOK(createApiKeyResponse);
        final Map<String, Object> apiKeyMap = responseAsMap(createApiKeyResponse);
        final String encodedRemoteAccessApiKey = (String) apiKeyMap.get("encoded");
        final var indexDocRequest = new Request("POST", "/apikey/_doc");
        // Store API key credential so that QC can fetch and use it for authentication
        indexDocRequest.setJsonEntity("{\"apikey\": \"" + encodedRemoteAccessApiKey + "\"}");
        assertOK(adminClient().performRequest(indexDocRequest));
    }

    private RestClient buildClient(final String url) throws IOException {
        final int portSeparator = url.lastIndexOf(':');
        final var httpHost = new HttpHost(
            url.substring(0, portSeparator),
            Integer.parseInt(url.substring(portSeparator + 1)),
            getProtocol()
        );
        return buildClient(Settings.EMPTY, new HttpHost[] { httpHost });
    }

    public static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }
}
