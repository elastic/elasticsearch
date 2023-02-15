/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRestIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRemoteAccessForCrossClusterSearch() throws Exception {
        configureRemoteClustersWithApiKey("""
            [
               {
                 "names": ["index*", "not_found_index"],
                 "privileges": ["read", "read_cross_cluster"]
               }
             ]""");

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final var indexDocRequest = new Request("POST", "/index1/_doc?refresh=true");
            indexDocRequest.setJsonEntity("{\"foo\": \"bar\"}");
            assertOK(performRequestAgainstFulfillingCluster(indexDocRequest));

            final var indexDocRequest2 = new Request("POST", "/index2/_doc?refresh=true");
            indexDocRequest2.setJsonEntity("{\"bar\": \"foo\"}");
            assertOK(performRequestAgainstFulfillingCluster(indexDocRequest2));

            final var indexDocRequest3 = new Request("POST", "/prefixed_index/_doc?refresh=true");
            indexDocRequest3.setJsonEntity("{\"baz\": \"fee\"}");
            assertOK(performRequestAgainstFulfillingCluster(indexDocRequest3));
        }

        // Query cluster
        {
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

    // TODO centralize common usage of this across all tests
    private static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }
}
