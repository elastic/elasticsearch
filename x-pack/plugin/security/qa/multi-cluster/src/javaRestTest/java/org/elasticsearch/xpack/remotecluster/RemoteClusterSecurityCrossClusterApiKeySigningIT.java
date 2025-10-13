/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import io.netty.handler.codec.http.HttpMethod;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityCrossClusterApiKeySigningIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .configFile("signing_ca.crt", Resource.fromClasspath("signing/root.crt"))
            .setting("cluster.remote.signing.certificate_authorities", "signing_ca.crt")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .configFile("signing.crt", Resource.fromClasspath("signing/signing.crt"))
            .setting("cluster.remote.my_remote_cluster.signing.certificate", "signing.crt")
            .configFile("signing.key", Resource.fromClasspath("signing/signing.key"))
            .setting("cluster.remote.my_remote_cluster.signing.key", "signing.key")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                            "search": [
                              {
                                "names": ["index*", "not_found_index"]
                              }
                            ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .keystore("cluster.remote.invalid_remote.credentials", randomEncodedApiKey())
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSearchWithCrossClusterApiKeySigning() throws Exception {
        indexTestData();
        assertCrossClusterSearchSuccessfulWithResult();

        // Change the CA to something that doesn't trust the signing cert
        updateClusterSettingsFulfillingCluster(
            Settings.builder().put("cluster.remote.signing.certificate_authorities", "transport-ca.crt").build()
        );
        assertCrossClusterAuthFail();

        // Update settings on query cluster to ignore unavailable remotes
        updateClusterSettings(Settings.builder().put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(true)).build());

        assertCrossClusterSearchSuccessfulWithoutResult();

        // TODO add test for certificate identity configured for API key but no signature provided (should 401)

        // TODO add test for certificate identity not configured for API key but signature provided (should 200)

        // TODO add test for certificate identity not configured for API key but wrong signature provided (should 401)

        // TODO add test for certificate identity regex matching (should 200)
    }

    private void assertCrossClusterAuthFail() {
        var responseException = assertThrows(ResponseException.class, () -> simpleCrossClusterSearch(randomBoolean()));
        assertThat(responseException.getResponse().getStatusLine().getStatusCode(), equalTo(401));
        assertThat(responseException.getMessage(), containsString("Failed to verify cross cluster api key signature certificate from [("));
    }

    private void assertCrossClusterSearchSuccessfulWithoutResult() throws IOException {
        boolean alsoSearchLocally = randomBoolean();
        final Response response = simpleCrossClusterSearch(alsoSearchLocally);
        assertOK(response);
    }

    private void assertCrossClusterSearchSuccessfulWithResult() throws IOException {
        boolean alsoSearchLocally = randomBoolean();
        final Response response = simpleCrossClusterSearch(alsoSearchLocally);
        assertOK(response);
        final SearchResponse searchResponse;
        try (var parser = responseAsParser(response)) {
            searchResponse = SearchResponseUtils.parseSearchResponse(parser);
        }
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            if (alsoSearchLocally) {
                assertThat(actualIndices, containsInAnyOrder("index1", "local_index"));
            } else {
                assertThat(actualIndices, containsInAnyOrder("index1"));
            }
        } finally {
            searchResponse.decRef();
        }
    }

    private Response simpleCrossClusterSearch(boolean alsoSearchLocally) throws IOException {
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
        return performRequestWithRemoteAccessUser(searchRequest);
    }

    private void indexTestData() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster
        {
            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "index2" } }
                { "bar": "foo" }
                { "index": { "_index": "prefixed_index" } }
                { "baz": "fee" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
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
                  "description": "role with privileges for remote and local indices",
                  "cluster": ["manage_own_api_key"],
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
        }
    }

    private void updateClusterSettingsFulfillingCluster(Settings settings) throws IOException {
        final var request = newXContentRequest(HttpMethod.PUT, "/_cluster/settings", (builder, params) -> {
            builder.startObject("persistent");
            settings.toXContent(builder, params);
            return builder.endObject();
        });

        performRequestWithAdminUser(fulfillingClusterClient, request);
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }
}
