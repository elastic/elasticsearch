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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.Version;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * BWC test which ensures that users and API keys with new remote_indices privileges can still be used to query legacy remote clusters.
 */
public class RemoteClusterSecurityBwcRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final Version OLD_CLUSTER_VERSION = Version.fromString(System.getProperty("tests.old_cluster_version"));

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .version(OLD_CLUSTER_VERSION)
            .distribution(DistributionType.INTEG_TEST)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .build();

        queryCluster = ElasticsearchCluster.local()
            .version(Version.CURRENT)
            .distribution(DistributionType.INTEG_TEST)
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRemoteAccessForCrossClusterSearch() throws Exception {
        // Update remote cluster settings on QC with the API key
        final Settings.Builder builder = Settings.builder();
        final boolean isProxyMode = randomBoolean();
        if (isProxyMode) {
            builder.put("cluster.remote.my_remote_cluster.mode", "proxy")
                .put("cluster.remote.my_remote_cluster.proxy_address", fulfillingCluster.getTransportEndpoint(0));
        } else {
            builder.put("cluster.remote.my_remote_cluster.mode", "sniff")
                .putList("cluster.remote.my_remote_cluster.seeds", fulfillingCluster.getTransportEndpoint(0));
        }
        updateClusterSettings(builder.build());

        // Ensure remote cluster is connected
        final int numberOfFcNodes = fulfillingCluster.getHttpAddresses().split(",").length;
        final Request remoteInfoRequest = new Request("GET", "/_remote/info");
        assertBusy(() -> {
            final Response remoteInfoResponse = adminClient().performRequest(remoteInfoRequest);
            assertOK(remoteInfoResponse);
            final Map<String, Object> remoteInfoMap = responseAsMap(remoteInfoResponse);
            assertThat(remoteInfoMap, hasKey("my_remote_cluster"));
            assertThat(org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.connected", remoteInfoMap), is(true));
            if (false == isProxyMode) {
                assertThat(
                    org.elasticsearch.xcontent.ObjectPath.eval("my_remote_cluster.num_nodes_connected", remoteInfoMap),
                    equalTo(numberOfFcNodes)
                );
            }
        });

        // Fulfilling cluster
        {
            // Spread the shards to all nodes
            final Request createIndexRequest = new Request("PUT", "shared-metrics");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                    "number_of_shards": 3,
                    "number_of_replicas": 0
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest));

            // Index some documents, so we can attempt to search them from the querying cluster
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "index1" } }
                { "foo": "bar" }
                { "index": { "_index": "index2" } }
                { "bar": "foo" }
                { "index": { "_index": "prefixed_index" } }
                { "baz": "fee" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }\n"""));
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
                  "cluster": ["manage_own_api_key"],
                  "indices": [
                    {
                      "names": ["local_index", "index1", "not_found_index"],
                      "privileges": ["read", "read_cross_cluster"]
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
            final var putRoleRequestFulfilling = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequestFulfilling.setJsonEntity("""
                {
                  "cluster": ["manage_own_api_key"],
                  "indices": [
                    {
                      "names": ["local_index", "index1", "not_found_index"],
                      "privileges": ["read", "read_cross_cluster"]
                    }
                  ]
                }""");
            assertOK(performRequestAgainstFulfillingCluster(putRoleRequestFulfilling));
            final var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["remote_search"]
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Create API key (with REMOTE_SEARCH_USER as owner) which can be used for remote cluster search
            final var createApiKeyRequest = new Request("PUT", "/_security/api_key");
            // Note: index2 is added to remote_indices for the API key,
            // but it should be ignored when intersected since owner user does not have access to it.
            createApiKeyRequest.setJsonEntity(randomBoolean() ? """
                {
                  "name": "qc_api_key_with_remote_access",
                  "role_descriptors": {
                    "my_remote_access_role": {
                      "indices": [
                        {
                          "names": ["local_index", "index1", "index2"],
                          "privileges": ["read"]
                        }
                      ],
                      "remote_indices": [
                        {
                          "names": ["index1", "not_found_index", "prefixed_index", "index2"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["my_remote_*", "non_existing_remote_cluster"]
                        }
                      ]
                    }
                  }
                }""" : """
                {
                  "name": "qc_api_key_with_remote_access",
                  "role_descriptors": {}
                }""");
            final var createApiKeyResponse = performRequestWithRemoteAccessUser(createApiKeyRequest);
            assertOK(createApiKeyResponse);
            var createApiKeyResponsePath = ObjectPath.createFromResponse(createApiKeyResponse);
            final String apiKeyEncoded = createApiKeyResponsePath.evaluate("encoded");
            final String apiKeyId = createApiKeyResponsePath.evaluate("id");
            assertThat(apiKeyEncoded, notNullValue());
            assertThat(apiKeyId, notNullValue());

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
            final String sendRequestWith = randomFrom("user", "apikey");
            final Response response = sendRequestWith.equals("user")
                ? performRequestWithRemoteAccessUser(searchRequest)
                : performRequestWithApiKey(searchRequest, apiKeyEncoded);
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
        }
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithApiKey(final Request request, final String encoded) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
    }

}
