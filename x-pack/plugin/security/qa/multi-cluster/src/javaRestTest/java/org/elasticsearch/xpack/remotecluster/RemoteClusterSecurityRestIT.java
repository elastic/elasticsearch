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
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.cluster.util.resource.Resource;
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
import static org.hamcrest.Matchers.notNullValue;

public class RemoteClusterSecurityRestIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
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
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRemoteAccessForCrossClusterSearch() throws Exception {
        final String remoteAccessApiKeyId = configureRemoteClustersWithApiKey("""
            [
               {
                 "names": ["index*", "not_found_index", "shared-metrics"],
                 "privileges": ["read", "read_cross_cluster"]
               }
             ]""");

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
                          "names": ["local_index"],
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

            // Check remote metric users can search metric documents from all FC nodes
            final var metricSearchRequest = new Request(
                "GET",
                String.format(Locale.ROOT, "/my_remote_cluster:*/_search?ccs_minimize_roundtrips=%s", randomBoolean())
            );
            final SearchResponse metricSearchResponse = SearchResponse.fromXContent(
                responseAsParser(performRequestWithRemoteMetricUser(metricSearchRequest))
            );
            assertThat(metricSearchResponse.getHits().getTotalHits().value, equalTo(4L));
            assertThat(
                Arrays.stream(metricSearchResponse.getHits().getHits()).map(SearchHit::getIndex).collect(Collectors.toSet()),
                containsInAnyOrder("shared-metrics")
            );

            // Check that access is denied because of user/API key privileges
            final Request index2SearchRequest = new Request("GET", "/my_remote_cluster:index2/_search");
            final ResponseException exception = expectThrows(
                ResponseException.class,
                sendRequestWith.equals("user")
                    ? () -> performRequestWithRemoteAccessUser(index2SearchRequest)
                    : () -> performRequestWithApiKey(index2SearchRequest, apiKeyEncoded)
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception.getMessage(),
                sendRequestWith.equals("user")
                    ? containsString(
                        "action [indices:data/read/search] towards remote cluster is unauthorized for user [remote_search_user] "
                            + "with assigned roles [remote_search] authenticated by API key id ["
                            + remoteAccessApiKeyId
                            + "] of user [test_user] on indices [index2]"
                    )
                    : containsString(
                        "action [indices:data/read/search] towards remote cluster is unauthorized for API key id ["
                            + apiKeyId
                            + "] of user ["
                            + REMOTE_SEARCH_USER
                            + "] authenticated by API key id ["
                            + remoteAccessApiKeyId
                            + "] of user [test_user] on indices [index2]"
                    )
            );

            // Check that access is denied because of API key privileges
            final Request prefixedIndexSearchRequest = new Request("GET", "/my_remote_cluster:prefixed_index/_search");
            final ResponseException exception2 = expectThrows(
                ResponseException.class,
                sendRequestWith.equals("user")
                    ? () -> performRequestWithRemoteAccessUser(prefixedIndexSearchRequest)
                    : () -> performRequestWithApiKey(prefixedIndexSearchRequest, apiKeyEncoded)
            );
            assertThat(exception2.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception2.getMessage(),
                sendRequestWith.equals("user")
                    ? containsString(
                        "action [indices:data/read/search] towards remote cluster is unauthorized for user [remote_search_user] "
                            + "with assigned roles [remote_search] authenticated by API key id ["
                            + remoteAccessApiKeyId
                            + "] of user [test_user] on indices [prefixed_index]"
                    )
                    : containsString(
                        "action [indices:data/read/search] towards remote cluster is unauthorized for API key id ["
                            + apiKeyId
                            + "] of user ["
                            + REMOTE_SEARCH_USER
                            + "] authenticated by API key id ["
                            + remoteAccessApiKeyId
                            + "] of user [test_user] on indices [prefixed_index]"
                    )
            );

            // Check access is denied when user has no remote indices privileges
            final var putLocalSearchRoleRequest = new Request("PUT", "/_security/role/local_search");
            putLocalSearchRoleRequest.setJsonEntity(Strings.format("""
                {
                  "cluster": ["manage_own_api_key"],
                  "indices": [
                    {
                      "names": ["local_index"],
                      "privileges": ["read"]
                    }
                  ]%s
                }""", randomBoolean() ? "" : """
                ,
                "remote_indices": [
                   {
                     "names": ["*"],
                     "privileges": ["read", "read_cross_cluster"],
                     "clusters": ["other_remote_*"]
                   }
                 ]"""));
            assertOK(adminClient().performRequest(putLocalSearchRoleRequest));
            final var putlocalSearchUserRequest = new Request("PUT", "/_security/user/local_search_user");
            putlocalSearchUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["local_search"]
                }""");
            assertOK(adminClient().performRequest(putlocalSearchUserRequest));
            final var createLocalApiKeyRequest = new Request("PUT", "/_security/api_key");
            String localApiKeyRoleDescriptors = Strings.format("""
                  "my_local_access_role": {
                      "indices": [
                        {
                          "names": ["local_index"],
                          "privileges": ["read"]
                        }
                      ]%s
                  }
                """, randomBoolean() ? "" : """
                ,
                "remote_indices": [
                   {
                     "names": ["*"],
                     "privileges": ["read", "read_cross_cluster"],
                     "clusters": ["other_remote_*"]
                   }
                 ]""");
            createLocalApiKeyRequest.setJsonEntity(Strings.format("""
                {
                  "name": "qc_api_key_with_remote_access",
                  "role_descriptors": { %s }
                }""", randomBoolean() ? localApiKeyRoleDescriptors : ""));
            final var createLocalApiKeyResponse = performRequestWithLocalSearchUser(createLocalApiKeyRequest);
            assertOK(createApiKeyResponse);
            var createLocalApiKeyResponsePath = ObjectPath.createFromResponse(createLocalApiKeyResponse);
            final String localApiKeyEncoded = createLocalApiKeyResponsePath.evaluate("encoded");
            final String localApiKeyId = createLocalApiKeyResponsePath.evaluate("id");
            assertThat(localApiKeyEncoded, notNullValue());
            assertThat(localApiKeyId, notNullValue());

            final Request randomRemoteSearch = new Request(
                "GET",
                "/" + randomFrom("my_remote_cluster:*", "*:*", "*,*:*", "my_*:*,local_index") + "/_search"
            );
            final ResponseException exception3 = expectThrows(
                ResponseException.class,
                sendRequestWith.equals("user")
                    ? () -> performRequestWithLocalSearchUser(randomRemoteSearch)
                    : () -> performRequestWithApiKey(randomRemoteSearch, localApiKeyEncoded)
            );
            assertThat(exception3.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception3.getMessage(),
                sendRequestWith.equals("user")
                    ? containsString(
                        "action [indices:data/read/search] towards remote cluster [my_remote_cluster]"
                            + " is unauthorized for user [local_search_user] with effective roles [local_search]"
                            + " because no remote indices privileges apply for the target cluster"
                    )
                    : containsString(
                        "action [indices:data/read/search] towards remote cluster [my_remote_cluster] "
                            + "is unauthorized for API key id ["
                            + localApiKeyId
                            + "] of user [local_search_user] because no remote indices privileges apply for the target cluster"
                    )
            );

            // Check that authentication fails if we use a non-existent API key
            updateClusterSettings(
                Settings.builder()
                    .put(
                        "cluster.remote.my_remote_cluster.authorization",
                        randomValueOtherThan(remoteAccessApiKeyId, () -> randomEncodedApiKey())
                    )
                    .build()
            );
            final ResponseException exception4 = expectThrows(
                ResponseException.class,
                sendRequestWith.equals("user")
                    ? () -> performRequestWithRemoteAccessUser(new Request("GET", "/my_remote_cluster:index1/_search"))
                    : () -> performRequestWithApiKey(new Request("GET", "/my_remote_cluster:index1/_search"), localApiKeyEncoded)
            );
            if (sendRequestWith.equals("user")) {
                assertThat(exception4.getResponse().getStatusLine().getStatusCode(), equalTo(401));
                assertThat(exception4.getMessage(), containsString("unable to authenticate user"));
                assertThat(exception4.getMessage(), containsString("unable to find apikey"));
            } else {
                assertThat(exception4.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(exception4.getMessage(), containsString("unauthorized for API key id [" + localApiKeyId + "]"));
                assertThat(exception4.getMessage(), containsString("no remote indices privileges apply for the target cluster"));
            }
        }
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_METRIC_USER, PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithLocalSearchUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue("local_search_user", PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithApiKey(final Request request, final String encoded) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
    }

    // TODO centralize common usage of this across all tests
    private static String randomEncodedApiKey() {
        return Base64.getEncoder().encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
    }
}
