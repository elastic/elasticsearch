/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class RemoteClusterSecurityRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<Map<String, Object>> REST_API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
            .apply(commonClusterConfig)
            .setting("remote_cluster_server.enabled", "true")
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["index*", "not_found_index", "shared-metrics"]
                            }
                          ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            // Define a bogus API key for another remote cluster
            .keystore("cluster.remote.invalid_remote.credentials", randomEncodedApiKey())
            // Define remote with a REST API key to observe expected failure
            .keystore("cluster.remote.wrong_api_key_type.credentials", () -> {
                if (REST_API_KEY_MAP_REF.get() == null) {
                    initFulfillingClusterClient();
                    final var createApiKeyRequest = new Request("POST", "/_security/api_key");
                    createApiKeyRequest.setJsonEntity("""
                        {
                          "name": "rest_api_key"
                        }""");
                    try {
                        final Response createApiKeyResponse = performRequestWithAdminUser(fulfillingClusterClient, createApiKeyRequest);
                        assertOK(createApiKeyResponse);
                        REST_API_KEY_MAP_REF.set(responseAsMap(createApiKeyResponse));
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                }
                return (String) REST_API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics", false)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    // `SSL_ENABLED_REF` is used to control the SSL-enabled setting on the test clusters
    // We set it here, since randomization methods are not available in the static initialize context above
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> SSL_ENABLED_REF.set(usually())))
        .around(fulfillingCluster)
        .around(queryCluster);

    public void testCrossClusterSearch() throws Exception {
        configureRemoteCluster();
        final String crossClusterAccessApiKeyId = (String) API_KEY_MAP_REF.get().get("id");

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
            final Response response = performRequestWithRemoteSearchUser(searchRequest);
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

            // Check that access is denied because of user privileges
            final ResponseException exception = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(new Request("GET", "/my_remote_cluster:index2/_search"))
            );
            assertThat(exception.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception.getMessage(),
                containsString(
                    "action [indices:data/read/search] towards remote cluster is unauthorized for user [remote_search_user] "
                        + "with assigned roles [remote_search] authenticated by API key id ["
                        + crossClusterAccessApiKeyId
                        + "] of user [test_user] on indices [index2]"
                )
            );

            // Check that access is denied because of API key privileges
            final ResponseException exception2 = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(new Request("GET", "/my_remote_cluster:prefixed_index/_search"))
            );
            assertThat(exception2.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception2.getMessage(),
                containsString(
                    "action [indices:data/read/search] towards remote cluster is unauthorized for user [remote_search_user] "
                        + "with assigned roles [remote_search] authenticated by API key id ["
                        + crossClusterAccessApiKeyId
                        + "] of user [test_user] on indices [prefixed_index]"
                )
            );

            // Check access is denied when user has no remote indices privileges
            final var putLocalSearchRoleRequest = new Request("PUT", "/_security/role/local_search");
            putLocalSearchRoleRequest.setJsonEntity(Strings.format("""
                {
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
            final ResponseException exception3 = expectThrows(
                ResponseException.class,
                () -> performRequestWithLocalSearchUser(
                    new Request("GET", "/" + randomFrom("my_remote_cluster:*", "*:*", "*,*:*", "my_*:*,local_index") + "/_search")
                )
            );
            assertThat(exception3.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                exception3.getMessage(),
                containsString(
                    "action [indices:data/read/search] towards remote cluster [my_remote_cluster]"
                        + " is unauthorized for user [local_search_user] with effective roles [local_search]"
                        + " because no remote indices privileges apply for the target cluster"
                )
            );

            // Check that authentication fails if we use a non-existent API key
            updateClusterSettings(
                randomBoolean()
                    ? Settings.builder()
                        .put("cluster.remote.invalid_remote.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
                    : Settings.builder()
                        .put("cluster.remote.invalid_remote.mode", "proxy")
                        .put("cluster.remote.invalid_remote.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
            );
            final ResponseException exception4 = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(new Request("GET", "/invalid_remote:index1/_search"))
            );
            assertThat(exception4.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(
                exception4.getMessage(),
                allOf(containsString("unable to authenticate user "), containsString("unable to find apikey"))
            );

            // check that REST API key is not supported by cross cluster access
            updateClusterSettings(
                randomBoolean()
                    ? Settings.builder()
                        .put("cluster.remote.wrong_api_key_type.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
                    : Settings.builder()
                        .put("cluster.remote.wrong_api_key_type.mode", "proxy")
                        .put("cluster.remote.wrong_api_key_type.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
            );
            final ResponseException exception5 = expectThrows(
                ResponseException.class,
                () -> performRequestWithRemoteSearchUser(new Request("GET", "/wrong_api_key_type:*/_search"))
            );
            assertThat(exception5.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(
                exception5.getMessage(),
                containsString(
                    "authentication expected API key type of [cross_cluster], but API key ["
                        + REST_API_KEY_MAP_REF.get().get("id")
                        + "] has type [rest]"
                )
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testNodesInfo() throws IOException {
        final Request request = new Request("GET", "/_nodes/transport,remote_cluster_server");
        final Response response = performRequestAgainstFulfillingCluster(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        assertThat(ObjectPath.eval("_nodes.total", responseMap), equalTo(3));
        final Map<String, Object> nodes = ObjectPath.eval("nodes", responseMap);
        nodes.forEach((k, v) -> {
            final Map<String, Object> node = (Map<String, Object>) v;
            // remote cluster is not reported in transport profiles
            assertThat(ObjectPath.eval("transport.profiles", node), anEmptyMap());

            final List<String> boundAddresses = ObjectPath.eval("remote_cluster_server.bound_address", node);
            assertThat(boundAddresses, notNullValue());
            assertThat(boundAddresses, not(empty()));
            final String publishAddress = ObjectPath.eval("remote_cluster_server.publish_address", node);
            assertThat(publishAddress, notNullValue());
        });
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
        );
        return client().performRequest(request);
    }

    private Response performRequestWithLocalSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod("local_search_user", PASS))
        );
        return client().performRequest(request);
    }

    private String headerFromRandomAuthMethod(final String username, final SecureString password) throws IOException {
        final boolean useBearerTokenAuth = randomBoolean();
        if (useBearerTokenAuth) {
            final Request request = new Request(HttpPost.METHOD_NAME, "/_security/oauth2/token");
            request.setJsonEntity(String.format(Locale.ROOT, """
                {
                  "grant_type":"password",
                  "username":"%s",
                  "password":"%s"
                }
                """, username, password));
            final Map<String, Object> responseBody = entityAsMap(adminClient().performRequest(request));
            return "Bearer " + responseBody.get("access_token");
        } else {
            return basicAuthHeaderValue(username, password);
        }
    }
}
