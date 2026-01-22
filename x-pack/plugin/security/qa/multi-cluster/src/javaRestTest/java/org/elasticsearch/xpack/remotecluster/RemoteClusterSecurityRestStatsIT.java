/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
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

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityRestStatsIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<Map<String, Object>> REST_API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final int FULFILL_NODE_COUNT = 3;
    private static final Logger logger = LogManager.getLogger(RemoteClusterSecurityRestStatsIT.class);

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(FULFILL_NODE_COUNT)
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .node(0, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .node(1, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE1_RCS_SERVER_ENABLED.get())))
            .node(2, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE2_RCS_SERVER_ENABLED.get())))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["*"]
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
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> {
        SSL_ENABLED_REF.set(usually());
        NODE1_RCS_SERVER_ENABLED.set(randomBoolean());
        NODE2_RCS_SERVER_ENABLED.set(randomBoolean());
    })).around(fulfillingCluster).around(queryCluster);

    public void testCrossClusterStats() throws Exception {
        configureRemoteCluster();
        setupRoleAndUserQueryCluster();
        addDocToIndexFulfillingCluster("index1");

        // search #1
        searchFulfillingClusterFromQueryCluster("index1");
        Map<String, Object> statsResponseAsMap = getFulfillingClusterStatsFromQueryCluster();
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs.clusters.my_remote_cluster.nodes_count"), equalTo(FULFILL_NODE_COUNT));
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.clusters.my_remote_cluster.total"), equalTo(1));
        int initialIndexCount = ObjectPath.evaluate(statsResponseAsMap, "ccs.clusters.my_remote_cluster.indices_count");

        // search #2
        searchFulfillingClusterFromQueryCluster("index1");
        statsResponseAsMap = getFulfillingClusterStatsFromQueryCluster();
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.total"), equalTo(2));
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.clusters.my_remote_cluster.total"), equalTo(2));

        // search #3
        expectThrows(Exception.class, () -> searchFulfillingClusterFromQueryCluster("junk"));
        statsResponseAsMap = getFulfillingClusterStatsFromQueryCluster();
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.total"), equalTo(3));
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.clusters.my_remote_cluster.total"), equalTo(2));

        // search #4
        addDocToIndexFulfillingCluster("index2");
        searchFulfillingClusterFromQueryCluster("index2");
        statsResponseAsMap = getFulfillingClusterStatsFromQueryCluster();
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.total"), equalTo(4));
        assertThat(ObjectPath.evaluate(statsResponseAsMap, "ccs._search.clusters.my_remote_cluster.total"), equalTo(3));
        int updatedIndexCount = ObjectPath.evaluate(statsResponseAsMap, "ccs.clusters.my_remote_cluster.indices_count");
        assertThat(updatedIndexCount, equalTo(initialIndexCount + 1));
    }

    private Map<String, Object> getFulfillingClusterStatsFromQueryCluster() throws IOException {
        return getFulfillingClusterStatsFromQueryCluster(false);
    }

    private Map<String, Object> getFulfillingClusterStatsFromQueryCluster(boolean humanDebug) throws IOException {
        Request stats = new Request("GET", "_cluster/stats?include_remotes=true&filter_path=ccs");
        Response statsResponse = performRequestWithRemoteSearchUser(stats);
        if (humanDebug) {
            debugResponse(statsResponse);
        }
        return entityAsMap(statsResponse.getEntity());
    }

    private void searchFulfillingClusterFromQueryCluster(String index, boolean humanDebug) throws IOException {
        final var searchRequest = new Request(
            "GET",
            String.format(
                Locale.ROOT,
                "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                randomFrom("my_remote_cluster", "*", "my_remote_*"),
                index,
                randomBoolean()
            )
        );
        Response response = performRequestWithRemoteSearchUser(searchRequest);
        if (humanDebug) {
            debugResponse(response);
        }
        assertOK(response);
        final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
        try {
            final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toList());
            assertThat(actualIndices, containsInAnyOrder(index));

        } finally {
            searchResponse.decRef();
        }
    }

    private void searchFulfillingClusterFromQueryCluster(String index) throws IOException {
        searchFulfillingClusterFromQueryCluster(index, false);
    }

    private void addDocToIndexFulfillingCluster(String index) throws IOException {
        // Index some documents, so we can attempt to search them from the querying cluster
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "%s" } }
            { "foo": "bar" }
            """, index));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
    }

    private void setupRoleAndUserQueryCluster() throws IOException {
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "description": "Role with privileges for remote indices and stats.",
              "cluster": ["monitor_stats"],
              "remote_indices": [
                {
                  "names": ["*"],
                  "privileges": ["read", "read_cross_cluster"],
                  "clusters": ["*"]
                }
              ],
              "remote_cluster": [
               {
                  "privileges": ["monitor_stats"],
                   "clusters": ["*"]
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

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    // helper method for humans see the responses for debug purposes, when used will always fail the test
    private void debugResponse(Response response) throws IOException {
        String jsonString = XContentHelper.convertToJson(
            new BytesArray(EntityUtils.toString(response.getEntity())),
            true,
            true,
            XContentType.JSON
        );
        logger.error(jsonString);
        assertFalse(true); // boom
    }
}
