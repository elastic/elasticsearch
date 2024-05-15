/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.util.EntityUtils;
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
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.action.apikey.CrossClusterApiKeyRoleDescriptorBuilder;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityLegacyCrossClusterApiKeysWithDlsFlsIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<Map<String, Object>> CCR_API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();

    private static final String CCR_USER = "ccr_user";

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(3)
            .module("x-pack-ccr")
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
            .name("query-cluster")
            .apply(commonClusterConfig)
            .module("x-pack-ccr")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["shared-metrics"]
                            }
                          ],
                          "replication": [
                            {
                                "names": ["shared-metrics"]
                            }
                          ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .keystore("cluster.remote.my_ccr_cluster.credentials", () -> {
                if (CCR_API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["leader-index", "shared-*", "metrics-*"]
                            }
                          ],
                          "replication": [
                            {
                                "names": ["leader-index", "shared-*", "metrics-*"]
                            }
                          ]
                        }""");
                    CCR_API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) CCR_API_KEY_MAP_REF.get().get("encoded");
            })
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics", false)
            .user(CCR_USER, PASS.toString(), "ccr_user_role", false)
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

    public void testCrossClusterSearchBlockedIfApiKeyInvalid() throws Exception {
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
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster -- test searching works (the API key is valid)
        {
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "my_remote_*"),
                    randomFrom("shared-metrics", "*"),
                    randomBoolean()
                )
            );
            final Response response = performRequestWithRemoteMetricUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
            try {
                final List<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                    .map(SearchHit::getIndex)
                    .collect(Collectors.toList());
                assertThat(Set.copyOf(actualIndices), containsInAnyOrder("shared-metrics"));
            } finally {
                searchResponse.decRef();
            }
        }

        // make API key invalid
        addDlsQueryToApiKeyDoc(crossClusterAccessApiKeyId);

        // since we updated the API key doc directly, caches need to be clearer manually -- this would also happen during a rolling restart
        // to the FC, during an upgrade
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/role/*/_clear_cache")));
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/api_key/*/_clear_cache")));

        // check that GET still works
        getCrossClusterApiKeys(crossClusterAccessApiKeyId);
        // check that query still works
        validateQueryCrossClusterApiKeys(crossClusterAccessApiKeyId);

        {
            final var searchRequest = new Request(
                "GET",
                String.format(
                    Locale.ROOT,
                    "/%s:%s/_search?ccs_minimize_roundtrips=%s",
                    "my_remote_cluster",
                    "shared-metrics",
                    randomBoolean()
                )
            );
            updateClusterSettings(
                Settings.builder().put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(true)).build()
            );
            final var response = performRequestWithRemoteMetricUser(searchRequest);
            assertThat(response.getStatusLine().getStatusCode(), equalTo(200));
            String responseJson = EntityUtils.toString(response.getEntity());
            assertThat(responseJson, containsString("\"status\":\"skipped\""));
            assertThat(responseJson, containsString("search does not support document or field level security if replication is assigned"));

            updateClusterSettings(
                Settings.builder().put("cluster.remote.my_remote_cluster.skip_unavailable", Boolean.toString(false)).build()
            );
            final ResponseException ex = expectThrows(ResponseException.class, () -> performRequestWithRemoteMetricUser(searchRequest));
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(
                ex.getMessage(),
                containsString("search does not support document or field level security if replication is assigned")
            );
        }
    }

    public void testCrossClusterReplicationBlockedIfApiKeyInvalid() throws Exception {
        // TODO improve coverage to test:
        // * auto-follow
        // * follow successfully, then break key
        configureRemoteCluster("my_ccr_cluster");
        final String crossClusterAccessApiKeyId = (String) CCR_API_KEY_MAP_REF.get().get("id");

        // fulfilling cluster
        {
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "leader-index" } }
                { "name": "doc-1" }
                { "index": { "_index": "leader-index" } }
                { "name": "doc-2" }
                { "index": { "_index": "leader-index" } }
                { "name": "doc-3" }
                { "index": { "_index": "leader-index" } }
                { "name": "doc-4" }
                { "index": { "_index": "private-index" } }
                { "name": "doc-5" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // make API key invalid
        addDlsQueryToApiKeyDoc(crossClusterAccessApiKeyId);
        // since we updated the API key doc directly, caches need to be clearer manually -- this would also happen during a rolling restart
        // to the FC, during an upgrade
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/role/*/_clear_cache")));
        assertOK(performRequestAgainstFulfillingCluster(new Request("POST", "/_security/api_key/*/_clear_cache")));

        // query cluster
        {
            final String followIndexName = "follower-index";
            final Request putCcrRequest = new Request("PUT", "/" + followIndexName + "/_ccr/follow?wait_for_active_shards=1");
            putCcrRequest.setJsonEntity("""
                {
                  "remote_cluster": "my_ccr_cluster",
                  "leader_index": "leader-index"
                }""");

            final ResponseException ex = expectThrows(ResponseException.class, () -> performRequestWithCcrUser(putCcrRequest));
            assertThat(
                ex.getMessage(),
                containsString("search does not support document or field level security if replication is assigned")
            );
            assertThat(ex.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        }
    }

    @SuppressWarnings("unchecked")
    private void addDlsQueryToApiKeyDoc(String crossClusterAccessApiKeyId) throws IOException {
        Map<String, Object> apiKeyAsMap = getCrossClusterApiKeys(crossClusterAccessApiKeyId);
        Map<String, Object> roleDescriptors = (Map<String, Object>) apiKeyAsMap.get("role_descriptors");
        Map<String, Object> crossCluster = (Map<String, Object>) roleDescriptors.get("cross_cluster");
        List<Map<String, Object>> indices = (List<Map<String, Object>>) crossCluster.get("indices");
        indices.forEach(index -> {
            List<String> privileges = (List<String>) index.get("privileges");
            if (Arrays.equals(privileges.toArray(String[]::new), CrossClusterApiKeyRoleDescriptorBuilder.CCS_INDICES_PRIVILEGE_NAMES)) {
                index.put("query", "{\"match_all\": {}}");
                index.put("privileges", List.of("read", "read_cross_cluster", "view_index_metadata")); // ensure privs emulate pre 8.14
            }
        });
        crossCluster.put("cluster", List.of("cross_cluster_search", "cross_cluster_replication")); // ensure privs emulate pre 8.14
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.field("cross_cluster", crossCluster);
        builder.endObject();
        updateApiKey(crossClusterAccessApiKeyId, org.elasticsearch.common.Strings.toString(builder));
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> getCrossClusterApiKeys(String id) throws IOException {
        final var request = new Request(HttpGet.METHOD_NAME, "/_security/api_key");
        request.addParameters(Map.of("id", id));

        Response response = performRequestAgainstFulfillingCluster(request);
        Map<String, Object> responseMap = entityAsMap(response);
        List<Map<String, Object>> apiKeys = (List<Map<String, Object>>) responseMap.get("api_keys");
        assertThat(apiKeys.size(), equalTo(1));
        assertNotNull(ObjectPath.eval("role_descriptors.cross_cluster", apiKeys.get(0)));
        return apiKeys.get(0);
    }

    @SuppressWarnings("unchecked")
    private void validateQueryCrossClusterApiKeys(String id) throws IOException {
        final var request = new Request(HttpGet.METHOD_NAME, "/_security/_query/api_key");
        request.setJsonEntity(Strings.format("""
            {
              "query": {
                "ids": {
                  "values": [
                    "%s"
                  ]
                }
              }
            }
            """, id));

        Response response = performRequestAgainstFulfillingCluster(request);
        Map<String, Object> responseMap = entityAsMap(response);
        assertThat(responseMap.get("total"), equalTo(1));
        assertThat(responseMap.get("count"), equalTo(1));
        List<Map<String, Object>> apiKeys = (List<Map<String, Object>>) responseMap.get("api_keys");
        assertThat(apiKeys.size(), equalTo(1));
        // assumes this method is only called after we manually update the API key doc with the DLS query
        String query = ObjectPath.eval("role_descriptors.cross_cluster.indices.0.query", apiKeys.get(0));
        try {
            assertThat(query, equalTo("{\"match_all\": {}}"));
        } catch (AssertionError e) {
            // it's ugly, but the query could be in the 0 or 1 position.
            query = ObjectPath.eval("role_descriptors.cross_cluster.indices.1.query", apiKeys.get(0));
            assertThat(query, equalTo("{\"match_all\": {}}"));
        }
    }

    private static XContentParser getParser(Response response) throws IOException {
        final byte[] responseBody = EntityUtils.toByteArray(response.getEntity());
        return XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, responseBody);
    }

    static void updateApiKey(String id, String payload) throws IOException {
        final Request request = new Request("POST", "/.security/_update/" + id + "?refresh=true");
        request.setJsonEntity(Strings.format("""
            {
              "doc": {
                "role_descriptors": %s
              }
            }
            """, payload));
        expectWarnings(
            request,
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );
        Response response = performRequestAgainstFulfillingCluster(request);
        assertOK(response);
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
        );
        return client().performRequest(request);
    }

    static void expectWarnings(Request request, String... expectedWarnings) {
        final Set<String> expected = Set.of(expectedWarnings);
        RequestOptions options = request.getOptions().toBuilder().setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expected) == false;
        }).build();
        request.setOptions(options);
    }

    private Response performRequestWithCcrUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(CCR_USER, PASS)));
        return client().performRequest(request);
    }
}
