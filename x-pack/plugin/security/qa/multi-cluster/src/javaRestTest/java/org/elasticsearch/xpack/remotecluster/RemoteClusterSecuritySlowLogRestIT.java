/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;

public class RemoteClusterSecuritySlowLogRestIT extends AbstractRemoteClusterSecurityTestCase {

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
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                            "search": [
                              {
                                "names": ["slow_log_*", "run_as_*"]
                              }
                            ]
                        }""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testCrossClusterSlowLogAuthenticationContext() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster setup
        {
            // Create an index with slow log settings enabled
            final Request createIndexRequest = new Request("PUT", "/slow_log_test");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                    "index.search.slowlog.threshold.query.trace": "0ms",
                    "index.search.slowlog.include.user": true
                  },
                  "mappings": {
                    "properties": {
                      "content": { "type": "text" },
                      "timestamp": { "type": "date" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest));

            // test documents
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity("""
                { "index": { "_index": "slow_log_test" } }
                { "content": "test content for slow log", "timestamp": "2024-01-01T10:00:00Z" }
                { "index": { "_index": "slow_log_test" } }
                { "content": "another test document", "timestamp": "2024-01-01T11:00:00Z" }
                """);
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster setup
        {
            // Create user role with remote cluster privileges
            final var putRoleRequest = new Request("PUT", "/_security/role/slow_log_remote_role");
            putRoleRequest.setJsonEntity("""
                {
                  "description": "Role for testing slow log auth context with cross-cluster access",
                  "cluster": ["manage_own_api_key"],
                  "remote_indices": [
                    {
                      "names": ["slow_log_*"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));

            // Create test user
            final var putUserRequest = new Request("PUT", "/_security/user/slow_log_test_user");
            putUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles": ["slow_log_remote_role"],
                  "full_name": "Slow Log Test User"
                }""");
            assertOK(adminClient().performRequest(putUserRequest));

            // Create API key for the test user
            final var createApiKeyRequest = new Request("PUT", "/_security/api_key");
            createApiKeyRequest.setJsonEntity("""
                {
                  "name": "slow_log_test_api_key",
                  "role_descriptors": {
                    "slow_log_access": {
                      "remote_indices": [
                        {
                          "names": ["slow_log_*"],
                          "privileges": ["read", "read_cross_cluster"],
                          "clusters": ["my_remote_cluster"]
                        }
                      ]
                    }
                  }
                }""");
            final var createApiKeyResponse = performRequestWithSlowLogTestUser(createApiKeyRequest);
            assertOK(createApiKeyResponse);

            var createApiKeyResponsePath = ObjectPath.createFromResponse(createApiKeyResponse);
            final String apiKeyEncoded = createApiKeyResponsePath.evaluate("encoded");
            final String apiKeyId = createApiKeyResponsePath.evaluate("id");
            assertThat(apiKeyEncoded, notNullValue());
            assertThat(apiKeyId, notNullValue());

            // Perform cross-cluster search that should generate slow log entries
            final var searchRequest = new Request("GET", "/my_remote_cluster:slow_log_test/_search");
            searchRequest.setJsonEntity("""
                {
                  "query": {
                    "match": {
                      "content": "test"
                    }
                  },
                  "sort": [
                    { "timestamp": { "order": "desc" } }
                  ]
                }""");

            // Execute search with API key authentication
            final Response searchResponse = performRequestWithApiKey(searchRequest, apiKeyEncoded);
            assertOK(searchResponse);

            // Verify slow log contains correct authentication context from the original user
            Map<String, Object> expectedAuthContext = Map.of(
                "user.name",
                "slow_log_test_user",
                "user.realm",
                "_es_api_key",
                "user.full_name",
                "Slow Log Test User",
                "auth.type",
                "API_KEY",
                "apikey.id",
                apiKeyId,
                "apikey.name",
                "slow_log_test_api_key"
            );

            verifySlowLogAuthenticationContext(expectedAuthContext);
        }
    }

    public void testRunAsUserInCrossClusterSlowLog() throws Exception {
        configureRemoteCluster();

        // Fulfilling cluster setup
        {
            final Request createIndexRequest = new Request("PUT", "/run_as_test");
            createIndexRequest.setJsonEntity("""
                {
                  "settings": {
                    "index.search.slowlog.threshold.query.trace": "0ms",
                    "index.search.slowlog.include.user": true
                  },
                  "mappings": {
                    "properties": {
                      "data": { "type": "text" }
                    }
                  }
                }""");
            assertOK(performRequestAgainstFulfillingCluster(createIndexRequest));

            final Request indexRequest = new Request("POST", "/run_as_test/_doc?refresh=true");
            indexRequest.setJsonEntity("""
                { "data": "run as test data" }""");
            assertOK(performRequestAgainstFulfillingCluster(indexRequest));
        }

        // Query cluster setup
        {
            // Create role that allows run-as and remote access
            final var putRunAsRoleRequest = new Request("PUT", "/_security/role/run_as_remote_role");
            putRunAsRoleRequest.setJsonEntity("""
                {
                  "description": "Role that can run as other users and access remote clusters",
                  "cluster": ["manage_own_api_key"],
                  "run_as": ["target_user"],
                  "remote_indices": [
                    {
                      "names": ["run_as_*"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRunAsRoleRequest));

            // Create the run-as user
            final var putRunAsUserRequest = new Request("PUT", "/_security/user/run_as_user");
            putRunAsUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles": ["run_as_remote_role"],
                  "full_name": "Run As User"
                }""");
            assertOK(adminClient().performRequest(putRunAsUserRequest));

            // Create target user (who will be run as)
            final var putTargetUserRequest = new Request("PUT", "/_security/user/target_user");
            putTargetUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles": ["run_as_remote_role"],
                  "full_name": "Target User"
                }""");
            assertOK(adminClient().performRequest(putTargetUserRequest));

            // Perform search with run-as header
            final var runAsSearchRequest = new Request("GET", "/my_remote_cluster:run_as_test/_search");
            runAsSearchRequest.setJsonEntity("""
                {
                  "query": { "match_all": {} }
                }""");

            // Add both authentication and run-as headers
            runAsSearchRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", basicAuthHeaderValue("run_as_user", PASS))
                    .addHeader("es-security-runas-user", "target_user")
            );

            final Response runAsResponse = client().performRequest(runAsSearchRequest);
            assertOK(runAsResponse);

            Map<String, Object> expectedRunAsAuthContext = Map.of(
                "user.name",
                "run_as_user",
                "user.realm",
                "default_native",
                "user.full_name",
                "Run As User",
                "user.effective.name",
                "target_user",
                "user.effective.realm",
                "default_native",
                "user.effective.full_name",
                "Target User",
                "auth.type",
                "REALM"
            );

            verifySlowLogAuthenticationContext(expectedRunAsAuthContext);
        }
    }

    private void verifySlowLogAuthenticationContext(Map<String, Object> expectedAuthContext) throws Exception {
        assertBusy(() -> {
            try (var slowLog = fulfillingCluster.getNodeLog(0, LogType.SEARCH_SLOW)) {
                final List<String> lines = Streams.readAllLines(slowLog);
                assertThat(lines, not(empty()));

                // Get the most recent slow log entry
                String lastLogLine = lines.get(lines.size() - 1);
                Map<String, Object> logEntry = XContentHelper.convertToMap(XContentType.JSON.xContent(), lastLogLine, true);

                for (Map.Entry<String, Object> expectedEntry : expectedAuthContext.entrySet()) {
                    assertThat(
                        "Slow log should contain " + expectedEntry.getKey() + " with value " + expectedEntry.getValue(),
                        logEntry,
                        hasKey(expectedEntry.getKey())
                    );
                    assertThat(
                        "Slow log " + expectedEntry.getKey() + " should match expected value",
                        logEntry.get(expectedEntry.getKey()),
                        equalTo(expectedEntry.getValue())
                    );
                }
            }
        }, 10, TimeUnit.SECONDS);
    }

    private Response performRequestWithSlowLogTestUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue("slow_log_test_user", PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithApiKey(final Request request, final String encoded) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
    }
}
