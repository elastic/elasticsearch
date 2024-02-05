/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecurityEsqlIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<Map<String, Object>> REST_API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicInteger INVALID_SECRET_LENGTH = new AtomicInteger();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("ingest-common")
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
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("ingest-common")
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
                                "names": ["index*", "not_found_index", "employees"]
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
            // Define a remote with invalid API key secret length
            .keystore(
                "cluster.remote.invalid_secret_length.credentials",
                () -> Base64.getEncoder()
                    .encodeToString(
                        (UUIDs.base64UUID() + ":" + randomAlphaOfLength(INVALID_SECRET_LENGTH.get())).getBytes(StandardCharsets.UTF_8)
                    )
            )
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
        INVALID_SECRET_LENGTH.set(randomValueOtherThan(22, () -> randomIntBetween(0, 99)));
    })).around(fulfillingCluster).around(queryCluster);

    public void populateData() throws Exception {
        CheckedConsumer<RestClient, IOException> setupEnrich = client -> {
            Request createIndex = new Request("PUT", "countries");
            createIndex.setJsonEntity("""
                {
                    "mappings": {
                        "properties": {
                          "emp_id": { "type": "keyword" },
                          "country": { "type": "text" }
                        }
                    }
                }
                """);
            assertOK(performRequestWithAdminUser(client, createIndex));
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "countries" } }
                { "emp_id": "1", "country": "usa"}
                { "index": { "_index": "countries" } }
                { "emp_id": "2", "country": "canada"}
                { "index": { "_index": "countries" } }
                { "emp_id": "3", "country": "germany"}
                { "index": { "_index": "countries" } }
                { "emp_id": "4", "country": "spain"}
                { "index": { "_index": "countries" } }
                { "emp_id": "5", "country": "japan"}
                { "index": { "_index": "countries" } }
                { "emp_id": "6", "country": "france"}
                { "index": { "_index": "countries" } }
                { "emp_id": "7", "country": "usa"}
                { "index": { "_index": "countries" } }
                { "emp_id": "8", "country": "canada"}
                { "index": { "_index": "countries" } }
                { "emp_id": "9", "country": "usa"}
                """));
            assertOK(performRequestWithAdminUser(client, bulkRequest));

            Request createEnrich = new Request("PUT", "/_enrich/policy/countries");
            createEnrich.setJsonEntity("""
                {
                    "match": {
                        "indices": "countries",
                        "match_field": "emp_id",
                        "enrich_fields": ["country"]
                    }
                }
                """);
            assertOK(performRequestWithAdminUser(client, createEnrich));
            assertOK(performRequestWithAdminUser(client, new Request("PUT", "_enrich/policy/countries/_execute")));
            performRequestWithAdminUser(client, new Request("DELETE", "/countries"));
        };
        // Fulfilling cluster
        {
            setupEnrich.accept(fulfillingClusterClient);
            Request createIndex = new Request("PUT", "employees");
            createIndex.setJsonEntity("""
                {
                    "mappings": {
                        "properties": {
                          "emp_id": { "type": "keyword" },
                          "department": {"type": "keyword" }
                        }
                    }
                }
                """);
            assertOK(performRequestAgainstFulfillingCluster(createIndex));
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "employees" } }
                { "emp_id": "1", "department" : "engineering" }
                { "index": { "_index": "employees" } }
                { "emp_id": "3", "department" : "sales" }
                { "index": { "_index": "employees" } }
                { "emp_id": "5", "department" : "marketing" }
                { "index": { "_index": "employees" } }
                { "emp_id": "7", "department" : "engineering" }
                { "index": { "_index": "employees" } }
                { "emp_id": "9", "department" : "sales" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }
        // Querying cluster
        // Index some documents, to use them in a mixed-cluster search
        setupEnrich.accept(client());
        Request createIndex = new Request("PUT", "employees");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                      "emp_id": { "type": "keyword" },
                      "department": { "type": "keyword" }
                    }
                }
            }
            """);
        assertOK(adminClient().performRequest(createIndex));
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "employees" } }
            { "emp_id": "2", "department" : "management" }
            { "index": { "_index": "employees"} }
            { "emp_id": "4", "department" : "engineering" }
            { "index": { "_index": "employees" } }
            { "emp_id": "6", "department" : "marketing"}
            { "index": { "_index": "employees"} }
            { "emp_id": "8", "department" : "support"}
            """));
        assertOK(client().performRequest(bulkRequest));

        // Create user role with privileges for remote and local indices
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["employees"],
                  "privileges": ["read"]
                }
              ],
              "cluster": [ "monitor_enrich" ],
              "remote_indices": [
                {
                  "names": ["employees"],
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

    @After
    public void wipeData() throws Exception {
        CheckedConsumer<RestClient, IOException> wipe = client -> {
            performRequestWithAdminUser(client, new Request("DELETE", "/employees"));
            performRequestWithAdminUser(client, new Request("DELETE", "/_enrich/policy/countries"));
        };
        wipe.accept(fulfillingClusterClient);
        wipe.accept(client());
    }

    @AwaitsFix(bugUrl = "cross-clusters query doesn't work with RCS 2.0")
    public void testCrossClusterQuery() throws Exception {
        configureRemoteCluster();
        populateData();
        // Query cluster
        {
            {
                Response response = performRequestWithRemoteSearchUser(esqlRequest("""
                    FROM my_remote_cluster:employees
                    | SORT emp_id ASC
                    | LIMIT 2
                    | KEEP emp_id, department"""));
                assertOK(response);
                Map<String, Object> values = entityAsMap(response);
            }
            {
                Response response = performRequestWithRemoteSearchUser(esqlRequest("""
                    FROM my_remote_cluster:employees,employees
                    | SORT emp_id ASC
                    | LIMIT 10"""));
                assertOK(response);

            }
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
            for (String indices : List.of("my_remote_cluster:employees,employees", "my_remote_cluster:employees")) {
                ResponseException error = expectThrows(ResponseException.class, () -> {
                    var q = "FROM " + indices + "|  SORT emp_id DESC | LIMIT 10";
                    performRequestWithLocalSearchUser(esqlRequest(q));
                });
                assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(401));
                assertThat(error.getMessage(), containsString("unable to find apikey"));
            }
        }
    }

    @AwaitsFix(bugUrl = "cross-clusters enrich doesn't work with RCS 2.0")
    public void testCrossClusterEnrich() throws Exception {
        configureRemoteCluster();
        populateData();
        // Query cluster
        {
            // ESQL with enrich is okay when user has access to enrich polices
            Response response = performRequestWithRemoteSearchUser(esqlRequest("""
                FROM my_remote_cluster:employees,employees
                | ENRICH countries
                | STATS size=count(*) by country
                | SORT size DESC
                | LIMIT 2"""));
            assertOK(response);
            Map<String, Object> values = entityAsMap(response);

            // ESQL with enrich is denied when user has no access to enrich policies
            final var putLocalSearchRoleRequest = new Request("PUT", "/_security/role/local_search");
            putLocalSearchRoleRequest.setJsonEntity("""
                {
                  "indices": [
                    {
                      "names": ["employees"],
                      "privileges": ["read"]
                    }
                  ],
                  "cluster": [ ],
                  "remote_indices": [
                    {
                      "names": ["employees"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putLocalSearchRoleRequest));
            final var putlocalSearchUserRequest = new Request("PUT", "/_security/user/local_search_user");
            putlocalSearchUserRequest.setJsonEntity("""
                {
                  "password": "x-pack-test-password",
                  "roles" : ["local_search"]
                }""");
            assertOK(adminClient().performRequest(putlocalSearchUserRequest));
            for (String indices : List.of("my_remote_cluster:employees,employees", "my_remote_cluster:employees")) {
                ResponseException error = expectThrows(ResponseException.class, () -> {
                    var q = "FROM " + indices + "| ENRICH countries | STATS size=count(*) by country | SORT size | LIMIT 2";
                    performRequestWithLocalSearchUser(esqlRequest(q));
                });
                assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
                assertThat(
                    error.getMessage(),
                    containsString(
                        "action [cluster:monitor/xpack/enrich/esql/resolve_policy] towards remote cluster [my_remote_cluster]"
                            + " is unauthorized for user [local_search_user] with effective roles [local_search]"
                    )
                );
            }
        }
    }

    protected Request esqlRequest(String command) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();
        body.startObject();
        body.field("query", command);
        if (Build.current().isSnapshot() && randomBoolean()) {
            Settings.Builder settings = Settings.builder();
            if (randomBoolean()) {
                settings.put("page_size", between(1, 5));
            }
            if (randomBoolean()) {
                settings.put("exchange_buffer_size", between(1, 2));
            }
            if (randomBoolean()) {
                settings.put("data_partitioning", randomFrom("shard", "segment", "doc"));
            }
            if (randomBoolean()) {
                settings.put("enrich_max_workers", between(1, 5));
            }
            Settings pragmas = settings.build();
            if (pragmas != Settings.EMPTY) {
                body.startObject("pragma");
                body.value(pragmas);
                body.endObject();
            }
        }
        body.endObject();
        Request request = new Request("POST", "_query");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));
        return request;
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    private Response performRequestWithLocalSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod("local_search_user", PASS))
        );
        return client().performRequest(request);
    }
}
