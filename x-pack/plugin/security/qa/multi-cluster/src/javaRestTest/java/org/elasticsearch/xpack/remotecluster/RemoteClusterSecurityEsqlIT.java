/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.Build;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.CheckedBiConsumer;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.After;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.not;

// uses RCS 2.0
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
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.ml.enabled", "false")
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
            .module("x-pack-autoscaling")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .module("x-pack-ml")
            .module("ingest-common")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        {
                          "search": [
                            {
                                "names": ["index*", "alias*", "not_found_index", "employees", "employees2"]
                            },
                            {
                                "names": ["employees3"],
                                "query": {"term" : {"department" : "engineering"}}
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
        setupEnrich.accept(fulfillingClusterClient);
        String employeesMapping = """
            {
                "mappings": {
                    "properties": {
                      "emp_id": { "type": "keyword" },
                      "department": { "type": "keyword" }
                    }
                }
            }
            """;
        Request createIndex = new Request("PUT", "employees");
        createIndex.setJsonEntity(employeesMapping);
        assertOK(performRequestAgainstFulfillingCluster(createIndex));
        Request createIndex2 = new Request("PUT", "employees2");
        createIndex2.setJsonEntity(employeesMapping);
        assertOK(performRequestAgainstFulfillingCluster(createIndex2));
        Request createIndex3 = new Request("PUT", "employees3");
        createIndex3.setJsonEntity(employeesMapping);
        assertOK(performRequestAgainstFulfillingCluster(createIndex3));
        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
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
            { "index": { "_index": "employees2" } }
            { "emp_id": "11", "department" : "engineering" }
            { "index": { "_index": "employees2" } }
            { "emp_id": "13", "department" : "sales" }
             { "index": { "_index": "employees3" } }
            { "emp_id": "21", "department" : "engineering" }
            { "index": { "_index": "employees3" } }
            { "emp_id": "23", "department" : "sales" }
            { "index": { "_index": "employees3" } }
            { "emp_id": "25", "department" : "engineering" }
            { "index": { "_index": "employees3" } }
            { "emp_id": "27", "department" : "sales" }
            """));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        // Querying cluster
        // Index some documents, to use them in a mixed-cluster search
        setupEnrich.accept(client());

        createIndex = new Request("PUT", "employees");
        createIndex.setJsonEntity(employeesMapping);
        assertOK(adminClient().performRequest(createIndex));
        createIndex2 = new Request("PUT", "employees2");
        createIndex2.setJsonEntity(employeesMapping);
        assertOK(adminClient().performRequest(createIndex2));
        createIndex3 = new Request("PUT", "employees3");
        createIndex3.setJsonEntity(employeesMapping);
        assertOK(adminClient().performRequest(createIndex3));
        bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity(Strings.format("""
            { "index": { "_index": "employees" } }
            { "emp_id": "2", "department" : "management" }
            { "index": { "_index": "employees"} }
            { "emp_id": "4", "department" : "engineering" }
            { "index": { "_index": "employees" } }
            { "emp_id": "6", "department" : "marketing"}
            { "index": { "_index": "employees"} }
            { "emp_id": "8", "department" : "support"}
            { "index": { "_index": "employees2"} }
            { "emp_id": "10", "department" : "management"}
            { "index": { "_index": "employees2"} }
            { "emp_id": "12", "department" : "engineering"}
            { "index": { "_index": "employees3"} }
            { "emp_id": "20", "department" : "management"}
            { "index": { "_index": "employees3"} }
            { "emp_id": "22", "department" : "engineering"}
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
              "cluster": [ "monitor_enrich", "manage_own_api_key" ],
              "remote_indices": [
                {
                  "names": ["employees"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ],
              "remote_cluster": [
                {
                  "privileges": ["monitor_enrich"],
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

    private static String populateOtherUser() throws IOException {
        String otherUser = REMOTE_SEARCH_USER + "_other";

        final var putUserRequest = new Request("PUT", "/_security/user/" + otherUser);
        putUserRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles" : ["remote_search"]
            }""");
        assertOK(adminClient().performRequest(putUserRequest));
        return otherUser;
    }

    private void performRequestWithAdminUserIgnoreNotFound(RestClient targetFulfillingClusterClient, Request request) throws IOException {
        try {
            performRequestWithAdminUser(targetFulfillingClusterClient, request);
        } catch (ResponseException e) {
            if (e.getResponse().getStatusLine().getStatusCode() != 404) {
                throw e;
            }
            logger.info("Ignored \"not found\" exception", e);
        }
    }

    @After
    public void wipeData() throws Exception {
        CheckedConsumer<RestClient, IOException> wipe = client -> {
            performRequestWithAdminUserIgnoreNotFound(client, new Request("DELETE", "/employees"));
            performRequestWithAdminUserIgnoreNotFound(client, new Request("DELETE", "/employees2"));
            performRequestWithAdminUserIgnoreNotFound(client, new Request("DELETE", "/employees3"));
            performRequestWithAdminUserIgnoreNotFound(client, new Request("DELETE", "/_enrich/policy/countries"));
        };
        wipe.accept(fulfillingClusterClient);
        wipe.accept(client());
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterQuery() throws Exception {
        configureRemoteCluster();
        populateData();

        Map<String, Object> esqlCcsLicenseFeatureUsage = fetchEsqlCcsFeatureUsageFromNode(client());

        Object ccsLastUsedTimestampAtStartOfTest = null;
        if (esqlCcsLicenseFeatureUsage.isEmpty() == false) {
            // some test runs will have a usage value already, so capture that to compare at end of test
            ccsLastUsedTimestampAtStartOfTest = esqlCcsLicenseFeatureUsage.get("last_used");
        }

        // query remote cluster only
        Request request = esqlRequest("""
            FROM my_remote_cluster:employees
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department""");
        Response response = performRequestWithRemoteSearchUser(request);
        assertRemoteOnlyResults(response);

        // same as above but authenticate with API key
        response = performRequestWithRemoteSearchUserViaAPIKey(request, createRemoteSearchUserAPIKey());
        assertRemoteOnlyResults(response);

        // query remote and local cluster
        response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees,employees
            | SORT emp_id ASC
            | LIMIT 10"""));
        assertRemoteAndLocalResults(response);

        // update role to include both employees and employees2 for the remote cluster
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["employees*"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        response = adminClient().performRequest(putRoleRequest);
        assertOK(response);

        // query remote cluster only - but also include employees2 which the user now access
        response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees,my_remote_cluster:employees2
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department"""));
        assertRemoteOnlyAgainst2IndexResults(response);

        // check that the esql-ccs license feature is now present and that the last_used field has been updated
        esqlCcsLicenseFeatureUsage = fetchEsqlCcsFeatureUsageFromNode(client());
        assertThat(esqlCcsLicenseFeatureUsage.size(), equalTo(5));
        Object lastUsed = esqlCcsLicenseFeatureUsage.get("last_used");
        assertNotNull("lastUsed should not be null", lastUsed);
        if (ccsLastUsedTimestampAtStartOfTest != null) {
            assertThat(lastUsed.toString(), not(equalTo(ccsLastUsedTimestampAtStartOfTest.toString())));
        }
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterQueryWithRemoteDLSAndFLS() throws Exception {
        configureRemoteCluster();
        populateData();

        // ensure user has access to the employees3 index
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["employees*"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]

                }
              ]
            }""");
        Response response = adminClient().performRequest(putRoleRequest);
        assertOK(response);

        response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees3
            | SORT emp_id ASC
            | LIMIT 10
            | KEEP emp_id, department"""));
        assertOK(response);

        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(2, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        // the APIKey has DLS set to : "query": {"term" : {"department" : "engineering"}}
        assertThat(flatList, containsInAnyOrder("21", "25", "engineering", "engineering"));

        // add DLS to the remote indices in the role to restrict access to only emp_id = 21
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["employees*"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"],
                  "query": {"term" : {"emp_id" : "21"}}

                }
              ]
            }""");
        response = adminClient().performRequest(putRoleRequest);
        assertOK(response);

        response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees3
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department"""));
        assertOK(response);

        responseAsMap = entityAsMap(response);
        columns = (List<?>) responseAsMap.get("columns");
        values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(1, values.size());
        flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        // the APIKey has DLS set to : "query": {"term" : {"department" : "engineering"}}
        // AND this role has DLS set to: "query": {"term" : {"emp_id" : "21"}}
        assertThat(flatList, containsInAnyOrder("21", "engineering"));

        // add FLS to the remote indices in the role to restrict access to only access department
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["employees*"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"],
                  "query": {"term" : {"emp_id" : "21"}},
                  "field_security": {"grant": [ "department" ]}
                }
              ]
            }""");
        response = adminClient().performRequest(putRoleRequest);
        assertOK(response);

        response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees3
            | LIMIT 2
            """));
        assertOK(response);
        responseAsMap = entityAsMap(response);
        columns = (List<?>) responseAsMap.get("columns");
        values = (List<?>) responseAsMap.get("values");
        assertEquals(1, columns.size());
        assertEquals(1, values.size());
        flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        // the APIKey has DLS set to : "query": {"term" : {"department" : "engineering"}}
        // AND this role has DLS set to: "query": {"term" : {"emp_id" : "21"}}
        // AND this role has FLS set to: "field_security": {"grant": [ "department" ]}
        assertThat(flatList, containsInAnyOrder("engineering"));
    }

    /**
     * Note: invalid_remote is "invalid" because it has a bogus API key
     */
    @SuppressWarnings("unchecked")
    public void testCrossClusterQueryAgainstInvalidRemote() throws Exception {
        configureRemoteCluster();
        populateData();

        final boolean skipUnavailable = randomBoolean();

        // avoids getting 404 errors
        updateClusterSettings(
            randomBoolean()
                ? Settings.builder()
                    .put("cluster.remote.invalid_remote.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                    .put("cluster.remote.invalid_remote.skip_unavailable", Boolean.toString(skipUnavailable))
                    .build()
                : Settings.builder()
                    .put("cluster.remote.invalid_remote.mode", "proxy")
                    .put("cluster.remote.invalid_remote.skip_unavailable", Boolean.toString(skipUnavailable))
                    .put("cluster.remote.invalid_remote.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                    .build()
        );

        // invalid remote with local index should return local results
        {
            var q = "FROM invalid_remote:employees,employees | SORT emp_id DESC | LIMIT 10";
            if (skipUnavailable) {
                Response response = performRequestWithRemoteSearchUser(esqlRequest(q));
                // this does not yet happen because field-caps returns nothing for this cluster, rather
                // than an error, so the current code cannot detect that error. Follow on PR will handle this.
                assertLocalOnlyResultsAndSkippedRemote(response);
            } else {
                // errors from invalid remote should throw an exception if the cluster is marked with skip_unavailable=false
                ResponseException error = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(esqlRequest(q)));
                assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
                // TODO: in follow on PR, figure out why this is returning the wrong error - should be "cannot connect to invalid_remote"
                assertThat(error.getMessage(), containsString("Unknown index [invalid_remote:employees]"));
            }
        }
        {
            var q = "FROM invalid_remote:employees | SORT emp_id DESC | LIMIT 10";
            // errors from invalid remote should be ignored if the cluster is marked with skip_unavailable=true
            if (skipUnavailable) {
                // expected response:
                // {"took":1,"columns":[],"values":[],"_clusters":{"total":1,"successful":0,"running":0,"skipped":1,"partial":0,
                // "failed":0,"details":{"invalid_remote":{"status":"skipped","indices":"employees","took":1,"_shards":
                // {"total":0,"successful":0,"skipped":0,"failed":0},"failures":[{"shard":-1,"index":null,"reason":
                // {"type":"remote_transport_exception",
                // "reason":"[connect_transport_exception - unable to connect to remote cluster]"}}]}}}}
                Response response = performRequestWithRemoteSearchUser(esqlRequest(q));
                assertOK(response);
                Map<String, Object> responseAsMap = entityAsMap(response);
                List<?> columns = (List<?>) responseAsMap.get("columns");
                List<?> values = (List<?>) responseAsMap.get("values");
                assertThat(columns.size(), equalTo(1));
                Map<String, ?> column1 = (Map<String, ?>) columns.get(0);
                assertThat(column1.get("name").toString(), equalTo("<no-fields>"));
                assertThat(values.size(), equalTo(0));
                Map<String, ?> clusters = (Map<String, ?>) responseAsMap.get("_clusters");
                Map<String, ?> details = (Map<String, ?>) clusters.get("details");
                Map<String, ?> invalidRemoteEntry = (Map<String, ?>) details.get("invalid_remote");
                assertThat(invalidRemoteEntry.get("status").toString(), equalTo("skipped"));
                List<?> failures = (List<?>) invalidRemoteEntry.get("failures");
                assertThat(failures.size(), equalTo(1));
                Map<String, ?> failuresMap = (Map<String, ?>) failures.get(0);
                Map<String, ?> reason = (Map<String, ?>) failuresMap.get("reason");
                assertThat(reason.get("type").toString(), equalTo("remote_transport_exception"));
                assertThat(reason.get("reason").toString(), containsString("unable to connect to remote cluster"));

            } else {
                // errors from invalid remote should throw an exception if the cluster is marked with skip_unavailable=false
                ResponseException error = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(esqlRequest(q)));
                assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(401));
                // TODO: in follow on PR, figure out why this is returning the wrong error - should be "cannot connect to invalid_remote"
                assertThat(error.getMessage(), containsString("unable to find apikey"));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterQueryWithOnlyRemotePrivs() throws Exception {
        configureRemoteCluster();
        populateData();

        // Query cluster
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["employees"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        // query appropriate privs
        Response response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department"""));
        assertRemoteOnlyResults(response);

        // without the remote index priv
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "remote_indices": [
                {
                  "names": ["idontexist"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        ResponseException error = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department""")));
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        assertThat(error.getMessage(), containsString("Unknown index [my_remote_cluster:employees]"));

        // no local privs at all will fail
        final var putRoleNoLocalPrivs = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleNoLocalPrivs.setJsonEntity("""
            {
              "indices": [],
              "remote_indices": [
                {
                  "names": ["employees"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleNoLocalPrivs));

        error = expectThrows(ResponseException.class, () -> { performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department""")); });

        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            error.getMessage(),
            containsString(
                "action [indices:data/read/esql] is unauthorized for user [remote_search_user] with effective roles [remote_search], "
                    + "this action is granted by the index privileges [read,read_cross_cluster,all]"
            )
        );

        // query remote cluster only - but also include employees2 which the user does not have access to
        error = expectThrows(ResponseException.class, () -> { performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees,my_remote_cluster:employees2
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department""")); });

        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            error.getMessage(),
            containsString(
                "action [indices:data/read/esql] is unauthorized for user [remote_search_user] with effective roles "
                    + "[remote_search], this action is granted by the index privileges [read,read_cross_cluster,all]"
            )
        );

        // query remote and local cluster - but also include employees2 which the user does not have access to
        error = expectThrows(ResponseException.class, () -> { performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees,my_remote_cluster:employees2,employees,employees2
            | SORT emp_id ASC
            | LIMIT 10""")); });

        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
        assertThat(
            error.getMessage(),
            containsString(
                "action [indices:data/read/esql] is unauthorized for user [remote_search_user] with effective roles "
                    + "[remote_search], this action is granted by the index privileges [read,read_cross_cluster,all]"
            )
        );
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterEnrich() throws Exception {
        boolean isProxyMode = randomBoolean();
        boolean skipUnavailable = randomBoolean();
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, false, isProxyMode, skipUnavailable);
        populateData();
        // Query cluster
        {
            // ESQL with enrich is okay when user has access to enrich polices
            Request request = esqlRequest("""
                FROM my_remote_cluster:employees,employees
                | ENRICH countries
                | STATS size=count(*) by country
                | SORT size DESC
                | LIMIT 2""");

            Response response = performRequestWithRemoteSearchUser(request);
            assertWithEnrich(response);

            // same as above but authenticate with API key
            response = performRequestWithRemoteSearchUserViaAPIKey(request, createRemoteSearchUserAPIKey());
            assertWithEnrich(response);

            // Query cluster
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);

            // no remote_cluster privs should fail the request
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
                      "privileges": ["read"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            assertOK(adminClient().performRequest(putRoleRequest));

            ResponseException error = expectThrows(ResponseException.class, () -> { performRequestWithRemoteSearchUser(esqlRequest("""
                FROM my_remote_cluster:employees,employees
                | ENRICH countries
                | STATS size=count(*) by country
                | SORT size DESC
                | LIMIT 2""")); });

            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                error.getMessage(),
                containsString(
                    "action [cluster:monitor/xpack/enrich/esql/resolve_policy] towards remote cluster is unauthorized for user "
                        + "[remote_search_user] with assigned roles [remote_search] authenticated by API key id ["
                )
            );
            assertThat(
                error.getMessage(),
                containsString(
                    "this action is granted by the cluster privileges "
                        + "[cross_cluster_search,monitor_enrich,manage_enrich,monitor,manage,all]"
                )
            );
        }
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterEnrichWithOnlyRemotePrivs() throws Exception {
        configureRemoteCluster();
        populateData();

        // Query cluster
        final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);

        // local cross_cluster_search cluster priv is required for enrich
        // ideally, remote only enrichment wouldn't need this local privilege, however remote only enrichment is not currently supported
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read_cross_cluster"]}],
              "cluster": ["cross_cluster_search"],
              "remote_indices": [
                {
                  "names": ["employees"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                }
              ],
              "remote_cluster": [
                {
                  "privileges": ["monitor_enrich"],
                   "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));

        // Query cluster
        // ESQL with enrich is okay when user has access to enrich polices
        Response response = performRequestWithRemoteSearchUser(esqlRequest("""
            FROM my_remote_cluster:employees
            | ENRICH countries
            | STATS size=count(*) by country
            | SORT size DESC
            | LIMIT 2"""));
        assertOK(response);

        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(2, values.size());
        List<?> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<?>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder(1, 3, "usa", "germany"));
    }

    private void createAliases() throws Exception {
        Request createAlias = new Request("POST", "_aliases");
        createAlias.setJsonEntity("""
            {
                "actions": [
                    {
                        "add": {
                            "index": "employees",
                            "alias": "alias-employees"
                        }
                    },
                    {
                        "add": {
                            "index": "employees",
                            "alias": "alias-engineering",
                            "filter": { "match": { "department": "engineering" }}
                        }
                    },
                    {
                        "add": {
                            "index": "employees",
                            "alias": "alias-management",
                            "filter": { "match": { "department": "management" }}
                        }
                    },
                    {
                        "add": {
                           "index": "employees2",
                           "alias": "alias-employees2"
                        }
                    }
                ]
            }
            """);
        assertOK(performRequestAgainstFulfillingCluster(createAlias));
    }

    private void removeAliases() throws Exception {
        var removeAlias = new Request("POST", "/_aliases/");
        removeAlias.setJsonEntity("""
            {
                "actions": [
                    {
                        "remove": {
                            "index": "employees",
                            "alias": "alias-employees"
                        }
                    },
                    {
                        "remove": {
                            "index": "employees",
                            "alias": "alias-engineering"
                        }
                    },
                    {
                        "remove": {
                            "index": "employees",
                            "alias": "alias-management"
                        }
                    },
                    {
                        "remove": {
                           "index": "employees2",
                           "alias": "alias-employees2"
                        }
                    }
                ]
            }
            """);
        assertOK(performRequestAgainstFulfillingCluster(removeAlias));
    }

    public void testAlias() throws Exception {
        configureRemoteCluster();
        populateData();
        createAliases();
        var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "indices": [{"names": [""], "privileges": ["read"]}],
              "cluster": ["cross_cluster_search"],
              "remote_indices": [
                {
                  "names": ["alias-engineering"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                },
                {
                  "names": ["employees2"],
                  "privileges": ["read"],
                  "clusters": ["my_remote_cluster"]
                },
                {
                  "names": ["employees3"],
                  "privileges": ["view_index_metadata", "read_cross_cluster"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleRequest));
        // query `employees2`
        for (String index : List.of("*:employees2", "*:employee*")) {
            Request request = esqlRequest("FROM " + index + " | KEEP emp_id | SORT emp_id | LIMIT 100");
            Response response = performRequestWithRemoteSearchUser(request);
            assertOK(response);
            Map<String, Object> responseAsMap = entityAsMap(response);
            List<?> ids = (List<?>) responseAsMap.get("values");
            assertThat(ids, equalTo(List.of(List.of("11"), List.of("13"))));
        }

        // query `employees2` and `alias-engineering`
        for (var index : List.of("*:employees2,*:alias-engineering", "*:emp*,*:alias-engineering", "*:emp*,my*:alias*")) {
            Request request = esqlRequest("FROM " + index + " | KEEP emp_id | SORT emp_id | LIMIT 100");
            Response response = performRequestWithRemoteSearchUser(request);
            assertOK(response);
            Map<String, Object> responseAsMap = entityAsMap(response);
            List<?> ids = (List<?>) responseAsMap.get("values");
            assertThat(ids, equalTo(List.of(List.of("1"), List.of("11"), List.of("13"), List.of("7"))));
        }
        // none
        for (var index : List.of("*:employees1", "*:employees3", "*:employees1,employees3", "*:alias-employees,*:alias-management")) {
            Request request = esqlRequest("FROM " + index + " | KEEP emp_id | SORT emp_id | LIMIT 100");
            ResponseException error = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(request));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            String expectedIndexExpressionInError = index.replace("*", "my_remote_cluster");
            Pattern p = Pattern.compile("Unknown index \\[([^\\]]+)\\]");
            Matcher m = p.matcher(error.getMessage());
            assertTrue("Pattern matcher to parse error message did not find matching string: " + error.getMessage(), m.find());
            String unknownIndexExpressionInErrorMessage = m.group(1);
            Set<String> actualUnknownIndexes = org.elasticsearch.common.Strings.commaDelimitedListToSet(
                unknownIndexExpressionInErrorMessage
            );
            Set<String> expectedUnknownIndexes = org.elasticsearch.common.Strings.commaDelimitedListToSet(expectedIndexExpressionInError);
            assertThat(actualUnknownIndexes, equalTo(expectedUnknownIndexes));
        }

        for (var index : List.of(
            Tuple.tuple("*:employee*,*:alias-employees,*:employees3", "alias-employees,employees3"),
            Tuple.tuple("*:alias*,my*:employees1", "employees1"),
            Tuple.tuple("*:alias*,my*:employees3", "employees3")
        )) {
            Request request = esqlRequest("FROM " + index.v1() + " | KEEP emp_id | SORT emp_id | LIMIT 100");
            ResponseException error = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(request));
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(
                error.getMessage(),
                containsString("unauthorized for user [remote_search_user] with assigned roles [remote_search]")
            );
            assertThat(error.getMessage(), containsString("user [test_user] on indices [" + index.v2() + "]"));
        }

        // query `alias-engineering`
        Request request = esqlRequest("FROM *:alias* | KEEP emp_id | SORT emp_id | LIMIT 100");
        Response response = performRequestWithRemoteSearchUser(request);
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> ids = (List<?>) responseAsMap.get("values");
        assertThat(ids, equalTo(List.of(List.of("1"), List.of("7"))));

        removeAliases();
    }

    @SuppressWarnings("unchecked")
    public void testSearchesAgainstNonMatchingIndices() throws Exception {
        boolean skipUnavailable = randomBoolean();
        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, false, randomBoolean(), skipUnavailable);
        populateData();

        {
            final var putRoleRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "indices": [{"names": ["employees*"], "privileges": ["read","read_cross_cluster"]}],
                  "cluster": [ "manage_own_api_key" ],
                  "remote_indices": [
                    {
                      "names": ["employees*"],
                      "privileges": ["read"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""");
            Response response = adminClient().performRequest(putRoleRequest);
            assertOK(response);
        }

        String remoteSearchUserAPIKey = createRemoteSearchUserAPIKey();

        // sanity check - init queries to ensure we can query employees on local and employees,employees2 on remote
        {
            Request request = esqlRequest("""
                FROM employees,my_remote_cluster:employees,my_remote_cluster:employees2
                | SORT emp_id ASC
                | LIMIT 5
                | KEEP emp_id, department""");

            CheckedConsumer<Response, Exception> verifier = resp -> {
                assertOK(resp);
                Map<String, Object> map = responseAsMap(resp);
                assertThat(((List<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
                assertThat(((List<?>) map.get("values")).size(), greaterThanOrEqualTo(1));
                assertExpectedClustersForMissingIndicesTests(
                    map,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                        new ExpectedCluster("(local)", "nomatch*", "successful", null),
                        new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "employees,employees2", "successful", null)
                    )
                );
            };

            final Response response = performRequestWithRemoteSearchUser(request);
            assertOK(response);
            verifier.accept(performRequestWithRemoteSearchUser(request));
            verifier.accept(performRequestWithRemoteSearchUserViaAPIKey(request, remoteSearchUserAPIKey));
        }

        // missing concrete local index is an error
        {
            String q = "FROM employees_nomatch,my_remote_cluster:employees";

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit1));
            assertThat(e.getMessage(), containsString("Unknown index [employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [employees_nomatch]"));

            Request limit0 = esqlRequest(q + " | LIMIT 0");
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit0));
            assertThat(e.getMessage(), containsString("Unknown index [employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit0, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [employees_nomatch]"));
        }

        // missing concrete remote index is fatal error when skip_unavailable=false
        {
            String q = "FROM employees,my_remote_cluster:employees_nomatch";

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit1));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));

            Request limit0 = esqlRequest(q + " | LIMIT 0");
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit0));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit0, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
        }

        // since there is at least one matching index in the query, the missing wildcarded local index is not an error
        {
            String q = "FROM employees_nomatch*,my_remote_cluster:employees";

            CheckedBiConsumer<Response, Boolean, Exception> verifier = (response, limit0) -> {
                assertOK(response);
                Map<String, Object> map = responseAsMap(response);
                assertThat(((List<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
                if (limit0) {
                    assertThat(((List<?>) map.get("values")).size(), equalTo(0));
                } else {
                    assertThat(((List<?>) map.get("values")).size(), greaterThanOrEqualTo(1));
                }
                assertExpectedClustersForMissingIndicesTests(
                    map,
                    List.of(
                        // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                        new ExpectedCluster("(local)", "employees_nomatch*", "successful", 0),
                        new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "employees", "successful", limit0 ? 0 : null)
                    )
                );
            };

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            verifier.accept(performRequestWithRemoteSearchUser(limit1), false);
            verifier.accept(performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey), false);

            Request limit0 = esqlRequest(q + " | LIMIT 0");
            verifier.accept(performRequestWithRemoteSearchUser(limit0), true);
            verifier.accept(performRequestWithRemoteSearchUserViaAPIKey(limit0, remoteSearchUserAPIKey), true);
        }

        // an error is thrown if there are no matching indices at all
        {
            // with non-matching concrete index
            String q = "FROM my_remote_cluster:employees_nomatch";

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit1));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));

            Request limit0 = esqlRequest(q + " | LIMIT 0");
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit0));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit0, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
        }

        // an error is thrown if there are no matching indices at all
        {
            String localExpr = randomFrom("nomatch", "nomatch*");
            String remoteExpr = randomFrom("nomatch", "nomatch*");
            String q = Strings.format("FROM %s,%s:%s", localExpr, REMOTE_CLUSTER_ALIAS, remoteExpr);

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit1));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));

            Request limit0 = esqlRequest(q + " | LIMIT 0");
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit0));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));
            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit0, remoteSearchUserAPIKey));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));
        }

        // error since the remote cluster specified a concrete index that is not found
        {
            String q = "FROM employees,my_remote_cluster:employees_nomatch,my_remote_cluster:employees*";

            Request limit1 = esqlRequest(q + " | LIMIT 1");
            ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit1));
            /* Example error:
             *{"error":{"root_cause":[{"type":"security_exception","reason":"action [indices:data/read/esql/cluster] towards
             * remote cluster is unauthorized for user [remote_search_user] with assigned roles [remote_search] authenticated by
             * API key id [zaeMK5MBeGk5jCIiFtqB] of user [test_user] on indices [employees_nomatch], this action is granted by
             * the index privileges [read,all]"}],"type":"security_exception","reason":"action [indices:data/read/esql/cluster]
             * towards remote cluster is unauthorized for user [remote_search_user] with assigned roles [remote_search] authenticated
             * by API key id [zaeMK5MBeGk5jCIiFtqB] of user [test_user] on indices [employees_nomatch], this action is granted by the
             * index privileges [read,all]"},"status":403}"
             */
            assertThat(e.getMessage(), containsString("unauthorized for user [remote_search_user]"));
            assertThat(e.getMessage(), containsString("on indices [employees_nomatch]"));
            assertThat(e.getMessage(), containsString("security_exception"));

            e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUserViaAPIKey(limit1, remoteSearchUserAPIKey));
            /* Example error:
             * {"error":{"root_cause":[{"type":"security_exception","reason":"action [indices:data/read/esql/cluster] towards
             * remote cluster is unauthorized for API key id [sxuSK5MBSfGSGj4YFLyv] of user [remote_search_user] authenticated by
             * API key id [cUiRK5MB5j18U5stsvQj] of user [test_user] on indices [employees_nomatch], this action is granted by
             * the index privileges [read,all]"}],"type":"security_exception","reason":"action [indices:data/read/esql/cluster]
             * towards remote cluster is unauthorized for API key id [sxuSK5MBSfGSGj4YFLyv] of user [remote_search_user] authenticated
             * by API key id [cUiRK5MB5j18U5stsvQj] of user [test_user] on indices [employees_nomatch], this action is granted by the
             * index privileges [read,all]"},"status":403}"
             */
            assertThat(e.getMessage(), containsString("unauthorized for API key id"));
            assertThat(e.getMessage(), containsString("of user [remote_search_user]"));
            assertThat(e.getMessage(), containsString("on indices [employees_nomatch]"));
            assertThat(e.getMessage(), containsString("security_exception"));

            // TODO: in follow on PR, add support for throwing a VerificationException for this scenario - no exception is currently thrown
            // Request limit0 = esqlRequest(q + " | LIMIT 0");
            // e = expectThrows(ResponseException.class, () -> performRequestWithRemoteSearchUser(limit0));
            // assertThat(e.getMessage(), containsString("Unknown index [my_remote_cluster:employees_nomatch]"));
        }
    }

    public void testCrossClusterAsyncQuery() throws Exception {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        configureRemoteCluster();
        populateData();
        String otherUser = populateOtherUser();

        // Adding a delay there so that the async query is not completed before we check the status
        Request request = esqlRequestAsync("""
            FROM employees, *:employees
            | SORT emp_id ASC
            | LIMIT 10
            | WHERE delay(10ms)
            | KEEP emp_id, department""");
        Response response = performRequestWithRemoteSearchUser(request);
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assumeTrue("Query finished too fast, can not test", (boolean) responseAsMap.get("is_running"));

        String asyncId = (String) responseAsMap.get("id");
        response = performRequestWithRemoteSearchUser(esqlAsyncGetRequest(asyncId));
        assertOK(response);
        responseAsMap = entityAsMap(response);
        assertThat(responseAsMap.get("is_running"), equalTo(true));

        // Other user can't see the async query
        ResponseException error = expectThrows(
            ResponseException.class,
            () -> performRequestWithUser(esqlAsyncGetRequest(asyncId), otherUser)
        );
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(error.getMessage(), containsString("resource_not_found_exception"));

        // Clean up
        response = performRequestWithRemoteSearchUser(esqlAsyncDeleteRequest(asyncId));
        assertOK(response);
    }

    public void testCrossClusterAsyncQueryStop() throws Exception {
        assumeTrue("delay() is only available in snapshot builds", Build.current().isSnapshot());
        configureRemoteCluster();
        populateData();
        String otherUser = populateOtherUser();

        // query remote cluster only
        Request request = esqlRequestAsync("""
            FROM employees, *:employees
            | SORT emp_id ASC
            | LIMIT 10
            | WHERE delay(10ms)
            | KEEP emp_id, department""");
        Response response = performRequestWithRemoteSearchUser(request);
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assertThat(responseAsMap.get("is_running"), equalTo(true));
        String asyncId = (String) responseAsMap.get("id");

        response = performRequestWithRemoteSearchUser(esqlAsyncGetRequest(asyncId));
        assertOK(response);
        responseAsMap = entityAsMap(response);
        assertThat(responseAsMap.get("is_running"), equalTo(true));

        // Other user can't see the async query
        ResponseException error = expectThrows(
            ResponseException.class,
            () -> performRequestWithUser(esqlAsyncStopRequest(asyncId), otherUser)
        );
        assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        assertThat(error.getMessage(), containsString("resource_not_found_exception"));

        response = performRequestWithRemoteSearchUser(esqlAsyncStopRequest(asyncId));
        assertOK(response);
        responseAsMap = entityAsMap(response);
        assertThat(responseAsMap.get("is_running"), equalTo(false));

        // Clean up
        response = performRequestWithRemoteSearchUser(esqlAsyncDeleteRequest(asyncId));
        assertOK(response);
    }

    protected Request esqlRequest(String command) throws IOException {
        XContentBuilder body = getBody(command, null);
        Request request = new Request("POST", "_query");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));
        return request;
    }

    protected Request esqlRequestAsync(String command) throws IOException {
        XContentBuilder body = getBody(command, Map.of("wait_for_completion_timeout", "1ms"));
        Request request = new Request("POST", "_query/async");
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));
        return request;
    }

    protected Request esqlAsyncGetRequest(String asyncID) {
        Request request = new Request("GET", "_query/async/" + asyncID);
        request.addParameter("wait_for_completion_timeout", "1ms");
        return request;
    }

    protected Request esqlAsyncStopRequest(String asyncID) {
        Request request = new Request("POST", "_query/async/" + asyncID + "/stop");
        return request;
    }

    protected Request esqlAsyncDeleteRequest(String asyncID) {
        Request request = new Request("DELETE", "_query/async/" + asyncID);
        return request;
    }

    private static XContentBuilder getBody(String command, @Nullable Map<String, String> extraParams) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();
        body.startObject();
        body.field("query", command);
        body.field("include_ccs_metadata", true);
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
        if (extraParams != null) {
            extraParams.forEach((name, value) -> {
                try {
                    body.field(name, value);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        }
        body.endObject();
        return body;
    }

    private Response performRequestWithRemoteSearchUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        return client().performRequest(request);
    }

    private Response performRequestWithUser(final Request request, String user) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(user, PASS)));
        return client().performRequest(request);
    }

    private Response performRequestWithRemoteSearchUserViaAPIKey(Request request, String encodedApiKey) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encodedApiKey));
        return client().performRequest(request);
    }

    private String createRemoteSearchUserAPIKey() throws IOException {
        final Request createApiKeyRequest = new Request("POST", "_security/api_key");
        createApiKeyRequest.setJsonEntity("""
            {
              "name": "myapikey"
            }""");
        // ensure that the API key is created with the correct user
        createApiKeyRequest.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_SEARCH_USER, PASS))
        );
        Response response = client().performRequest(createApiKeyRequest);
        assertOK(response);
        final Map<String, Object> responseAsMap = responseAsMap(response);
        final String encoded = (String) responseAsMap.get("encoded");
        return encoded;
    }

    @SuppressWarnings("unchecked")
    private void assertRemoteOnlyResults(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(2, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder("1", "3", "engineering", "sales"));
    }

    @SuppressWarnings("unchecked")
    private void assertRemoteOnlyAgainst2IndexResults(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(2, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder("1", "11", "engineering", "engineering"));
    }

    @SuppressWarnings("unchecked")
    private void assertLocalOnlyResultsAndSkippedRemote(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(4, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        // local results
        assertThat(flatList, containsInAnyOrder("2", "4", "6", "8", "support", "management", "engineering", "marketing"));
        Map<String, ?> clusters = (Map<String, ?>) responseAsMap.get("_clusters");

        /*
          clusters map:
          {running=0, total=2, details={
            invalid_remote={_shards={total=0, failed=0, successful=0, skipped=0}, took=176, indices=employees,
                            failures=[{reason={reason=Unable to connect to [invalid_remote], type=connect_transport_exception},
                            index=null, shard=-1}], status=skipped},
           (local)={_shards={total=1, failed=0, successful=1, skipped=0}, took=298, indices=employees, status=successful}},
                    failed=0, partial=0, successful=1, skipped=1}
         */

        assertThat((int) clusters.get("total"), equalTo(2));
        assertThat((int) clusters.get("successful"), equalTo(1));
        assertThat((int) clusters.get("skipped"), equalTo(1));

        Map<String, ?> details = (Map<String, ?>) clusters.get("details");
        Map<String, ?> invalidRemoteMap = (Map<String, ?>) details.get("invalid_remote");
        assertThat(invalidRemoteMap.get("status").toString(), equalTo("skipped"));
        List<?> failures = (List<?>) invalidRemoteMap.get("failures");
        assertThat(failures.size(), equalTo(1));
        Map<String, ?> failureMap = (Map<String, ?>) failures.get(0);
        Map<String, ?> reasonMap = (Map<String, ?>) failureMap.get("reason");
        assertThat(reasonMap.get("reason").toString(), containsString("Unable to connect to [invalid_remote]"));
        assertThat(reasonMap.get("type").toString(), containsString("connect_transport_exception"));

        Map<String, ?> localCluster = (Map<String, ?>) details.get("(local)");
        assertThat(localCluster.get("status").toString(), equalTo("successful"));
    }

    @SuppressWarnings("unchecked")
    private void assertRemoteAndLocalResults(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(9, values.size());
        List<String> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<String>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(
            flatList,
            containsInAnyOrder(
                "1",
                "2",
                "3",
                "4",
                "5",
                "6",
                "7",
                "8",
                "9",
                "engineering",
                "engineering",
                "engineering",
                "management",
                "sales",
                "sales",
                "marketing",
                "marketing",
                "support"
            )
        );
    }

    private void assertWithEnrich(Response response) throws IOException {
        assertOK(response);
        Map<String, Object> responseAsMap = entityAsMap(response);
        List<?> columns = (List<?>) responseAsMap.get("columns");
        List<?> values = (List<?>) responseAsMap.get("values");
        assertEquals(2, columns.size());
        assertEquals(2, values.size());
        List<?> flatList = values.stream()
            .flatMap(innerList -> innerList instanceof List ? ((List<?>) innerList).stream() : Stream.empty())
            .collect(Collectors.toList());
        assertThat(flatList, containsInAnyOrder(2, 3, "usa", "canada"));
    }

    record ExpectedCluster(String clusterAlias, String indexExpression, String status, Integer totalShards) {}

    @SuppressWarnings("unchecked")
    void assertExpectedClustersForMissingIndicesTests(Map<String, Object> responseMap, List<ExpectedCluster> expected) {
        Map<String, ?> clusters = (Map<String, ?>) responseMap.get("_clusters");
        assertThat((int) responseMap.get("took"), greaterThan(0));

        Map<String, ?> detailsMap = (Map<String, ?>) clusters.get("details");
        assertThat(detailsMap.size(), is(expected.size()));

        assertThat((int) clusters.get("total"), is(expected.size()));
        assertThat((int) clusters.get("successful"), is((int) expected.stream().filter(ec -> ec.status().equals("successful")).count()));
        assertThat((int) clusters.get("skipped"), is((int) expected.stream().filter(ec -> ec.status().equals("skipped")).count()));
        assertThat((int) clusters.get("failed"), is((int) expected.stream().filter(ec -> ec.status().equals("failed")).count()));

        for (ExpectedCluster expectedCluster : expected) {
            Map<String, ?> clusterDetails = (Map<String, ?>) detailsMap.get(expectedCluster.clusterAlias());
            String msg = expectedCluster.clusterAlias();

            assertThat(msg, (int) clusterDetails.get("took"), greaterThan(0));
            assertThat(msg, clusterDetails.get("status"), is(expectedCluster.status()));
            Map<String, ?> shards = (Map<String, ?>) clusterDetails.get("_shards");
            if (expectedCluster.totalShards() == null) {
                assertThat(msg, (int) shards.get("total"), greaterThan(0));
            } else {
                assertThat(msg, (int) shards.get("total"), is(expectedCluster.totalShards()));
            }

            if (expectedCluster.status().equals("successful")) {
                assertThat((int) shards.get("successful"), is((int) shards.get("total")));
                assertThat((int) shards.get("skipped"), is(0));

            } else if (expectedCluster.status().equals("skipped")) {
                assertThat((int) shards.get("successful"), is(0));
                assertThat((int) shards.get("skipped"), is((int) shards.get("total")));
                ArrayList<?> failures = (ArrayList<?>) clusterDetails.get("failures");
                assertThat(failures.size(), is(1));
                Map<String, ?> failure1 = (Map<String, ?>) failures.get(0);
                Map<String, ?> innerReason = (Map<String, ?>) failure1.get("reason");
                String expectedMsg = "Unknown index [" + expectedCluster.indexExpression() + "]";
                assertThat(innerReason.get("reason").toString(), containsString(expectedMsg));
                assertThat(innerReason.get("type").toString(), containsString("verification_exception"));

            } else {
                fail(msg + "; Unexpected status: " + expectedCluster.status());
            }
            // currently failed shards is always zero - change this once we start allowing partial data for individual shard failures
            assertThat((int) shards.get("failed"), is(0));
        }
    }

    private static Map<String, Object> fetchEsqlCcsFeatureUsageFromNode(RestClient client) throws IOException {
        Request request = new Request(HttpGet.METHOD_NAME, "_license/feature_usage");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(USER, PASS)));
        Response response = client.performRequest(request);
        ObjectPath path = ObjectPath.createFromResponse(response);
        List<Map<String, Object>> features = path.evaluate("features");
        for (var feature : features) {
            if ("esql-ccs".equals(feature.get("name"))) {
                return feature;
            }
        }
        return Collections.emptyMap();
    }
}
