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
import org.elasticsearch.core.Tuple;
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
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.containsInAnyOrder;
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

    @After
    public void wipeData() throws Exception {
        CheckedConsumer<RestClient, IOException> wipe = client -> {
            performRequestWithAdminUser(client, new Request("DELETE", "/employees"));
            performRequestWithAdminUser(client, new Request("DELETE", "/employees2"));
            performRequestWithAdminUser(client, new Request("DELETE", "/employees3"));
            performRequestWithAdminUser(client, new Request("DELETE", "/_enrich/policy/countries"));
        };
        wipe.accept(fulfillingClusterClient);
        wipe.accept(client());
    }

    @SuppressWarnings("unchecked")
    public void testCrossClusterQuery() throws Exception {
        configureRemoteCluster();
        populateData();

        // query remote cluster only
        Request request = esqlRequest("""
            FROM my_remote_cluster:employees
            | SORT emp_id ASC
            | LIMIT 2
            | KEEP emp_id, department""");
        Response response = performRequestWithRemoteSearchUser(request);
        assertRemoteOnlyResults(response);

        // same as above but authenticate with API key
        response = performRequestWithRemoteSearchUserViaAPIKey(request);
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
        var q = "FROM invalid_remote:employees,employees |  SORT emp_id DESC | LIMIT 10";
        Response response = performRequestWithRemoteSearchUser(esqlRequest(q));
        assertLocalOnlyResults(response);

        // only calling an invalid remote should error
        ResponseException error = expectThrows(ResponseException.class, () -> {
            var q2 = "FROM invalid_remote:employees |  SORT emp_id DESC | LIMIT 10";
            performRequestWithRemoteSearchUser(esqlRequest(q2));
        });

        if (skipUnavailable == false) {
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(401));
            assertThat(error.getMessage(), containsString("unable to find apikey"));
        } else {
            assertThat(error.getResponse().getStatusLine().getStatusCode(), equalTo(500));
            assertThat(error.getMessage(), containsString("Unable to connect to [invalid_remote]"));
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
        configureRemoteCluster();
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
            response = performRequestWithRemoteSearchUserViaAPIKey(request);
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
            assertThat(error.getMessage(), containsString(" Unknown index [" + index + "]"));
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

    private Response performRequestWithRemoteSearchUserViaAPIKey(final Request request) throws IOException {
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
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", "ApiKey " + encoded));
        return client().performRequest(request);
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
    private void assertLocalOnlyResults(Response response) throws IOException {
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

}
