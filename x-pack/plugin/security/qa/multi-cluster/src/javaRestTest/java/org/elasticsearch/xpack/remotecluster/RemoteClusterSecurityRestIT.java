/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.Build;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.Streams;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.LogType;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.xcontent.ObjectPath;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class RemoteClusterSecurityRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicReference<Map<String, Object>> REST_API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicBoolean NODE2_RCS_SERVER_ENABLED = new AtomicBoolean();
    private static final AtomicInteger INVALID_SECRET_LENGTH = new AtomicInteger();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .distribution(DistributionType.DEFAULT)
            .name("fulfilling-cluster")
            .nodes(3)
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

    public void testTaskCancellation() throws Exception {
        assumeTrue("[error_query] is only available in snapshot builds", Build.current().isSnapshot());
        configureRemoteCluster();

        final String indexName = "index_fulfilling";
        final String roleName = "taskCancellationRoleName";
        final String userName = "taskCancellationUsername";
        String asyncSearchOpaqueId = "async-search-opaque-id-" + randomUUID();
        try {
            // create some index on the fulfilling cluster, to be searched from the querying cluster
            {
                Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
                bulkRequest.setJsonEntity(Strings.format("""
                    { "index": { "_index": "%s" } }
                    { "foo": "bar" }
                    """, indexName));
                assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
            }

            // Create user and role with privileges for remote indices
            var putRoleRequest = new Request("PUT", "/_security/role/" + roleName);
            putRoleRequest.setJsonEntity(Strings.format("""
                {
                  "description": "Role with privileges for remote index for the test of task cancellation.",
                  "remote_indices": [
                    {
                      "names": ["%s"],
                      "privileges": ["read", "read_cross_cluster"],
                      "clusters": ["my_remote_cluster"]
                    }
                  ]
                }""", indexName));
            assertOK(adminClient().performRequest(putRoleRequest));
            var putUserRequest = new Request("PUT", "/_security/user/" + userName);
            putUserRequest.setJsonEntity(Strings.format("""
                {
                  "password": "%s",
                  "roles" : ["%s"]
                }""", PASS, roleName));
            assertOK(adminClient().performRequest(putUserRequest));
            var submitAsyncSearchRequest = new Request(
                "POST",
                Strings.format(
                    "/%s:%s/_async_search?ccs_minimize_roundtrips=%s",
                    randomFrom("my_remote_cluster", "*", "my_remote_*"),
                    indexName,
                    randomBoolean()
                )
            );

            // submit a stalling remote async search
            submitAsyncSearchRequest.setJsonEntity("""
                {
                  "query": {
                    "error_query": {
                      "indices": [
                        {
                          "name": "*:*",
                          "error_type": "exception",
                          "stall_time_seconds": 10
                        }
                      ]
                    }
                  }
                }""");
            submitAsyncSearchRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder()
                    .addHeader("Authorization", headerFromRandomAuthMethod(userName, PASS))
                    .addHeader("X-Opaque-Id", asyncSearchOpaqueId)
            );
            Response submitAsyncSearchResponse = client().performRequest(submitAsyncSearchRequest);
            assertOK(submitAsyncSearchResponse);
            Map<String, Object> submitAsyncSearchResponseMap = XContentHelper.convertToMap(
                JsonXContent.jsonXContent,
                EntityUtils.toString(submitAsyncSearchResponse.getEntity()),
                false
            );
            assertThat(submitAsyncSearchResponseMap.get("is_running"), equalTo(true));
            String asyncSearchId = (String) submitAsyncSearchResponseMap.get("id");
            assertThat(asyncSearchId, notNullValue());
            // wait for the tasks to show up on the querying cluster
            assertBusy(() -> {
                try {
                    Response queryingClusterTasks = adminClient().performRequest(new Request("GET", "/_tasks"));
                    assertOK(queryingClusterTasks);
                    Map<String, Object> responseMap = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(queryingClusterTasks.getEntity()),
                        false
                    );
                    AtomicBoolean someTasks = new AtomicBoolean(false);
                    selectTasksWithOpaqueId(responseMap, asyncSearchOpaqueId, task -> {
                        // search tasks should not be cancelled at this point (but some transitory ones might be,
                        // e.g. for action "indices:admin/seq_no/global_checkpoint_sync")
                        if (task.get("action") instanceof String action && action.contains("indices:data/read/search")) {
                            assertThat(task.get("cancelled"), equalTo(false));
                            someTasks.set(true);
                        }
                    });
                    assertTrue(someTasks.get());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // wait for the tasks to show up on the fulfilling cluster
            assertBusy(() -> {
                try {
                    Response fulfillingClusterTasks = performRequestAgainstFulfillingCluster(new Request("GET", "/_tasks"));
                    assertOK(fulfillingClusterTasks);
                    Map<String, Object> responseMap = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(fulfillingClusterTasks.getEntity()),
                        false
                    );
                    AtomicBoolean someTasks = new AtomicBoolean(false);
                    selectTasksWithOpaqueId(responseMap, asyncSearchOpaqueId, task -> {
                        // search tasks should not be cancelled at this point (but some transitory ones might be,
                        // e.g. for action "indices:admin/seq_no/global_checkpoint_sync")
                        if (task.get("action") instanceof String action && action.contains("indices:data/read/search")) {
                            assertThat(task.get("cancelled"), equalTo(false));
                            someTasks.set(true);
                        }
                    });
                    assertTrue(someTasks.get());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
            // delete the stalling async search
            var deleteAsyncSearchRequest = new Request("DELETE", Strings.format("/_async_search/%s", asyncSearchId));
            deleteAsyncSearchRequest.setOptions(
                RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(userName, PASS))
            );
            assertOK(client().performRequest(deleteAsyncSearchRequest));
            // ensure any remaining tasks are all cancelled on the querying cluster
            {
                Response queryingClusterTasks = adminClient().performRequest(new Request("GET", "/_tasks"));
                assertOK(queryingClusterTasks);
                Map<String, Object> responseMap = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    EntityUtils.toString(queryingClusterTasks.getEntity()),
                    false
                );
                selectTasksWithOpaqueId(responseMap, asyncSearchOpaqueId, task -> assertThat(task.get("cancelled"), equalTo(true)));
            }
            // ensure any remaining tasks are all cancelled on the fulfilling cluster
            {
                Response fulfillingClusterTasks = performRequestAgainstFulfillingCluster(new Request("GET", "/_tasks"));
                assertOK(fulfillingClusterTasks);
                Map<String, Object> responseMap = XContentHelper.convertToMap(
                    JsonXContent.jsonXContent,
                    EntityUtils.toString(fulfillingClusterTasks.getEntity()),
                    false
                );
                selectTasksWithOpaqueId(responseMap, asyncSearchOpaqueId, task -> assertThat(task.get("cancelled"), equalTo(true)));
            }
        } finally {
            assertOK(adminClient().performRequest(new Request("DELETE", "/_security/user/" + userName)));
            assertOK(adminClient().performRequest(new Request("DELETE", "/_security/role/" + roleName)));
            assertOK(performRequestAgainstFulfillingCluster(new Request("DELETE", indexName)));
            // wait for search related tasks to finish on the query cluster
            assertBusy(() -> {
                try {
                    Response queryingClusterTasks = adminClient().performRequest(new Request("GET", "/_tasks"));
                    assertOK(queryingClusterTasks);
                    Map<String, Object> responseMap = XContentHelper.convertToMap(
                        JsonXContent.jsonXContent,
                        EntityUtils.toString(queryingClusterTasks.getEntity()),
                        false
                    );
                    selectTasksWithOpaqueId(responseMap, asyncSearchOpaqueId, task -> {
                        if (task.get("action") instanceof String action && action.contains("indices:data/read/search")) {
                            fail("there are still search tasks running");
                        }
                    });
                } catch (ResponseException e) {
                    fail(e.getMessage());
                }
            });
        }
    }

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
                { "name": "metric4" }
                """));
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
                  "description": "Role with privileges for remote and local indices.",
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
            final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(responseAsParser(response));
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

            // Check remote metric users can search metric documents from all FC nodes
            final var metricSearchRequest = new Request(
                "GET",
                String.format(Locale.ROOT, "/my_remote_cluster:*/_search?ccs_minimize_roundtrips=%s", randomBoolean())
            );
            final SearchResponse metricSearchResponse = SearchResponseUtils.parseSearchResponse(
                responseAsParser(performRequestWithRemoteMetricUser(metricSearchRequest))
            );
            try {
                assertThat(metricSearchResponse.getHits().getTotalHits().value(), equalTo(4L));
                assertThat(
                    Arrays.stream(metricSearchResponse.getHits().getHits()).map(SearchHit::getIndex).collect(Collectors.toSet()),
                    containsInAnyOrder("shared-metrics")
                );
            } finally {
                metricSearchResponse.decRef();
            }

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
                  "description": "Role with privileges for searching local only indices.",
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

            // Check that authentication fails if we use a non-existent API key (when skip_unavailable=false)
            boolean skipUnavailable = randomBoolean();
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
            if (skipUnavailable) {
                /*
                  when skip_unavailable=true, response should be something like:
                  {"took":1,"timed_out":false,"num_reduce_phases":0,"_shards":{"total":0,"successful":0,"skipped":0,"failed":0},
                   "_clusters":{"total":1,"successful":0,"skipped":1,"running":0,"partial":0,"failed":0,
                                "details":{"invalid_remote":{"status":"skipped","indices":"index1","timed_out":false,
                                "failures":[{"shard":-1,"index":null,"reason":{"type":"connect_transport_exception",
                                             "reason":"Unable to connect to [invalid_remote]"}}]}}},
                    "hits":{"total":{"value":0,"relation":"eq"},"max_score":null,"hits":[]}}
                 */
                Response invalidRemoteResponse = performRequestWithRemoteSearchUser(new Request("GET", "/invalid_remote:index1/_search"));
                assertThat(invalidRemoteResponse.getStatusLine().getStatusCode(), equalTo(200));
                String responseJson = EntityUtils.toString(invalidRemoteResponse.getEntity());
                assertThat(responseJson, containsString("\"status\":\"skipped\""));
                assertThat(responseJson, containsString("connect_transport_exception"));
            } else {
                final ResponseException exception4 = expectThrows(
                    ResponseException.class,
                    () -> performRequestWithRemoteSearchUser(new Request("GET", "/invalid_remote:index1/_search"))
                );
                assertThat(exception4.getResponse().getStatusLine().getStatusCode(), equalTo(401));
                assertThat(exception4.getMessage(), containsString("unable to find apikey"));
            }

            // check that REST API key is not supported by cross cluster access (when skip_unavailable=false)
            skipUnavailable = randomBoolean();
            updateClusterSettings(
                randomBoolean()
                    ? Settings.builder()
                        .put("cluster.remote.wrong_api_key_type.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .put("cluster.remote.wrong_api_key_type.skip_unavailable", Boolean.toString(skipUnavailable))
                        .build()
                    : Settings.builder()
                        .put("cluster.remote.wrong_api_key_type.mode", "proxy")
                        .put("cluster.remote.wrong_api_key_type.skip_unavailable", Boolean.toString(skipUnavailable))
                        .put("cluster.remote.wrong_api_key_type.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
            );
            if (skipUnavailable) {
                Response invalidRemoteResponse = performRequestWithRemoteSearchUser(new Request("GET", "/wrong_api_key_type:*/_search"));
                assertThat(invalidRemoteResponse.getStatusLine().getStatusCode(), equalTo(200));
                String responseJson = EntityUtils.toString(invalidRemoteResponse.getEntity());
                assertThat(responseJson, containsString("\"status\":\"skipped\""));
                assertThat(responseJson, containsString("connect_transport_exception"));
            } else {
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

            // Check invalid cross-cluster API key length is rejected (and gets security error when skip_unavailable=false)
            skipUnavailable = randomBoolean();
            updateClusterSettings(
                randomBoolean()
                    ? Settings.builder()
                        .put("cluster.remote.invalid_secret_length.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .put("cluster.remote.invalid_secret_length.skip_unavailable", Boolean.toString(skipUnavailable))
                        .build()
                    : Settings.builder()
                        .put("cluster.remote.invalid_secret_length.mode", "proxy")
                        .put("cluster.remote.invalid_secret_length.skip_unavailable", Boolean.toString(skipUnavailable))
                        .put("cluster.remote.invalid_secret_length.proxy_address", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                        .build()
            );
            if (skipUnavailable) {
                Response invalidRemoteResponse = performRequestWithRemoteSearchUser(new Request("GET", "/invalid_secret_length:*/_search"));
                assertThat(invalidRemoteResponse.getStatusLine().getStatusCode(), equalTo(200));
                String responseJson = EntityUtils.toString(invalidRemoteResponse.getEntity());
                assertThat(responseJson, containsString("\"status\":\"skipped\""));
                assertThat(responseJson, containsString("connect_transport_exception"));
            } else {
                final ResponseException exception6 = expectThrows(
                    ResponseException.class,
                    () -> performRequestWithRemoteSearchUser(new Request("GET", "/invalid_secret_length:*/_search"))
                );
                assertThat(exception6.getResponse().getStatusLine().getStatusCode(), equalTo(401));
                assertThat(exception6.getMessage(), containsString("invalid cross-cluster API key value"));
            }
        }
        assertNoRcs1DeprecationWarnings();
    }

    @SuppressWarnings("unchecked")
    public void testNodesInfo() throws IOException {
        final Request request = new Request("GET", "/_nodes/settings,transport,remote_cluster_server");
        final Response response = performRequestAgainstFulfillingCluster(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        assertThat(ObjectPath.eval("_nodes.total", responseMap), equalTo(3));
        final Map<String, Object> nodes = ObjectPath.eval("nodes", responseMap);
        int numberOfRemoteClusterServerNodes = 0;
        for (Map.Entry<String, Object> entry : nodes.entrySet()) {
            final Map<String, Object> node = (Map<String, Object>) entry.getValue();
            // remote cluster is not reported in transport profiles
            assertThat(ObjectPath.eval("transport.profiles", node), anEmptyMap());

            if (Boolean.parseBoolean(ObjectPath.eval("settings.remote_cluster_server.enabled", node))) {
                numberOfRemoteClusterServerNodes += 1;
                final List<String> boundAddresses = ObjectPath.eval("remote_cluster_server.bound_address", node);
                assertThat(boundAddresses, notNullValue());
                assertThat(boundAddresses, not(empty()));
                final String publishAddress = ObjectPath.eval("remote_cluster_server.publish_address", node);
                assertThat(publishAddress, notNullValue());
            } else {
                assertThat(ObjectPath.eval("remote_cluster_server", node), nullValue());
            }
        }
        assertThat(
            numberOfRemoteClusterServerNodes,
            equalTo(1 + (NODE1_RCS_SERVER_ENABLED.get() ? 1 : 0) + (NODE2_RCS_SERVER_ENABLED.get() ? 1 : 0))
        );
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

    @SuppressWarnings("unchecked")
    private static void selectTasksWithOpaqueId(
        Map<String, Object> tasksResponse,
        String opaqueId,
        Consumer<Map<String, Object>> taskConsumer
    ) {
        Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) tasksResponse.get("nodes");
        for (Map<String, Object> node : nodes.values()) {
            Map<String, Map<String, Object>> tasks = (Map<String, Map<String, Object>>) node.get("tasks");
            for (Map<String, Object> task : tasks.values()) {
                if (task.get("headers") != null) {
                    Map<String, Object> headers = (Map<String, Object>) task.get("headers");
                    if (opaqueId.equals(headers.get("X-Opaque-Id"))) {
                        taskConsumer.accept(task);
                    }
                }
            }
        }
    }

    private void assertNoRcs1DeprecationWarnings() throws IOException {
        for (int i = 0; i < queryCluster.getNumNodes(); i++) {
            try (InputStream log = queryCluster.getNodeLog(i, LogType.DEPRECATION)) {
                Streams.readAllLines(
                    log,
                    line -> assertThat(
                        line,
                        not(
                            containsString(
                                "The certificate-based security model is deprecated and will be removed in a future major version. "
                                    + "Migrate the remote cluster from the certificate-based to the API key-based security model."
                            )
                        )
                    )
                );
            }
        }
    }
}
