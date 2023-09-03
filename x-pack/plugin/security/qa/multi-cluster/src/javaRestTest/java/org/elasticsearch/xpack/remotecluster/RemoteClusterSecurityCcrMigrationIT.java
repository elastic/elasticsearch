/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.AnnotationTestOrdering;
import org.elasticsearch.test.AnnotationTestOrdering.Order;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@TestCaseOrdering(AnnotationTestOrdering.class)
public class RemoteClusterSecurityCcrMigrationIT extends AbstractRemoteClusterSecurityTestCase {

    private static final String CCR_USER = "ccr_user";
    private static final String CCR_USER_ROLE = "ccr_user_role";
    private static final AtomicInteger DOC_COUNTER = new AtomicInteger(0);
    private static final MutableSettingsProvider keystoreSettings = new MutableSettingsProvider();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
            .module("x-pack-ccr")
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
            .module("x-pack-ccr")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore(keystoreSettings)
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @Order(10)
    public void testInitialSetup() throws IOException {
        // For manual follow
        indexDocsToLeaderCluster("leader-index", 2);

        // Create ccr_user on follower cluster
        final Request putUserRequest = new Request("PUT", "/_security/user/" + CCR_USER);
        putUserRequest.setJsonEntity(Strings.format("""
            {"password":"%s","roles":["%s"]}
            """, PASS, CCR_USER_ROLE));
        assertOK(performRequestWithAdminUser(putUserRequest));
    }

    @Order(20)
    public void testRcs1Setup() throws Exception {
        // Create role on leader cluster
        final Request putRoleRequest = new Request("POST", "/_security/role/" + CCR_USER_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": [ "read_ccr" ],
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": [ "manage", "read" ]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        // Create role on follower cluster
        putRoleRequest.setJsonEntity("""
            {
              "cluster": [ "manage_ccr", "monitor" ],
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": [ "monitor", "read", "write", "manage_follow_index" ]
                }
              ]
            }""");
        performRequestWithAdminUser(putRoleRequest);

        // Configure the remote cluster using RCS 1.0
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());

        // Create manual follow
        final String followIndexName = "follower-index";
        final Request putCcrRequest = new Request("PUT", "/" + followIndexName + "/_ccr/follow?wait_for_active_shards=1");
        putCcrRequest.setJsonEntity("""
            {
              "remote_cluster": "my_remote_cluster",
              "leader_index": "leader-index"
            }""");
        final Response putCcrResponse = performRequestWithCcrUser(putCcrRequest);
        assertOK(putCcrResponse);
        responseAsMap(putCcrResponse).forEach((k, v) -> assertThat(k, v, is(true)));

        // Ensure data is replicated
        verifyReplicatedDocuments(2L, followIndexName);
        assertFollowerInfo(followIndexName, "my_remote_cluster", "leader-index", "active");
        assertFollowerStats(followIndexName);

        // Create auto follow
        final var putAllowFollowRequest = new Request("PUT", "/_ccr/auto_follow/my_auto_follow_pattern");
        putAllowFollowRequest.setJsonEntity("""
            {
              "remote_cluster" : "my_remote_cluster",
              "leader_index_patterns" : [ "metrics-*" ]
            }""");
        final Response putAutoFollowResponse = performRequestWithCcrUser(putAllowFollowRequest);
        assertOK(putAutoFollowResponse);

        // Auto follow should work
        indexDocsToLeaderCluster("metrics-000", 2);
        indexDocsToLeaderCluster("metrics-001", 1);
        verifyIndicesExists("metrics-000", "metrics-001");
        verifyReplicatedDocuments(3L, "metrics-000", "metrics-001");
    }

    // First migrate to RCS 2.0
    @Order(30)
    public void testFollowerClusterRestartForRcs2() throws IOException {
        // Update the ccr_user_role so that it is sufficient for both RCS 1.0 and 2.0
        final Request putRoleRequest = new Request("POST", "/_security/role/" + CCR_USER_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": [ "manage_ccr", "monitor" ],
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": [ "monitor", "read", "write", "manage_follow_index" ]
                }
              ],
              "remote_indices": [
                {
                  "clusters": [ "*" ],
                  "names": [ "*" ],
                  "privileges": [ "cross_cluster_replication" ]
                }
              ]
            }""");
        performRequestWithAdminUser(putRoleRequest);

        // Remove the RCS 1.0 remote cluster
        removeRemoteCluster();

        // Index 2 more docs to leader index (total 4)
        indexDocsToLeaderCluster("leader-index", 2);
        // Index more docs and create new index for auto follow (total 3 indices, 5 docs)
        indexDocsToLeaderCluster("metrics-001", 1);
        indexDocsToLeaderCluster("metrics-002", 1);

        // Create cross-cluster API key, add it to the key store and restart follower cluster
        final Map<String, Object> crossClusterAccessApiKey = createCrossClusterAccessApiKey("""
            {
              "replication": [
                {
                   "names": ["leader-index", "metrics-*"]
                }
              ]
            }""");
        keystoreSettings.put("cluster.remote.my_remote_cluster.credentials", (String) crossClusterAccessApiKey.get("encoded"));
        queryCluster.restart(false);
        closeClients();
    }

    @Order(40)
    public void testRcs2Setup() throws Exception {
        // Configure a new remote cluster using RCS 2.0
        configureRemoteCluster("my_remote_cluster");

        final String followIndexName = "follower-index";
        verifyReplicatedDocuments(4L, followIndexName);
        assertFollowerInfo(followIndexName, "my_remote_cluster", "leader-index", "active");
        assertFollowerStats(followIndexName);

        verifyIndicesExists("metrics-000", "metrics-001", "metrics-002");
        verifyReplicatedDocuments(5L, "metrics-000", "metrics-001", "metrics-002");

        // Leader cluster no longer needs the ccr_user_role. It can be removed (optional)
        if (randomBoolean()) {
            final Request deleteRoleRequest = new Request("DELETE", "/_security/role/" + CCR_USER_ROLE);
            assertOK(performRequestAgainstFulfillingCluster(deleteRoleRequest));
        }

        // Index 2 more docs to leader index (total 6)
        indexDocsToLeaderCluster("leader-index", 2);
        verifyReplicatedDocuments(6L, followIndexName);

        // More for auto follow (total 4 indices, 7 docs)
        indexDocsToLeaderCluster("metrics-002", 1);
        indexDocsToLeaderCluster("metrics-003", 1);
        verifyIndicesExists("metrics-000", "metrics-001", "metrics-002", "metrics-003");
        verifyReplicatedDocuments(7L, "metrics-000", "metrics-001", "metrics-002", "metrics-003");
    }

    // Second migrate back to RCS 1.0
    @Order(50)
    public void testFollowerClusterRestartAgainForRcs1() throws IOException {
        // Remove the RCS 2.0 remote cluster
        removeRemoteCluster();

        // Optionally update the role on follower cluster to remove its remote_indices privileges
        if (randomBoolean()) {
            final Request putRoleRequest = new Request("POST", "/_security/role/" + CCR_USER_ROLE);
            putRoleRequest.setJsonEntity("""
                {
                  "cluster": [ "manage_ccr", "monitor" ],
                  "indices": [
                    {
                      "names": [ "*" ],
                      "privileges": [ "monitor", "read", "write", "manage_follow_index" ]
                    }
                  ]
                }""");
            performRequestWithAdminUser(putRoleRequest);
        }

        // Index 2 more docs to leader index (total 8)
        indexDocsToLeaderCluster("leader-index", 2);
        // Index more docs and create new index for auto follow (total 5 indices, 9 docs)
        indexDocsToLeaderCluster("metrics-003", 1);
        indexDocsToLeaderCluster("metrics-004", 1);

        // Remove remote cluster credentials to revert back to RCS 1.0
        keystoreSettings.remove("cluster.remote.my_remote_cluster.credentials");
        queryCluster.restart(false);
        closeClients();
    }

    @Order(60)
    public void testRcs1SetupAgain() throws Exception {
        // Create role on leader cluster (in case it was removed in previous step)
        final Request putRoleRequest = new Request("POST", "/_security/role/" + CCR_USER_ROLE);
        putRoleRequest.setJsonEntity("""
            {
              "cluster": [ "read_ccr" ],
              "indices": [
                {
                  "names": [ "*" ],
                  "privileges": [ "manage", "read" ]
                }
              ]
            }""");
        performRequestAgainstFulfillingCluster(putRoleRequest);

        // Configure the remote cluster using RCS 1.0
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());

        // Ensure data is replicated for manual follow
        final String followIndexName = "follower-index";
        verifyReplicatedDocuments(8L, followIndexName);
        assertFollowerInfo(followIndexName, "my_remote_cluster", "leader-index", "active");
        assertFollowerStats(followIndexName);
        // Replicated for auto-follow
        verifyIndicesExists("metrics-000", "metrics-001", "metrics-002", "metrics-003", "metrics-004");
        verifyReplicatedDocuments(9L, "metrics-000", "metrics-001", "metrics-002", "metrics-003", "metrics-004");

        // Index 2 more docs (total 10)
        indexDocsToLeaderCluster("leader-index", 2);
        verifyReplicatedDocuments(10L, followIndexName);

        // More for auto follow (total 6 indices, 11 docs)
        indexDocsToLeaderCluster("metrics-004", 1);
        indexDocsToLeaderCluster("metrics-005", 2);
        verifyIndicesExists("metrics-000", "metrics-001", "metrics-002", "metrics-003", "metrics-004", "metrics-005");
        verifyReplicatedDocuments(12L, "metrics-000", "metrics-001", "metrics-002", "metrics-003", "metrics-004", "metrics-005");
    }

    private void indexDocsToLeaderCluster(String indexName, int numberOfDocs) throws IOException {
        final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        final String payload = IntStream.range(0, numberOfDocs).mapToObj(i -> Strings.format("""
            { "index": { "_index": "%s" } }
            { "name": "doc-%s" }
            """, indexName, DOC_COUNTER.getAndIncrement())).collect(Collectors.joining());
        bulkRequest.setJsonEntity(payload);
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
    }

    private void removeRemoteCluster() throws IOException {
        updateClusterSettings(
            Settings.builder()
                .putNull("cluster.remote.my_remote_cluster.mode")
                .putNull("cluster.remote.my_remote_cluster.skip_unavailable")
                .putNull("cluster.remote.my_remote_cluster.proxy_address")
                .putNull("cluster.remote.my_remote_cluster.seeds")
                .build()
        );
    }

    private Response performRequestWithAdminUser(final Request request) throws IOException {
        return performRequestWithAdminUser(client(), request);
    }

    private Response performRequestWithCcrUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(CCR_USER, PASS)));
        return client().performRequest(request);
    }

    private void verifyIndicesExists(String... indices) throws Exception {
        assertBusy(() -> {
            ensureHealth(String.join(",", indices), request -> {
                request.addParameter("wait_for_status", "yellow");
                request.addParameter("wait_for_active_shards", String.valueOf(indices.length));
                request.addParameter("wait_for_no_relocating_shards", "true");
                request.addParameter("wait_for_no_initializing_shards", "true");
                request.addParameter("timeout", "5s");
                request.addParameter("level", "shards");
            });
        });
    }

    private void verifyReplicatedDocuments(long numberOfDocs, String... indices) throws Exception {
        final Request searchRequest = new Request("GET", "/" + arrayToCommaDelimitedString(indices) + "/_search?size=100");
        assertBusy(() -> {
            final Response response;
            try {
                response = performRequestWithCcrUser(searchRequest);
            } catch (ResponseException e) {
                throw new AssertionError(e);
            }
            assertOK(response);
            final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(numberOfDocs));
            assertThat(
                Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).collect(Collectors.toUnmodifiableSet()),
                equalTo(Set.of(indices))
            );
        }, 30, TimeUnit.SECONDS);
    }

    private void assertFollowerInfo(String followIndexName, String leaderClusterName, String leadIndexName, String status)
        throws IOException {
        final Response response = performRequestWithCcrUser(new Request("GET", "/" + followIndexName + "/_ccr/info"));
        assertOK(response);
        final List<Map<String, Object>> followerIndices = ObjectPath.createFromResponse(response).evaluate("follower_indices");
        assertThat(followerIndices, hasSize(1));

        final Map<String, Object> follower = followerIndices.get(0);
        assertThat(ObjectPath.evaluate(follower, "follower_index"), equalTo(followIndexName));
        assertThat(ObjectPath.evaluate(follower, "leader_index"), equalTo(leadIndexName));
        assertThat(ObjectPath.evaluate(follower, "remote_cluster"), equalTo(leaderClusterName));
        assertThat(ObjectPath.evaluate(follower, "status"), equalTo(status));
    }

    private void assertFollowerStats(String followIndexName) throws IOException {
        final Response response = performRequestWithCcrUser(new Request("GET", "/" + followIndexName + "/_ccr/stats"));
        assertOK(response);
        final List<Map<String, Object>> followerIndices = ObjectPath.createFromResponse(response).evaluate("indices");
        assertThat(followerIndices, hasSize(1));

        final Map<String, Object> follower = followerIndices.get(0);
        assertThat(ObjectPath.evaluate(follower, "index"), equalTo(followIndexName));
    }
}
