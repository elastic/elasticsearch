/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.greaterThan;

public class CrossClusterEsqlRCS1UnavailableRemotesIT extends AbstractRemoteClusterSecurityTestCase {
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(1)
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .node(0, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .module("x-pack-esql")
            .module("x-pack-enrich")
            .apply(commonClusterConfig)
            .setting("xpack.ml.enabled", "false")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", () -> String.valueOf(SSL_ENABLED_REF.get()))
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    @Before
    public void setupPreRequisites() throws IOException {
        setupRolesAndPrivileges();
        loadData();
    }

    public void testEsqlRcs1UnavailableRemoteScenarios() throws Exception {
        clusterShutDownWithRandomSkipUnavailable();
        remoteClusterShutdownWithSkipUnavailableTrue();
        remoteClusterShutdownWithSkipUnavailableFalse();
    }

    private void clusterShutDownWithRandomSkipUnavailable() throws Exception {
        // skip_unavailable is set to a random boolean value.
        // However, no clusters are stopped. Hence, we do not expect any other behaviour
        // other than a 200-OK.

        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());
        String query = "FROM *,my_remote_cluster:* | LIMIT 10";
        Response response = client().performRequest(esqlRequest(query));

        Map<String, Object> map = responseAsMap(response);
        ArrayList<?> columns = (ArrayList<?>) map.get("columns");
        ArrayList<?> values = (ArrayList<?>) map.get("values");
        Map<?, ?> clusters = (Map<?, ?>) map.get("_clusters");
        Map<?, ?> clusterDetails = (Map<?, ?>) clusters.get("details");
        Map<?, ?> localClusterDetails = (Map<?, ?>) clusterDetails.get("(local)");
        Map<?, ?> remoteClusterDetails = (Map<?, ?>) clusterDetails.get("my_remote_cluster");

        assertOK(response);
        assertThat((int) map.get("took"), greaterThan(0));
        assertThat(columns.size(), is(4));
        assertThat(values.size(), is(9));
        assertThat((boolean) map.get("is_partial"), is(false));

        assertThat((int) clusters.get("total"), is(2));
        assertThat((int) clusters.get("successful"), is(2));
        assertThat((int) clusters.get("running"), is(0));
        assertThat((int) clusters.get("skipped"), is(0));
        assertThat((int) clusters.get("partial"), is(0));
        assertThat((int) clusters.get("failed"), is(0));

        assertThat(clusterDetails.size(), is(2));
        assertThat((int) localClusterDetails.get("took"), greaterThan(0));
        assertThat(localClusterDetails.get("status"), is("successful"));

        assertThat((int) remoteClusterDetails.get("took"), greaterThan(0));
        assertThat(remoteClusterDetails.get("status"), is("successful"));
    }

    @SuppressWarnings("unchecked")
    private void remoteClusterShutdownWithSkipUnavailableTrue() throws Exception {
        // Remote cluster is stopped and skip unavailable is set to true.
        // We expect no exception and partial results from the remaining open cluster.

        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), true);

        try {
            // Stop remote cluster.
            fulfillingCluster.stop(true);

            // A simple query that targets our remote cluster.
            String query = "FROM *,my_remote_cluster:* | LIMIT 10";
            Response response = client().performRequest(esqlRequest(query));

            Map<String, Object> map = responseAsMap(response);
            ArrayList<String> columns = (ArrayList<String>) map.get("columns");
            ArrayList<String> values = (ArrayList<String>) map.get("values");
            Map<String, ?> clusters = (Map<String, ?>) map.get("_clusters");
            Map<String, ?> clusterDetails = (Map<String, ?>) clusters.get("details");
            Map<String, ?> localClusterDetails = (Map<String, ?>) clusterDetails.get("(local)");
            Map<String, ?> remoteClusterDetails = (Map<String, ?>) clusterDetails.get("my_remote_cluster");

            // Assert results obtained from the local cluster and that remote cluster was
            // skipped.
            assertOK(response);
            assertThat((int) map.get("took"), greaterThan(0));

            assertThat(columns.size(), is(2));
            assertThat(values.size(), is(5));

            assertThat((int) clusters.get("total"), is(2));
            assertThat((int) clusters.get("successful"), is(1));
            assertThat((int) clusters.get("skipped"), is(1));
            assertThat((int) clusters.get("running"), is(0));
            assertThat((int) clusters.get("partial"), is(0));
            assertThat((int) clusters.get("failed"), is(0));

            assertThat(clusterDetails.size(), is(2));
            assertThat((int) localClusterDetails.get("took"), greaterThan(0));
            assertThat(localClusterDetails.get("status"), is("successful"));

            assertThat((int) remoteClusterDetails.get("took"), greaterThan(0));
            assertThat(remoteClusterDetails.get("status"), is("skipped"));

        } catch (ResponseException r) {
            throw new AssertionError(r);
        } finally {
            fulfillingCluster.start();
            closeFulfillingClusterClient();
            initFulfillingClusterClient();
        }
    }

    private void remoteClusterShutdownWithSkipUnavailableFalse() throws Exception {
        // Remote cluster is stopped and skip_unavailable is set to false.
        // Although the other cluster is open, we expect an Exception.

        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), false);

        try {
            // Stop remote cluster.
            fulfillingCluster.stop(true);

            // A simple query that targets our remote cluster.
            String query = "FROM *,my_remote_cluster:* | LIMIT 10";
            ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(query)));
            assertThat(
                ex.getMessage(),
                anyOf(
                    containsString("connect_transport_exception"),
                    containsString("node_disconnected_exception"),
                    containsString("node_not_connected_exception")
                )
            );
        } finally {
            fulfillingCluster.start();
            closeFulfillingClusterClient();
            initFulfillingClusterClient();
        }
    }

    private void setupRolesAndPrivileges() throws IOException {
        var putUserRequest = new Request("PUT", "/_security/user/" + REMOTE_SEARCH_USER);
        putUserRequest.setJsonEntity("""
            {
              "password": "x-pack-test-password",
              "roles" : ["remote_search"]
            }""");
        assertOK(adminClient().performRequest(putUserRequest));

        var putRoleOnRemoteClusterRequest = new Request("PUT", "/_security/role/" + REMOTE_SEARCH_ROLE);
        putRoleOnRemoteClusterRequest.setJsonEntity("""
            {
              "indices": [
                {
                  "names": ["points", "squares"],
                  "privileges": ["read", "read_cross_cluster", "create_index", "monitor"]
                }
              ],
              "remote_indices": [
                {
                  "names": ["points", "squares"],
                  "privileges": ["read", "read_cross_cluster", "create_index", "monitor"],
                  "clusters": ["my_remote_cluster"]
                }
              ]
            }""");
        assertOK(adminClient().performRequest(putRoleOnRemoteClusterRequest));
    }

    private void loadData() throws IOException {
        Request createIndex = new Request("PUT", "points");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                      "id": { "type": "integer" },
                      "score": { "type": "integer" }
                    }
                }
            }
            """);
        assertOK(client().performRequest(createIndex));

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "points" } }
            { "id": 1, "score": 75}
            { "index": { "_index": "points" } }
            { "id": 2, "score": 125}
            { "index": { "_index": "points" } }
            { "id": 3, "score": 100}
            { "index": { "_index": "points" } }
            { "id": 4, "score": 50}
            { "index": { "_index": "points" } }
            { "id": 5, "score": 150}
            """);
        assertOK(client().performRequest(bulkRequest));

        createIndex = new Request("PUT", "squares");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                      "num": { "type": "integer" },
                      "square": { "type": "integer" }
                    }
                }
            }
            """);
        assertOK(performRequestAgainstFulfillingCluster(createIndex));

        bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": {"_index": "squares"}}
            { "num": 1, "square": 1 }
            { "index": {"_index": "squares"}}
            { "num": 2, "square": 4 }
            { "index": {"_index": "squares"}}
            { "num": 3, "square": 9 }
            { "index": {"_index": "squares"}}
            { "num": 4, "square": 16 }
            """);
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
    }

    private Request esqlRequest(String query) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();

        body.startObject();
        body.field("query", query);
        body.field("include_ccs_metadata", true);
        body.endObject();

        Request request = new Request("POST", "_query");
        request.setJsonEntity(Strings.toString(body));

        return request;
    }
}
