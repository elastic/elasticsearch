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
import java.util.function.Function;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasItemInArray;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;

public class CrossClusterEsqlRCS1EnrichUnavailableRemotesIT extends AbstractRemoteClusterSecurityTestCase {
    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(1)
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
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    private final String[] modes = { "_coordinator", "_remote" };

    // These are the IDs used in the test data in setSourceData().
    private final Integer[] employeeIDs = { 1, 2, 3, 4, 5, 6 };

    @Before
    public void setupPreRequisites() throws IOException {
        setupRolesAndPrivileges();
        setSourceData();

        var policy = createPolicy("employees-policy", "employees", "email", new String[] { "id", "designation" });
        // Create the enrich policy on both clusters.
        assertOK(client().performRequest(policy));
        assertOK(performRequestAgainstFulfillingCluster(policy));

        // Execute the enrich policy on both clusters.
        var exec = executePolicy("employees-policy");
        assertOK(client().performRequest(exec));
        assertOK(performRequestAgainstFulfillingCluster(exec));
    }

    public void testEsqlEnrichWithSkipUnavailable() throws Exception {
        esqlEnrichWithRandomSkipUnavailable();
        esqlEnrichWithSkipUnavailableTrue();
        esqlEnrichWithSkipUnavailableFalse();
    }

    private void esqlEnrichWithRandomSkipUnavailable() throws Exception {
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), randomBoolean());

        String query = "FROM to-be-enr*,my_remote_cluster:to-be-enr* | ENRICH " + randomFrom(modes) + ":employees-policy | LIMIT 10";
        Response response = client().performRequest(esqlRequest(query));

        Map<String, Object> map = responseAsMap(response);
        ArrayList<?> values = (ArrayList<?>) map.get("values");
        Map<?, ?> clusters = (Map<?, ?>) map.get("_clusters");
        Map<?, ?> clusterDetails = (Map<?, ?>) clusters.get("details");
        Map<?, ?> localClusterDetails = (Map<?, ?>) clusterDetails.get("(local)");
        Map<?, ?> remoteClusterDetails = (Map<?, ?>) clusterDetails.get("my_remote_cluster");

        Function<String, String> info = (msg) -> "test: esqlEnrichWithRandomSkipUnavailable: " + msg;

        assertOK(response);
        assertThat(info.apply("overall took"), (int) map.get("took"), greaterThan(0));
        assertThat(info.apply("overall num values"), values.size(), is(6));
        for (int i = 0; i < 6; i++) {
            ArrayList<?> value = (ArrayList<?>) values.get(i);
            // Size is 3: ID, Email, Designation.
            assertThat(info.apply("should be id, email, designation, so size 3"), value.size(), is(3));
            // Email
            assertThat(info.apply("email was: " + value.get(0)), (String) value.get(0), endsWith("@corp.co"));
            // ID
            assertThat(info.apply("id"), employeeIDs, hasItemInArray((int) value.get(1)));
        }

        assertThat(info.apply("total clusters"), (int) clusters.get("total"), is(2));
        assertThat(info.apply("successful clusters"), (int) clusters.get("successful"), is(2));
        assertThat(info.apply("running clusters"), (int) clusters.get("running"), is(0));
        assertThat(info.apply("skipped clusters"), (int) clusters.get("skipped"), is(0));
        assertThat(info.apply("partial clusters"), (int) clusters.get("partial"), is(0));
        assertThat(info.apply("failed clusters"), (int) clusters.get("failed"), is(0));

        assertThat(clusterDetails.size(), is(2));
        assertThat(info.apply("local cluster took"), (int) localClusterDetails.get("took"), greaterThan(0));
        assertThat(info.apply("local cluster status"), localClusterDetails.get("status"), is("successful"));

        assertThat(info.apply("remote cluster took"), (int) remoteClusterDetails.get("took"), greaterThan(0));
        assertThat(info.apply("remote cluster status"), remoteClusterDetails.get("status"), is("successful"));
    }

    @SuppressWarnings("unchecked")
    private void esqlEnrichWithSkipUnavailableTrue() throws Exception {
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), true);

        try {
            fulfillingCluster.stop(true);

            String query = "FROM to-be-enriched,my_remote_cluster:to-be-enriched | ENRICH employees-policy | LIMIT 10";
            Response response = client().performRequest(esqlRequest(query));

            Map<String, Object> map = responseAsMap(response);
            ArrayList<?> values = (ArrayList<?>) map.get("values");
            Map<?, ?> clusters = (Map<?, ?>) map.get("_clusters");
            Map<?, ?> clusterDetails = (Map<?, ?>) clusters.get("details");
            Map<?, ?> localClusterDetails = (Map<?, ?>) clusterDetails.get("(local)");
            Map<?, ?> remoteClusterDetails = (Map<?, ?>) clusterDetails.get("my_remote_cluster");

            Function<String, String> info = (msg) -> "test: esqlEnrichWithSkipUnavailableTrue: " + msg;

            assertOK(response);
            assertThat(info.apply("overall took"), (int) map.get("took"), greaterThan(0));
            assertThat(info.apply("overall values.size"), values.size(), is(3));

            // We only have 3 values since the remote cluster is turned off.
            for (int i = 0; i < 3; i++) {
                ArrayList<?> value = (ArrayList<?>) values.get(i);
                // Size is 3: ID, Email, Designation.
                assertThat(info.apply("should be id, email, designation but had size: "), value.size(), is(3));
                // Email
                assertThat(info.apply("email was: " + value.get(0)), (String) value.get(0), endsWith("@corp.co"));
                // ID
                assertThat(info.apply("id"), employeeIDs, hasItemInArray((int) value.get(1)));
            }

            assertThat(info.apply("total clusters"), (int) clusters.get("total"), is(2));
            assertThat(info.apply("successful clusters"), (int) clusters.get("successful"), is(1));
            assertThat(info.apply("running clusters"), (int) clusters.get("running"), is(0));
            assertThat(info.apply("skipped clusters"), (int) clusters.get("skipped"), is(1));
            assertThat(info.apply("partial clusters"), (int) clusters.get("partial"), is(0));
            assertThat(info.apply("failed clusters"), (int) clusters.get("failed"), is(0));

            assertThat(info.apply("cluster details size"), clusterDetails.size(), is(2));
            assertThat(info.apply("local cluster took"), (int) localClusterDetails.get("took"), greaterThan(0));
            assertThat(info.apply("local cluster status"), localClusterDetails.get("status"), is("successful"));

            assertThat(info.apply("remote cluster took"), (int) remoteClusterDetails.get("took"), greaterThan(0));
            assertThat(info.apply("remote cluster status"), remoteClusterDetails.get("status"), is("skipped"));

            ArrayList<?> remoteClusterFailures = (ArrayList<?>) remoteClusterDetails.get("failures");
            assertThat(info.apply("remote cluster failure count"), remoteClusterFailures.size(), equalTo(1));
            Map<String, ?> failuresMap = (Map<String, ?>) remoteClusterFailures.get(0);

            Map<String, ?> reason = (Map<String, ?>) failuresMap.get("reason");
            assertThat(
                info.apply("unexpected failure reason: " + reason),
                reason.get("type").toString(),
                oneOf("node_disconnected_exception", "connect_transport_exception", "node_not_connected_exception")
            );

        } finally {
            fulfillingCluster.start();
            closeFulfillingClusterClient();
            initFulfillingClusterClient();
        }
    }

    private void esqlEnrichWithSkipUnavailableFalse() throws Exception {
        configureRemoteCluster("my_remote_cluster", fulfillingCluster, true, randomBoolean(), false);

        try {
            fulfillingCluster.stop(true);

            String query = "FROM to-be-enr*,my_remote_cluster:to-be-enr* | ENRICH " + randomFrom(modes) + ":employees-policy | LIMIT 10";
            ResponseException ex = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(query)));

            assertThat(
                "esqlEnrichWithSkipUnavailableFalse failure",
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
                  "names": ["*"],
                  "privileges": ["read"]
                }
              ],
              "cluster": [ "monitor_enrich", "manage_own_api_key" ],
              "remote_indices": [
                {
                  "names": ["*"],
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
        assertOK(adminClient().performRequest(putRoleOnRemoteClusterRequest));
    }

    private void setSourceData() throws IOException {
        Request createIndex = new Request("PUT", "employees");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                      "id": { "type": "integer" },
                      "email": { "type": "text" },
                      "designation": { "type": "text" }
                    }
                }
            }
            """);
        assertOK(client().performRequest(createIndex));
        assertOK(performRequestAgainstFulfillingCluster(createIndex));

        Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "employees" } }
            { "id": 1, "email": "a@corp.co", "designation": "SDE intern"}
            { "index": { "_index": "employees" } }
            { "id": 2, "email": "b@corp.co", "designation": "SDE 1"}
            { "index": { "_index": "employees" } }
            { "id": 3, "email": "c@corp.co", "designation": "SDE 2"}
            { "index": { "_index": "employees" } }
            { "id": 4, "email": "d@corp.co", "designation": "SSE"}
            { "index": { "_index": "employees" } }
            { "id": 5, "email": "e@corp.co", "designation": "PSE 1"}
            { "index": { "_index": "employees" } }
            { "id": 6, "email": "f@corp.co", "designation": "PSE 2"}
            """);
        assertOK(client().performRequest(bulkRequest));
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));

        createIndex = new Request("PUT", "to-be-enriched");
        createIndex.setJsonEntity("""
            {
                "mappings": {
                    "properties": {
                      "email": { "type": "text" }
                    }
                }
            }
            """);
        assertOK(client().performRequest(createIndex));
        assertOK(performRequestAgainstFulfillingCluster(createIndex));

        bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "to-be-enriched" } }
            { "email": "a@corp.co"}
            { "index": { "_index": "to-be-enriched" } }
            { "email": "b@corp.co"}
            { "index": { "_index": "to-be-enriched" } }
            { "email": "c@corp.co"}
            """);
        assertOK(client().performRequest(bulkRequest));

        bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": { "_index": "to-be-enriched" } }
            { "email": "d@corp.co"}
            { "index": { "_index": "to-be-enriched" } }
            { "email": "e@corp.co"}
            { "index": { "_index": "to-be-enriched" } }
            { "email": "f@corp.co"}
            """);
        assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
    }

    private Request createPolicy(String policyName, String matchIndex, String matchField, String[] enrichFields) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();
        body.startObject();
        body.startObject("match");
        body.field("indices", matchIndex);
        body.field("match_field", matchField);
        body.field("enrich_fields", enrichFields);

        body.endObject();
        body.endObject();

        return makeRequest("PUT", "_enrich/policy/" + policyName, body);
    }

    private Request executePolicy(String policyName) {
        return new Request("PUT", "_enrich/policy/employees-policy/_execute");
    }

    private Request esqlRequest(String query) throws IOException {
        XContentBuilder body = JsonXContent.contentBuilder();

        body.startObject();
        body.field("query", query);
        body.field("include_ccs_metadata", true);
        body.endObject();

        return makeRequest("POST", "_query", body);
    }

    private Request makeRequest(String method, String endpoint, XContentBuilder requestBody) {
        Request request = new Request(method, endpoint);
        request.setJsonEntity(Strings.toString(requestBody));
        return request;
    }
}
