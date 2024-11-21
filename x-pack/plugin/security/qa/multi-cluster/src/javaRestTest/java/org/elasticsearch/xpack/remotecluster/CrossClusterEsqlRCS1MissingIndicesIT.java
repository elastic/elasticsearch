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
import org.hamcrest.Matchers;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests cross-cluster ES|QL queries under RCS1.0 security model for cases where index expressions do not match
 * to ensure handling of those matches the expected rules defined in EsqlSessionCrossClusterUtils.
 */
public class CrossClusterEsqlRCS1MissingIndicesIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicBoolean SSL_ENABLED_REF = new AtomicBoolean();

    static {
        // remote cluster
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

        // "local" cluster
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

    private static final String INDEX1 = "points"; // on local cluster only
    private static final String INDEX2 = "squares"; // on local and remote clusters

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

    @SuppressWarnings("unchecked")
    public void testSearchesAgainstNonMatchingIndicesWithSkipUnavailableTrue() throws Exception {
        setupRolesAndPrivileges();
        setupIndex();

        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), true);

        // missing concrete local index is an error
        {
            String q = Strings.format("FROM nomatch,%s:%s | STATS count(*)", REMOTE_CLUSTER_ALIAS, INDEX2);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString("Unknown index [nomatch]"));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), Matchers.containsString("Unknown index [nomatch]"));
        }

        // missing concrete remote index is not fatal when skip_unavailable=true (as long as an index matches on another cluster)
        {
            String q = Strings.format("FROM %s,%s:nomatch | STATS count(*)", INDEX1, REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            Response response = client().performRequest(esqlRequest(limit1));
            assertOK(response);

            Map<String, Object> map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), greaterThanOrEqualTo(1));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    new ExpectedCluster("(local)", INDEX1, "successful", null),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch", "skipped", 0)
                )
            );

            String limit0 = q + " | LIMIT 0";
            response = client().performRequest(esqlRequest(limit0));
            assertOK(response);

            map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), is(0));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    new ExpectedCluster("(local)", INDEX1, "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch", "skipped", 0)
                )
            );
        }

        // since there is at least one matching index in the query, the missing wildcarded local index is not an error
        {
            String q = Strings.format("FROM nomatch*,%s:%s", REMOTE_CLUSTER_ALIAS, INDEX2);

            String limit1 = q + " | LIMIT 1";
            Response response = client().performRequest(esqlRequest(limit1));
            assertOK(response);

            Map<String, Object> map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), greaterThanOrEqualTo(1));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                    new ExpectedCluster("(local)", "nomatch*", "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, INDEX2, "successful", null)
                )
            );

            String limit0 = q + " | LIMIT 0";
            response = client().performRequest(esqlRequest(limit0));
            assertOK(response);

            map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), is(0));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                    new ExpectedCluster("(local)", "nomatch*", "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, INDEX2, "successful", 0)
                )
            );
        }

        // since at least one index of the query matches on some cluster, a missing wildcarded index on skip_un=true is not an error
        {
            String q = Strings.format("FROM %s,%s:nomatch*", INDEX1, REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            Response response = client().performRequest(esqlRequest(limit1));
            assertOK(response);

            Map<String, Object> map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), greaterThanOrEqualTo(1));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    new ExpectedCluster("(local)", INDEX1, "successful", null),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch*", "skipped", 0)
                )
            );

            String limit0 = q + " | LIMIT 0";
            response = client().performRequest(esqlRequest(limit0));
            assertOK(response);

            map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), is(0));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    new ExpectedCluster("(local)", INDEX1, "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch*", "skipped", 0)
                )
            );
        }

        // an error is thrown if there are no matching indices at all, even when the cluster is skip_unavailable=true
        {
            // with non-matching concrete index
            String q = Strings.format("FROM %s:nomatch", REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));
        }

        // an error is thrown if there are no matching indices at all, even when the cluster is skip_unavailable=true and the
        // index was wildcarded
        {
            String q = Strings.format("FROM %s:nomatch*", REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch*]", REMOTE_CLUSTER_ALIAS)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch*]", REMOTE_CLUSTER_ALIAS)));
        }

        // an error is thrown if there are no matching indices at all
        {
            String localExpr = randomFrom("nomatch", "nomatch*");
            String remoteExpr = randomFrom("nomatch", "nomatch*");
            String q = Strings.format("FROM %s,%s:%s", localExpr, REMOTE_CLUSTER_ALIAS, remoteExpr);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));
        }

        // TODO uncomment and test in follow-on PR which does skip_unavailable handling at execution time
        // {
        // String q = Strings.format("FROM %s,%s:nomatch,%s:%s*", INDEX1, REMOTE_CLUSTER_ALIAS, REMOTE_CLUSTER_ALIAS, INDEX2);
        //
        // String limit1 = q + " | LIMIT 1";
        // Response response = client().performRequest(esqlRequest(limit1));
        // assertOK(response);
        //
        // Map<String, Object> map = responseAsMap(response);
        // assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
        // assertThat(((ArrayList<?>) map.get("values")).size(), greaterThanOrEqualTo(1));
        //
        // assertExpectedClustersForMissingIndicesTests(map,
        // List.of(
        // new ExpectedCluster("(local)", INDEX1, "successful", null),
        // new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch," + INDEX2 + "*", "skipped", 0)
        // )
        // );
        //
        // String limit0 = q + " | LIMIT 0";
        // response = client().performRequest(esqlRequest(limit0));
        // assertOK(response);
        //
        // map = responseAsMap(response);
        // assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
        // assertThat(((ArrayList<?>) map.get("values")).size(), is(0));
        //
        // assertExpectedClustersForMissingIndicesTests(map,
        // List.of(
        // new ExpectedCluster("(local)", INDEX1, "successful", 0),
        // new ExpectedCluster(REMOTE_CLUSTER_ALIAS, "nomatch," + INDEX2 + "*", "skipped", 0)
        // )
        // );
        // }
    }

    @SuppressWarnings("unchecked")
    public void testSearchesAgainstNonMatchingIndicesWithSkipUnavailableFalse() throws Exception {
        // Remote cluster is closed and skip_unavailable is set to false.
        // Although the other cluster is open, we expect an Exception.

        setupRolesAndPrivileges();
        setupIndex();

        configureRemoteCluster(REMOTE_CLUSTER_ALIAS, fulfillingCluster, true, randomBoolean(), false);

        // missing concrete local index is an error
        {
            String q = Strings.format("FROM nomatch,%s:%s | STATS count(*)", REMOTE_CLUSTER_ALIAS, INDEX2);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString("Unknown index [nomatch]"));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString("Unknown index [nomatch]"));
        }

        // missing concrete remote index is not fatal when skip_unavailable=true (as long as an index matches on another cluster)
        {
            String q = Strings.format("FROM %s,%s:nomatch | STATS count(*)", INDEX1, REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));
        }

        // since there is at least one matching index in the query, the missing wildcarded local index is not an error
        {
            String q = Strings.format("FROM nomatch*,%s:%s", REMOTE_CLUSTER_ALIAS, INDEX2);

            String limit1 = q + " | LIMIT 1";
            Response response = client().performRequest(esqlRequest(limit1));
            assertOK(response);

            Map<String, Object> map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), greaterThanOrEqualTo(1));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                    new ExpectedCluster("(local)", "nomatch*", "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, INDEX2, "successful", null)
                )
            );

            String limit0 = q + " | LIMIT 0";
            response = client().performRequest(esqlRequest(limit0));
            assertOK(response);

            map = responseAsMap(response);
            assertThat(((ArrayList<?>) map.get("columns")).size(), greaterThanOrEqualTo(1));
            assertThat(((ArrayList<?>) map.get("values")).size(), is(0));

            assertExpectedClustersForMissingIndicesTests(
                map,
                List.of(
                    // local cluster is never marked as SKIPPED even when no matching indices - just marked as 0 shards searched
                    new ExpectedCluster("(local)", "nomatch*", "successful", 0),
                    new ExpectedCluster(REMOTE_CLUSTER_ALIAS, INDEX2, "successful", 0)
                )
            );
        }

        // query is fatal since the remote cluster has skip_unavailable=false and has no matching indices
        {
            String q = Strings.format("FROM %s,%s:nomatch*", INDEX1, REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch*]", REMOTE_CLUSTER_ALIAS)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), Matchers.containsString(Strings.format("Unknown index [%s:nomatch*]", REMOTE_CLUSTER_ALIAS)));
        }

        // an error is thrown if there are no matching indices at all
        {
            // with non-matching concrete index
            String q = Strings.format("FROM %s:nomatch", REMOTE_CLUSTER_ALIAS);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));
        }

        // an error is thrown if there are no matching indices at all
        {
            String localExpr = randomFrom("nomatch", "nomatch*");
            String remoteExpr = randomFrom("nomatch", "nomatch*");
            String q = Strings.format("FROM %s,%s:%s", localExpr, REMOTE_CLUSTER_ALIAS, remoteExpr);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));

            String limit0 = q + " | LIMIT 0";
            e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            assertThat(e.getMessage(), containsString("Unknown index"));
            assertThat(e.getMessage(), containsString(Strings.format("%s:%s", REMOTE_CLUSTER_ALIAS, remoteExpr)));
        }

        // error since the remote cluster with skip_unavailable=false specified a concrete index that is not found
        {
            String q = Strings.format("FROM %s,%s:nomatch,%s:%s*", INDEX1, REMOTE_CLUSTER_ALIAS, REMOTE_CLUSTER_ALIAS, INDEX2);

            String limit1 = q + " | LIMIT 1";
            ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit1)));
            assertThat(e.getMessage(), containsString(Strings.format("no such index [nomatch]", REMOTE_CLUSTER_ALIAS)));
            assertThat(e.getMessage(), containsString(Strings.format("index_not_found_exception", REMOTE_CLUSTER_ALIAS)));

            // TODO: in follow on PR, add support for throwing a VerificationException from this scenario
            // String limit0 = q + " | LIMIT 0";
            // e = expectThrows(ResponseException.class, () -> client().performRequest(esqlRequest(limit0)));
            // assertThat(e.getMessage(), containsString(Strings.format("Unknown index [%s:nomatch]", REMOTE_CLUSTER_ALIAS)));
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

    private void setupIndex() throws IOException {
        Request createIndex = new Request("PUT", INDEX1);
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

        createIndex = new Request("PUT", INDEX2);
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
        assertOK(client().performRequest(createIndex));

        bulkRequest = new Request("POST", "/_bulk?refresh=true");
        bulkRequest.setJsonEntity("""
            { "index": {"_index": "squares"}}
            { "num": 1, "square": 1 }
            { "index": {"_index": "squares"}}
            { "num": 4, "square": 4 }
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
        request.setJsonEntity(org.elasticsearch.common.Strings.toString(body));

        return request;
    }
}
