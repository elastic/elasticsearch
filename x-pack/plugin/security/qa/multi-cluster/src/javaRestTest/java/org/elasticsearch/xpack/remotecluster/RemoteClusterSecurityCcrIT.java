/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.remotecluster;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.elasticsearch.common.Strings.arrayToCommaDelimitedString;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class RemoteClusterSecurityCcrIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();

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
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .module("x-pack-ccr")
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .keystore("cluster.remote.my_remote_cluster.credentials", () -> {
                API_KEY_MAP_REF.updateAndGet(v -> v != null ? v : createCrossClusterAccessApiKey("""
                    {
                      "replication": [
                        {
                           "names": ["leader-index", "leader-alias", "metrics-*"]
                        }
                      ]
                    }"""));
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .user("ccr_user", PASS.toString(), "ccr_user_role")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testFollow() throws Exception {
        configureRemoteCluster();

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

            final Request putIndexRequest = new Request("PUT", "/shared-index");
            putIndexRequest.setJsonEntity("""
                {
                  "aliases": {
                    "shared-alias": {}
                  }
                }
                """);
            assertOK(performRequestAgainstFulfillingCluster(putIndexRequest));
        }

        // query cluster
        {
            final String followIndexName = "follower-index";
            final Request putCcrRequest = new Request("PUT", "/" + followIndexName + "/_ccr/follow?wait_for_active_shards=1");
            putCcrRequest.setJsonEntity("""
                {
                  "remote_cluster": "my_remote_cluster",
                  "leader_index": "leader-index"
                }""");

            final Response putCcrResponse = performRequestWithCcrUser(putCcrRequest);
            assertOK(putCcrResponse);

            final Map<String, Object> responseMap = responseAsMap(putCcrResponse);
            responseMap.forEach((k, v) -> assertThat(k, v, is(true)));

            // Ensure data is replicated
            verifyReplicatedDocuments(4L, followIndexName);

            assertFollowerInfo(followIndexName, "leader-index", "active");
            assertFollowerStats(followIndexName);

            // unfollow and then follow and then index a few docs in leader index:
            pauseFollow(followIndexName);
            assertFollowerInfo(followIndexName, "leader-index", "paused");
            resumeFollow(followIndexName);
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "leader-index" } }
                { "name": "doc-5" }
                { "index": { "_index": "leader-index" } }
                { "name": "doc-6" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
            verifyReplicatedDocuments(6L, followIndexName);

            pauseFollow(followIndexName);
            closeIndex(followIndexName);
            unfollow(followIndexName);
            assertNoFollowerInfo(followIndexName);
            final var e = expectThrows(ResponseException.class, () -> resumeFollow(followIndexName));
            assertThat(e.getMessage(), containsString("follow index [" + followIndexName + "] does not have ccr metadata"));
        }

        // query cluster error cases - no privileges
        {

            final Request putCcrRequest = new Request("PUT", "/follower-index-2/_ccr/follow?wait_for_active_shards=1");
            putCcrRequest.setJsonEntity("""
                {
                  "remote_cluster": "my_remote_cluster",
                  "leader_index": "private-index"
                }""");
            final ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithCcrUser(putCcrRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(403));
            assertThat(e.getMessage(), containsString("insufficient privileges to follow index [private-index]"));
        }

        // query cluster error cases - aliases not supported
        {
            final Request putCcrRequest = new Request("PUT", "/follower-index-3/_ccr/follow?wait_for_active_shards=1");
            putCcrRequest.setJsonEntity("""
                {
                  "remote_cluster": "my_remote_cluster",
                  "leader_index": "shared-alias"
                }""");
            final ResponseException e = expectThrows(ResponseException.class, () -> performRequestWithCcrUser(putCcrRequest));
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
            assertThat(e.getMessage(), containsString("cannot follow [shared-alias], because it is a ALIAS"));
        }
    }

    public void testAutoFollow() throws Exception {
        configureRemoteCluster();

        // follow cluster
        {
            final var putAllowFollowRequest = new Request("PUT", "/_ccr/auto_follow/my_auto_follow_pattern");
            putAllowFollowRequest.setJsonEntity("""
                {
                  "remote_cluster" : "my_remote_cluster",
                  "leader_index_patterns" : [ "metrics-*" ],
                  "leader_index_exclusion_patterns": [ "metrics-001" ]
                }""");

            final Response putAutoFollowResponse = performRequestWithCcrUser(putAllowFollowRequest);
            assertOK(putAutoFollowResponse);
        }

        // leader cluster
        {
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "metrics-000" } }
                { "name": "doc-1" }
                { "index": { "_index": "metrics-000" } }
                { "name": "doc-2" }
                { "index": { "_index": "metrics-001" } }
                { "name": "doc-3" }
                { "index": { "_index": "metrics-002" } }
                { "name": "doc-4" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // follow cluster
        {
            assertBusy(() -> {
                ensureHealth("metrics-000,metrics-002", request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("wait_for_active_shards", "2");
                    request.addParameter("wait_for_no_relocating_shards", "true");
                    request.addParameter("wait_for_no_initializing_shards", "true");
                    request.addParameter("timeout", "5s");
                    request.addParameter("level", "shards");
                });
            });
            verifyReplicatedDocuments(3L, "metrics-000", "metrics-002");

            final Response statsResponse = performRequestWithCcrUser(new Request("GET", "/_ccr/stats"));
            assertOK(statsResponse);
            assertThat(
                ObjectPath.createFromResponse(statsResponse).evaluate("auto_follow_stats.number_of_successful_follow_indices"),
                equalTo(2)
            );
            assertFollowerInfo("metrics-000", "metrics-000", "active");
            assertFollowerInfo("metrics-002", "metrics-002", "active");

            // Pause and resume
            pauseAutoFollow("my_auto_follow_pattern");
            resumeAutoFollow("my_auto_follow_pattern");
            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "metrics-000" } }
                { "name": "doc-5" }
                { "index": { "_index": "metrics-002" } }
                { "name": "doc-6" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
            verifyReplicatedDocuments(5L, "metrics-000", "metrics-002");

            // Delete
            deleteAutoFollow("my_auto_follow_pattern");
            final ResponseException e = expectThrows(
                ResponseException.class,
                () -> performRequestWithCcrUser(new Request("GET", "/_ccr/auto_follow/my_auto_follow_pattern"))
            );
            assertThat(e.getResponse().getStatusLine().getStatusCode(), equalTo(404));
        }
    }

    private Response performRequestWithCcrUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue("ccr_user", PASS)));
        return client().performRequest(request);
    }

    private void verifyReplicatedDocuments(long numberOfDocs, String... indices) throws Exception {
        final Request searchRequest = new Request("GET", "/" + arrayToCommaDelimitedString(indices) + "/_search");
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
        });
    }

    private void assertFollowerInfo(String followIndexName, String leadIndexName, String status) throws IOException {
        final Response response = performRequestWithCcrUser(new Request("GET", "/" + followIndexName + "/_ccr/info"));
        assertOK(response);
        final List<Map<String, Object>> followerIndices = ObjectPath.createFromResponse(response).evaluate("follower_indices");
        assertThat(followerIndices, hasSize(1));

        final Map<String, Object> follower = followerIndices.get(0);
        assertThat(ObjectPath.evaluate(follower, "follower_index"), equalTo(followIndexName));
        assertThat(ObjectPath.evaluate(follower, "leader_index"), equalTo(leadIndexName));
        assertThat(ObjectPath.evaluate(follower, "remote_cluster"), equalTo("my_remote_cluster"));
        assertThat(ObjectPath.evaluate(follower, "status"), equalTo(status));
    }

    private void assertNoFollowerInfo(String followIndexName) throws IOException {
        final Response response = performRequestWithCcrUser(new Request("GET", "/" + followIndexName + "/_ccr/info"));
        assertOK(response);
        final List<Map<String, Object>> followerIndices = ObjectPath.createFromResponse(response).evaluate("follower_indices");
        assertThat(followerIndices, empty());
    }

    private void assertFollowerStats(String followIndexName) throws IOException {
        final Response response = performRequestWithCcrUser(new Request("GET", "/" + followIndexName + "/_ccr/stats"));
        assertOK(response);
        final List<Map<String, Object>> followerIndices = ObjectPath.createFromResponse(response).evaluate("indices");
        assertThat(followerIndices, hasSize(1));

        final Map<String, Object> follower = followerIndices.get(0);
        assertThat(ObjectPath.evaluate(follower, "index"), equalTo(followIndexName));
    }

    private void pauseFollow(String followIndexName) throws IOException {
        assertOK(performRequestWithCcrUser(new Request("POST", "/" + followIndexName + "/_ccr/pause_follow")));
    }

    private void resumeFollow(String followIndexName) throws IOException {
        final Request resumeFollowRequest = new Request("POST", "/" + followIndexName + "/_ccr/resume_follow");
        resumeFollowRequest.setJsonEntity("{\"read_poll_timeout\": \"10ms\"}");
        assertOK(performRequestWithCcrUser(resumeFollowRequest));
    }

    private void unfollow(String followIndexName) throws IOException {
        assertOK(performRequestWithCcrUser(new Request("POST", "/" + followIndexName + "/_ccr/unfollow")));
    }

    private void pauseAutoFollow(String name) throws IOException {
        assertOK(performRequestWithCcrUser(new Request("POST", "/_ccr/auto_follow/" + name + "/pause")));
    }

    private void resumeAutoFollow(String name) throws IOException {
        assertOK(performRequestWithCcrUser(new Request("POST", "/_ccr/auto_follow/" + name + "/resume")));
    }

    private void deleteAutoFollow(String name) throws IOException {
        assertOK(performRequestWithCcrUser(new Request("DELETE", "/_ccr/auto_follow/" + name)));
    }
}
