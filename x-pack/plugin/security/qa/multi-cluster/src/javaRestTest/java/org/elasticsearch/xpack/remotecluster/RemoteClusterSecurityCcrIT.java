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
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
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
                if (API_KEY_MAP_REF.get() == null) {
                    final Map<String, Object> apiKeyMap = createCrossClusterAccessApiKey("""
                        [
                          {
                             "names": ["leader-index", "metrics-*"],
                             "privileges": ["manage", "read"]
                          }
                        ]""");
                    API_KEY_MAP_REF.set(apiKeyMap);
                }
                return (String) API_KEY_MAP_REF.get().get("encoded");
            })
            .user("ccr_user", PASS.toString(), "ccr_user_role")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testFollow() throws Exception {
        configureRemoteClusters();

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
                { "name": "doc-4" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // query cluster
        {
            final Request putCcrRequest = new Request("PUT", "/follower-index/_ccr/follow?wait_for_active_shards=1");
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
            final Request searchRequest = new Request("GET", "/follower-index/_search");
            assertBusy(() -> {
                final Response response = performRequestWithCcrUser(searchRequest);
                assertOK(response);
                final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(4L));
            });
        }
    }

    public void testAutoFollow() throws Exception {
        configureRemoteClusters();

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
                { "name": "doc-4" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // follow cluster
        {
            final Request searchRequest = new Request("GET", "/metrics-*/_search");
            assertBusy(() -> {
                ensureHealth("", request -> {
                    request.addParameter("wait_for_status", "yellow");
                    request.addParameter("wait_for_active_shards", "1");
                    request.addParameter("wait_for_no_relocating_shards", "true");
                    request.addParameter("wait_for_no_initializing_shards", "true");
                    request.addParameter("timeout", "5s");
                    request.addParameter("level", "shards");
                });

                final Response response = performRequestWithCcrUser(searchRequest);
                assertOK(response);
                final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
                assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
                assertThat(
                    Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).collect(Collectors.toUnmodifiableSet()),
                    equalTo(Set.of("metrics-000", "metrics-002"))
                );
            });
        }
    }

    private Response performRequestWithCcrUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue("ccr_user", PASS)));
        return client().performRequest(request);
    }
}
