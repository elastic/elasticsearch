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
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

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
                             "names": ["leader-index"],
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

        // Fulfilling cluster
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

        // Query cluster
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

    private Response performRequestWithCcrUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue("ccr_user", PASS)));
        return client().performRequest(request);
    }
}
