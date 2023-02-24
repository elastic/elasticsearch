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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Strings;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;

public class RemoteClusterSecuritySniffIT extends AbstractRemoteClusterSecurityTestCase {

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .nodes(3)
            .name("fulfilling-cluster")
            .apply(commonClusterConfig)
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
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_client.ssl.certificate_authorities", "remote-cluster-ca.crt")
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_SEARCH_USER, PASS.toString(), "read_remote_shared_metrics")
            .build();
    }

    @ClassRule
    // Use a RuleChain to ensure that fulfilling cluster is started before query cluster
    public static TestRule clusterRule = RuleChain.outerRule(fulfillingCluster).around(queryCluster);

    public void testRemoteAccessForCrossClusterSearch() throws Exception {
        final Map<String, Object> apiKeyMap = createRemoteAccessApiKey("""
            [
               {
                 "names": ["shared-*"],
                 "privileges": ["read", "read_cross_cluster"]
               }
             ]""");

        updateClusterSettings(
            Settings.builder()
                .put("cluster.remote.my_remote_cluster.mode", "sniff")
                .putList("cluster.remote.my_remote_cluster.seeds", fulfillingCluster.getRemoteClusterServerEndpoint(0))
                .put("cluster.remote.my_remote_cluster.authorization", (String) apiKeyMap.get("encoded"))
                .build()
        );

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

            final Request bulkRequest = new Request("POST", "/_bulk?refresh=true");
            bulkRequest.setJsonEntity(Strings.format("""
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }\n"""));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster
        {
            // Check that we can search the fulfilling cluster from the querying cluster
            final var searchRequest = new Request(
                "GET",
                String.format(Locale.ROOT, "/my_remote_cluster:*/_search?ccs_minimize_roundtrips=%s", randomBoolean())
            );
            final Response response = performRequestWithRemoteAccessUser(searchRequest);
            assertOK(response);
            final SearchResponse searchResponse = SearchResponse.fromXContent(responseAsParser(response));
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(4L));
            final Set<String> actualIndices = Arrays.stream(searchResponse.getHits().getHits())
                .map(SearchHit::getIndex)
                .collect(Collectors.toSet());
            assertThat(actualIndices, containsInAnyOrder("shared-metrics"));
        }
    }

    private Response performRequestWithRemoteAccessUser(final Request request) throws IOException {
        request.setOptions(RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", basicAuthHeaderValue(REMOTE_SEARCH_USER, PASS)));
        return client().performRequest(request);
    }
}
