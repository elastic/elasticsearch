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
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.junit.RunnableTestRuleAdapter;
import org.elasticsearch.test.rest.ObjectPath;
import org.junit.ClassRule;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RemoteClusterSecurityTopologyRestIT extends AbstractRemoteClusterSecurityTestCase {

    private static final AtomicReference<Map<String, Object>> API_KEY_MAP_REF = new AtomicReference<>();
    private static final AtomicBoolean NODE1_RCS_SERVER_ENABLED = new AtomicBoolean();

    static {
        fulfillingCluster = ElasticsearchCluster.local()
            .name("fulfilling-cluster")
            .nodes(3)
            .apply(commonClusterConfig)
            .setting("remote_cluster.port", "0")
            .setting("xpack.security.remote_cluster_server.ssl.enabled", "true")
            .setting("xpack.security.remote_cluster_server.ssl.key", "remote-cluster.key")
            .setting("xpack.security.remote_cluster_server.ssl.certificate", "remote-cluster.crt")
            .setting("xpack.security.authc.token.enabled", "true")
            .keystore("xpack.security.remote_cluster_server.ssl.secure_key_passphrase", "remote-cluster-password")
            .node(0, spec -> spec.setting("remote_cluster_server.enabled", "true"))
            .node(1, spec -> spec.setting("remote_cluster_server.enabled", () -> String.valueOf(NODE1_RCS_SERVER_ENABLED.get())))
            // at least one remote node has server disabled
            .node(2, spec -> spec.setting("remote_cluster_server.enabled", "false"))
            .build();

        queryCluster = ElasticsearchCluster.local()
            .name("query-cluster")
            .apply(commonClusterConfig)
            .setting("xpack.security.remote_cluster_client.ssl.enabled", "true")
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
            .rolesFile(Resource.fromClasspath("roles.yml"))
            .user(REMOTE_METRIC_USER, PASS.toString(), "read_remote_shared_metrics", false)
            .build();
    }

    @ClassRule
    public static TestRule clusterRule = RuleChain.outerRule(new RunnableTestRuleAdapter(() -> {
        NODE1_RCS_SERVER_ENABLED.set(randomBoolean());
    })).around(fulfillingCluster).around(queryCluster);

    public void testCrossClusterScrollWithSniffModeWhenSomeRemoteNodesAreNotDirectlyAccessible() throws Exception {
        configureRemoteCluster(false);

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
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric1" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric2" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric3" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric4" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric5" }
                { "index": { "_index": "shared-metrics" } }
                { "name": "metric6" }
                """));
            assertOK(performRequestAgainstFulfillingCluster(bulkRequest));
        }

        // Query cluster
        {
            final var documentFieldValues = new HashSet<>();
            final var searchRequest = new Request("GET", "/my_remote_cluster:*/_search?scroll=1h&size=1");
            final SearchResponse searchResponse = SearchResponseUtils.parseSearchResponse(
                responseAsParser(performRequestWithRemoteMetricUser(searchRequest))
            );
            final Request scrollRequest = new Request("GET", "/_search/scroll");
            final String scrollId;
            try {
                assertThat(searchResponse.getHits().getTotalHits().value(), equalTo(6L));
                assertThat(Arrays.stream(searchResponse.getHits().getHits()).map(SearchHit::getIndex).toList(), contains("shared-metrics"));
                documentFieldValues.add(searchResponse.getHits().getHits()[0].getSourceAsMap().get("name"));
                scrollId = searchResponse.getScrollId();
                // Scroll should be able to fetch all documents from all nodes even when some nodes are not directly accessible in sniff
                // mode
                scrollRequest.setJsonEntity(Strings.format("""
                    { "scroll_id": "%s" }
                    """, scrollId));
            } finally {
                searchResponse.decRef();
            }

            // Fetch all documents
            for (int i = 0; i < 5; i++) {
                final SearchResponse scrollResponse = SearchResponseUtils.parseSearchResponse(
                    responseAsParser(performRequestWithRemoteMetricUser(scrollRequest))
                );
                try {
                    assertThat(scrollResponse.getHits().getTotalHits().value(), equalTo(6L));
                    assertThat(
                        Arrays.stream(scrollResponse.getHits().getHits()).map(SearchHit::getIndex).toList(),
                        contains("shared-metrics")
                    );
                    documentFieldValues.add(scrollResponse.getHits().getHits()[0].getSourceAsMap().get("name"));
                } finally {
                    scrollResponse.decRef();
                }
            }
            assertThat(documentFieldValues, containsInAnyOrder("metric1", "metric2", "metric3", "metric4", "metric5", "metric6"));

            // Scroll from all nodes should be freed
            final Request deleteScrollRequest = new Request("DELETE", "/_search/scroll");
            deleteScrollRequest.setJsonEntity(Strings.format("""
                { "scroll_id": "%s" }
                """, scrollId));
            final ObjectPath deleteScrollObjectPath = assertOKAndCreateObjectPath(performRequestWithRemoteMetricUser(deleteScrollRequest));
            assertThat(deleteScrollObjectPath.evaluate("succeeded"), is(true));
            assertThat(deleteScrollObjectPath.evaluate("num_freed"), equalTo(3));
        }
    }

    private Response performRequestWithRemoteMetricUser(final Request request) throws IOException {
        request.setOptions(
            RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", headerFromRandomAuthMethod(REMOTE_METRIC_USER, PASS))
        );
        return client().performRequest(request);
    }
}
