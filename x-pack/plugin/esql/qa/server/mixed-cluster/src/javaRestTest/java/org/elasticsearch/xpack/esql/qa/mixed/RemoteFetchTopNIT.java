/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RemoteFetchTopNIT extends ESRestTestCase {
    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testRemoteFetchTopNFallsBackWhenAnyDataNodeIsOld() throws Exception {
        String index = "remote_fetch_topn_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createTestIndex(index);
        indexDocs(index);

        CurrentNodeClient currentNodeClient = currentNodeClient();
        assertThat(currentNodeClient.oldNodeAddresses(), not(empty()));
        assertThat(currentNodeClient.currentNodeAddresses(), not(empty()));

        try (RestClient currentClient = currentNodeClient.client()) {
            Map<String, Object> response = runEsql(
                currentClient,
                "FROM " + index + " | SORT unique_sort + 1 DESC | LIMIT 5 | KEEP unique_sort, payload"
            );
            assertThat(
                response.get("values"),
                equalTo(
                    List.of(
                        List.of(63, "payload-63"),
                        List.of(62, "payload-62"),
                        List.of(61, "payload-61"),
                        List.of(60, "payload-60"),
                        List.of(59, "payload-59")
                    )
                )
            );
            assertFalse(
                "remote fetch must be disabled while any data node lacks support",
                containsRemoteFetchOperator(response.get("profile"))
            );
        }
    }

    private void createTestIndex(String index) throws IOException {
        Request request = new Request("PUT", "/" + index);
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.startObject("settings");
            builder.field("number_of_shards", 4);
            builder.field("number_of_replicas", 0);
            builder.endObject();
            builder.startObject("mappings");
            builder.startObject("properties");
            fieldMapping(builder, "unique_sort", "long");
            fieldMapping(builder, "payload", "keyword");
            builder.endObject();
            builder.endObject();
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
        }
        assertOK(client().performRequest(request));
    }

    private void indexDocs(String index) throws IOException {
        StringBuilder bulk = new StringBuilder();
        for (int i = 0; i < 64; i++) {
            try (XContentBuilder action = XContentFactory.jsonBuilder()) {
                action.startObject();
                action.startObject("index");
                action.field("_id", i);
                action.endObject();
                action.endObject();
                bulk.append(Strings.toString(action)).append('\n');
            }
            try (XContentBuilder doc = XContentFactory.jsonBuilder()) {
                doc.startObject();
                doc.field("unique_sort", i);
                doc.field("payload", "payload-" + i);
                doc.endObject();
                bulk.append(Strings.toString(doc)).append('\n');
            }
        }
        Request request = new Request("POST", "/" + index + "/_bulk?refresh=true");
        request.setJsonEntity(bulk.toString());
        assertOK(client().performRequest(request));
    }

    private static void fieldMapping(XContentBuilder builder, String field, String type) throws IOException {
        builder.startObject(field);
        builder.field("type", type);
        builder.endObject();
    }

    private Map<String, Object> runEsql(RestClient restClient, String query) throws IOException {
        Request request = new Request("POST", "/_query");
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.startObject();
            builder.field("query", query);
            builder.field("profile", true);
            builder.field("accept_pragma_risks", true);
            builder.startObject("pragma");
            builder.field("node_level_reduction", true);
            builder.field("remote_fetch_topn", true);
            builder.field("data_partitioning", "shard");
            builder.field("task_concurrency", 1);
            builder.field("max_concurrent_nodes_per_cluster", 10);
            builder.field("max_concurrent_shards_per_node", 10);
            builder.endObject();
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
        }
        return entityAsMap(restClient.performRequest(request));
    }

    private CurrentNodeClient currentNodeClient() throws IOException {
        List<HttpHost> oldNodes = List.of(HttpHost.create(cluster.getHttpAddress(0)), HttpHost.create(cluster.getHttpAddress(2)));
        List<HttpHost> currentNodes = List.of(HttpHost.create(cluster.getHttpAddress(1)), HttpHost.create(cluster.getHttpAddress(3)));
        return new CurrentNodeClient(buildClient(restClientSettings(), currentNodes.toArray(new HttpHost[0])), currentNodes, oldNodes);
    }

    private static boolean containsRemoteFetchOperator(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object operator = map.get("operator");
            if (operator instanceof String operatorName && operatorName.startsWith("RemoteFetchOperator")) {
                return true;
            }
            for (Object child : map.values()) {
                if (containsRemoteFetchOperator(child)) {
                    return true;
                }
            }
        } else if (value instanceof List<?> list) {
            for (Object child : list) {
                if (containsRemoteFetchOperator(child)) {
                    return true;
                }
            }
        }
        return false;
    }

    private record CurrentNodeClient(RestClient client, List<HttpHost> currentNodeAddresses, List<HttpHost> oldNodeAddresses) {}
}
