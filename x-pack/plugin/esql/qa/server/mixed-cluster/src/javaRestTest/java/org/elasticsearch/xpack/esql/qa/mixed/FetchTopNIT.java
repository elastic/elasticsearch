/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.http.HttpHost;
import org.elasticsearch.TransportVersion;
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

import static org.hamcrest.Matchers.equalTo;

/**
 * Ensures mixed-version negotiation falls back to eager field loading when any data node lacks the fetch-boundary wire contract.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FetchTopNIT extends ESRestTestCase {
    private static final TransportVersion FETCH_TOPN_TRANSPORT_VERSION = TransportVersion.fromName("esql_fetch_boundary");

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testFetchTopNFallsBackWhenAnyDataNodeIsOld() throws Exception {
        assertTrue("the current version must support fetch TopN", TransportVersion.current().supports(FETCH_TOPN_TRANSPORT_VERSION));
        assertFalse(
            "the mixed cluster minimum transport version must not support fetch TopN",
            minimumTransportVersion().supports(FETCH_TOPN_TRANSPORT_VERSION)
        );

        String index = "fetch_topn_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createTestIndex(index);
        indexDocs(index);

        try (RestClient currentClient = currentNodeClient()) {
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
            assertFalse("fetch must be disabled while any data node lacks support", containsFetchOperator(response.get("profile")));
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
            builder.field("fetch_topn", true);
            // Keep the data path deterministic and ensure the test exercises shard-level node reduction.
            builder.field("data_partitioning", "shard");
            builder.field("task_concurrency", 1);
            builder.endObject();
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
        }
        return entityAsMap(restClient.performRequest(request));
    }

    private RestClient currentNodeClient() throws IOException {
        // Clusters.mixedVersionCluster creates nodes in old/current/old/current order. Do not classify by version string:
        // detached BWC refs can report the same version as current nodes.
        HttpHost[] currentNodes = { HttpHost.create(cluster.getHttpAddress(1)), HttpHost.create(cluster.getHttpAddress(3)) };
        return buildClient(restClientSettings(), currentNodes);
    }

    private static boolean containsFetchOperator(Object value) {
        if (value instanceof Map<?, ?> map) {
            Object operator = map.get("operator");
            if (operator instanceof String operatorName && operatorName.startsWith("FetchOperator")) {
                return true;
            }
            for (Object child : map.values()) {
                if (containsFetchOperator(child)) {
                    return true;
                }
            }
        } else if (value instanceof List<?> list) {
            for (Object child : list) {
                if (containsFetchOperator(child)) {
                    return true;
                }
            }
        }
        return false;
    }
}
