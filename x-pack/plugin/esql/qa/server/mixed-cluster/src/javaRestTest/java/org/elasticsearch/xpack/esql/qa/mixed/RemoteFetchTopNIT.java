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
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assume.assumeFalse;

@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class RemoteFetchTopNIT extends ESRestTestCase {
    private static final TransportVersion REMOTE_FETCH_TOPN_TRANSPORT_VERSION = TransportVersion.fromName(
        "esql_remote_fetch_topn_reduction"
    );

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.mixedVersionCluster();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    public void testRemoteFetchTopNFallsBackWhenAnyDataNodeIsOld() throws Exception {
        assertTrue(
            "the current version must support remote-fetch TopN",
            TransportVersion.current().supports(REMOTE_FETCH_TOPN_TRANSPORT_VERSION)
        );
        assumeFalse(
            "the mixed cluster minimum transport version must not support remote-fetch TopN",
            minimumTransportVersion().supports(REMOTE_FETCH_TOPN_TRANSPORT_VERSION)
        );

        String index = "remote_fetch_topn_" + randomAlphaOfLength(8).toLowerCase(Locale.ROOT);
        createTestIndex(index);
        indexDocs(index);
        waitForIndexReady(index);

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

    /**
     * Mixed-cluster and serverless BWC tests can index on one tier while the coordinator runs on another.
     * Yellow is not enough on serverless: {@code INDEX_ONLY} primaries satisfy cluster health while
     * {@code searchShards()} — used by field-caps and ES|QL {@code FROM} — still sees no searchable copy.
     * Wait until field-caps succeeds before issuing {@code FROM} against a pinned coordinator.
     */
    private void waitForIndexReady(String index) throws Exception {
        ensureYellowAndNoInitializingShards(index, "120s");
        assertBusy(() -> {
            Request request = new Request("GET", "/" + index + "/_field_caps");
            request.addParameter("fields", "unique_sort,payload");
            assertOK(client().performRequest(request));
        }, 120, TimeUnit.SECONDS);
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
            // Keep the data path deterministic and ensure the test exercises shard-level node reduction.
            builder.field("data_partitioning", "shard");
            builder.field("task_concurrency", 1);
            builder.endObject();
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
        }
        return entityAsMap(restClient.performRequest(request));
    }

    /**
     * Build a REST client pinned to current-version nodes that support remote-fetch TopN.
     * <p>
     * Hosts are resolved from {@code GET /_nodes}, following the same live lookup pattern as the mixed-cluster
     * csv-spec coordinator helpers. Among capable nodes, prefer ones that can serve {@code searchShards()}
     * (stateful {@code data} nodes or stateless {@code search} tier nodes). Index-only coordinators are
     * kept as a fallback: their {@code INDEX_ONLY} primaries are not searchable.
     */
    private RestClient currentNodeClient() throws IOException {
        ObjectPath nodes = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> nodesMap = nodes.evaluate("nodes");
        List<HttpHost> preferred = new ArrayList<>();
        List<HttpHost> fallback = new ArrayList<>();
        for (String id : nodesMap.keySet()) {
            TransportVersion transportVersion = getTransportVersionWithFallback(
                nodes.evaluate("nodes." + id + ".version"),
                nodes.evaluate("nodes." + id + ".transport_version"),
                TransportVersion::minimumCompatible
            );
            if (transportVersion.supports(REMOTE_FETCH_TOPN_TRANSPORT_VERSION) == false) {
                continue;
            }
            HttpHost host = HttpHost.create(nodes.evaluate("nodes." + id + ".http.publish_address"));
            List<?> roles = nodes.evaluate("nodes." + id + ".roles");
            if (nodeCanServeSearch(roles)) {
                preferred.add(host);
            } else {
                fallback.add(host);
            }
        }
        List<HttpHost> selected = preferred.isEmpty() ? fallback : preferred;
        if (selected.isEmpty()) {
            throw new IllegalStateException("no nodes support remote-fetch TopN");
        }
        return buildClient(restClientSettings(), selected.toArray(HttpHost[]::new));
    }

    /**
     * Whether a node hosts searchable shard copies. Stateful data nodes qualify; stateless search-tier
     * nodes hold the copies {@code searchShards()} can use. Combined index+search nodes qualify via
     * the search role. Index-only nodes do not.
     */
    private static boolean nodeCanServeSearch(List<?> roles) {
        boolean hasData = false;
        boolean hasSearch = false;
        for (Object role : roles) {
            String roleName = role.toString();
            if ("data".equals(roleName)) {
                hasData = true;
            } else if ("search".equals(roleName)) {
                hasSearch = true;
            }
        }
        return hasData || hasSearch;
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
}
