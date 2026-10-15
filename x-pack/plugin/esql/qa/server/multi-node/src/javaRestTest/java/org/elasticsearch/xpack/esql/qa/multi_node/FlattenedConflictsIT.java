/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.multi_node;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.WarningsHandler;
import org.elasticsearch.common.Strings;
import org.elasticsearch.test.TestClustersThreadFilter;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Cross-index type conflicts where a field is a concrete type in one index
 * and a sub-key of a {@code flattened} field in another.
 * <p>
 *     Each scenario has a {@code SameNode} sibling that pins both indices to the
 *     same node.
 * </p>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class FlattenedConflictsIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(ignored -> {});

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    private List<String> nodeNames;

    @Before
    public void discoverNodes() throws Exception {
        assumeFalse("Cannot pin shards to specific nodes in serverless mode", isServerless());

        Request nodesRequest = new Request("GET", "/_nodes");
        nodesRequest.addParameter("filter_path", "nodes.*.name");
        Map<String, Object> nodesResponse = entityAsMap(client().performRequest(nodesRequest));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) nodesResponse.get("nodes");
        nodeNames = new ArrayList<>();
        for (Object nodeInfo : nodes.values()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> info = (Map<String, Object>) nodeInfo;
            nodeNames.add((String) info.get("name"));
        }
        assertThat("Need at least 2 nodes", nodeNames.size(), greaterThanOrEqualTo(2));
    }

    /**
     * {@code metadata.time} is {@code date} (element type LONG) in one index
     * and a sub-key of a {@code flattened} field in another.
     */
    public void testDateVsFlattened() throws Exception {
        testDateVsFlattened(nodeNames.get(0), nodeNames.get(1));
    }

    /** Same as {@link #testDateVsFlattened()} but both indices on the same node. */
    public void testDateVsFlattenedSameNode() throws Exception {
        testDateVsFlattened(nodeNames.get(0), nodeNames.get(0));
    }

    /**
     * {@code features.topic_id} is {@code integer} (element type INT) in one
     * index and a sub-key of a {@code flattened} field in another.
     * With an explicit {@code ::long} cast the coordinator resolves the union
     * but the converter must be applied on both the integer and flattened shards.
     */
    public void testIntegerVsFlattenedWithCast() throws Exception {
        testIntegerVsFlattenedWithCast(nodeNames.get(0), nodeNames.get(1));
    }

    /** Same as {@link #testIntegerVsFlattenedWithCast()} but both indices on the same node. */
    public void testIntegerVsFlattenedWithCastSameNode() throws Exception {
        testIntegerVsFlattenedWithCast(nodeNames.get(0), nodeNames.get(0));
    }

    public void testDateVsFlattened(String node1, String node2) throws Exception {
        createIndexPinned("idx_date", """
            { "properties": { "metadata": { "properties": { "time": { "type": "date" } } } } }""", node1);
        createIndexPinned("idx_flat_date", """
            { "properties": { "metadata": { "type": "flattened" } } }""", node2);
        ensureGreen("idx_date");
        ensureGreen("idx_flat_date");
        for (int i = 0; i < 5; i++) {
            indexDoc("idx_date", Integer.toString(i), Strings.format("""
                {"metadata": {"time": "2024-01-0%dT00:00:00.000Z"}}""", i + 1));
        }
        for (int i = 0; i < 5; i++) {
            indexDoc("idx_flat_date", Integer.toString(i), Strings.format("""
                {"metadata": {"time": "2024-02-0%dT00:00:00.000Z"}}""", i + 1));
        }
        refresh("idx_date");
        refresh("idx_flat_date");

        // The flattened sub-key is not in the real mapping so the data node treats it
        // as absent — only the 5 docs from the date index contribute.
        List<List<Object>> values = esql("""
            FROM idx_date, idx_flat_date
            | STATS c = COUNT(metadata.time)
            """);
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).get(0), equalTo(5));
    }

    public void testIntegerVsFlattenedWithCast(String node1, String node2) throws Exception {
        createIndexPinned("idx_obj_int", """
            { "properties": { "features": { "properties": { "topic_id": { "type": "integer" } } } } }""", node1);
        createIndexPinned("idx_flat_int", """
            { "properties": { "features": { "type": "flattened" } } }""", node2);
        ensureGreen("idx_obj_int");
        ensureGreen("idx_flat_int");
        for (int i = 0; i < 10; i++) {
            indexDoc("idx_obj_int", Integer.toString(i), Strings.format("""
                {"features": {"topic_id": %d}}""", i + 1));
        }
        for (int i = 0; i < 5; i++) {
            indexDoc("idx_flat_int", Integer.toString(i), Strings.format("""
                {"features": {"topic_id": %d}}""", (i + 1) * 10));
        }
        refresh("idx_obj_int");
        refresh("idx_flat_int");

        // int sum: 1+2+...+10 = 55; flattened sub-key treated as absent.
        List<List<Object>> values = esql("""
            FROM idx_obj_int, idx_flat_int
            | STATS s = SUM(features.topic_id::long)
            """);
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).get(0), equalTo(55));
    }

    private void createIndexPinned(String name, String mapping, String node) throws IOException {
        Request request = new Request("PUT", "/" + name);
        request.setJsonEntity(Strings.format("""
            {
              "settings": {
                "index.number_of_shards": 1,
                "index.number_of_replicas": 0,
                "index.routing.allocation.require._name": "%s"
              },
              "mappings": %s
            }""", node, mapping));
        assertOK(client().performRequest(request));
    }

    private void indexDoc(String index, String id, String body) throws IOException {
        Request request = new Request("PUT", "/" + index + "/_doc/" + id);
        request.setJsonEntity(body);
        request.addParameter("refresh", "false");
        assertOK(client().performRequest(request));
    }

    private boolean isServerless() throws IOException {
        for (Map<?, ?> nodeInfo : getNodesInfo(client()).values()) {
            @SuppressWarnings("unchecked")
            List<Map<?, ?>> modules = (List<Map<?, ?>>) nodeInfo.get("modules");
            for (Map<?, ?> module : modules) {
                if (module.get("name").toString().startsWith("serverless-")) {
                    return true;
                }
            }
        }
        return false;
    }

    private List<List<Object>> esql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
        request.addParameter("allow_partial_results", "false");
        request.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(WarningsHandler.PERMISSIVE).build());
        String escaped = query.replace("\"", "\\\"").replace("\n", "\\n");
        request.setJsonEntity("{\"query\": \"" + escaped + "\"}");
        Map<String, Object> result = entityAsMap(client().performRequest(request));
        assertMap("no partial failures", result, matchesMap().extraOk().entry("is_partial", false));
        @SuppressWarnings("unchecked")
        List<List<Object>> values = (List<List<Object>>) result.get("values");
        return values;
    }
}
