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
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.nullValue;

/**
 * Cross-index nested-vs-object type skew (#154011): field caps filters {@code -nested},
 * so the coordinator plans the object type and the nested shard must contribute nulls.
 * <p>
 *     Each scenario has a {@code SameNode} sibling that pins both indices to the
 *     same node.
 * </p>
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class NestedFieldConflictsIT extends ESRestTestCase {

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
     * {@code item.value} is {@code integer} under nested in one index and
     * {@code long} under a plain object in another.
     */
    public void testIntegerVsLong() throws Exception {
        testIntegerVsLong(nodeNames.get(0), nodeNames.get(1));
    }

    /** Same as {@link #testIntegerVsLong()} but both indices on the same node. */
    public void testIntegerVsLongSameNode() throws Exception {
        testIntegerVsLong(nodeNames.get(0), nodeNames.get(0));
    }

    public void testDoubleVsLong() throws Exception {
        testDoubleVsLong(nodeNames.get(0), nodeNames.get(1));
    }

    public void testDoubleVsLongSameNode() throws Exception {
        testDoubleVsLong(nodeNames.get(0), nodeNames.get(0));
    }

    public void testKeywordVsDate() throws Exception {
        testKeywordVsDate(nodeNames.get(0), nodeNames.get(1));
    }

    public void testKeywordVsDateSameNode() throws Exception {
        testKeywordVsDate(nodeNames.get(0), nodeNames.get(0));
    }

    /**
     * Same type on both indices, but the nested mapping sets {@code include_in_root}.
     * Pre-fix this leaked nested values into ES|QL; they must still be null.
     */
    public void testIncludeInRootSameType() throws Exception {
        testIncludeInRootSameType(nodeNames.get(0), nodeNames.get(1));
    }

    public void testIncludeInRootSameTypeSameNode() throws Exception {
        testIncludeInRootSameType(nodeNames.get(0), nodeNames.get(0));
    }

    private void testIntegerVsLong(String node1, String node2) throws Exception {
        String nested = "nest_int_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_long_" + getTestName().toLowerCase(Locale.ROOT);
        createIndexPinned(nested, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "type": "nested", "properties": { "value": { "type": "integer" } } } } }""", node1);
        createIndexPinned(object, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "properties": { "value": { "type": "long" } } } } }""", node2);
        ensureGreen(nested);
        ensureGreen(object);
        for (int i = 0; i < 20; i++) {
            indexDoc(nested, Integer.toString(i), Strings.format("""
                {"id": "n%02d", "item": [{"value": %d}]}""", i, i + 100));
            indexDoc(object, Integer.toString(i), Strings.format("""
                {"id": "o%02d", "item": {"value": %d}}""", i, i + 1));
        }
        refresh(nested);
        refresh(object);

        String from = "FROM " + nested + ", " + object;
        List<List<Object>> values = esql(from + """
             | STATS s = SUM(item.value), c = COUNT(item.value)
            """);
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).get(0), equalTo(210));
        assertThat(values.get(0).get(1), equalTo(20));

        // Columnar extract + TopN: pre-fix ClassCastException / sanityCheckBlock.
        values = esql(from + """
             | KEEP id, item.value | SORT id | LIMIT 5
            """);
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0).get(0), equalTo("n00"));
        assertThat(values.get(0).get(1), nullValue());
    }

    private void testDoubleVsLong(String node1, String node2) throws Exception {
        String nested = "nest_dbl_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_lng_" + getTestName().toLowerCase(Locale.ROOT);
        createIndexPinned(nested, """
            { "properties": { "item": { "type": "nested", "properties": { "value": { "type": "double" } } } } }""", node1);
        createIndexPinned(object, """
            { "properties": { "item": { "properties": { "value": { "type": "long" } } } } }""", node2);
        ensureGreen(nested);
        ensureGreen(object);
        for (int i = 0; i < 20; i++) {
            indexDoc(nested, Integer.toString(i), Strings.format("""
                {"item": [{"value": %s}]}""", (i + 1) + 0.5));
            indexDoc(object, Integer.toString(i), Strings.format("""
                {"item": {"value": %d}}""", i + 1));
        }
        refresh(nested);
        refresh(object);

        List<List<Object>> values = esql("FROM " + nested + ", " + object + """
             | STATS s = SUM(item.value), c = COUNT(item.value)
            """);
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).get(0), equalTo(210));
        assertThat(values.get(0).get(1), equalTo(20));
    }

    private void testKeywordVsDate(String node1, String node2) throws Exception {
        String nested = "nest_kw_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_dt_" + getTestName().toLowerCase(Locale.ROOT);
        createIndexPinned(nested, """
            { "properties": { "item": { "type": "nested", "properties": { "value": { "type": "keyword" } } } } }""", node1);
        createIndexPinned(object, """
            { "properties": { "item": { "properties": { "value": { "type": "date" } } } } }""", node2);
        ensureGreen(nested);
        ensureGreen(object);
        for (int i = 0; i < 20; i++) {
            indexDoc(nested, Integer.toString(i), Strings.format("""
                {"item": [{"value": "nested-%d"}]}""", i));
            indexDoc(object, Integer.toString(i), Strings.format("""
                {"item": {"value": "2024-01-%02dT00:00:00.000Z"}}""", i + 1));
        }
        refresh(nested);
        refresh(object);

        List<List<Object>> values = esql("FROM " + nested + ", " + object + """
             | STATS c = COUNT(item.value)
            """);
        assertThat(values.size(), equalTo(1));
        assertThat(values.get(0).get(0), equalTo(20));
    }

    private void testIncludeInRootSameType(String node1, String node2) throws Exception {
        String nested = "nest_root_" + getTestName().toLowerCase(Locale.ROOT);
        String object = "obj_root_" + getTestName().toLowerCase(Locale.ROOT);
        createIndexPinned(nested, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "type": "nested", "include_in_root": true, "properties": { "value": { "type": "long" } } } } }""", node1);
        createIndexPinned(object, """
            { "properties": { "id": { "type": "keyword" }, "item": {
              "properties": { "value": { "type": "long" } } } } }""", node2);
        ensureGreen(nested);
        ensureGreen(object);
        for (int i = 0; i < 20; i++) {
            indexDoc(nested, Integer.toString(i), Strings.format("""
                {"id": "n%02d", "item": [{"value": %d}]}""", i, i + 100));
            indexDoc(object, Integer.toString(i), Strings.format("""
                {"id": "o%02d", "item": {"value": %d}}""", i, i + 1));
        }
        refresh(nested);
        refresh(object);

        String from = "FROM " + nested + ", " + object;
        List<List<Object>> values = esql(from + """
             | STATS s = SUM(item.value), c = COUNT(item.value)
            """);
        assertThat(values.size(), equalTo(1));
        // Pre-fix leaked nested 100..119 and summed to 2400 with count 40.
        assertThat(values.get(0).get(0), equalTo(210));
        assertThat(values.get(0).get(1), equalTo(20));

        values = esql(from + """
             | KEEP id, item.value | SORT id | LIMIT 5
            """);
        assertThat(values.size(), equalTo(5));
        assertThat(values.get(0).get(0), equalTo("n00"));
        assertThat(values.get(0).get(1), nullValue());
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
