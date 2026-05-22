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
import org.elasticsearch.xpack.esql.CsvTestsDataLoader;
import org.junit.Before;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.MapMatcher.assertMap;
import static org.elasticsearch.test.MapMatcher.matchesMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * Tests for {@snippet lang="esql" :
 * | STATS COUNT_DISTINCT(CASE(foldable, ...))
 * }
 * when foldable at different places.
 */
@ThreadLeakFilters(filters = TestClustersThreadFilter.class)
public class AggOnCaseFoldIT extends ESRestTestCase {

    @ClassRule
    public static ElasticsearchCluster cluster = Clusters.testCluster(ignored -> {});

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Before
    public void enableChangeLogging() throws IOException {
        Request request = new Request("PUT", "/_cluster/settings");
        request.setJsonEntity(
            "{\"transient\": {\"logger.org.elasticsearch.xpack.esql.optimizer.LocalLogicalPlanOptimizer.changes\": \"TRACE\"}}"
        );
        assertOK(client().performRequest(request));
    }

    @Before
    public void loadAndPinIndices() throws Exception {
        assumeFalse("Cannot pin shards to specific nodes in serverless mode", isServerless());
        CsvTestsDataLoader.loadDatasetsIntoEs(client(), List.of("event_alerts", "event_logs"));

        // Pin each index to a different node so they are optimized independently of one another.
        Request nodesRequest = new Request("GET", "/_nodes");
        nodesRequest.addParameter("filter_path", "nodes.*.name");
        Map<String, Object> nodesResponse = entityAsMap(client().performRequest(nodesRequest));
        @SuppressWarnings("unchecked")
        Map<String, Object> nodes = (Map<String, Object>) nodesResponse.get("nodes");
        List<String> nodeNames = new ArrayList<>();
        for (Object nodeInfo : nodes.values()) {
            @SuppressWarnings("unchecked")
            Map<String, Object> info = (Map<String, Object>) nodeInfo;
            nodeNames.add((String) info.get("name"));
        }
        assertThat("Need at least 2 nodes", nodeNames.size(), greaterThanOrEqualTo(2));

        Request pinAlerts = new Request("PUT", "/event_alerts/_settings");
        pinAlerts.setJsonEntity(Strings.format("""
            {
              "index.routing.allocation.require._name": "%s",
              "index.number_of_replicas": 0
            }
            """, nodeNames.get(0)));
        assertOK(client().performRequest(pinAlerts));

        Request pinLogs = new Request("PUT", "/event_logs/_settings");
        pinLogs.setJsonEntity(Strings.format("""
            {
              "index.routing.allocation.require._name": "%s",
              "index.number_of_replicas": 0
            }
            """, nodeNames.get(1)));
        assertOK(client().performRequest(pinLogs));

        ensureGreen("event_alerts");
        ensureGreen("event_logs");

        // Paranoidly wait until the shards have landed on the nodes we expect
        Request shardsRequest = new Request("GET", "/_cat/shards/event_alerts,event_logs");
        shardsRequest.addParameter("format", "json");
        assertBusy(() -> {
            List<Object> shards = entityAsList(client().performRequest(shardsRequest));
            Map<String, List<Map<String, Object>>> shardByIndex = new HashMap<>();
            for (Object entry : shards) {
                @SuppressWarnings("unchecked")
                Map<String, Object> shard = (Map<String, Object>) entry;
                shardByIndex.computeIfAbsent((String) shard.get("index"), k -> new ArrayList<>()).add(shard);
            }
            String ctx = "nodeNames=" + nodeNames + " shards=" + shards;
            List<Map<String, Object>> alertsShards = shardByIndex.get("event_alerts");
            assertNotNull("event_alerts shards not found in: " + ctx, alertsShards);
            for (Map<String, Object> shard : alertsShards) {
                assertThat(ctx + " event_alerts shard must be STARTED", shard.get("state"), equalTo("STARTED"));
                assertThat(ctx + " event_alerts shard on wrong node", shard.get("node"), equalTo(nodeNames.get(0)));
            }
            List<Map<String, Object>> logsShards = shardByIndex.get("event_logs");
            assertNotNull("event_logs shards not found in: " + ctx, logsShards);
            for (Map<String, Object> shard : logsShards) {
                assertThat(ctx + " event_logs shard must be STARTED", shard.get("state"), equalTo("STARTED"));
                assertThat(ctx + " event_logs shard on wrong node", shard.get("node"), equalTo(nodeNames.get(1)));
            }
        }, 30, TimeUnit.SECONDS);
    }

    public void testCoordinatingNodeFoldInline() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | STATS COUNT_DISTINCT(CASE(false, username, null))
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldInline() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | STATS COUNT_DISTINCT(CASE(event_type == "alert", username, null))
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldEval() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL username = CASE(event_type == "alert", username, null)
            | STATS COUNT_DISTINCT(username)
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldEvalAnd() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL foo = CASE(event_type == "alert" AND severity > 0, username, null)
            | STATS COUNT_DISTINCT(foo)
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldEvalAndTwoBranches() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL foo = CASE(event_type == "alert" AND severity > 0, username,
                              event_type == "alert" AND severity > 1, label,
                              null)
            | STATS COUNT_DISTINCT(foo)
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldInlineTwoBranches() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | STATS COUNT_DISTINCT(CASE(event_type == "alert" AND severity > 0, username,
                                        event_type == "alert" AND severity > 1, label,
                                        null))
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeFoldEvalOr() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL foo = CASE(event_type == "alert" OR severity > 0, username, null)
            | STATS COUNT_DISTINCT(foo)
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeNotIn() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL foo = CASE(NOT event_type IN ("login", "other"), username, null)
            | STATS COUNT_DISTINCT(foo)
            """);
        assertThat(values, equalTo(List.of(List.of(0))));
    }

    public void testDataNodeMultipleCases() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL severity_case = CASE(event_type == "alert", severity, null),
                   username_case = CASE(event_type == "login", username, null)
            | STATS COUNT_DISTINCT(severity_case), COUNT_DISTINCT(username_case)
              BY ts_month = DATE_TRUNC(1 month, @timestamp)
            | SORT ts_month
            """);
        assertThat(values, equalTo(List.of(List.of(3, 0, "2024-01-01T00:00:00.000Z"), List.of(0, 3, "2024-02-01T00:00:00.000Z"))));
    }

    public void testDataNodeMultipleCasesAnd() throws IOException {
        List<List<Object>> values = esql("""
            FROM event_alerts, event_logs
            | EVAL case1 = CASE(event_type == "alert" AND severity > 0, username, null),
                   case2 = CASE(NOT event_type IN ("alert", "other"), username, null)
            | STATS COUNT_DISTINCT(case1), COUNT_DISTINCT(case2)
              BY ts_month = DATE_TRUNC(1 month, @timestamp)
            | SORT ts_month
            """);
        assertThat(values, equalTo(List.of(List.of(0, 0, "2024-01-01T00:00:00.000Z"), List.of(0, 3, "2024-02-01T00:00:00.000Z"))));
    }

    /**
     * Returns true when the cluster is running in serverless mode, where index settings like
     * {@code index.number_of_replicas} and shard allocation filters are not available.
     */
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

    /**
     * Runs an ES|QL query, asserts the result is not partial, and returns the values rows.
     * The query may be a text block with newlines between pipe stages.
     */
    private List<List<Object>> esql(String query) throws IOException {
        Request request = new Request("POST", "/_query");
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
