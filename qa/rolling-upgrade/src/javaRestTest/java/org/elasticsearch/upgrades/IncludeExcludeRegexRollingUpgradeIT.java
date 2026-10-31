/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

/**
 * The length of a terms aggregation's include/exclude pattern is checked by the shard that builds the aggregator, so while a
 * cluster is being upgraded the node holding the shard decides whether a pattern over {@code index.max_regex_length} runs:
 * a shard on an old node accepts it and a shard on an upgraded node rejects it. This pins that for each phase, with a
 * one-shard index placed on a node of each kind.
 */
public class IncludeExcludeRegexRollingUpgradeIT extends AbstractRollingUpgradeTestCase {

    private static final String LENGTH_LIMIT_CAPABILITY = "aggregation_include_exclude_regex_length_limit";
    private static final String INDEX = "include_exclude_regex_length";
    /** Longer than the default {@code index.max_regex_length} of 1000. */
    private static final String TOO_LONG = "a".repeat(1500);

    private final int upgradedNodes;

    public IncludeExcludeRegexRollingUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
        this.upgradedNodes = upgradedNodes;
    }

    public void testShardNodeDecidesWhetherALongPatternRuns() throws IOException {
        if (isOldCluster()) {
            assumeOldNodesPredateTheCheck();
            createOneShardIndex(INDEX, null);
            assertAccepted(INDEX);
        } else if (isMixedCluster()) {
            assumeOldNodesPredateTheCheck();
            Map<String, Node> nodes = nodes();
            String oldNode = nodes.values().stream().filter(Node::old).findFirst().orElseThrow().name();
            String upgradedNode = nodes.values().stream().filter(n -> n.old() == false).findFirst().orElseThrow().name();

            // fresh indices, since a shard cannot move back from an upgraded node to an old one
            String onOld = INDEX + "_on_old_" + upgradedNodes;
            createOneShardIndex(onOld, oldNode);
            assertShardOn(onOld, oldNode, nodes);
            assertAccepted(onOld);

            String onUpgraded = INDEX + "_on_upgraded_" + upgradedNodes;
            createOneShardIndex(onUpgraded, upgradedNode);
            assertShardOn(onUpgraded, upgradedNode, nodes);
            assertRejected(onUpgraded);
        } else if (isUpgradedCluster()) {
            // absent when the old phase was skipped because the old nodes already had the check
            assumeTrue("old nodes must predate the include/exclude length check", indexExists(INDEX));
            assertRejected(INDEX);
        }
    }

    /**
     * Old nodes must lack the check for the old and mixed phases to mean anything. In the mixed phase the capabilities API
     * answers for every node, and the upgraded ones have it, so a negative answer is the old nodes'; an absent answer means
     * the old nodes predate the API itself.
     */
    private static void assumeOldNodesPredateTheCheck() throws IOException {
        boolean oldNodesLackTheCheck = clusterHasCapability("GET", "/_search", List.of(), List.of(LENGTH_LIMIT_CAPABILITY)).map(
            has -> has == false
        ).orElse(true);
        assumeTrue("old nodes must predate the include/exclude length check", oldNodesLackTheCheck);
    }

    private record Node(String name, boolean old) {}

    private static Map<String, Node> nodes() throws IOException {
        ObjectPath response = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/_nodes")));
        Map<String, Object> byId = response.evaluate("nodes");
        Map<String, Node> nodes = new HashMap<>();
        for (String id : byId.keySet()) {
            String name = response.evaluate("nodes." + id + ".name");
            String version = response.evaluate("nodes." + id + ".version");
            String buildHash = response.evaluate("nodes." + id + ".build_hash");
            nodes.put(id, new Node(name, isOldClusterVersion(version, buildHash)));
        }
        return nodes;
    }

    private void createOneShardIndex(String index, String nodeName) throws IOException {
        Settings.Builder settings = Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0);
        if (nodeName != null) {
            settings.put("index.routing.allocation.include._name", nodeName);
        }
        createIndex(index, settings.build(), "\"properties\": {\"str\": {\"type\": \"keyword\"}}");
        Request doc = new Request("POST", "/" + index + "/_doc");
        doc.addParameter("refresh", "true");
        doc.setJsonEntity("{\"str\": \"apple\"}");
        assertOK(client().performRequest(doc));
        ensureGreen(index);
    }

    private static void assertShardOn(String index, String nodeName, Map<String, Node> nodes) throws IOException {
        ObjectPath response = assertOKAndCreateObjectPath(client().performRequest(new Request("GET", "/" + index + "/_search_shards")));
        String nodeId = response.evaluate("shards.0.0.node");
        assertThat(index, nodes.get(nodeId).name(), equalTo(nodeName));
    }

    private static Request search(String index) {
        Request request = new Request("POST", "/" + index + "/_search");
        request.setJsonEntity(
            "{\"size\": 0, \"aggs\": {\"terms_agg\": {\"terms\": {\"field\": \"str\", \"include\": \"" + TOO_LONG + "\"}}}}"
        );
        return request;
    }

    private static void assertAccepted(String index) throws IOException {
        ObjectPath response = assertOKAndCreateObjectPath(client().performRequest(search(index)));
        List<Object> buckets = response.evaluate("aggregations.terms_agg.buckets");
        assertThat(index, buckets, empty());
    }

    private static void assertRejected(String index) throws IOException {
        ResponseException e = expectThrows(ResponseException.class, () -> client().performRequest(search(index)));
        assertThat(index, e.getResponse().getStatusLine().getStatusCode(), equalTo(400));
        String body = EntityUtils.toString(e.getResponse().getEntity(), StandardCharsets.UTF_8);
        assertThat(body, containsString("The length of regex [" + TOO_LONG.length() + "] used in the [include] of an aggregation"));
        assertThat(body, containsString("index.max_regex_length"));
    }
}
