/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.shutdown;

import com.carrotsearch.randomizedtesting.annotations.Name;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.hamcrest.Matcher;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;

/**
 * Exercises node shutdown records across a rolling upgrade: a node is marked for shutdown in the
 * old cluster, another once one node is upgraded, and a third once two nodes are upgraded, then
 * all three are asserted to still be {@code COMPLETE} once the cluster is fully upgraded.
 * <p>
 * Note: because each parameterized stage ({@code upgradedNodes=0,1,2,3}) both creates new
 * shutdown records and asserts on records created by earlier stages, this test is not safe under
 * per-test smart-retry (rerunning e.g. only {@code upgradedNodes=2} in isolation would spuriously
 * fail, since the shutdown calls performed by the skipped earlier stages would never have run in
 * that JVM). The whole task must be rerun together on failure.
 */
public class NodeShutdownUpgradeIT extends NodeShutdownRollingUpgradeTestCase {

    /**
     * Stable reason string written into every shutdown record. Must not change across parameterized
     * stages — {@code getTestName()} includes the current {@code upgradedNodes} parameter and would
     * produce a different value in each stage, causing cross-stage assertions to fail.
     */
    private static final String SHUTDOWN_REASON = "NodeShutdownUpgradeIT#testShutdown";

    List<String> namesSorted;
    Map<String, String> nodeNameToIdMap;

    public NodeShutdownUpgradeIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IOException {
        final Request getNodesReq = new Request("GET", "_nodes");
        final Response getNodesResp = adminClient().performRequest(getNodesReq);
        final Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) entityAsMap(getNodesResp).get("nodes");
        nodeNameToIdMap = nodes.entrySet().stream().collect(Collectors.toMap(e -> (String) (e.getValue().get("name")), e -> e.getKey()));
        namesSorted = nodeNameToIdMap.keySet().stream().sorted().collect(Collectors.toList());
    }

    public void testShutdown() throws Exception {
        String nodeIdToShutdown;
        if (isOldCluster()) {
            nodeIdToShutdown = nodeIdToShutdown(0);
            assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

            assertBusy(() -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0))));
        } else if (isFirstMixedCluster()) {
            // after upgrade the record still exist
            assertBusy(() -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0))));

            nodeIdToShutdown = nodeIdToShutdown(1);
            assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

            assertBusy(
                () -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1)))
            );
        } else if (isMixedCluster()) {
            assertBusy(
                () -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1)))
            );

            nodeIdToShutdown = nodeIdToShutdown(2);
            assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

            assertBusy(
                () -> assertThat(
                    getShutdownStatus(),
                    containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1), shutdownStatusCompleteFor(2))
                )
            );
        } else if (isUpgradedCluster()) {
            assertBusy(
                () -> assertThat(
                    getShutdownStatus(),
                    containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1), shutdownStatusCompleteFor(2))
                )
            );
        } else {
            throw new AssertionError("Unknown cluster upgrade stage");
        }
    }

    private Matcher<Map<String, Object>> shutdownStatusCompleteFor(int i) {
        return allOf(
            hasEntry("node_id", nodeIdToShutdown(i)),
            hasEntry("reason", SHUTDOWN_REASON),
            hasEntry("status", SingleNodeShutdownMetadata.Status.COMPLETE.toString())
        );
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> getShutdownStatus() throws IOException {
        final Request getShutdownsReq = new Request("GET", "_nodes/shutdown");
        final Response getShutdownsResp = client().performRequest(getShutdownsReq);
        return (List<Map<String, Object>>) entityAsMap(getShutdownsResp).get("nodes");
    }

    private Request shutdownNode(String nodeIdToShutdown) throws IOException {
        final Request putShutdownRequest = new Request("PUT", "_nodes/" + nodeIdToShutdown + "/shutdown");
        try (XContentBuilder putBody = JsonXContent.contentBuilder()) {
            putBody.startObject();
            {
                putBody.field("type", "restart");
                putBody.field("reason", SHUTDOWN_REASON);
            }
            putBody.endObject();
            putShutdownRequest.setJsonEntity(Strings.toString(putBody));
        }
        return putShutdownRequest;
    }

    private String nodeIdToShutdown(int nodeNumber) {
        final String nodeName = namesSorted.get(nodeNumber);
        return nodeNameToIdMap.get(nodeName);
    }
}
