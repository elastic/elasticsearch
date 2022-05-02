/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

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

public class NodeShutdownUpgradeIT extends AbstractUpgradeTestCase {
    List<String> namesSorted;
    Map<String, String> nodeNameToIdMap;

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
        switch (CLUSTER_TYPE) {
            case OLD:
                nodeIdToShutdown = nodeIdToShutdown(0);
                assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

                assertBusy(() -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0))));
                break;

            case MIXED:
                if (FIRST_MIXED_ROUND) {
                    // after upgrade the record still exist
                    assertBusy(() -> assertThat(getShutdownStatus(), containsInAnyOrder(shutdownStatusCompleteFor(0))));

                    nodeIdToShutdown = nodeIdToShutdown(1);
                    assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

                    assertBusy(
                        () -> assertThat(
                            getShutdownStatus(),
                            containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1))
                        )
                    );

                } else {
                    assertBusy(
                        () -> assertThat(
                            getShutdownStatus(),
                            containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1))
                        )
                    );

                    nodeIdToShutdown = nodeIdToShutdown(2);
                    assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));

                    assertBusy(
                        () -> assertThat(
                            getShutdownStatus(),
                            containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1), shutdownStatusCompleteFor(2))
                        )
                    );
                }

                break;

            case UPGRADED:
                assertBusy(
                    () -> assertThat(
                        getShutdownStatus(),
                        containsInAnyOrder(shutdownStatusCompleteFor(0), shutdownStatusCompleteFor(1), shutdownStatusCompleteFor(2))
                    )
                );
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    private Matcher<Map<String, Object>> shutdownStatusCompleteFor(int i) {
        return allOf(
            hasEntry("node_id", nodeIdToShutdown(i)),
            hasEntry("reason", this.getTestName()),
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
                putBody.field("reason", this.getTestName());
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
