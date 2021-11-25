/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;


import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.notNullValue;

public class NodeShutdownUpgradeIT extends AbstractUpgradeTestCase {
    List<String> namesSorted;
    Map<String, String> nodeNameToIdMap;

    @SuppressWarnings("unchecked")
    @Before
    public void init() throws IOException {
        final Request getNodesReq = new Request("GET", "_nodes");
        final Response getNodesResp = adminClient().performRequest(getNodesReq);
        final Map<String, Map<String, Object>> nodes = (Map<String, Map<String, Object>>) entityAsMap(getNodesResp).get("nodes");
        nodeNameToIdMap = nodes.entrySet().stream()
            .collect(Collectors.toMap(e -> (String) (e.getValue().get("name")), e -> e.getKey()));
        namesSorted = nodeNameToIdMap.keySet().stream()
            .sorted()
            .collect(Collectors.toList());
        System.out.println(namesSorted);
    }

    @SuppressWarnings("unchecked")
    public void testShutdown() throws IOException {
        String nodeIdToShutdown;
        switch (CLUSTER_TYPE) {
            case OLD:
                nodeIdToShutdown = nodeIdToShutdown(0);
                assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));
                break;
            case MIXED:
                if (FIRST_MIXED_ROUND) {
                    nodeIdToShutdown = nodeIdToShutdown(1);
                } else {
                    nodeIdToShutdown = nodeIdToShutdown(2);
                }
                assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));
                break;

            case UPGRADED:
                final Request getShutdownsReq = new Request("GET", "_nodes/shutdown");
                final Response getShutdownsResp = client().performRequest(getShutdownsReq);
                final List<Map<String, Object>> shutdowns = (List<Map<String, Object>>) entityAsMap(getShutdownsResp).get("nodes");
                assertThat("there should be exactly one shutdown registered", shutdowns, hasSize(6));
                final Map<String, Object> shutdown = shutdowns.get(0);
                assertThat(shutdown.get("node_id"), notNullValue()); // Since we randomly determine the node ID, we can't check it
                assertThat(shutdown.get("reason"), equalTo(this.getTestName()));
                assertThat(
                    (String) shutdown.get("status"),
                    anyOf(
                        Arrays.stream(SingleNodeShutdownMetadata.Status.values())
                            .map(value -> equalToIgnoringCase(value.toString()))
                            .collect(Collectors.toList())
                    )
                );
                break;

            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }

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
