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
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.hamcrest.Matchers.hasSize;

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
                assertBusy(
                    () -> assertThat(getShutdownStatus(nodeIdToShutdown), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE)),
                    30,
                    TimeUnit.SECONDS
                );
                break;

            case MIXED:
                if (FIRST_MIXED_ROUND) {
                    assertBusy(
                        () -> assertThat(getShutdownStatus(nodeIdToShutdown(0)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE)),
                        30,
                        TimeUnit.SECONDS
                    );
                    nodeIdToShutdown = nodeIdToShutdown(1);
                } else {
                    nodeIdToShutdown = nodeIdToShutdown(2);
                    assertBusy(
                        () -> {
                            assertThat(getShutdownStatus(nodeIdToShutdown(0)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
                            assertThat(getShutdownStatus(nodeIdToShutdown(1)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
                        },
                        30,
                        TimeUnit.SECONDS
                    );
                }
                assertOK(client().performRequest(shutdownNode(nodeIdToShutdown)));
                assertBusy(
                    () -> assertThat(getShutdownStatus(nodeIdToShutdown), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE)),
                    30,
                    TimeUnit.SECONDS
                );
                break;

            case UPGRADED:
                assertBusy(
                    () -> {
                        assertThat(getShutdownStatus(nodeIdToShutdown(0)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
                        assertThat(getShutdownStatus(nodeIdToShutdown(1)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
                        assertThat(getShutdownStatus(nodeIdToShutdown(2)), equalTo(SingleNodeShutdownMetadata.Status.COMPLETE));
                    },
                    30,
                    TimeUnit.SECONDS
                );
                break;
            default:
                throw new UnsupportedOperationException("Unknown cluster type [" + CLUSTER_TYPE + "]");
        }
    }

    @SuppressWarnings("unchecked")
    private SingleNodeShutdownMetadata.Status getShutdownStatus(String nodeIdToShutdown) throws IOException {
        final Request getShutdownsReq = new Request("GET", "_nodes/shutdown");
        final Response getShutdownsResp = client().performRequest(getShutdownsReq);
        final List<Map<String, Object>> shutdowns = (List<Map<String, Object>>) entityAsMap(getShutdownsResp).get("nodes");
        assertThat("there should be exactly one shutdown registered", shutdowns, hasSize(1));
        final Map<String, Object> shutdown = shutdowns.get(0);
        assertThat(shutdown.get("node_id"), equalTo(nodeIdToShutdown));
        assertThat(shutdown.get("reason"), equalTo(this.getTestName()));
        assertThat(
            (String) shutdown.get("status"),
            anyOf(
                Arrays.stream(SingleNodeShutdownMetadata.Status.values())
                    .map(value -> equalToIgnoringCase(value.toString()))
                    .collect(Collectors.toList())
            )
        );
        return SingleNodeShutdownMetadata.Status.valueOf((String) shutdown.get("status"));
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
