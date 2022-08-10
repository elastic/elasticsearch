/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.cluster.metadata.DesiredNode;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;

public class DesiredNodesUpgradeIT extends AbstractRollingTestCase {
    public void testUpgradeDesiredNodes() throws Exception {
        // Desired nodes was introduced in 8.1
        if (UPGRADE_FROM_VERSION.before(Version.V_8_1_0)) {
            return;
        }

        switch (CLUSTER_TYPE) {
            case OLD -> addClusterNodesToDesiredNodesWithIntegerProcessors(1);
            case MIXED -> {
                int version = FIRST_MIXED_ROUND ? 2 : 3;
                if (UPGRADE_FROM_VERSION.onOrAfter(DesiredNode.RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
                    addClusterNodesToDesiredNodesWithFloatProcessorsOrProcessorRanges(version);
                } else {
                    // Processor ranges or float processors are forbidden during upgrades: 8.2 -> 8.3 clusters
                    final var responseException = expectThrows(
                        ResponseException.class,
                        () -> addClusterNodesToDesiredNodesWithFloatProcessorsOrProcessorRanges(version)
                    );
                    final var statusCode = responseException.getResponse().getStatusLine().getStatusCode();
                    assertThat(statusCode, is(equalTo(400)));
                }
            }
            case UPGRADED -> {
                assertAllDesiredNodesAreActualized();
                addClusterNodesToDesiredNodesWithFloatProcessorsOrProcessorRanges(4);
            }
        }

        final var getDesiredNodesRequest = new Request("GET", "/_internal/desired_nodes/_latest");
        final var response = client().performRequest(getDesiredNodesRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(equalTo(200)));
    }

    private void assertAllDesiredNodesAreActualized() throws Exception {
        final var request = new Request("GET", "_cluster/state/metadata");
        final var response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), is(equalTo(200)));
        Map<String, Object> responseMap = responseAsMap(response);
        List<Map<String, Object>> nodes = extractValue(responseMap, "metadata.desired_nodes.latest.nodes");
        assertThat(nodes.size(), is(greaterThan(0)));
        for (Map<String, Object> desiredNode : nodes) {
            final int status = extractValue(desiredNode, "status");
            assertThat((short) status, is(equalTo(DesiredNodeWithStatus.Status.ACTUALIZED.getValue())));
        }
    }

    private void addClusterNodesToDesiredNodesWithFloatProcessorsOrProcessorRanges(int version) throws Exception {
        final List<DesiredNode> nodes;
        if (randomBoolean()) {
            nodes = getNodeNames().stream()
                .map(
                    nodeName -> new DesiredNode(
                        Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                        0.5f,
                        ByteSizeValue.ofGb(randomIntBetween(10, 24)),
                        ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                        Version.CURRENT
                    )
                )
                .toList();
        } else {
            nodes = getNodeNames().stream()
                .map(
                    nodeName -> new DesiredNode(
                        Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                        new DesiredNode.ProcessorsRange(randomIntBetween(1, 10), (float) randomIntBetween(20, 30)),
                        ByteSizeValue.ofGb(randomIntBetween(10, 24)),
                        ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                        Version.CURRENT
                    )
                )
                .toList();
        }
        updateDesiredNodes(nodes, version);
    }

    private void addClusterNodesToDesiredNodesWithIntegerProcessors(int version) throws Exception {
        final var nodes = getNodeNames().stream()
            .map(
                nodeName -> new DesiredNode(
                    Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                    randomIntBetween(1, 24),
                    ByteSizeValue.ofGb(randomIntBetween(10, 24)),
                    ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                    Version.CURRENT
                )
            )
            .toList();
        updateDesiredNodes(nodes, version);
    }

    private void updateDesiredNodes(List<DesiredNode> nodes, int version) throws IOException {
        final var request = new Request("PUT", "/_internal/desired_nodes/history/" + version);
        try (var builder = JsonXContent.contentBuilder()) {
            builder.startObject();
            builder.xContentList(UpdateDesiredNodesRequest.NODES_FIELD.getPreferredName(), nodes);
            builder.endObject();
            request.setJsonEntity(Strings.toString(builder));
            final var response = client().performRequest(request);
            final var statusCode = response.getStatusLine().getStatusCode();
            assertThat(statusCode, equalTo(200));
        }
    }

    private List<String> getNodeNames() throws Exception {
        final var request = new Request("GET", "/_nodes");
        final var response = client().performRequest(request);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Map<String, Object>> nodes = extractValue(responseMap, "nodes");
        final List<String> nodeNames = new ArrayList<>();
        for (Map.Entry<String, Map<String, Object>> nodeInfoEntry : nodes.entrySet()) {
            final String nodeName = extractValue(nodeInfoEntry.getValue(), "name");
            nodeNames.add(nodeName);
        }

        return nodeNames;
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
