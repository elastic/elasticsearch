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
import org.elasticsearch.common.unit.Processors;
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
    private enum ProcessorsPrecision {
        DOUBLE,
        FLOAT
    }

    public void testUpgradeDesiredNodes() throws Exception {
        // Desired nodes was introduced in 8.1
        if (UPGRADE_FROM_VERSION.before(Version.V_8_1_0)) {
            return;
        }

        if (UPGRADE_FROM_VERSION.onOrAfter(Processors.DOUBLE_PROCESSORS_SUPPORT_VERSION)) {
            assertUpgradedNodesCanReadDesiredNodes();
        } else if (UPGRADE_FROM_VERSION.onOrAfter(DesiredNode.RANGE_FLOAT_PROCESSORS_SUPPORT_VERSION)) {
            assertDesiredNodesUpdatedWithRoundedUpFloatsAreIdempotent();
        } else {
            assertDesiredNodesWithFloatProcessorsAreRejectedInOlderVersions();
        }
    }

    private void assertUpgradedNodesCanReadDesiredNodes() throws Exception {
        final int desiredNodesVersion = switch (CLUSTER_TYPE) {
            case OLD -> 1;
            case MIXED -> FIRST_MIXED_ROUND ? 2 : 3;
            case UPGRADED -> 4;
        };

        if (CLUSTER_TYPE != ClusterType.OLD) {
            final Map<String, Object> desiredNodes = getLatestDesiredNodes();
            final String historyId = extractValue(desiredNodes, "history_id");
            final int version = extractValue(desiredNodes, "version");
            assertThat(historyId, is(equalTo("upgrade_test")));
            assertThat(version, is(equalTo(desiredNodesVersion - 1)));
        }

        addClusterNodesToDesiredNodesWithProcessorsOrProcessorRanges(desiredNodesVersion, ProcessorsPrecision.DOUBLE);
        assertAllDesiredNodesAreActualized();
    }

    private void assertDesiredNodesUpdatedWithRoundedUpFloatsAreIdempotent() throws Exception {
        // We define the same set of desired nodes to ensure that they are equal across all
        // the test runs, otherwise we cannot guarantee an idempotent update in this test
        final var desiredNodes = getNodeNames().stream()
            .map(
                nodeName -> new DesiredNode(
                    Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                    1238.49922909,
                    ByteSizeValue.ofGb(32),
                    ByteSizeValue.ofGb(128),
                    Version.CURRENT
                )
            )
            .toList();

        final int desiredNodesVersion = switch (CLUSTER_TYPE) {
            case OLD -> 1;
            case MIXED -> FIRST_MIXED_ROUND ? 2 : 3;
            case UPGRADED -> 4;
        };

        if (CLUSTER_TYPE != ClusterType.OLD) {
            updateDesiredNodes(desiredNodes, desiredNodesVersion - 1);
        }
        for (int i = 0; i < 2; i++) {
            updateDesiredNodes(desiredNodes, desiredNodesVersion);
        }

        final Map<String, Object> latestDesiredNodes = getLatestDesiredNodes();
        final int latestDesiredNodesVersion = extractValue(latestDesiredNodes, "version");
        assertThat(latestDesiredNodesVersion, is(equalTo(desiredNodesVersion)));

        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertAllDesiredNodesAreActualized();
        }
    }

    private void assertDesiredNodesWithFloatProcessorsAreRejectedInOlderVersions() throws Exception {
        switch (CLUSTER_TYPE) {
            case OLD -> addClusterNodesToDesiredNodesWithIntegerProcessors(1);
            case MIXED -> {
                int version = FIRST_MIXED_ROUND ? 2 : 3;
                // Processor ranges or float processors are forbidden during upgrades: 8.2 -> 8.3 clusters
                final var responseException = expectThrows(
                    ResponseException.class,
                    () -> addClusterNodesToDesiredNodesWithProcessorsOrProcessorRanges(version, ProcessorsPrecision.FLOAT)
                );
                final var statusCode = responseException.getResponse().getStatusLine().getStatusCode();
                assertThat(statusCode, is(equalTo(400)));
            }
            case UPGRADED -> {
                assertAllDesiredNodesAreActualized();
                addClusterNodesToDesiredNodesWithProcessorsOrProcessorRanges(4, ProcessorsPrecision.FLOAT);
            }
        }

        getLatestDesiredNodes();
    }

    private Map<String, Object> getLatestDesiredNodes() throws IOException {
        final var getDesiredNodesRequest = new Request("GET", "/_internal/desired_nodes/_latest");
        final var response = client().performRequest(getDesiredNodesRequest);
        assertThat(response.getStatusLine().getStatusCode(), is(equalTo(200)));
        return responseAsMap(response);
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

    private void addClusterNodesToDesiredNodesWithProcessorsOrProcessorRanges(int version, ProcessorsPrecision processorsPrecision)
        throws Exception {
        final List<DesiredNode> nodes;
        if (randomBoolean()) {
            nodes = getNodeNames().stream()
                .map(
                    nodeName -> new DesiredNode(
                        Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                        processorsPrecision == ProcessorsPrecision.DOUBLE ? randomDoubleProcessorCount() : randomFloatProcessorCount(),
                        ByteSizeValue.ofGb(randomIntBetween(10, 24)),
                        ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                        Version.CURRENT
                    )
                )
                .toList();
        } else {
            nodes = getNodeNames().stream().map(nodeName -> {
                double minProcessors = processorsPrecision == ProcessorsPrecision.DOUBLE
                    ? randomDoubleProcessorCount()
                    : randomFloatProcessorCount();
                return new DesiredNode(
                    Settings.builder().put(NODE_NAME_SETTING.getKey(), nodeName).build(),
                    new DesiredNode.ProcessorsRange(minProcessors, minProcessors + randomIntBetween(10, 20)),
                    ByteSizeValue.ofGb(randomIntBetween(10, 24)),
                    ByteSizeValue.ofGb(randomIntBetween(128, 256)),
                    Version.CURRENT
                );
            }).toList();
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
        final var request = new Request("PUT", "/_internal/desired_nodes/upgrade_test/" + version);
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

    private double randomDoubleProcessorCount() {
        return randomDoubleBetween(0.5, 512.1234, true);
    }

    private float randomFloatProcessorCount() {
        return randomIntBetween(1, 512) + randomFloat();
    }

    @SuppressWarnings("unchecked")
    private static <T> T extractValue(Map<String, Object> map, String path) {
        return (T) XContentMapValues.extractValue(path, map);
    }
}
