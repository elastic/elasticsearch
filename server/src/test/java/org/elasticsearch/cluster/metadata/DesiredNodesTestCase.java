/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.desirednodes.UpdateDesiredNodesRequest;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.common.util.CollectionUtils.concatLists;
import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public abstract class DesiredNodesTestCase extends ESTestCase {
    public static DesiredNodes randomDesiredNodes() {
        return DesiredNodes.create(
            UUIDs.randomBase64UUID(),
            randomIntBetween(1, 20),
            randomList(2, 10, DesiredNodesTestCase::randomDesiredNodeWithStatus)
        );
    }

    public static DesiredNodeWithStatus randomDesiredNodeWithStatus() {
        return new DesiredNodeWithStatus(randomDesiredNode(), randomFrom(DesiredNodeWithStatus.Status.values()));
    }

    public static DesiredNode randomDesiredNode() {
        return randomDesiredNode(Version.CURRENT, Settings.EMPTY);
    }

    public static DesiredNode randomDesiredNode(Settings settings) {
        return randomDesiredNode(Version.CURRENT, settings);
    }

    public static DesiredNode randomDesiredNode(Version version, Settings settings) {
        if (randomBoolean()) {
            return randomDesiredNode(version, settings, randomProcessorRange());
        } else {
            return randomDesiredNode(version, settings, randomIntBetween(1, 256) + randomFloat());
        }
    }

    public static DesiredNode randomDesiredNode(Version version, Settings settings, float processors) {
        return new DesiredNode(
            addExternalIdIfMissing(settings),
            processors,
            ByteSizeValue.ofGb(randomIntBetween(1, 1024)),
            ByteSizeValue.ofTb(randomIntBetween(1, 40)),
            version
        );
    }

    public static DesiredNode randomDesiredNode(Version version, Settings settings, DesiredNode.ProcessorsRange processorsRange) {
        return new DesiredNode(
            addExternalIdIfMissing(settings),
            processorsRange,
            ByteSizeValue.ofGb(randomIntBetween(1, 1024)),
            ByteSizeValue.ofTb(randomIntBetween(1, 40)),
            version
        );
    }

    public static DesiredNode.ProcessorsRange randomProcessorRange() {
        float minProcessors = randomFloat() + randomIntBetween(1, 16);
        return new DesiredNode.ProcessorsRange(minProcessors, randomBoolean() ? null : minProcessors + randomIntBetween(0, 10));
    }

    private static Settings addExternalIdIfMissing(Settings settings) {
        final var externalId = NODE_EXTERNAL_ID_SETTING.get(settings);
        if (externalId.isBlank() == false) {
            return settings;
        }

        final var settingsBuilder = Settings.builder();
        settingsBuilder.put(settings);
        if (randomBoolean()) {
            settingsBuilder.put(NODE_NAME_SETTING.getKey(), randomAlphaOfLength(10));
        } else {
            settingsBuilder.put(NODE_EXTERNAL_ID_SETTING.getKey(), randomAlphaOfLength(10));
        }

        return settingsBuilder.build();
    }

    public static void assertDesiredNodesStatusIsCorrect(
        ClusterState clusterState,
        List<DesiredNode> expectedActualizedNodes,
        List<DesiredNode> expectedPendingNodes
    ) {
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        assertDesiredNodesStatusIsCorrect(desiredNodes, expectedActualizedNodes, expectedPendingNodes);
    }

    public static void assertDesiredNodesStatusIsCorrect(
        DesiredNodes desiredNodes,
        List<DesiredNode> expectedActualizedNodes,
        List<DesiredNode> expectedPendingNodes
    ) {
        assertThat(expectedActualizedNodes, hasSize(desiredNodes.actualized().size()));
        assertThat(desiredNodes.actualized(), is(equalTo(Set.copyOf(expectedActualizedNodes))));

        assertThat(expectedPendingNodes, hasSize(desiredNodes.pending().size()));
        assertThat(desiredNodes.pending(), is(equalTo(Set.copyOf(expectedPendingNodes))));
    }

    public static UpdateDesiredNodesRequest randomUpdateDesiredNodesRequest() {
        return new UpdateDesiredNodesRequest(
            UUIDs.randomBase64UUID(random()),
            randomLongBetween(0, Long.MAX_VALUE - 1000),
            randomList(1, 100, DesiredNodesTestCase::randomDesiredNode),
            randomBoolean()
        );
    }

    public static ClusterState createClusterStateWithDiscoveryNodesAndDesiredNodes(
        List<DesiredNodeWithStatus> actualizedDesiredNodes,
        List<DesiredNodeWithStatus> pendingDesiredNodes,
        List<DesiredNodeWithStatus> joiningDesiredNodes,
        boolean withElectedMaster,
        boolean withJoiningNodesInDiscoveryNodes
    ) {
        final var masterNode = newDiscoveryNode("master");
        final var discoveryNodes = DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId());

        if (withElectedMaster) {
            discoveryNodes.masterNodeId(masterNode.getId());
        }

        for (DesiredNodeWithStatus actualizedDesiredNode : actualizedDesiredNodes) {
            discoveryNodes.add(newDiscoveryNode(actualizedDesiredNode.externalId()));
        }

        if (withJoiningNodesInDiscoveryNodes) {
            for (DesiredNodeWithStatus joiningDesiredNode : joiningDesiredNodes) {
                discoveryNodes.add(newDiscoveryNode(joiningDesiredNode.externalId()));
            }
        }

        // Add some nodes in the cluster that are not part of the desired nodes
        int extraNodesCount = randomInt(5);
        for (int i = 0; i < extraNodesCount; i++) {
            discoveryNodes.add(newDiscoveryNode(UUIDs.randomBase64UUID(random())));
        }

        final var desiredNodes = DesiredNodes.create(
            randomAlphaOfLength(10),
            randomInt(10),
            concatLists(concatLists(actualizedDesiredNodes, pendingDesiredNodes), joiningDesiredNodes)
        );

        return ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodes)
            .metadata(Metadata.builder().putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodes)))
            .build();
    }

    public static DiscoveryNode newDiscoveryNode(String nodeName) {
        return new DiscoveryNode(
            nodeName,
            UUIDs.randomBase64UUID(random()),
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
    }
}
