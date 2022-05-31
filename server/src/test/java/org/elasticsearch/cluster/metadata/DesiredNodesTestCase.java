/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.node.Node.NODE_EXTERNAL_ID_SETTING;
import static org.elasticsearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;

public abstract class DesiredNodesTestCase extends ESTestCase {
    public static DesiredNodes randomDesiredNodesWithRandomSettings() {
        return randomDesiredNodes(DesiredNodesTestCase::putRandomSetting);
    }

    public static DesiredNodes randomDesiredNodesWithRandomSettings(Version version) {
        return randomDesiredNodes(version, DesiredNodesTestCase::putRandomSetting);
    }

    public static DesiredNodes randomDesiredNodes() {
        return randomDesiredNodes((settings) -> {});
    }

    public static DesiredNodes randomDesiredNodes(Consumer<Settings.Builder> settingsConsumer) {
        return randomDesiredNodes(Version.CURRENT, settingsConsumer);
    }

    public static DesiredNodes randomDesiredNodes(Version version, Consumer<Settings.Builder> settingsConsumer) {
        return new DesiredNodes(UUIDs.randomBase64UUID(), randomIntBetween(1, 20), randomDesiredNodeList(version, settingsConsumer));
    }

    public static List<DesiredNode> randomDesiredNodeListWithRandomSettings(Version version) {
        return randomDesiredNodeList(version, DesiredNodesTestCase::putRandomSetting);
    }

    public static List<DesiredNode> randomDesiredNodeList(Version version, Consumer<Settings.Builder> settingsConsumer) {
        return randomList(2, 10, () -> randomDesiredNode(version, settingsConsumer));
    }

    public static DesiredNode randomDesiredNodeWithName(String nodeName) {
        return randomDesiredNode(Version.CURRENT, settings -> {
            settings.remove(NODE_EXTERNAL_ID_SETTING.getKey());
            settings.put(NODE_NAME_SETTING.getKey(), nodeName);
        });
    }

    public static DesiredNode randomDesiredNodeWithExternalId(String externalId) {
        return randomDesiredNode(Version.CURRENT, settings -> {
            settings.remove(NODE_NAME_SETTING.getKey());
            settings.put(NODE_EXTERNAL_ID_SETTING.getKey(), externalId);
        });
    }

    public static DesiredNode randomDesiredNodeWithRandomSettings() {
        return randomDesiredNodeWithRandomSettings(Version.CURRENT);
    }

    public static DesiredNode randomDesiredNodeWithRandomSettings(Version version) {
        return randomDesiredNode(version, DesiredNodesTestCase::putRandomSetting);
    }

    public static DesiredNode randomDesiredNode(Version version, Consumer<Settings.Builder> settingsProvider) {
        if (randomBoolean()) {
            return randomDesiredNode(version, randomProcessor(), settingsProvider);
        } else {
            return randomDesiredNode(version, randomIntBetween(1, 256) + randomFloat(), settingsProvider);
        }
    }

    public static DesiredNode randomDesiredNode(Version version, float processors, Consumer<Settings.Builder> settingsProvider) {
        return new DesiredNode(
            randomSettings(settingsProvider),
            processors,
            ByteSizeValue.ofGb(randomIntBetween(1, 1024)),
            ByteSizeValue.ofTb(randomIntBetween(1, 40)),
            version
        );
    }

    public static DesiredNode randomDesiredNode(
        Version version,
        DesiredNode.ProcessorsRange processorsRange,
        Consumer<Settings.Builder> settingsProvider
    ) {
        return new DesiredNode(
            randomSettings(settingsProvider),
            processorsRange,
            ByteSizeValue.ofGb(randomIntBetween(1, 1024)),
            ByteSizeValue.ofTb(randomIntBetween(1, 40)),
            version
        );
    }

    private static DesiredNode.ProcessorsRange randomProcessor() {
        float minProcessors = randomFloat() + randomIntBetween(1, 16);
        return new DesiredNode.ProcessorsRange(minProcessors, randomBoolean() ? null : minProcessors + randomIntBetween(0, 10));
    }

    public static Settings randomSettings(Consumer<Settings.Builder> settingsProvider) {
        int numSettings = randomIntBetween(1, 20);
        Settings.Builder settingsBuilder = Settings.builder();
        if (randomBoolean()) {
            settingsBuilder.put(NODE_EXTERNAL_ID_SETTING.getKey(), UUIDs.randomBase64UUID());
        } else {
            settingsBuilder.put(NODE_NAME_SETTING.getKey(), UUIDs.randomBase64UUID());
        }

        for (int i = 0; i < numSettings; i++) {
            settingsProvider.accept(settingsBuilder);
        }
        return settingsBuilder.build();
    }

    private static void putRandomSetting(Settings.Builder settings) {
        final String key = randomAlphaOfLength(10);
        switch (randomIntBetween(0, 7)) {
            case 0 -> settings.put(key, randomAlphaOfLength(20));
            case 1 -> settings.put(key, randomInt());
            case 2 -> settings.put(key, randomLong());
            case 3 -> settings.put(key, randomFloat());
            case 4 -> settings.put(key, randomDouble());
            case 5 -> settings.put(key, TimeValue.timeValueMillis(randomIntBetween(1, 1000)));
            case 6 -> settings.put(key, ByteSizeValue.ofGb(randomIntBetween(1, 20)));
            case 7 -> settings.putList(key, randomList(1, 10, () -> randomAlphaOfLength(20)));
            default -> throw new IllegalArgumentException();
        }
    }

    @SafeVarargs
    public static DesiredNodes createDesiredNodes(List<DesiredNode>... nodeLists) {
        return createDesiredNodes(UUIDs.randomBase64UUID(random()), 1, nodeLists);
    }

    @SafeVarargs
    public static DesiredNodes createDesiredNodes(String historyId, long version, List<DesiredNode>... nodeLists) {
        assertThat(nodeLists.length, is(greaterThan(0)));
        final List<DesiredNode> desiredNodes = new ArrayList<>();
        for (List<DesiredNode> nodeList : nodeLists) {
            desiredNodes.addAll(nodeList);
        }
        return new DesiredNodes(historyId, version, desiredNodes);
    }

    public static void assertDesiredNodesMembershipIsCorrect(
        ClusterState clusterState,
        List<DesiredNode> expectedMembersList,
        List<DesiredNode> expectedUnknownNodesList
    ) {
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        assertDesiredNodesMembershipIsCorrect(desiredNodes, expectedMembersList, expectedUnknownNodesList);
    }

    public static void assertDesiredNodesMembershipIsCorrect(
        DesiredNodes desiredNodes,
        List<DesiredNode> expectedMembersList,
        List<DesiredNode> expectedUnknownNodesList
    ) {
        final var expectedMembers = expectedMembersList.stream().collect(Collectors.toMap(DesiredNode::externalId, Function.identity()));
        final var expectedUnknownNodes = expectedUnknownNodesList.stream()
            .collect(Collectors.toMap(DesiredNode::externalId, Function.identity()));

        for (DesiredNode desiredNode : desiredNodes.members()) {
            assertThat(desiredNode.isMember(), is(equalTo(true)));
            assertThat("member not found", expectedMembers, hasKey(desiredNode.externalId()));
        }

        for (DesiredNode desiredNode : desiredNodes.notMembers()) {
            assertThat(desiredNode.isMember(), is(equalTo(false)));
            assertThat("not member not found", expectedUnknownNodes, hasKey(desiredNode.externalId()));
        }
    }
}
