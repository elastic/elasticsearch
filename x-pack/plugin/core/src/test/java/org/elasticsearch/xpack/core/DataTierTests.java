/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core;

import org.elasticsearch.Version;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.StreamSupport;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;

public class DataTierTests extends ESTestCase {

    private static final AtomicInteger idGenerator = new AtomicInteger();

    public void testNodeSelection() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] dataNodes =
            StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
                .map(n -> n.value)
                .filter(DiscoveryNode::isDataNode)
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        final String[] hotNodes =
            StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
                .map(n -> n.value)
                .filter(DataTier::isHotNode)
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        final String[] warmNodes =
            StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
                .map(n -> n.value)
                .filter(DataTier::isWarmNode)
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        final String[] coldNodes =
            StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
                .map(n -> n.value)
                .filter(DataTier::isColdNode)
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        final String[] frozenNodes =
            StreamSupport.stream(discoveryNodes.getNodes().values().spliterator(), false)
                .map(n -> n.value)
                .filter(DataTier::isFrozenNode)
                .map(DiscoveryNode::getId)
                .toArray(String[]::new);

        assertThat(discoveryNodes.resolveNodes("data:true"), arrayContainingInAnyOrder(dataNodes));
        assertThat(discoveryNodes.resolveNodes("data_hot:true"), arrayContainingInAnyOrder(hotNodes));
        assertThat(discoveryNodes.resolveNodes("data_warm:true"), arrayContainingInAnyOrder(warmNodes));
        assertThat(discoveryNodes.resolveNodes("data_cold:true"), arrayContainingInAnyOrder(coldNodes));
        assertThat(discoveryNodes.resolveNodes("data_frozen:true"), arrayContainingInAnyOrder(frozenNodes));
        Set<String> allTiers = new HashSet<>(Arrays.asList(hotNodes));
        allTiers.addAll(Arrays.asList(warmNodes));
        allTiers.addAll(Arrays.asList(coldNodes));
        allTiers.addAll(Arrays.asList(frozenNodes));
        assertThat(discoveryNodes.resolveNodes("data:true"), arrayContainingInAnyOrder(allTiers.toArray(Strings.EMPTY_ARRAY)));
    }

    private static DiscoveryNodes buildDiscoveryNodes() {
        int numNodes = randomIntBetween(3, 15);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = randomNodes(numNodes);
        for (DiscoveryNode node : nodesList) {
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.masterNodeId(randomFrom(nodesList).getId());
        return discoBuilder.build();
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return new DiscoveryNode("name_" + nodeId, "node_" + nodeId, buildNewFakeTransportAddress(), attributes, roles,
            Version.CURRENT);
    }

    private static List<DiscoveryNode> randomNodes(final int numNodes) {
        Set<DiscoveryNodeRole> allRoles = new HashSet<>(DiscoveryNodeRole.BUILT_IN_ROLES);
        allRoles.remove(DiscoveryNodeRole.DATA_ROLE);
        allRoles.add(DataTier.DATA_HOT_NODE_ROLE);
        allRoles.add(DataTier.DATA_WARM_NODE_ROLE);
        allRoles.add(DataTier.DATA_COLD_NODE_ROLE);
        allRoles.add(DataTier.DATA_FROZEN_NODE_ROLE);
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(allRoles));
            if (frequently()) {
                roles.add(new DiscoveryNodeRole("custom_role", "cr") {

                    @Override
                    public Setting<Boolean> legacySetting() {
                        return null;
                    }

                });
            }
            final DiscoveryNode node = newNode(idGenerator.getAndIncrement(), attributes, roles);
            nodesList.add(node);
        }
        return nodesList;
    }
}
