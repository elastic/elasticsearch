/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.node;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import org.elasticsearch.Version;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;

public class DiscoveryNodesTests extends ESTestCase {

    public void testResolveNodeByIdOrName() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(DiscoveryNode.class);
        DiscoveryNode node = randomFrom(nodes);
        DiscoveryNode resolvedNode = discoveryNodes.resolveNode(randomBoolean() ? node.getId() : node.getName());
        assertThat(resolvedNode.getId(), equalTo(node.getId()));
    }

    public void testResolveNodeByAttribute() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();
        NodeSelector nodeSelector = randomFrom(NodeSelector.values());
        Set<String> matchingNodeIds = nodeSelector.matchingNodeIds(discoveryNodes);
        try {
            DiscoveryNode resolvedNode = discoveryNodes.resolveNode(nodeSelector.selector);
            assertThat(matchingNodeIds.size(), equalTo(1));
            assertThat(resolvedNode.getId(), equalTo(matchingNodeIds.iterator().next()));
        } catch (IllegalArgumentException e) {
            if (matchingNodeIds.size() == 0) {
                assertThat(e.getMessage(), equalTo("failed to resolve [" + nodeSelector.selector + "], no matching nodes"));
            } else if (matchingNodeIds.size() > 1) {
                assertThat(e.getMessage(), containsString("where expected to be resolved to a single node"));
            } else {
                fail("resolveNode shouldn't have failed for [" + nodeSelector.selector + "]");
            }
        }
    }

    public void testResolveNodesIds() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        int numSelectors = randomIntBetween(1, 5);
        Set<String> nodeSelectors = new HashSet<>();
        Set<String> expectedNodeIdsSet = new HashSet<>();
        for (int i = 0; i < numSelectors; i++) {
            NodeSelector nodeSelector = randomFrom(NodeSelector.values());
            if (nodeSelectors.add(nodeSelector.selector)) {
                expectedNodeIdsSet.addAll(nodeSelector.matchingNodeIds(discoveryNodes));
            }
        }
        int numNodeIds = randomIntBetween(0, 3);
        String[] nodeIds = discoveryNodes.getNodes().keys().toArray(String.class);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
            expectedNodeIdsSet.add(nodeId);
        }
        int numNodeNames = randomIntBetween(0, 3);
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(DiscoveryNode.class);
        for (int i = 0; i < numNodeNames; i++) {
            DiscoveryNode discoveryNode = randomFrom(nodes);
            nodeSelectors.add(discoveryNode.getName());
            expectedNodeIdsSet.add(discoveryNode.getId());
        }

        String[] resolvedNodesIds = discoveryNodes.resolveNodes(nodeSelectors.toArray(new String[nodeSelectors.size()]));
        Arrays.sort(resolvedNodesIds);
        String[] expectedNodesIds = expectedNodeIdsSet.toArray(new String[expectedNodeIdsSet.size()]);
        Arrays.sort(expectedNodesIds);
        assertThat(resolvedNodesIds, equalTo(expectedNodesIds));
    }

    public void testDeltas() {
        Set<DiscoveryNode> nodesA = new HashSet<>();
        nodesA.addAll(randomNodes(1 + randomInt(10)));
        Set<DiscoveryNode> nodesB = new HashSet<>();
        nodesB.addAll(randomNodes(1 + randomInt(5)));
        for (DiscoveryNode node : randomSubsetOf(nodesA)) {
            if (randomBoolean()) {
                // change an attribute
                Map<String, String> attrs = new HashMap<>(node.getAttributes());
                attrs.put("new", "new");
                node = new DiscoveryNode(node.getName(), node.getId(), node.getAddress(), attrs, node.getRoles(), node.getVersion());
            }
            nodesB.add(node);
        }

        DiscoveryNode masterA = randomBoolean() ? null : RandomPicks.randomFrom(random(), nodesA);
        DiscoveryNode masterB = randomBoolean() ? null : RandomPicks.randomFrom(random(), nodesB);

        DiscoveryNodes.Builder builderA = DiscoveryNodes.builder();
        nodesA.stream().forEach(builderA::add);
        final String masterAId = masterA == null ? null : masterA.getId();
        builderA.masterNodeId(masterAId);
        builderA.localNodeId(RandomPicks.randomFrom(random(), nodesA).getId());

        DiscoveryNodes.Builder builderB = DiscoveryNodes.builder();
        nodesB.stream().forEach(builderB::add);
        final String masterBId = masterB == null ? null : masterB.getId();
        builderB.masterNodeId(masterBId);
        builderB.localNodeId(RandomPicks.randomFrom(random(), nodesB).getId());

        final DiscoveryNodes discoNodesA = builderA.build();
        final DiscoveryNodes discoNodesB = builderB.build();
        logger.info("nodes A: {}", discoNodesA);
        logger.info("nodes B: {}", discoNodesB);

        DiscoveryNodes.Delta delta = discoNodesB.delta(discoNodesA);

        if (masterB == null || Objects.equals(masterAId, masterBId)) {
            assertFalse(delta.masterNodeChanged());
            assertThat(delta.previousMasterNode(), nullValue());
            assertThat(delta.newMasterNode(), nullValue());
        } else {
            assertTrue(delta.masterNodeChanged());
            assertThat(delta.newMasterNode().getId(), equalTo(masterBId));
            assertThat(delta.previousMasterNode() != null ? delta.previousMasterNode().getId() : null,
                equalTo(masterAId));
        }

        Set<DiscoveryNode> newNodes = new HashSet<>(nodesB);
        newNodes.removeAll(nodesA);
        assertThat(delta.added(), equalTo(newNodes.isEmpty() == false));
        assertThat(delta.addedNodes(), containsInAnyOrder(newNodes.stream().collect(Collectors.toList()).toArray()));
        assertThat(delta.addedNodes().size(), equalTo(newNodes.size()));

        Set<DiscoveryNode> removedNodes = new HashSet<>(nodesA);
        removedNodes.removeAll(nodesB);
        assertThat(delta.removed(), equalTo(removedNodes.isEmpty() == false));
        assertThat(delta.removedNodes(), containsInAnyOrder(removedNodes.stream().collect(Collectors.toList()).toArray()));
        assertThat(delta.removedNodes().size(), equalTo(removedNodes.size()));
    }

    private static AtomicInteger idGenerator = new AtomicInteger();

    private static List<DiscoveryNode> randomNodes(final int numNodes) {
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAlphaOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(idGenerator.getAndIncrement(), attributes,
                new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))));
            nodesList.add(node);
        }
        return nodesList;
    }

    private static DiscoveryNodes buildDiscoveryNodes() {
        int numNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = randomNodes(numNodes);
        for (DiscoveryNode node : nodesList) {
            discoBuilder = discoBuilder.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.masterNodeId(randomFrom(nodesList).getId());
        return discoBuilder.build();
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode("name_" + nodeId, "node_" + nodeId, buildNewFakeTransportAddress(), attributes, roles,
            Version.CURRENT);
    }

    private enum NodeSelector {
        LOCAL("_local") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getLocalNodeId());
            }
        }, ELECTED_MASTER("_master") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getMasterNodeId());
            }
        }, MASTER_ELIGIBLE(DiscoveryNode.Role.MASTER.getRoleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getMasterNodes().keysIt().forEachRemaining(ids::add);
                return ids;
            }
        }, DATA(DiscoveryNode.Role.DATA.getRoleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getDataNodes().keysIt().forEachRemaining(ids::add);
                return ids;
            }
        }, INGEST(DiscoveryNode.Role.INGEST.getRoleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getIngestNodes().keysIt().forEachRemaining(ids::add);
                return ids;
            }
        }, CUSTOM_ATTRIBUTE("attr:value") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                Set<String> ids = new HashSet<>();
                nodes.getNodes().valuesIt().forEachRemaining(node -> {
                    if ("value".equals(node.getAttributes().get("attr"))) {
                        ids.add(node.getId());
                    }
                });
                return ids;
            }
        };

        private final String selector;

        NodeSelector(String selector) {
            this.selector = selector;
        }

        abstract Set<String> matchingNodeIds(DiscoveryNodes nodes);
    }

    public void testMaxMinNodeVersion() {
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        discoBuilder.add(new DiscoveryNode("name_" + 1, "node_" + 1, buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))),
            Version.fromString("5.1.0")));
        discoBuilder.add(new DiscoveryNode("name_" + 2, "node_" + 2, buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))),
            Version.fromString("6.3.0")));
        discoBuilder.add(new DiscoveryNode("name_" + 3, "node_" + 3, buildNewFakeTransportAddress(), Collections.emptyMap(),
            new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))),
            Version.fromString("1.1.0")));
        discoBuilder.localNodeId("name_1");
        discoBuilder.masterNodeId("name_2");
        DiscoveryNodes build = discoBuilder.build();
        assertEquals( Version.fromString("6.3.0"), build.getMaxNodeVersion());
        assertEquals( Version.fromString("1.1.0"), build.getMinNodeVersion());
    }
}
