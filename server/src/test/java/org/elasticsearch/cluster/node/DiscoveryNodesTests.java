/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.node;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
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
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.ObjLongConsumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

public class DiscoveryNodesTests extends ESTestCase {

    public void testResolveNodeByIdOrName() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(DiscoveryNode[]::new);
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

    public void testResolveNodesNull() {
        DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        // if assertions are enabled (should be the case for tests, but not in production), resolving null throws
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes(Collections.singletonList(null).toArray(new String[0])));
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes(null, "someNode"));
        expectThrows(AssertionError.class, () -> discoveryNodes.resolveNodes("someNode", null, "someOtherNode"));
    }

    public void testAll() {
        final DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] allNodes = StreamSupport.stream(discoveryNodes.spliterator(), false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);
        assertThat(discoveryNodes.resolveNodes(), arrayContainingInAnyOrder(allNodes));
        assertThat(discoveryNodes.resolveNodes(new String[0]), arrayContainingInAnyOrder(allNodes));
        assertThat(discoveryNodes.resolveNodes("_all"), arrayContainingInAnyOrder(allNodes));

        final String[] nonMasterNodes = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(n -> n.isMasterNode() == false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);
        assertThat(discoveryNodes.resolveNodes("_all", "master:false"), arrayContainingInAnyOrder(nonMasterNodes));

        assertThat(discoveryNodes.resolveNodes("master:false", "_all"), arrayContainingInAnyOrder(allNodes));
    }

    public void testCoordinatorOnlyNodes() {
        final DiscoveryNodes discoveryNodes = buildDiscoveryNodes();

        final String[] coordinatorOnlyNodes = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(n -> n.canContainData() == false && n.isIngestNode() == false && n.isMasterNode() == false)
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        final String[] nonCoordinatorOnlyNodes = discoveryNodes.getNodes()
            .values()
            .stream()
            .filter(n -> n.isMasterNode() || n.canContainData() || n.isIngestNode())
            .map(DiscoveryNode::getId)
            .toArray(String[]::new);

        assertThat(discoveryNodes.resolveNodes("coordinating_only:true"), arrayContainingInAnyOrder(coordinatorOnlyNodes));
        assertThat(
            discoveryNodes.resolveNodes("_all", "data:false", "ingest:false", "master:false"),
            arrayContainingInAnyOrder(coordinatorOnlyNodes)
        );
        assertThat(discoveryNodes.resolveNodes("_all", "coordinating_only:false"), arrayContainingInAnyOrder(nonCoordinatorOnlyNodes));
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
        String[] nodeIds = discoveryNodes.getNodes().keySet().toArray(new String[0]);
        for (int i = 0; i < numNodeIds; i++) {
            String nodeId = randomFrom(nodeIds);
            nodeSelectors.add(nodeId);
            expectedNodeIdsSet.add(nodeId);
        }
        int numNodeNames = randomIntBetween(0, 3);
        DiscoveryNode[] nodes = discoveryNodes.getNodes().values().toArray(DiscoveryNode[]::new);
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

    public void testMastersFirst() {
        final List<DiscoveryNode> inputNodes = randomNodes(10);
        final DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        inputNodes.forEach(discoBuilder::add);
        final List<DiscoveryNode> returnedNodes = discoBuilder.build().mastersFirstStream().toList();
        assertEquals(returnedNodes.size(), inputNodes.size());
        assertEquals(new HashSet<>(returnedNodes), new HashSet<>(inputNodes));

        boolean mastersOk = true;
        final var message = returnedNodes.toString();
        for (final var discoveryNode : returnedNodes) {
            if (discoveryNode.isMasterNode()) {
                assertTrue(message, mastersOk);
            } else {
                mastersOk = false;
            }
        }
    }

    public void testDeltaListsMultipleNodes() {
        final List<DiscoveryNode> discoveryNodes = randomNodes(3);

        final DiscoveryNodes nodes0 = DiscoveryNodes.builder().add(discoveryNodes.get(0)).build();
        final DiscoveryNodes nodes01 = DiscoveryNodes.builder(nodes0).add(discoveryNodes.get(1)).build();
        final DiscoveryNodes nodes012 = DiscoveryNodes.builder(nodes01).add(discoveryNodes.get(2)).build();

        assertThat(nodes01.delta(nodes0).shortSummary(), equalTo("added {" + noAttr(discoveryNodes.get(1)) + "}"));
        assertThat(
            nodes012.delta(nodes0).shortSummary(),
            oneOf(
                "added {" + noAttr(discoveryNodes.get(1)) + ", " + noAttr(discoveryNodes.get(2)) + "}",
                "added {" + noAttr(discoveryNodes.get(2)) + ", " + noAttr(discoveryNodes.get(1)) + "}"
            )
        );

        assertThat(nodes0.delta(nodes01).shortSummary(), equalTo("removed {" + noAttr(discoveryNodes.get(1)) + "}"));
        assertThat(
            nodes0.delta(nodes012).shortSummary(),
            oneOf(
                "removed {" + noAttr(discoveryNodes.get(1)) + ", " + noAttr(discoveryNodes.get(2)) + "}",
                "removed {" + noAttr(discoveryNodes.get(2)) + ", " + noAttr(discoveryNodes.get(1)) + "}"
            )
        );

        final DiscoveryNodes nodes01Local = DiscoveryNodes.builder(nodes01).localNodeId(discoveryNodes.get(1).getId()).build();
        final DiscoveryNodes nodes02Local = DiscoveryNodes.builder(nodes012).localNodeId(discoveryNodes.get(1).getId()).build();

        assertThat(nodes01Local.delta(nodes0).shortSummary(), equalTo(""));
        assertThat(nodes02Local.delta(nodes0).shortSummary(), equalTo("added {" + noAttr(discoveryNodes.get(2)) + "}"));

        assertThat(nodes0.delta(nodes01Local).shortSummary(), equalTo("removed {" + noAttr(discoveryNodes.get(1)) + "}"));
        assertThat(
            nodes0.delta(nodes02Local).shortSummary(),
            oneOf(
                "removed {" + noAttr(discoveryNodes.get(1)) + ", " + noAttr(discoveryNodes.get(2)) + "}",
                "removed {" + noAttr(discoveryNodes.get(2)) + ", " + noAttr(discoveryNodes.get(1)) + "}"
            )
        );
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
                final TransportAddress nodeAddress = node.getAddress();
                node = new DiscoveryNode(
                    node.getName(),
                    node.getId(),
                    node.getEphemeralId(),
                    nodeAddress.address().getHostString(),
                    nodeAddress.getAddress(),
                    nodeAddress,
                    attrs,
                    node.getRoles(),
                    node.getVersionInformation()
                );
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

        if (masterA == null) {
            assertThat(delta.previousMasterNode(), nullValue());
        } else {
            assertThat(delta.previousMasterNode().getId(), equalTo(masterAId));
        }
        if (masterB == null) {
            assertThat(delta.newMasterNode(), nullValue());
        } else {
            assertThat(delta.newMasterNode().getId(), equalTo(masterBId));
        }

        if (Objects.equals(masterAId, masterBId)) {
            assertFalse(delta.masterNodeChanged());
        } else {
            assertTrue(delta.masterNodeChanged());
        }

        Set<DiscoveryNode> newNodes = new HashSet<>(nodesB);
        newNodes.removeAll(nodesA);
        assertThat(delta.added(), equalTo(newNodes.isEmpty() == false));
        assertThat(delta.addedNodes(), containsInAnyOrder(newNodes.stream().toList().toArray()));
        assertThat(delta.addedNodes().size(), equalTo(newNodes.size()));

        Set<DiscoveryNode> removedNodes = new HashSet<>(nodesA);
        removedNodes.removeAll(nodesB);
        assertThat(delta.removed(), equalTo(removedNodes.isEmpty() == false));
        assertThat(delta.removedNodes(), containsInAnyOrder(removedNodes.stream().toList().toArray()));
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
            final Set<DiscoveryNodeRole> roles = new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles()));
            if (frequently()) {
                roles.add(new DiscoveryNodeRole("custom_role", "cr"));
            }
            final DiscoveryNode node = newNode(idGenerator.getAndIncrement(), attributes, roles);
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

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNodeRole> roles) {
        return DiscoveryNodeUtils.builder("node_" + nodeId).name("name_" + nodeId).attributes(attributes).roles(roles).build();
    }

    private enum NodeSelector {
        LOCAL("_local") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getLocalNodeId());
            }
        },
        ELECTED_MASTER("_master") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return Collections.singleton(nodes.getMasterNodeId());
            }
        },
        MASTER_ELIGIBLE(DiscoveryNodeRole.MASTER_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getMasterNodes().keySet();
            }
        },
        DATA(DiscoveryNodeRole.DATA_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getDataNodes().keySet();
            }
        },
        INGEST(DiscoveryNodeRole.INGEST_ROLE.roleName() + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getIngestNodes().keySet();
            }
        },
        COORDINATING_ONLY(DiscoveryNode.COORDINATING_ONLY + ":true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getCoordinatingOnlyNodes().keySet();
            }
        },
        CUSTOM_ATTRIBUTE("attr:value") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getNodes()
                    .values()
                    .stream()
                    .filter(node -> "value".equals(node.getAttributes().get("attr")))
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());
            }
        },
        CUSTOM_ROLE("custom_role:true") {
            @Override
            Set<String> matchingNodeIds(DiscoveryNodes nodes) {
                return nodes.getNodes()
                    .values()
                    .stream()
                    .filter(node -> node.getRoles().stream().anyMatch(role -> role.roleName().equals("custom_role")))
                    .map(DiscoveryNode::getId)
                    .collect(Collectors.toSet());
            }
        };

        private final String selector;

        NodeSelector(String selector) {
            this.selector = selector;
        }

        abstract Set<String> matchingNodeIds(DiscoveryNodes nodes);
    }

    public void testMinMaxNodeVersions() {
        assertEquals(Version.CURRENT, DiscoveryNodes.EMPTY_NODES.getMaxNodeVersion());
        assertEquals(Version.CURRENT.minimumCompatibilityVersion(), DiscoveryNodes.EMPTY_NODES.getMinNodeVersion());
        assertEquals(IndexVersion.current(), DiscoveryNodes.EMPTY_NODES.getMaxDataNodeCompatibleIndexVersion());
        assertEquals(IndexVersion.MINIMUM_COMPATIBLE, DiscoveryNodes.EMPTY_NODES.getMinSupportedIndexVersion());

        // use a mix of versions with major, minor, and patch numbers
        List<VersionInformation> dataVersions = List.of(
            new VersionInformation(Version.fromString("3.2.5"), IndexVersion.fromId(2000099), IndexVersion.fromId(3020599)),
            new VersionInformation(Version.fromString("3.0.7"), IndexVersion.fromId(2000099), IndexVersion.fromId(3000799)),
            new VersionInformation(Version.fromString("2.1.0"), IndexVersion.fromId(1050099), IndexVersion.fromId(2010099))
        );
        List<VersionInformation> observerVersions = List.of(
            new VersionInformation(Version.fromString("5.0.17"), IndexVersion.fromId(0), IndexVersion.fromId(5001799)),
            new VersionInformation(Version.fromString("2.0.1"), IndexVersion.fromId(1000099), IndexVersion.fromId(2000199)),
            new VersionInformation(Version.fromString("1.6.0"), IndexVersion.fromId(0), IndexVersion.fromId(1060099))
        );

        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        for (int i = 0; i < dataVersions.size(); i++) {
            discoBuilder.add(
                DiscoveryNodeUtils.builder("data_" + i)
                    .version(dataVersions.get(i))
                    .roles(Set.of(randomBoolean() ? DiscoveryNodeRole.DATA_ROLE : DiscoveryNodeRole.MASTER_ROLE))
                    .build()
            );
        }
        for (int i = 0; i < observerVersions.size(); i++) {
            discoBuilder.add(DiscoveryNodeUtils.builder("observer_" + i).version(observerVersions.get(i)).roles(Set.of()).build());
        }
        DiscoveryNodes build = discoBuilder.build();

        assertEquals(Version.fromString("5.0.17"), build.getMaxNodeVersion());
        assertEquals(Version.fromString("1.6.0"), build.getMinNodeVersion());
        assertEquals(Version.fromString("2.1.0"), build.getSmallestNonClientNodeVersion());  // doesn't include 1.6.0 observer
        assertEquals(IndexVersion.fromId(2010099), build.getMaxDataNodeCompatibleIndexVersion());   // doesn't include 2000199 observer
        assertEquals(IndexVersion.fromId(2000099), build.getMinSupportedIndexVersion());            // also includes observers
    }

    private static String noAttr(DiscoveryNode discoveryNode) {
        final StringBuilder stringBuilder = new StringBuilder();
        discoveryNode.appendDescriptionWithoutAttributes(stringBuilder);
        return stringBuilder.toString();
    }

    public void testNodeLeftGeneration() {

        final ObjLongConsumer<Consumer<DiscoveryNodes.Builder>> testHarness = new ObjLongConsumer<>() {
            DiscoveryNodes discoveryNodes;

            @Override
            public void accept(Consumer<DiscoveryNodes.Builder> update, long expectedGeneration) {
                final var builder = discoveryNodes == null ? DiscoveryNodes.builder() : DiscoveryNodes.builder(discoveryNodes);
                update.accept(builder);
                discoveryNodes = builder.build();
                if (randomBoolean()) {
                    try {
                        discoveryNodes = copyWriteable(
                            discoveryNodes,
                            writableRegistry(),
                            in -> DiscoveryNodes.readFrom(in, null),
                            TransportVersion.current()
                        );
                    } catch (IOException e) {
                        throw new AssertionError("unexpected", e);
                    }
                }
                assertEquals(expectedGeneration, discoveryNodes.getNodeLeftGeneration());
            }
        };

        final BiFunction<Integer, VersionInformation, DiscoveryNode> nodeVersionFactory = (i, v) -> new DiscoveryNode(
            "name" + i,
            "id" + i,
            buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(randomSubsetOf(DiscoveryNodeRole.roles())),
            v
        );

        final IntFunction<DiscoveryNode> nodeFactory = i -> nodeVersionFactory.apply(i, VersionInformation.CURRENT);

        final var node0 = nodeVersionFactory.apply(
            0,
            new VersionInformation(VersionUtils.randomVersion(random()), IndexVersion.MINIMUM_COMPATIBLE, IndexVersion.current())
        );
        testHarness.accept(builder -> builder.add(node0), 0L);

        final var node1 = nodeFactory.apply(1);
        testHarness.accept(builder -> builder.add(node1), 0L);

        // removing a node by ID increments the generation
        testHarness.accept(builder -> builder.remove(node0.getId()), 1L);

        // no-op removal by ID changes nothing
        testHarness.accept(builder -> builder.remove("not-an-id"), 1L);

        // no-op removal by instance changes nothing
        final var node2 = nodeFactory.apply(2);
        testHarness.accept(builder -> builder.remove(node2), 1L);

        // adding another node changes nothing
        testHarness.accept(builder -> builder.add(node2), 1L);

        // and removing it by instance increments the generation
        testHarness.accept(builder -> builder.remove(node2), 2L);

        // if old nodes are present then the generation is forced to zero
        final var node3 = nodeVersionFactory.apply(3, VersionInformation.inferVersions(Version.V_8_8_0));
        testHarness.accept(builder -> builder.add(node3), 0L);

        // and it remains at zero while the old node is present
        testHarness.accept(builder -> builder.remove(node1), 0L);

        // but it starts incrementing again when the old node is removed
        final var node4 = nodeFactory.apply(4);
        testHarness.accept(builder -> builder.add(node4).remove(node3), 1L);

        // removing multiple nodes at once increments it only by one
        final var node5 = nodeFactory.apply(5);
        final var node6 = nodeFactory.apply(6);
        final var node7 = nodeFactory.apply(7);
        testHarness.accept(builder -> builder.add(node5).add(node6).add(node7), 1L);
        testHarness.accept(builder -> builder.remove(node5).remove(node6).remove(node7), 2L);
    }
}
