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

import org.elasticsearch.Version;
import org.elasticsearch.common.transport.DummyTransportAddress;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;

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
        } catch(IllegalArgumentException e) {
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

        String[] resolvedNodesIds = discoveryNodes.resolveNodesIds(nodeSelectors.toArray(new String[nodeSelectors.size()]));
        Arrays.sort(resolvedNodesIds);
        String[] expectedNodesIds = expectedNodeIdsSet.toArray(new String[expectedNodeIdsSet.size()]);
        Arrays.sort(expectedNodesIds);
        assertThat(resolvedNodesIds, equalTo(expectedNodesIds));
    }

    private static DiscoveryNodes buildDiscoveryNodes() {
        int numNodes = randomIntBetween(1, 10);
        DiscoveryNodes.Builder discoBuilder = DiscoveryNodes.builder();
        List<DiscoveryNode> nodesList = new ArrayList<>();
        for (int i = 0; i < numNodes; i++) {
            Map<String, String> attributes = new HashMap<>();
            if (frequently()) {
                attributes.put("custom", randomBoolean() ? "match" : randomAsciiOfLengthBetween(3, 5));
            }
            final DiscoveryNode node = newNode(i, attributes, new HashSet<>(randomSubsetOf(Arrays.asList(DiscoveryNode.Role.values()))));
            discoBuilder = discoBuilder.put(node);
            nodesList.add(node);
        }
        discoBuilder.localNodeId(randomFrom(nodesList).getId());
        discoBuilder.masterNodeId(randomFrom(nodesList).getId());
        return discoBuilder.build();
    }

    private static DiscoveryNode newNode(int nodeId, Map<String, String> attributes, Set<DiscoveryNode.Role> roles) {
        return new DiscoveryNode("name_" + nodeId, "node_" + nodeId, DummyTransportAddress.INSTANCE, attributes, roles, Version.CURRENT);
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
        },CUSTOM_ATTRIBUTE("attr:value") {
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
}
