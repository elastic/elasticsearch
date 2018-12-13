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
package org.elasticsearch.action.admin.cluster.bootstrap;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.bootstrap.BootstrapConfiguration.NodeDescription;
import org.elasticsearch.cluster.coordination.CoordinationMetaData.VotingConfiguration;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNode.Role;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.test.EqualsHashCodeTestUtils.CopyFunction;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singleton;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class BootstrapConfigurationTests extends ESTestCase {

    public void testEqualsHashcodeSerialization() {
        // Note: the explicit cast of the CopyFunction is needed for some IDE (specifically Eclipse 4.8.0) to infer the right type
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(randomBootstrapConfiguration(),
                (CopyFunction<BootstrapConfiguration>) bootstrapConfiguration -> copyWriteable(bootstrapConfiguration, writableRegistry(),
                        BootstrapConfiguration::new),
                this::mutate);
    }

    public void testNodeDescriptionResolvedByName() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode expectedNode = randomFrom(discoveryNodes);
        assertThat(new NodeDescription(null, expectedNode.getName()).resolve(discoveryNodes), equalTo(expectedNode));
    }

    public void testNodeDescriptionResolvedByIdAndName() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode expectedNode = randomFrom(discoveryNodes);
        assertThat(new NodeDescription(expectedNode).resolve(discoveryNodes), equalTo(expectedNode));
    }

    public void testRejectsMismatchedId() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode expectedNode = randomFrom(discoveryNodes);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new NodeDescription(randomAlphaOfLength(11), expectedNode.getName()).resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("node id mismatch comparing "));
    }

    public void testRejectsMismatchedName() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode expectedNode = randomFrom(discoveryNodes);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new NodeDescription(expectedNode.getId(), randomAlphaOfLength(11)).resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("node name mismatch comparing "));
    }

    public void testFailsIfNoMatch() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> randomNodeDescription().resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("no node matching "));
    }

    public void testFailsIfDuplicateMatchOnName() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode discoveryNode = randomFrom(discoveryNodes);
        discoveryNodes.add(new DiscoveryNode(discoveryNode.getName(), randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(),
            singleton(Role.MASTER), Version.CURRENT));
        final ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new NodeDescription(null, discoveryNode.getName()).resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("discovered multiple nodes matching "));
    }

    public void testFailsIfDuplicatedNode() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final DiscoveryNode discoveryNode = randomFrom(discoveryNodes);
        discoveryNodes.add(discoveryNode);
        final ElasticsearchException e = expectThrows(ElasticsearchException.class,
            () -> new NodeDescription(discoveryNode).resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("discovered multiple nodes matching "));
    }

    public void testResolvesEntireConfiguration() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final List<DiscoveryNode> selectedNodes = randomSubsetOf(randomIntBetween(1, discoveryNodes.size()), discoveryNodes);
        final BootstrapConfiguration bootstrapConfiguration = new BootstrapConfiguration(selectedNodes.stream()
            .map(discoveryNode -> randomBoolean() ? new NodeDescription(discoveryNode) : new NodeDescription(null, discoveryNode.getName()))
            .collect(Collectors.toList()));

        final VotingConfiguration expectedConfiguration
            = new VotingConfiguration(selectedNodes.stream().map(DiscoveryNode::getId).collect(Collectors.toSet()));
        final VotingConfiguration votingConfiguration = bootstrapConfiguration.resolve(discoveryNodes);
        assertThat(votingConfiguration, equalTo(expectedConfiguration));
    }

    public void testRejectsDuplicatedDescriptions() {
        final List<DiscoveryNode> discoveryNodes = randomDiscoveryNodes();
        final List<DiscoveryNode> selectedNodes = randomSubsetOf(randomIntBetween(1, discoveryNodes.size()), discoveryNodes);
        final List<NodeDescription> selectedNodeDescriptions = selectedNodes.stream()
            .map(discoveryNode -> randomBoolean() ? new NodeDescription(discoveryNode) : new NodeDescription(null, discoveryNode.getName()))
            .collect(Collectors.toList());
        final NodeDescription toDuplicate = randomFrom(selectedNodeDescriptions);
        selectedNodeDescriptions.add(randomBoolean() ? toDuplicate : new NodeDescription(null, toDuplicate.getName()));
        final BootstrapConfiguration bootstrapConfiguration = new BootstrapConfiguration(selectedNodeDescriptions);

        final ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> bootstrapConfiguration.resolve(discoveryNodes));
        assertThat(e.getMessage(), startsWith("multiple nodes matching "));
    }

    private NodeDescription mutate(NodeDescription original) {
        if (randomBoolean()) {
            return new NodeDescription(original.getId(), randomAlphaOfLength(21 - original.getName().length()));
        } else {
            if (original.getId() == null) {
                return new NodeDescription(randomAlphaOfLength(10), original.getName());
            } else if (randomBoolean()) {
                return new NodeDescription(randomAlphaOfLength(21 - original.getId().length()), original.getName());
            } else {
                return new NodeDescription(null, original.getName());
            }
        }
    }

    protected BootstrapConfiguration mutate(BootstrapConfiguration original) {
        final List<NodeDescription> newDescriptions = new ArrayList<>(original.getNodeDescriptions());
        final int mutateElement = randomIntBetween(0, newDescriptions.size());
        if (mutateElement == newDescriptions.size()) {
            newDescriptions.add(randomIntBetween(0, newDescriptions.size()), randomNodeDescription());
        } else {
            if (newDescriptions.size() > 1 && randomBoolean()) {
                newDescriptions.remove(mutateElement);
            } else {
                newDescriptions.set(mutateElement, mutate(newDescriptions.get(mutateElement)));
            }
        }
        return new BootstrapConfiguration(newDescriptions);
    }

    protected NodeDescription randomNodeDescription() {
        return new NodeDescription(randomBoolean() ? null : randomAlphaOfLength(10), randomAlphaOfLength(10));
    }

    protected BootstrapConfiguration randomBootstrapConfiguration() {
        final int size = randomIntBetween(1, 5);
        final List<NodeDescription> nodeDescriptions = new ArrayList<>(size);
        while (nodeDescriptions.size() <= size) {
            nodeDescriptions.add(randomNodeDescription());
        }
        return new BootstrapConfiguration(nodeDescriptions);
    }

    protected List<DiscoveryNode> randomDiscoveryNodes() {
        final int size = randomIntBetween(1, 5);
        final List<DiscoveryNode> nodes = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            nodes.add(new DiscoveryNode(randomAlphaOfLength(10), randomAlphaOfLength(10), buildNewFakeTransportAddress(), emptyMap(),
                singleton(Role.MASTER), Version.CURRENT));
        }
        return nodes;
    }
}
