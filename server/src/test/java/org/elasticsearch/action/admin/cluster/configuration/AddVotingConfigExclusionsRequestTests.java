/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.Set;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class AddVotingConfigExclusionsRequestTests extends ESTestCase {
    private static final String NODE_IDENTIFIERS_INCORRECTLY_SET_MSG = "You must set [node_names] or [node_ids] but not both";

    public void testSerializationForNodeIdOrNodeName() throws IOException {
        AddVotingConfigExclusionsRequest originalRequest = new AddVotingConfigExclusionsRequest(
            new String[] { "nodeId1", "nodeId2" },
            Strings.EMPTY_ARRAY,
            TimeValue.ZERO
        );
        AddVotingConfigExclusionsRequest deserialized = copyWriteable(
            originalRequest,
            writableRegistry(),
            AddVotingConfigExclusionsRequest::new
        );

        assertThat(deserialized.getNodeIds(), equalTo(originalRequest.getNodeIds()));
        assertThat(deserialized.getNodeNames(), equalTo(originalRequest.getNodeNames()));
        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));

        originalRequest = new AddVotingConfigExclusionsRequest("nodeName1", "nodeName2");
        deserialized = copyWriteable(originalRequest, writableRegistry(), AddVotingConfigExclusionsRequest::new);

        assertThat(deserialized.getNodeIds(), equalTo(originalRequest.getNodeIds()));
        assertThat(deserialized.getNodeNames(), equalTo(originalRequest.getNodeNames()));
        assertThat(deserialized.getTimeout(), equalTo(originalRequest.getTimeout()));
    }

    public void testResolve() {
        final DiscoveryNode localNode = TestDiscoveryNode.create(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = TestDiscoveryNode.create(
            "other1",
            "other1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = TestDiscoveryNode.create(
            "other2",
            "other2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        final DiscoveryNode otherDataNode = TestDiscoveryNode.create(
            "data",
            "data",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet()
        );

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(new Builder().add(localNode).add(otherNode1).add(otherNode2).add(otherDataNode).localNodeId(localNode.getId()))
            .build();

        assertThat(
            new AddVotingConfigExclusionsRequest("local", "other1", "other2").resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion)
        );
        assertThat(new AddVotingConfigExclusionsRequest("local").resolveVotingConfigExclusions(clusterState), contains(localNodeExclusion));
        assertThat(
            new AddVotingConfigExclusionsRequest("other1", "other2").resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
        assertThat(
            new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[] { "other1", "other2" }, TimeValue.ZERO)
                .resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testResolveAllNodeIdentifiersNullOrEmpty() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, Strings.EMPTY_ARRAY, TimeValue.ZERO)
            ).getMessage(),
            equalTo(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG)
        );
    }

    public void testResolveMoreThanOneNodeIdentifiersSet() {
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new AddVotingConfigExclusionsRequest(new String[] { "nodeId" }, new String[] { "nodeName" }, TimeValue.ZERO)
            ).getMessage(),
            equalTo(NODE_IDENTIFIERS_INCORRECTLY_SET_MSG)
        );
    }

    public void testResolveByNodeIds() {
        final DiscoveryNode node1 = TestDiscoveryNode.create(
            "nodeName1",
            "nodeId1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion node1Exclusion = new VotingConfigExclusion(node1);

        final DiscoveryNode node2 = TestDiscoveryNode.create(
            "nodeName2",
            "nodeId2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = TestDiscoveryNode.create(
            "nodeName3",
            "nodeId3",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final VotingConfigExclusion unresolvableVotingConfigExclusion = new VotingConfigExclusion(
            "unresolvableNodeId",
            VotingConfigExclusion.MISSING_VALUE_MARKER
        );

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId()))
            .build();

        assertThat(
            new AddVotingConfigExclusionsRequest(new String[] { "nodeId1", "nodeId2" }, Strings.EMPTY_ARRAY, TimeValue.ZERO)
                .resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(node1Exclusion, node2Exclusion)
        );

        assertThat(
            new AddVotingConfigExclusionsRequest(new String[] { "nodeId1", "unresolvableNodeId" }, Strings.EMPTY_ARRAY, TimeValue.ZERO)
                .resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(node1Exclusion, unresolvableVotingConfigExclusion)
        );
    }

    public void testResolveByNodeNames() {
        final DiscoveryNode node1 = TestDiscoveryNode.create(
            "nodeName1",
            "nodeId1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion node1Exclusion = new VotingConfigExclusion(node1);

        final DiscoveryNode node2 = TestDiscoveryNode.create(
            "nodeName2",
            "nodeId2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = TestDiscoveryNode.create(
            "nodeName3",
            "nodeId3",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final VotingConfigExclusion unresolvableVotingConfigExclusion = new VotingConfigExclusion(
            VotingConfigExclusion.MISSING_VALUE_MARKER,
            "unresolvableNodeName"
        );

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId()))
            .build();

        assertThat(
            new AddVotingConfigExclusionsRequest("nodeName1", "nodeName2").resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(node1Exclusion, node2Exclusion)
        );

        assertThat(
            new AddVotingConfigExclusionsRequest("nodeName1", "unresolvableNodeName").resolveVotingConfigExclusions(clusterState),
            containsInAnyOrder(node1Exclusion, unresolvableVotingConfigExclusion)
        );
    }

    public void testResolveAmbiguousName() {
        final DiscoveryNode node1 = TestDiscoveryNode.create(
            "ambiguous-name",
            "nodeId1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final DiscoveryNode node2 = TestDiscoveryNode.create(
            "ambiguous-name",
            "nodeId2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(new Builder().add(node1).add(node2).localNodeId(node1.getId()))
            .build();

        final var request = new AddVotingConfigExclusionsRequest("ambiguous-name");
        assertThat(
            expectThrows(IllegalArgumentException.class, () -> request.resolveVotingConfigExclusions(clusterState)).getMessage(),
            allOf(
                containsString("node name [ambiguous-name] is ambiguous"),
                containsString(node1.descriptionWithoutAttributes()),
                containsString(node2.descriptionWithoutAttributes())
            )
        );
    }

    public void testResolveRemoveExistingVotingConfigExclusions() {
        final DiscoveryNode node1 = TestDiscoveryNode.create(
            "nodeName1",
            "nodeId1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final DiscoveryNode node2 = TestDiscoveryNode.create(
            "nodeName2",
            "nodeId2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion node2Exclusion = new VotingConfigExclusion(node2);

        final DiscoveryNode node3 = TestDiscoveryNode.create(
            "nodeName3",
            "nodeId3",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final VotingConfigExclusion existingVotingConfigExclusion = new VotingConfigExclusion(node1);

        Metadata metadata = Metadata.builder()
            .coordinationMetadata(CoordinationMetadata.builder().addVotingConfigExclusion(existingVotingConfigExclusion).build())
            .build();

        final ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(metadata)
            .nodes(new Builder().add(node1).add(node2).add(node3).localNodeId(node1.getId()))
            .build();

        assertThat(
            new AddVotingConfigExclusionsRequest(new String[] { "nodeId1", "nodeId2" }, Strings.EMPTY_ARRAY, TimeValue.ZERO)
                .resolveVotingConfigExclusions(clusterState),
            contains(node2Exclusion)
        );
    }

    public void testResolveAndCheckMaximum() {
        final DiscoveryNode localNode = TestDiscoveryNode.create(
            "local",
            "local",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion localNodeExclusion = new VotingConfigExclusion(localNode);
        final DiscoveryNode otherNode1 = TestDiscoveryNode.create(
            "other1",
            "other1",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );
        final VotingConfigExclusion otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        final DiscoveryNode otherNode2 = TestDiscoveryNode.create(
            "other2",
            "other2",
            buildNewFakeTransportAddress(),
            emptyMap(),
            Set.of(DiscoveryNodeRole.MASTER_ROLE)
        );

        final ClusterState.Builder builder = ClusterState.builder(new ClusterName("cluster"))
            .nodes(new Builder().add(localNode).add(otherNode1).add(otherNode2).localNodeId(localNode.getId()));
        builder.metadata(
            Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().addVotingConfigExclusion(otherNode1Exclusion).build())
        );
        final ClusterState clusterState = builder.build();

        assertThat(
            new AddVotingConfigExclusionsRequest("local").resolveVotingConfigExclusionsAndCheckMaximum(clusterState, 2, "setting.name"),
            contains(localNodeExclusion)
        );
        assertThat(
            expectThrows(
                IllegalArgumentException.class,
                () -> new AddVotingConfigExclusionsRequest("local").resolveVotingConfigExclusionsAndCheckMaximum(
                    clusterState,
                    1,
                    "setting.name"
                )
            ).getMessage(),
            equalTo(
                "add voting config exclusions request for nodes named [local] would add [1] exclusions to the existing [1] which "
                    + "would exceed the maximum of [1] set by [setting.name]"
            )
        );
    }

}
