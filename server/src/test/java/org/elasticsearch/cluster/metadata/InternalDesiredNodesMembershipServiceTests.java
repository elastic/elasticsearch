/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.Version;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;

public class InternalDesiredNodesMembershipServiceTests extends DesiredNodesTestCase {
    private TestThreadPool threadPool;
    private ClusterService clusterService;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        clusterService = ClusterServiceUtils.createClusterService(threadPool);
    }

    @After
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        final boolean terminated = terminate(threadPool);
        assert terminated;
        clusterService.close();
    }

    public void testDesiredNodeIsConsideredAsMemberOnceSeen() {
        final var tracker = InternalDesiredNodesMembershipService.create(clusterService);

        applyClusterState("add new nodes", this::withNewNodes);

        assertThat(DesiredNodesMetadata.fromClusterState(clusterService.state()), is(equalTo(DesiredNodesMetadata.EMPTY)));

        final var membersBeforeDesiredNodesAreAdded = tracker.getMembers();
        assertThat(membersBeforeDesiredNodesAreAdded.count(), is(equalTo(0)));

        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        assertThat(desiredNodes.nodes(), is(not(empty())));

        final var membersAfterDesiredNodesAreAdded = tracker.getMembers();
        for (var desiredNode : desiredNodes) {
            assertThat(membersAfterDesiredNodesAreAdded.contains(desiredNode), is(equalTo(true)));
        }

        final var clusterState = clusterService.state();
        final var leavingNode = randomValueOtherThan(clusterState.nodes().getMasterNode(), () -> randomFrom(clusterState.nodes()));
        final var discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).remove(leavingNode);
        final var leavingDesiredNode = desiredNodes.find(leavingNode.getExternalId());

        applyClusterState("remove some nodes", state -> ClusterState.builder(state).nodes(discoveryNodes).build());

        final var membersAfterNodeLeaves = tracker.getMembers();
        // As long as the node remains in the DesiredNodes we consider it as member
        assertThat(membersAfterNodeLeaves.contains(leavingDesiredNode), is(equalTo(true)));
    }

    public void testMembershipIsUpdatedAfterDesiredNodesAreUpdated() {
        final var tracker = InternalDesiredNodesMembershipService.create(clusterService);

        applyClusterState("add new nodes", this::withNewNodes);

        assertThat(DesiredNodesMetadata.fromClusterState(clusterService.state()), is(equalTo(DesiredNodesMetadata.EMPTY)));

        final var membersBeforeDesiredNodesAreAdded = tracker.getMembers();
        assertThat(membersBeforeDesiredNodesAreAdded.count(), is(equalTo(0)));

        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var membersAfterDesiredNodesAreAdded = tracker.getMembers();
        assertThat(membersAfterDesiredNodesAreAdded.count(), is(greaterThan(0)));

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        assertThat(desiredNodes.nodes(), is(not(empty())));
        for (var desiredNode : desiredNodes) {
            assertThat(membersAfterDesiredNodesAreAdded.contains(desiredNode), is(equalTo(true)));
        }

        final var removedDesiredNodes = randomSubsetOf(randomInt(desiredNodes.nodes().size() - 1), desiredNodes.nodes());
        final var survivingDesiredNodes = new ArrayList<>(desiredNodes.nodes());
        survivingDesiredNodes.removeAll(removedDesiredNodes);

        final var updatedDesiredNodes = new DesiredNodes(desiredNodes.historyID(), desiredNodes.version() + 1, survivingDesiredNodes);

        applyClusterState(
            "Update desired nodes",
            state -> ClusterState.builder(state)
                .metadata(
                    Metadata.builder(state.metadata())
                        .putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(updatedDesiredNodes))
                        .build()
                )
                .build()
        );

        final var membersAfterUpdatingDesiredNodes = tracker.getMembers();

        for (DesiredNode survivingDesiredNode : survivingDesiredNodes) {
            assertThat(membersAfterUpdatingDesiredNodes.contains(survivingDesiredNode), is(equalTo(true)));
        }

        for (DesiredNode removedDesiredNode : removedDesiredNodes) {
            assertThat(membersAfterUpdatingDesiredNodes.contains(removedDesiredNode), is(equalTo(false)));
        }
    }

    public void testMoveToNewHistoryIdClearsPreviousMembers() {
        final var tracker = InternalDesiredNodesMembershipService.create(clusterService);

        applyClusterState("add a few nodes", this::withNewNodes);

        final var membersBeforeAddingDesiredNodes = tracker.getMembers();

        assertThat(membersBeforeAddingDesiredNodes.count(), is(equalTo(0)));

        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var membersAfterAddingDesiredNodes = tracker.getMembers();
        assertThat(membersAfterAddingDesiredNodes.count(), is(greaterThan(0)));

        final var clusterState = clusterService.state();
        final var originalDesiredNodes = DesiredNodes.latestFromClusterState(clusterState);

        final var desiredNodesWithNewHistoryId = new DesiredNodes(
            UUIDs.randomBase64UUID(),
            1,
            randomSubsetOf(1, originalDesiredNodes.nodes())
        );

        applyClusterState(
            "change desired nodes history id",
            state -> ClusterState.builder(state)
                .metadata(
                    Metadata.builder(state.metadata())
                        .putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodesWithNewHistoryId))
                        .build()
                )
                .build()
        );

        final var membersAfterUpgradingHistoryId = tracker.getMembers();

        for (DesiredNode desiredNode : originalDesiredNodes) {
            if (desiredNodesWithNewHistoryId.nodes().contains(desiredNode)) {
                continue;
            }
            assertThat(membersAfterUpgradingHistoryId.contains(desiredNode), is(equalTo(false)));
        }

        for (DesiredNode desiredNode : desiredNodesWithNewHistoryId) {
            assertThat(membersAfterUpgradingHistoryId.contains(desiredNode), is(equalTo(true)));
        }
    }

    public void testTrackerIsAddedWithExistingClusterState() {
        applyClusterState("add new nodes", this::withNewNodes);
        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var tracker = InternalDesiredNodesMembershipService.create(clusterService);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());

        applyClusterState("Unrelated update", clusterState -> ClusterState.builder(clusterState).incrementVersion().build());

        final var members = tracker.getMembers();
        for (DesiredNode desiredNode : desiredNodes) {
            assertThat(members.contains(desiredNode), is(equalTo(true)));
        }
    }

    public void testRemoveDesiredNodes() {
        final var tracker = InternalDesiredNodesMembershipService.create(clusterService);

        applyClusterState("add new nodes", this::withNewNodes);
        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());

        final var members = tracker.getMembers();
        for (DesiredNode desiredNode : desiredNodes) {
            assertThat(members.contains(desiredNode), is(true));
        }

        applyClusterState(
            "Remove desired nodes",
            clusterState -> ClusterState.builder(clusterState)
                .metadata(Metadata.builder(clusterState.metadata()).removeCustom(DesiredNodesMetadata.TYPE))
                .build()
        );

        final var membersAfterRemovingDesiredNodes = tracker.getMembers();
        for (DesiredNode desiredNode : desiredNodes) {
            assertThat(membersAfterRemovingDesiredNodes.contains(desiredNode), is(false));
        }
    }

    private ClusterState withNewNodes(ClusterState clusterState) {
        final var discoveryNodes = DiscoveryNodes.builder(clusterState.nodes());
        for (DiscoveryNode newNode : randomList(5, 10, this::newNode)) {
            discoveryNodes.add(newNode);
        }

        return ClusterState.builder(clusterState).nodes(discoveryNodes.build()).build();
    }

    private ClusterState desiredNodesWithAllClusterNodes(ClusterState clusterState) {
        final List<DesiredNode> desiredNodes = new ArrayList<>();
        for (DiscoveryNode node : clusterState.nodes()) {
            desiredNodes.add(newDesiredNode(node.getName()));
        }

        return ClusterState.builder(clusterState)
            .metadata(
                Metadata.builder(clusterState.metadata())
                    .putCustom(
                        DesiredNodesMetadata.TYPE,
                        new DesiredNodesMetadata(new DesiredNodes(UUIDs.randomBase64UUID(), 1, desiredNodes))
                    )
                    .build()
            )
            .build();
    }

    private void applyClusterState(final String reason, final Function<ClusterState, ClusterState> applier) {
        PlainActionFuture.<Void, RuntimeException>get(
            future -> clusterService.getClusterApplierService()
                .onNewClusterState(reason, () -> applier.apply(clusterService.state()), future),
            10,
            TimeUnit.SECONDS
        );
    }

    private DiscoveryNode newNode() {
        return newNode(UUIDs.randomBase64UUID());
    }

    private DiscoveryNode newNode(String name) {
        return new DiscoveryNode(
            name,
            UUIDs.randomBase64UUID(),
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            new HashSet<>(DiscoveryNodeRole.roles()),
            Version.CURRENT
        );
    }

    private DesiredNode newDesiredNode(String nodeName) {
        return new DesiredNode(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), nodeName).build(),
            randomIntBetween(1, 128),
            new ByteSizeValue(randomIntBetween(1, 64), ByteSizeUnit.GB),
            new ByteSizeValue(randomIntBetween(1, 64), ByteSizeUnit.GB),
            Version.CURRENT
        );
    }
}
