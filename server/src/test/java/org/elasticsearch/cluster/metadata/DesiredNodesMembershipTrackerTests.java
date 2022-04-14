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
import org.elasticsearch.core.TimeValue;
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
import static org.hamcrest.Matchers.nullValue;

public class DesiredNodesMembershipTrackerTests extends DesiredNodesTestCase {
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

    public void testSimpleTracking() throws Exception {
        final var tracker = DesiredNodesMembershipTracker.create(
            Settings.builder().put(DesiredNodesMembershipTracker.LEFT_NODE_GRACE_PERIOD.getKey(), TimeValue.timeValueMillis(500)).build(),
            clusterService
        );

        applyClusterState("add new nodes", this::withNewNodes);

        assertThat(DesiredNodesMetadata.fromClusterState(clusterService.state()), is(equalTo(DesiredNodesMetadata.EMPTY)));
        assertThat(tracker.trackedMembers(), is(equalTo(0)));

        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        assertThat(tracker.trackedMembers(), is(greaterThan(0)));

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        assertThat(desiredNodes.nodes(), is(not(empty())));
        for (var desiredNode : desiredNodes) {
            assertThat(tracker.isMember(desiredNode), is(equalTo(true)));
            assertThat(tracker.isQuarantined(desiredNode), is(equalTo(false)));
        }

        final var clusterState = clusterService.state();
        final var leavingNode = randomValueOtherThan(clusterState.nodes().getMasterNode(), () -> randomFrom(clusterState.nodes()));
        final var discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).remove(leavingNode);
        final var leavingDesiredNode = desiredNodes.find(leavingNode.getExternalId());

        applyClusterState("remove some nodes", state -> ClusterState.builder(state).nodes(discoveryNodes).build());

        assertThat(tracker.isMember(leavingDesiredNode), is(equalTo(true)));
        assertThat(tracker.isQuarantined(leavingDesiredNode), is(equalTo(true)));

        // After the grace period, the node is not considered a member
        assertBusy(() -> assertThat(tracker.isMember(leavingDesiredNode), is(equalTo(false))));
    }

    public void testFlappyNode() {
        final var tracker = DesiredNodesMembershipTracker.create(Settings.EMPTY, clusterService);

        applyClusterState("add new nodes", this::withNewNodes);
        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        final var clusterState = clusterService.state();
        final var leavingNode = randomValueOtherThan(clusterState.nodes().getMasterNode(), () -> randomFrom(clusterState.nodes()));
        final var discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).remove(leavingNode);
        final var leavingDesiredNode = desiredNodes.find(leavingNode.getExternalId());

        applyClusterState("remove some nodes", state -> ClusterState.builder(state).nodes(discoveryNodes).build());

        assertThat(tracker.isMember(leavingDesiredNode), is(equalTo(true)));
        assertThat(tracker.isQuarantined(leavingDesiredNode), is(equalTo(true)));

        applyClusterState(
            "Add node back",
            state -> ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(state.nodes()).add(newNode(leavingDesiredNode.externalId())))
                .build()
        );

        assertThat(tracker.isMember(leavingDesiredNode), is(equalTo(true)));
        assertThat(tracker.isQuarantined(leavingDesiredNode), is(equalTo(false)));
    }

    public void testQuarantinedNodesAreFreedAfterTheNodeReJoinsWithNewHistoryId() {
        final var tracker = DesiredNodesMembershipTracker.create(Settings.EMPTY, clusterService);

        applyClusterState("add new nodes", this::withNewNodes);
        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        final var clusterState = clusterService.state();
        final var leavingNode = randomValueOtherThan(clusterState.nodes().getMasterNode(), () -> randomFrom(clusterState.nodes()));
        final var discoveryNodes = DiscoveryNodes.builder(clusterState.nodes()).remove(leavingNode);
        final var leavingDesiredNode = desiredNodes.find(leavingNode.getExternalId());

        applyClusterState("remove some nodes", state -> ClusterState.builder(state).nodes(discoveryNodes).build());

        assertThat(tracker.isMember(leavingDesiredNode), is(equalTo(true)));
        assertThat(tracker.isQuarantined(leavingDesiredNode), is(equalTo(true)));

        final var desiredNodesWithNewHistoryId = new DesiredNodes(UUIDs.randomBase64UUID(), 1, desiredNodes.nodes());

        applyClusterState(
            "Add node back and a new desired nodes",
            state -> ClusterState.builder(state)
                .nodes(DiscoveryNodes.builder(clusterState.nodes()))
                .metadata(
                    Metadata.builder(state.metadata())
                        .putCustom(DesiredNodesMetadata.TYPE, new DesiredNodesMetadata(desiredNodesWithNewHistoryId))
                        .build()
                )
                .build()
        );

        for (DesiredNode desiredNode : desiredNodesWithNewHistoryId) {
            assertThat(tracker.isMember(desiredNode), is(equalTo(true)));
            assertThat(tracker.isQuarantined(desiredNode), is(equalTo(false)));
        }
    }

    public void testMasterDemotionClearsMembers() {
        final var tracker = DesiredNodesMembershipTracker.create(Settings.EMPTY, clusterService);
        assertThat(tracker.trackedMembers(), is(equalTo(0)));

        applyClusterState("add desired nodes node", this::desiredNodesWithAllClusterNodes);

        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterService.state());
        for (DesiredNode desiredNode : desiredNodes) {
            assertThat(tracker.isMember(desiredNode), is(equalTo(true)));
            assertThat(tracker.isQuarantined(desiredNode), is(equalTo(false)));
        }

        applyClusterState("demote master node", this::demoteMasterNode);

        assertThat(tracker.trackedMembers(), is(equalTo(0)));

        for (DesiredNode desiredNode : desiredNodes) {
            assertThat(tracker.isMember(desiredNode), is(equalTo(false)));
        }
    }

    public void testMoveToNewHistoryIdClearsPreviousMembers() {
        final var tracker = DesiredNodesMembershipTracker.create(Settings.EMPTY, clusterService);

        applyClusterState("add a few nodes", this::withNewNodes);

        assertThat(tracker.trackedMembers(), is(equalTo(0)));

        applyClusterState("add desired nodes", this::desiredNodesWithAllClusterNodes);

        assertThat(tracker.trackedMembers(), is(greaterThan(0)));

        final var clusterState = clusterService.state();
        final var originalDesiredNodes = DesiredNodes.latestFromClusterState(clusterState);

        final var desiredNodesWithNewHistoryId = new DesiredNodes(UUIDs.randomBase64UUID(), 1, randomSubsetOf(1, originalDesiredNodes));

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

        for (DesiredNode desiredNode : originalDesiredNodes) {
            if (desiredNodesWithNewHistoryId.contains(desiredNode)) {
                continue;
            }
            assertThat(tracker.isMember(desiredNode), is(equalTo(false)));
            assertThat(tracker.isQuarantined(desiredNode), is(equalTo(false)));
        }

        for (DesiredNode desiredNode : desiredNodesWithNewHistoryId) {
            assertThat(tracker.isMember(desiredNode), is(equalTo(true)));
            assertThat(tracker.isQuarantined(desiredNode), is(equalTo(false)));
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

    private ClusterState demoteMasterNode(final ClusterState currentState) {
        final DiscoveryNode node = new DiscoveryNode(
            "other",
            ESTestCase.buildNewFakeTransportAddress(),
            Collections.emptyMap(),
            DiscoveryNodeRole.roles(),
            Version.CURRENT
        );
        assertThat(currentState.nodes().get(node.getId()), nullValue());

        return ClusterState.builder(currentState)
            .nodes(DiscoveryNodes.builder(currentState.nodes()).add(node).masterNodeId(node.getId()))
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
            16,
            new ByteSizeValue(randomIntBetween(1, 64), ByteSizeUnit.GB),
            new ByteSizeValue(randomIntBetween(1, 64), ByteSizeUnit.GB),
            Version.CURRENT
        );
    }
}
