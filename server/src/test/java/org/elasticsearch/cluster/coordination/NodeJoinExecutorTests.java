/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Level;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.DesiredNodeWithStatus;
import org.elasticsearch.cluster.metadata.DesiredNodes;
import org.elasticsearch.cluster.metadata.DesiredNodesTestCase;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.allocation.AllocationService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.ReferenceDocs;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.assertDesiredNodesStatusIsCorrect;
import static org.elasticsearch.cluster.metadata.DesiredNodesTestCase.randomDesiredNode;
import static org.elasticsearch.test.VersionUtils.maxCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomCompatibleVersion;
import static org.elasticsearch.test.VersionUtils.randomVersion;
import static org.elasticsearch.test.VersionUtils.randomVersionBetween;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeJoinExecutorTests extends ESTestCase {

    private static final ActionListener<Void> NOT_COMPLETED_LISTENER = ActionListener.running(() -> {
        throw new AssertionError("should not complete publication");
    });

    public void testPreventJoinClusterWithNewerIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        NodeJoinExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);

        expectThrows(
            IllegalStateException.class,
            () -> NodeJoinExecutor.ensureIndexCompatibility(VersionUtils.getPreviousVersion(Version.CURRENT), metadata)
        );
    }

    public void testPreventJoinClusterWithUnsupportedIndices() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.fromString("6.8.0"))) // latest V6 released version
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        expectThrows(IllegalStateException.class, () -> NodeJoinExecutor.ensureIndexCompatibility(Version.CURRENT, metadata));
    }

    public void testPreventJoinClusterWithUnsupportedNodeVersions() {
        DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        final Version version = randomCompatibleVersion(random(), Version.CURRENT);
        builder.add(DiscoveryNodeUtils.create(UUIDs.base64UUID(), buildNewFakeTransportAddress(), version));
        builder.add(
            DiscoveryNodeUtils.create(UUIDs.base64UUID(), buildNewFakeTransportAddress(), randomCompatibleVersion(random(), version))
        );
        DiscoveryNodes nodes = builder.build();

        final Version maxNodeVersion = nodes.getMaxNodeVersion();
        final Version minNodeVersion = nodes.getMinNodeVersion();

        final Version tooLow = Version.fromId(maxNodeVersion.minimumCompatibilityVersion().id - 100);
        expectThrows(IllegalStateException.class, () -> {
            if (randomBoolean()) {
                NodeJoinExecutor.ensureNodesCompatibility(tooLow, nodes);
            } else {
                NodeJoinExecutor.ensureNodesCompatibility(tooLow, minNodeVersion, maxNodeVersion);
            }
        });

        final Version oldVersion = randomValueOtherThanMany(
            v -> v.onOrAfter(minNodeVersion),
            () -> rarely() ? Version.fromId(minNodeVersion.id - 1) : randomVersion(random())
        );
        expectThrows(IllegalStateException.class, () -> NodeJoinExecutor.ensureVersionBarrier(oldVersion, minNodeVersion));

        final Version minGoodVersion = maxNodeVersion.major == minNodeVersion.major ?
        // we have to stick with the same major
            minNodeVersion : maxNodeVersion.minimumCompatibilityVersion();
        final Version justGood = randomVersionBetween(random(), minGoodVersion, maxCompatibleVersion(minNodeVersion));

        if (randomBoolean()) {
            NodeJoinExecutor.ensureNodesCompatibility(justGood, nodes);
        } else {
            NodeJoinExecutor.ensureNodesCompatibility(justGood, minNodeVersion, maxNodeVersion);
        }
    }

    public void testPreventJoinClusterWithUnsupportedTransportVersion() {
        List<TransportVersion> versions = IntStream.range(0, randomIntBetween(2, 10))
            .mapToObj(i -> TransportVersionUtils.randomCompatibleVersion(random()))
            .toList();
        TransportVersion min = Collections.min(versions);

        // should not throw
        NodeJoinExecutor.ensureTransportVersionBarrier(
            TransportVersionUtils.randomVersionBetween(random(), min, TransportVersion.CURRENT),
            versions
        );
        expectThrows(
            IllegalStateException.class,
            () -> NodeJoinExecutor.ensureTransportVersionBarrier(
                TransportVersionUtils.randomVersionBetween(
                    random(),
                    TransportVersionUtils.getFirstVersion(),
                    TransportVersionUtils.getPreviousVersion(min)
                ),
                versions
            )
        );
    }

    public void testSuccess() {
        Settings.builder().build();
        Metadata.Builder metaBuilder = Metadata.builder();
        IndexMetadata indexMetadata = IndexMetadata.builder("test")
            .settings(randomCompatibleVersionSettings())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        indexMetadata = IndexMetadata.builder("test1")
            .settings(randomCompatibleVersionSettings())
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        metaBuilder.put(indexMetadata, false);
        Metadata metadata = metaBuilder.build();
        NodeJoinExecutor.ensureIndexCompatibility(Version.CURRENT, metadata);
    }

    public static Settings.Builder randomCompatibleVersionSettings() {
        Settings.Builder builder = Settings.builder();
        if (randomBoolean()) {
            Version createdVersion = getRandomCompatibleVersion();
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, createdVersion);
            if (randomBoolean()) {
                builder.put(
                    IndexMetadata.SETTING_VERSION_COMPATIBILITY,
                    VersionUtils.randomVersionBetween(random(), createdVersion, Version.CURRENT)
                );
            }
        } else {
            builder.put(IndexMetadata.SETTING_VERSION_CREATED, randomFrom(Version.fromString("5.0.0"), Version.fromString("6.0.0")));
            builder.put(IndexMetadata.SETTING_VERSION_COMPATIBILITY, getRandomCompatibleVersion());
        }
        return builder;
    }

    private static Version getRandomCompatibleVersion() {
        return VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumIndexCompatibilityVersion(), Version.CURRENT);
    }

    private static final JoinReason TEST_REASON = new JoinReason("test", null);

    public void testUpdatesNodeWithNewRoles() throws Exception {
        // Node roles vary by version, and new roles are suppressed for BWC. This means we can receive a join from a node that's already
        // in the cluster but with a different set of roles: the node didn't change roles, but the cluster state came via an older master.
        // In this case we must properly process its join to ensure that the roles are correct.

        final AllocationService allocationService = createAllocationService();
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final NodeJoinExecutor executor = new NodeJoinExecutor(allocationService, rerouteService);

        final DiscoveryNode masterNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());

        final DiscoveryNode actualNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());
        final DiscoveryNode bwcNode = new DiscoveryNode(
            actualNode.getName(),
            actualNode.getId(),
            actualNode.getEphemeralId(),
            actualNode.getHostName(),
            actualNode.getHostAddress(),
            actualNode.getAddress(),
            actualNode.getAttributes(),
            new HashSet<>(randomSubsetOf(actualNode.getRoles())),
            actualNode.getVersion()
        );
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()).add(bwcNode))
            .build();

        final var resultingState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            executor,
            List.of(JoinTask.singleNode(actualNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, 0L))
        );

        assertThat(resultingState.getNodes().get(actualNode.getId()).getRoles(), equalTo(actualNode.getRoles()));
    }

    public void testRejectsStatesWithStaleTerm() {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long executorTerm = randomLongBetween(0L, Long.MAX_VALUE - 1);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()).build())
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder().term(randomLongBetween(executorTerm + 1, Long.MAX_VALUE)).build())
                    .build()
            )
            .build();

        assertThat(
            expectThrows(
                NotMasterException.class,
                () -> ClusterStateTaskExecutorUtils.executeHandlingResults(
                    clusterState,
                    executor,
                    randomBoolean()
                        ? List.of(
                            JoinTask.singleNode(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm)
                        )
                        : List.of(
                            JoinTask.completingElection(
                                Stream.of(
                                    new JoinTask.NodeJoinTask(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)
                                ),
                                executorTerm
                            )
                        ),
                    t -> fail("should not succeed"),
                    (t, e) -> assertThat(e, instanceOf(NotMasterException.class))
                )
            ).getMessage(),
            containsString("there is a newer master")
        );
    }

    public void testRejectsStatesWithOtherMaster() {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long executorTerm = randomNonNegativeLong();
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(localNode)
                    .add(masterNode)
                    .localNodeId(localNode.getId())
                    .masterNodeId(masterNode.getId())
                    .build()
            )
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder().term(randomLongBetween(0L, executorTerm)).build())
                    .build()
            )
            .build();

        assertThat(
            expectThrows(
                NotMasterException.class,
                () -> ClusterStateTaskExecutorUtils.executeHandlingResults(
                    clusterState,
                    executor,
                    randomBoolean()
                        ? List.of(
                            JoinTask.singleNode(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm)
                        )
                        : List.of(
                            JoinTask.completingElection(
                                Stream.of(
                                    new JoinTask.NodeJoinTask(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)
                                ),
                                executorTerm
                            )
                        ),
                    t -> fail("should not succeed"),
                    (t, e) -> assertThat(e, instanceOf(NotMasterException.class))
                )
            ).getMessage(),
            containsString("not master for join request")
        );
    }

    public void testRejectsStatesWithNoMasterIfNotBecomingMaster() {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long executorTerm = randomNonNegativeLong();
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build())
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(CoordinationMetadata.builder().term(randomLongBetween(0L, executorTerm)).build())
                    .build()
            )
            .build();

        assertThat(
            expectThrows(
                NotMasterException.class,
                () -> ClusterStateTaskExecutorUtils.executeHandlingResults(
                    clusterState,
                    executor,
                    List.of(JoinTask.singleNode(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm)),
                    t -> fail("should not succeed"),
                    (t, e) -> assertThat(e, instanceOf(NotMasterException.class))
                )
            ).getMessage(),
            containsString("not master for join request")
        );
    }

    public void testRemovesOlderNodeInstancesWhenBecomingMaster() throws Exception {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long executorTerm = randomLongBetween(1, Long.MAX_VALUE);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var otherNodeOld = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var otherNodeNew = new DiscoveryNode(
            otherNodeOld.getName(),
            otherNodeOld.getId(),
            UUIDs.randomBase64UUID(random()),
            otherNodeOld.getHostName(),
            otherNodeOld.getHostAddress(),
            otherNodeOld.getAddress(),
            otherNodeOld.getAttributes(),
            otherNodeOld.getRoles(),
            otherNodeOld.getVersion()
        );

        final var afterElectionClusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            ClusterState.builder(ClusterName.DEFAULT)
                .nodes(DiscoveryNodes.builder().add(masterNode).add(otherNodeOld).localNodeId(masterNode.getId()).build())
                .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL).build())
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(CoordinationMetadata.builder().term(randomLongBetween(0, executorTerm - 1)).build())
                        .build()
                )
                .build(),
            executor,
            List.of(
                JoinTask.completingElection(
                    Stream.of(
                        new JoinTask.NodeJoinTask(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER),
                        new JoinTask.NodeJoinTask(otherNodeNew, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)
                    ),
                    executorTerm
                )
            )
        );
        assertThat(afterElectionClusterState.term(), equalTo(executorTerm));
        assertThat(afterElectionClusterState.nodes().getMasterNode(), equalTo(masterNode));
        assertFalse(afterElectionClusterState.blocks().hasGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL));
        assertThat(
            "existing node should be replaced by new one in an election",
            afterElectionClusterState.nodes().get(otherNodeOld.getId()).getEphemeralId(),
            equalTo(otherNodeNew.getEphemeralId())
        );

        assertThat(
            "existing node should not be replaced if not completing an election",
            ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                afterElectionClusterState,
                executor,
                List.of(
                    JoinTask.singleNode(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm),
                    JoinTask.singleNode(otherNodeOld, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm)
                )
            ).nodes().get(otherNodeNew.getId()).getEphemeralId(),
            equalTo(otherNodeNew.getEphemeralId())
        );
    }

    public void testUpdatesVotingConfigExclusionsIfNeeded() throws Exception {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long executorTerm = randomLongBetween(1, Long.MAX_VALUE);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var otherNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID(random()))
            .name(UUIDs.randomBase64UUID(random()))
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE))
            .build();

        var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ALL).build())
            .metadata(
                Metadata.builder()
                    .coordinationMetadata(
                        CoordinationMetadata.builder()
                            .term(randomLongBetween(0, executorTerm - 1))
                            .addVotingConfigExclusion(
                                new CoordinationMetadata.VotingConfigExclusion(
                                    CoordinationMetadata.VotingConfigExclusion.MISSING_VALUE_MARKER,
                                    otherNode.getName()
                                )
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        logger.info("--> {}", clusterState);
        logger.info("--> {}", otherNode);

        if (randomBoolean()) {
            clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterState,
                executor,
                List.of(
                    JoinTask.completingElection(
                        Stream.of(
                            new JoinTask.NodeJoinTask(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER),
                            new JoinTask.NodeJoinTask(otherNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)
                        ),
                        executorTerm
                    )
                )
            );
        } else {
            clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterState,
                executor,
                List.of(
                    JoinTask.completingElection(
                        Stream.of(new JoinTask.NodeJoinTask(masterNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)),
                        executorTerm
                    )
                )
            );
            clusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
                clusterState,
                executor,
                List.of(JoinTask.singleNode(otherNode, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, executorTerm))
            );
        }

        assertThat(clusterState.term(), equalTo(executorTerm));
        assertThat(clusterState.nodes().getMasterNode(), equalTo(masterNode));
        assertThat(
            clusterState.coordinationMetadata().getVotingConfigExclusions().iterator().next().getNodeId(),
            equalTo(otherNode.getId())
        );
    }

    public void testIgnoresOlderTerms() throws Exception {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final long currentTerm = randomLongBetween(100, 1000);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var masterNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID(random()));
        final var clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()).build())
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(currentTerm).build()).build())
            .build();

        var tasks = Stream.concat(
            Stream.generate(() -> createRandomTask(masterNode, randomLongBetween(0, currentTerm - 1))).limit(randomLongBetween(1, 10)),
            Stream.of(createRandomTask(masterNode, currentTerm))
        ).toList();

        ClusterStateTaskExecutorUtils.executeHandlingResults(
            clusterState,
            executor,
            tasks,
            t -> assertThat(t.term(), equalTo(currentTerm)),
            (t, e) -> {
                assertThat(t.term(), lessThan(currentTerm));
                assertThat(e, instanceOf(NotMasterException.class));
            }
        );
    }

    public void testDesiredNodesMembershipIsUpgradedWhenNewNodesJoin() throws Exception {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var actualizedDesiredNodes = randomList(0, 5, this::createActualizedDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);
        final var joiningDesiredNodes = randomList(1, 5, this::createPendingDesiredNode);

        final List<DiscoveryNode> joiningNodes = joiningDesiredNodes.stream()
            .map(desiredNode -> DesiredNodesTestCase.newDiscoveryNode(desiredNode.externalId()))
            .toList();

        final var clusterState = DesiredNodesTestCase.createClusterStateWithDiscoveryNodesAndDesiredNodes(
            actualizedDesiredNodes,
            pendingDesiredNodes,
            joiningDesiredNodes,
            true,
            false
        );
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);

        var tasks = joiningNodes.stream()
            .map(node -> JoinTask.singleNode(node, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, 0L))
            .toList();

        final var updatedClusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(clusterState, executor, tasks);

        final var updatedDesiredNodes = DesiredNodes.latestFromClusterState(clusterState);
        assertThat(updatedDesiredNodes, is(notNullValue()));

        assertThat(updatedDesiredNodes.nodes(), hasSize(desiredNodes.nodes().size()));
        assertDesiredNodesStatusIsCorrect(
            updatedClusterState,
            Stream.concat(actualizedDesiredNodes.stream(), joiningDesiredNodes.stream()).map(DesiredNodeWithStatus::desiredNode).toList(),
            pendingDesiredNodes.stream().map(DesiredNodeWithStatus::desiredNode).toList()
        );
    }

    public void testDesiredNodesMembershipIsUpgradedWhenANewMasterIsElected() throws Exception {
        final var allocationService = createAllocationService();
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);
        final var executor = new NodeJoinExecutor(allocationService, rerouteService);

        final var actualizedDesiredNodes = randomList(1, 5, this::createPendingDesiredNode);
        final var pendingDesiredNodes = randomList(0, 5, this::createPendingDesiredNode);

        final var clusterState = DesiredNodesTestCase.createClusterStateWithDiscoveryNodesAndDesiredNodes(
            actualizedDesiredNodes,
            pendingDesiredNodes,
            Collections.emptyList(),
            false,
            false
        );
        final var desiredNodes = DesiredNodes.latestFromClusterState(clusterState);

        final var completingElectionTask = JoinTask.completingElection(
            clusterState.nodes()
                .stream()
                .map(node -> new JoinTask.NodeJoinTask(node, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)),
            1L
        );

        final var updatedClusterState = ClusterStateTaskExecutorUtils.executeAndAssertSuccessful(
            clusterState,
            executor,
            List.of(completingElectionTask)
        );

        final var updatedDesiredNodes = DesiredNodes.latestFromClusterState(updatedClusterState);
        assertThat(updatedDesiredNodes, is(notNullValue()));

        assertThat(updatedDesiredNodes.nodes(), hasSize(desiredNodes.nodes().size()));
        assertDesiredNodesStatusIsCorrect(
            updatedClusterState,
            actualizedDesiredNodes.stream().map(DesiredNodeWithStatus::desiredNode).toList(),
            pendingDesiredNodes.stream().map(DesiredNodeWithStatus::desiredNode).toList()
        );
    }

    public void testPerNodeLogging() {
        final AllocationService allocationService = createAllocationService();
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        final RerouteService rerouteService = (reason, priority, listener) -> listener.onResponse(null);

        final NodeJoinExecutor executor = new NodeJoinExecutor(allocationService, rerouteService);

        final DiscoveryNode masterNode = DiscoveryNodeUtils.create(UUIDs.base64UUID());
        final ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).masterNodeId(masterNode.getId()))
            .build();

        final MockLogAppender appender = new MockLogAppender();
        final ThreadPool threadPool = new TestThreadPool("test");
        try (
            var ignored = appender.capturing(NodeJoinExecutor.class);
            var clusterService = ClusterServiceUtils.createClusterService(clusterState, threadPool)
        ) {
            final var node1 = DiscoveryNodeUtils.create(UUIDs.base64UUID());
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "info message",
                    LOGGER_NAME,
                    Level.INFO,
                    "node-join: [" + node1.descriptionWithoutAttributes() + "] with reason [" + TEST_REASON.message() + "]"
                )
            );
            assertNull(
                PlainActionFuture.<Void, RuntimeException>get(
                    future -> clusterService.getMasterService()
                        .createTaskQueue("test", Priority.NORMAL, executor)
                        .submitTask("test", JoinTask.singleNode(node1, TransportVersion.CURRENT, TEST_REASON, future, 0L), null),
                    10,
                    TimeUnit.SECONDS
                )
            );
            appender.assertAllExpectationsMatched();

            final var node2 = DiscoveryNodeUtils.create(UUIDs.base64UUID());
            final var testReasonWithLink = new JoinReason("test", ReferenceDocs.UNSTABLE_CLUSTER_TROUBLESHOOTING);
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "warn message with troubleshooting link",
                    LOGGER_NAME,
                    Level.WARN,
                    "node-join: ["
                        + node2.descriptionWithoutAttributes()
                        + "] with reason ["
                        + testReasonWithLink.message()
                        + "]; for troubleshooting guidance, see https://www.elastic.co/guide/en/elasticsearch/reference/*"
                )
            );
            assertNull(
                PlainActionFuture.<Void, RuntimeException>get(
                    future -> clusterService.getMasterService()
                        .createTaskQueue("test", Priority.NORMAL, executor)
                        .submitTask("test", JoinTask.singleNode(node2, TransportVersion.CURRENT, testReasonWithLink, future, 0L), null),
                    10,
                    TimeUnit.SECONDS
                )
            );
            appender.assertAllExpectationsMatched();
        } finally {
            TestThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
        }
    }

    private DesiredNodeWithStatus createActualizedDesiredNode() {
        return new DesiredNodeWithStatus(randomDesiredNode(), DesiredNodeWithStatus.Status.ACTUALIZED);
    }

    private DesiredNodeWithStatus createPendingDesiredNode() {
        return new DesiredNodeWithStatus(randomDesiredNode(), DesiredNodeWithStatus.Status.PENDING);
    }

    private static JoinTask createRandomTask(DiscoveryNode node, long term) {
        return randomBoolean()
            ? JoinTask.singleNode(node, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER, term)
            : JoinTask.completingElection(
                Stream.of(new JoinTask.NodeJoinTask(node, TransportVersion.CURRENT, TEST_REASON, NOT_COMPLETED_LISTENER)),
                term
            );
    }

    private static AllocationService createAllocationService() {
        final var allocationService = mock(AllocationService.class);
        when(allocationService.adaptAutoExpandReplicas(any())).then(invocationOnMock -> invocationOnMock.getArguments()[0]);
        when(allocationService.disassociateDeadNodes(any(), anyBoolean(), any())).then(
            invocationOnMock -> invocationOnMock.getArguments()[0]
        );
        return allocationService;
    }

    // Hard-coding the class name here because it is also mentioned in the troubleshooting docs, so should not be renamed without care.
    private static final String LOGGER_NAME = "org.elasticsearch.cluster.coordination.NodeJoinExecutor";

}
