/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.elasticsearch.exception.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListenerResponseHandler;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.coordination.Reconfigurator;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static java.util.Collections.emptySet;
import static org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;
import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

public class TransportAddVotingConfigExclusionsActionTests extends ESTestCase {

    private static ThreadPool threadPool;
    private static ClusterService clusterService;
    private static DiscoveryNode localNode, otherNode1, otherNode2, otherDataNode;
    private static VotingConfigExclusion localNodeExclusion, otherNode1Exclusion, otherNode2Exclusion;

    private TransportService transportService;
    private ClusterStateObserver clusterStateObserver;
    private ClusterSettings clusterSettings;
    private int staticMaximum;
    private FakeReconfigurator reconfigurator;

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = makeDiscoveryNode("local");
        localNodeExclusion = new VotingConfigExclusion(localNode);
        otherNode1 = makeDiscoveryNode("other1");
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = makeDiscoveryNode("other2");
        otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        otherDataNode = DiscoveryNodeUtils.builder("data").name("data").roles(emptySet()).build();
        clusterService = createClusterService(threadPool, localNode);
    }

    private static DiscoveryNode makeDiscoveryNode(String name) {
        return DiscoveryNodeUtils.builder(name).name(name).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
    }

    @AfterClass
    public static void shutdownThreadPoolAndClusterService() {
        clusterService.stop();
        threadPool.shutdown();
    }

    @Before
    public void setupForTest() {
        final MockTransport transport = new MockTransport();
        transportService = transport.createTransportService(
            Settings.EMPTY,
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundTransportAddress -> localNode,
            null,
            emptySet()
        );

        final Settings.Builder nodeSettingsBuilder = Settings.builder();
        if (randomBoolean()) {
            staticMaximum = between(5, 15);
            nodeSettingsBuilder.put(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey(), staticMaximum);
        } else {
            staticMaximum = MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.get(Settings.EMPTY);
        }
        final Settings nodeSettings = nodeSettingsBuilder.build();
        clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        reconfigurator = new FakeReconfigurator();

        new TransportAddVotingConfigExclusionsAction(
            nodeSettings,
            clusterSettings,
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            reconfigurator
        ); // registers action

        transportService.start();
        transportService.acceptIncomingRequests();

        final VotingConfiguration allNodesConfig = VotingConfiguration.of(localNode, otherNode1, otherNode2);

        setState(
            clusterService,
            builder(new ClusterName("cluster")).nodes(
                new Builder().add(localNode)
                    .add(otherNode1)
                    .add(otherNode2)
                    .add(otherDataNode)
                    .localNodeId(localNode.getId())
                    .masterNodeId(localNode.getId())
            )
                .metadata(
                    Metadata.builder()
                        .coordinationMetadata(
                            CoordinationMetadata.builder()
                                .lastAcceptedConfiguration(allNodesConfig)
                                .lastCommittedConfiguration(allNodesConfig)
                                .build()
                        )
                )
        );

        clusterStateObserver = new ClusterStateObserver(clusterService, null, logger, threadPool.getThreadContext());
    }

    private static void assertAllExclusionsApplied(ClusterState clusterState) {
        final var lastAcceptedConfiguration = clusterState.coordinationMetadata().getLastAcceptedConfiguration();
        final var lastCommittedConfiguration = clusterState.coordinationMetadata().getLastCommittedConfiguration();
        for (final var votingConfigExclusion : clusterState.getVotingConfigExclusions()) {
            assertThat(lastAcceptedConfiguration.getNodeIds(), not(hasItem(votingConfigExclusion.getNodeId())));
            assertThat(lastCommittedConfiguration.getNodeIds(), not(hasItem(votingConfigExclusion.getNodeId())));
        }
    }

    public void testWithdrawsVoteFromANode() {
        final var countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                final var state = clusterService.getClusterApplierService().state();
                assertThat(state.getVotingConfigExclusions(), contains(otherNode1Exclusion));
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testWithdrawsVotesFromMultipleNodes() {
        final var countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                final var state = clusterService.getClusterApplierService().state();
                assertThat(state.getVotingConfigExclusions(), containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testReturnsImmediatelyIfVoteAlreadyWithdrawn() {
        final ClusterState state = clusterService.state();
        setState(
            clusterService,
            builder(state).metadata(
                Metadata.builder(state.metadata())
                    .coordinationMetadata(
                        CoordinationMetadata.builder(state.coordinationMetadata())
                            .lastCommittedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                            .lastAcceptedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                            .build()
                    )
            )
        );

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // no observer to reconfigure
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                final var finalState = clusterService.getClusterApplierService().state();
                assertThat(finalState.getVotingConfigExclusions(), contains(otherNode1Exclusion));
                assertAllExclusionsApplied(finalState);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeAbsentNodesByNodeIds() {
        final var countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(
                TEST_REQUEST_TIMEOUT,
                new String[] { "absent_id" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(r -> {
                final var state = clusterService.getClusterApplierService().state();
                assertEquals(
                    Set.of(new VotingConfigExclusion("absent_id", VotingConfigExclusion.MISSING_VALUE_MARKER)),
                    state.getVotingConfigExclusions()
                );
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeExistingNodesByNodeIds() {
        final var countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(
                TEST_REQUEST_TIMEOUT,
                new String[] { "other1", "other2" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(r -> {
                assertNotNull(r);
                final var state = clusterService.getClusterApplierService().state();
                assertThat(state.getVotingConfigExclusions(), containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeAbsentNodesByNodeNames() {
        final var countDownLatch = new CountDownLatch(1);
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "absent_node"),
            expectSuccess(r -> {
                final var state = clusterService.getClusterApplierService().state();
                assertEquals(
                    Set.of(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, "absent_node")),
                    state.getVotingConfigExclusions()
                );
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeExistingNodesByNodeNames() {
        final var countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                final var state = clusterService.getClusterApplierService().state();
                assertThat(state.getVotingConfigExclusions(), containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion));
                assertAllExclusionsApplied(state);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testTriggersReconfigurationEvenIfAllExclusionsAlreadyAddedButStillInConfiguration() {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata()).addVotingConfigExclusion(otherNode1Exclusion).build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            randomFrom(
                new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1"),
                new AddVotingConfigExclusionsRequest(
                    TEST_REQUEST_TIMEOUT,
                    new String[] { "other1" },
                    Strings.EMPTY_ARRAY,
                    TimeValue.timeValueSeconds(30)
                )
            ),
            expectSuccess(r -> {
                assertNotNull(r);
                final var finalState = clusterService.getClusterApplierService().state();
                assertThat(finalState.getVotingConfigExclusions(), contains(otherNode1Exclusion));
                assertAllExclusionsApplied(finalState);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeByNodeIdSucceedsEvenIfAllExclusionsAlreadyAdded() {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata())
                        .lastCommittedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                        .lastAcceptedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                        .addVotingConfigExclusion(otherNode1Exclusion)
                        .build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions());
        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(
                TEST_REQUEST_TIMEOUT,
                new String[] { "other1" },
                Strings.EMPTY_ARRAY,
                TimeValue.timeValueSeconds(30)
            ),
            expectSuccess(r -> {
                assertNotNull(r);
                final var finalState = clusterService.getClusterApplierService().state();
                assertThat(finalState.getVotingConfigExclusions(), contains(otherNode1Exclusion));
                assertAllExclusionsApplied(finalState);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testExcludeByNodeNameSucceedsEvenIfAllExclusionsAlreadyAdded() {
        final ClusterState state = clusterService.state();
        final ClusterState.Builder builder = builder(state);
        builder.metadata(
            Metadata.builder(state.metadata())
                .coordinationMetadata(
                    CoordinationMetadata.builder(state.coordinationMetadata())
                        .lastCommittedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                        .lastAcceptedConfiguration(VotingConfiguration.of(localNode, otherNode2))
                        .addVotingConfigExclusion(otherNode1Exclusion)
                        .build()
                )
        );
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                final var finalState = clusterService.getClusterApplierService().state();
                assertThat(finalState.getVotingConfigExclusions(), contains(otherNode1Exclusion));
                assertAllExclusionsApplied(finalState);
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testReturnsErrorIfMaximumExclusionCountExceeded() {
        final Metadata.Builder metadataBuilder = Metadata.builder(clusterService.state().metadata());
        CoordinationMetadata.Builder coordinationMetadataBuilder = CoordinationMetadata.builder(
            clusterService.state().coordinationMetadata()
        ).addVotingConfigExclusion(localNodeExclusion);

        final int actualMaximum;
        if (randomBoolean()) {
            actualMaximum = staticMaximum;
        } else {
            actualMaximum = between(2, 15);
            clusterSettings.applySettings(
                Settings.builder()
                    .put(clusterService.state().metadata().persistentSettings())
                    .put(MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING.getKey(), actualMaximum)
                    .build()
            );
        }

        for (int i = 2; i < actualMaximum; i++) {
            coordinationMetadataBuilder.addVotingConfigExclusion(
                new VotingConfigExclusion(randomAlphaOfLength(10), randomAlphaOfLength(10))
            );
        }

        final int existingCount, newCount;
        if (randomBoolean()) {
            coordinationMetadataBuilder.addVotingConfigExclusion(otherNode1Exclusion);
            existingCount = actualMaximum;
            newCount = 1;
        } else {
            existingCount = actualMaximum - 1;
            newCount = 2;
        }

        metadataBuilder.coordinationMetadata(coordinationMetadataBuilder.build());

        final ClusterState.Builder builder = builder(clusterService.state()).metadata(metadataBuilder);
        setState(clusterService, builder);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(TEST_REQUEST_TIMEOUT, "other1", "other2"),
            expectError(e -> {
                final Throwable rootCause = e.getRootCause();
                assertThat(rootCause, instanceOf(IllegalArgumentException.class));
                assertThat(
                    rootCause.getMessage(),
                    equalTo(
                        "add voting config exclusions request for nodes named [other1, other2] would add ["
                            + newCount
                            + "] exclusions to the existing ["
                            + existingCount
                            + "] which would exceed the maximum of ["
                            + actualMaximum
                            + "] set by [cluster.max_voting_config_exclusions]"
                    )
                );
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testTimesOut() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(
                TEST_REQUEST_TIMEOUT,
                Strings.EMPTY_ARRAY,
                new String[] { "other1" },
                TimeValue.timeValueMillis(100)
            ),
            expectError(e -> {
                final Throwable rootCause = e.getRootCause();
                assertThat(rootCause, instanceOf(ElasticsearchTimeoutException.class));
                assertThat(rootCause.getMessage(), equalTo("timed out waiting for voting config exclusions to take effect"));
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    public void testCannotAddVotingConfigExclusionsWhenItIsDisabled() {
        final CountDownLatch countDownLatch = new CountDownLatch(1);

        reconfigurator.disableUserVotingConfigModifications();

        transportService.sendRequest(
            localNode,
            TransportAddVotingConfigExclusionsAction.TYPE.name(),
            new AddVotingConfigExclusionsRequest(
                TEST_REQUEST_TIMEOUT,
                Strings.EMPTY_ARRAY,
                new String[] { "other1" },
                TimeValue.timeValueMillis(100)
            ),
            expectError(e -> {
                final Throwable rootCause = e.getRootCause();
                assertThat(rootCause, instanceOf(IllegalStateException.class));
                assertThat(rootCause.getMessage(), startsWith("Unable to modify the voting configuration"));
                countDownLatch.countDown();
            })
        );
        safeAwait(countDownLatch);
    }

    private TransportResponseHandler<ActionResponse.Empty> expectSuccess(Consumer<ActionResponse.Empty> onResponse) {
        return new ActionListenerResponseHandler<>(
            ActionTestUtils.assertNoFailureListener(onResponse::accept),
            in -> ActionResponse.Empty.INSTANCE,
            TransportResponseHandler.TRANSPORT_WORKER
        );
    }

    private TransportResponseHandler<ActionResponse.Empty> expectError(Consumer<TransportException> onException) {
        return new TransportResponseHandler<>() {
            @Override
            public ActionResponse.Empty read(StreamInput in) {
                return ActionResponse.Empty.INSTANCE;
            }

            @Override
            public Executor executor() {
                return TransportResponseHandler.TRANSPORT_WORKER;
            }

            @Override
            public void handleResponse(ActionResponse.Empty response) {
                assert false : response;
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }
        };
    }

    private static class AdjustConfigurationForExclusions implements Listener {
        @Override
        public void onNewClusterState(ClusterState state) {
            final var prio = randomFrom(Priority.values());
            clusterService.getMasterService().submitUnbatchedStateUpdateTask("reconfiguration", new ClusterStateUpdateTask(prio) {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    if (prio.compareTo(Priority.URGENT) <= 0) {
                        assertThat(currentState, sameInstance(state));
                    }
                    final Set<String> votingNodeIds = new HashSet<>();
                    currentState.nodes().forEach(n -> votingNodeIds.add(n.getId()));
                    currentState.getVotingConfigExclusions().forEach(t -> votingNodeIds.remove(t.getNodeId()));
                    final VotingConfiguration votingConfiguration = new VotingConfiguration(votingNodeIds);
                    return builder(currentState).metadata(
                        Metadata.builder(currentState.metadata())
                            .coordinationMetadata(
                                CoordinationMetadata.builder(currentState.coordinationMetadata())
                                    .lastAcceptedConfiguration(votingConfiguration)
                                    .lastCommittedConfiguration(votingConfiguration)
                                    .build()
                            )
                    ).build();
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError("unexpected failure", e);
                }
            });
        }

        @Override
        public void onClusterServiceClose() {
            throw new AssertionError("unexpected close");
        }

        @Override
        public void onTimeout(TimeValue timeout) {
            throw new AssertionError("unexpected timeout");
        }
    }

    static class FakeReconfigurator extends Reconfigurator {
        private final AtomicBoolean canModifyVotingConfiguration = new AtomicBoolean(true);

        FakeReconfigurator() {
            super(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS));
        }

        @Override
        public void ensureVotingConfigCanBeModified() {
            if (canModifyVotingConfiguration.get() == false) {
                throw new IllegalStateException("Unable to modify the voting configuration");
            }
        }

        void disableUserVotingConfigModifications() {
            canModifyVotingConfiguration.set(false);
        }
    }

}
