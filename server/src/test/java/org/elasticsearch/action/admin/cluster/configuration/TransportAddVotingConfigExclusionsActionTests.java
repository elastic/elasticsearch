/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.admin.cluster.configuration;

import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.ClusterStateObserver.Listener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfigExclusion;
import org.elasticsearch.cluster.coordination.CoordinationMetadata.VotingConfiguration;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodes.Builder;
import org.elasticsearch.cluster.node.TestDiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
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
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.action.admin.cluster.configuration.TransportAddVotingConfigExclusionsAction.MAXIMUM_VOTING_CONFIG_EXCLUSIONS_SETTING;
import static org.elasticsearch.cluster.ClusterState.builder;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
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

    @BeforeClass
    public static void createThreadPoolAndClusterService() {
        threadPool = new TestThreadPool("test", Settings.EMPTY);
        localNode = makeDiscoveryNode("local");
        localNodeExclusion = new VotingConfigExclusion(localNode);
        otherNode1 = makeDiscoveryNode("other1");
        otherNode1Exclusion = new VotingConfigExclusion(otherNode1);
        otherNode2 = makeDiscoveryNode("other2");
        otherNode2Exclusion = new VotingConfigExclusion(otherNode2);
        otherDataNode = TestDiscoveryNode.create("data", "data", buildNewFakeTransportAddress(), emptyMap(), emptySet());
        clusterService = createClusterService(threadPool, localNode);
    }

    private static DiscoveryNode makeDiscoveryNode(String name) {
        return TestDiscoveryNode.create(name, name, buildNewFakeTransportAddress(), emptyMap(), Set.of(DiscoveryNodeRole.MASTER_ROLE));
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

        new TransportAddVotingConfigExclusionsAction(
            nodeSettings,
            clusterSettings,
            transportService,
            clusterService,
            threadPool,
            new ActionFilters(emptySet()),
            TestIndexNameExpressionResolver.newInstance(threadPool.getThreadContext())
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

    public void testWithdrawsVoteFromANode() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testWithdrawsVotesFromMultipleNodes() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testReturnsImmediatelyIfVoteAlreadyWithdrawn() throws InterruptedException {
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
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testExcludeAbsentNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(new String[] { "absent_id" }, Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(e -> countDownLatch.countDown())
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(
            Set.of(new VotingConfigExclusion("absent_id", VotingConfigExclusion.MISSING_VALUE_MARKER)),
            clusterService.getClusterApplierService().state().getVotingConfigExclusions()
        );
    }

    public void testExcludeExistingNodesByNodeIds() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(new String[] { "other1", "other2" }, Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testExcludeAbsentNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("absent_node"),
            expectSuccess(e -> countDownLatch.countDown())
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertEquals(
            Set.of(new VotingConfigExclusion(VotingConfigExclusion.MISSING_VALUE_MARKER, "absent_node")),
            clusterService.getClusterApplierService().state().getVotingConfigExclusions()
        );
    }

    public void testExcludeExistingNodesByNodeNames() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(2);

        clusterStateObserver.waitForNextChange(new AdjustConfigurationForExclusions(countDownLatch));
        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(
            clusterService.getClusterApplierService().state().getVotingConfigExclusions(),
            containsInAnyOrder(otherNode1Exclusion, otherNode2Exclusion)
        );
    }

    public void testSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
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

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeIdSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
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

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(new String[] { "other1" }, Strings.EMPTY_ARRAY, TimeValue.timeValueSeconds(30)),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testExcludeByNodeNameSucceedsEvenIfAllExclusionsAlreadyAdded() throws InterruptedException {
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

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1"),
            expectSuccess(r -> {
                assertNotNull(r);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        assertThat(clusterService.getClusterApplierService().state().getVotingConfigExclusions(), contains(otherNode1Exclusion));
    }

    public void testReturnsErrorIfMaximumExclusionCountExceeded() throws InterruptedException {
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
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest("other1", "other2"),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
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
    }

    public void testTimesOut() throws InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        final SetOnce<TransportException> exceptionHolder = new SetOnce<>();

        transportService.sendRequest(
            localNode,
            AddVotingConfigExclusionsAction.NAME,
            new AddVotingConfigExclusionsRequest(Strings.EMPTY_ARRAY, new String[] { "other1" }, TimeValue.timeValueMillis(100)),
            expectError(e -> {
                exceptionHolder.set(e);
                countDownLatch.countDown();
            })
        );

        assertTrue(countDownLatch.await(30, TimeUnit.SECONDS));
        final Throwable rootCause = exceptionHolder.get().getRootCause();
        assertThat(rootCause, instanceOf(ElasticsearchTimeoutException.class));
        assertThat(rootCause.getMessage(), startsWith("timed out waiting for voting config exclusions [{other1}"));
    }

    private TransportResponseHandler<ActionResponse.Empty> expectSuccess(Consumer<ActionResponse.Empty> onResponse) {
        return responseHandler(onResponse, e -> { throw new AssertionError("unexpected", e); });
    }

    private TransportResponseHandler<ActionResponse.Empty> expectError(Consumer<TransportException> onException) {
        return responseHandler(r -> { assert false : r; }, onException);
    }

    private TransportResponseHandler<ActionResponse.Empty> responseHandler(
        Consumer<ActionResponse.Empty> onResponse,
        Consumer<TransportException> onException
    ) {
        return new TransportResponseHandler<>() {
            @Override
            public void handleResponse(ActionResponse.Empty response) {
                onResponse.accept(response);
            }

            @Override
            public void handleException(TransportException exp) {
                onException.accept(exp);
            }

            @Override
            public ActionResponse.Empty read(StreamInput in) {
                return ActionResponse.Empty.INSTANCE;
            }
        };
    }

    private static class AdjustConfigurationForExclusions implements Listener {

        final CountDownLatch doneLatch;

        AdjustConfigurationForExclusions(CountDownLatch latch) {
            this.doneLatch = latch;
        }

        @Override
        public void onNewClusterState(ClusterState state) {
            clusterService.getMasterService().submitUnbatchedStateUpdateTask("reconfiguration", new ClusterStateUpdateTask() {
                @Override
                public ClusterState execute(ClusterState currentState) {
                    assertThat(currentState, sameInstance(state));
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

                @Override
                public void clusterStateProcessed(ClusterState oldState, ClusterState newState) {
                    doneLatch.countDown();
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

}
