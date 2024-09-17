/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gateway;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.block.ClusterBlockLevel;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.CoordinationMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.cluster.routing.ShardRoutingRoleStrategy;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.FakeThreadPoolMasterService;
import org.elasticsearch.cluster.service.MasterServiceTaskQueue;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.common.settings.ClusterSettings.createBuiltInClusterSettings;
import static org.elasticsearch.gateway.GatewayService.EXPECTED_DATA_NODES_SETTING;
import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_DATA_NODES_SETTING;
import static org.elasticsearch.gateway.GatewayService.RECOVER_AFTER_TIME_SETTING;
import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GatewayServiceTests extends ESTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;
    private AtomicInteger rerouteCount;
    private String dataNodeIdPrefix;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        deterministicTaskQueue = new DeterministicTaskQueue();
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(0L));
        rerouteCount = new AtomicInteger();
        dataNodeIdPrefix = randomAlphaOfLength(10) + "-";
    }

    private GatewayService createGatewayService(final Settings.Builder settingsBuilder, final ClusterState initialState) {
        return createGatewayService(createClusterService(settingsBuilder, initialState));
    }

    private GatewayService createGatewayService(final ClusterService clusterService) {
        final RerouteService rerouteService = (reason, priority, listener) -> {
            rerouteCount.incrementAndGet();
            listener.onResponse(null);
        };

        final var gatewayService = new GatewayService(
            clusterService.getSettings(),
            rerouteService,
            clusterService,
            ShardRoutingRoleStrategy.NO_SHARD_CREATION,
            clusterService.threadPool()
        );

        gatewayService.start();
        return gatewayService;
    }

    private ClusterService createClusterService(final Settings.Builder settingsBuilder, final ClusterState initialState) {
        final var threadPool = deterministicTaskQueue.getThreadPool();
        final var settings = settingsBuilder.build();
        final var clusterSettings = createBuiltInClusterSettings(settings);

        final var clusterService = new ClusterService(
            settings,
            clusterSettings,
            new FakeThreadPoolMasterService(initialState.nodes().getLocalNodeId(), threadPool, deterministicTaskQueue::scheduleNow),
            new ClusterApplierService(initialState.nodes().getLocalNodeId(), settings, clusterSettings, threadPool) {
                @Override
                protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
                    return deterministicTaskQueue.getPrioritizedEsThreadPoolExecutor();
                }
            }
        );

        clusterService.getClusterApplierService().setInitialState(initialState);
        clusterService.setNodeConnectionsService(ClusterServiceUtils.createNoOpNodeConnectionsService());
        clusterService.getMasterService()
            .setClusterStatePublisher(ClusterServiceUtils.createClusterStatePublisher(clusterService.getClusterApplierService()));
        clusterService.getMasterService().setClusterStateSupplier(clusterService.getClusterApplierService()::state);
        clusterService.start();
        return clusterService;
    }

    public void testDefaultRecoverAfterTime() {
        // check that the default is not set
        final ClusterState initialState = buildClusterState(1, 1);
        GatewayService service = createGatewayService(Settings.builder(), initialState);
        assertNull(service.recoverAfterTime());

        // ensure default is set when setting expected_data_nodes
        service = createGatewayService(Settings.builder().put("gateway.expected_data_nodes", 1), initialState);
        assertThat(service.recoverAfterTime(), equalTo(GatewayService.DEFAULT_RECOVER_AFTER_TIME_IF_EXPECTED_NODES_IS_SET));

        // ensure settings override default
        final TimeValue timeValue = TimeValue.timeValueHours(3);

        // ensure default is set when setting expected_nodes
        service = createGatewayService(Settings.builder().put("gateway.recover_after_time", timeValue.toString()), initialState);
        assertThat(service.recoverAfterTime().millis(), equalTo(timeValue.millis()));
    }

    public void testRecoverStateUpdateTask() throws Exception {
        final long expectedTerm = randomLongBetween(1, 42);
        ClusterState stateWithBlock = buildClusterState(1, expectedTerm);
        GatewayService service = createGatewayService(Settings.builder(), stateWithBlock);
        ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm);

        ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertNotEquals(recoveredState, stateWithBlock);
        assertThat(recoveredState.blocks().global(ClusterBlockLevel.METADATA_WRITE), not(hasItem(STATE_NOT_RECOVERED_BLOCK)));

        ClusterState clusterState = clusterStateUpdateTask.execute(recoveredState);
        assertSame(recoveredState, clusterState);
    }

    public void testRecoveryWillAbortIfExpectedTermDoesNotMatch() throws Exception {
        final long expectedTerm = randomLongBetween(1, 42);
        final ClusterState stateWithBlock = buildClusterState(1, randomLongBetween(43, 99));
        final GatewayService service = createGatewayService(Settings.builder(), stateWithBlock);
        final ClusterStateUpdateTask clusterStateUpdateTask = service.new RecoverStateUpdateTask(expectedTerm);

        final ClusterState recoveredState = clusterStateUpdateTask.execute(stateWithBlock);
        assertSame(recoveredState, stateWithBlock);
    }

    public void testNoActionWhenNodeIsNotMaster() {
        final String localNodeId = dataNodeIdPrefix + "0";
        final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder()
            .localNodeId(localNodeId)
            .add(DiscoveryNodeUtils.create(localNodeId));
        if (randomBoolean()) {
            final String masterNodeId = dataNodeIdPrefix + "1";
            nodesBuilder.masterNodeId(masterNodeId).add(DiscoveryNodeUtils.create(masterNodeId));
        }

        final ClusterState initialState = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesBuilder.build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();

        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        when(clusterChangedEvent.state()).thenReturn(initialState);

        final GatewayService gatewayService = createGatewayService(Settings.builder(), initialState);
        gatewayService.clusterChanged(clusterChangedEvent);
        assertThat(deterministicTaskQueue.hasAnyTasks(), is(false));
        assertThat(gatewayService.currentPendingStateRecovery, nullValue());
    }

    public void testNoActionWhenStateIsAlreadyRecovered() {
        final ClusterService clusterService = createClusterService(
            Settings.builder()
                .put(GatewayService.RECOVER_AFTER_DATA_NODES_SETTING.getKey(), 2)
                .put(GatewayService.EXPECTED_DATA_NODES_SETTING.getKey(), 4)
                .put(GatewayService.RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(10)),
            ClusterState.builder(buildClusterState(2, randomIntBetween(1, 42))).blocks(ClusterBlocks.builder()).build()
        );
        final GatewayService gatewayService = createGatewayService(clusterService);
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(0));

        final var taskQueue = createSetDataNodeCountTaskQueue(clusterService);
        final int newDataNodeCount = randomIntBetween(1, 5);
        taskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(newDataNodeCount), null);
        deterministicTaskQueue.runAllTasksInTimeOrder();

        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(0));
        assertThat(gatewayService.currentPendingStateRecovery, nullValue());
        assertThat(clusterService.state().nodes().getDataNodes().size(), equalTo(newDataNodeCount));
    }

    public void testImmediateRecovery() {
        final Settings.Builder settingsBuilder = Settings.builder();
        final int expectedNumberOfDataNodes = randomIntBetween(1, 3);
        // The cluster recover immediately because it either has the required expectedDataNodes
        // or both expectedDataNodes and recoverAfterTime are not configured
        if (randomBoolean()) {
            settingsBuilder.put(EXPECTED_DATA_NODES_SETTING.getKey(), expectedNumberOfDataNodes);
        }

        final ClusterState initialState = buildClusterState(expectedNumberOfDataNodes, 0);
        final ClusterService clusterService = createClusterService(settingsBuilder, initialState);
        final GatewayService gatewayService = createGatewayService(clusterService);
        assertClusterStateBlocks(clusterService, true);
        assertThat(rerouteCount.get(), equalTo(0));

        // Recover immediately
        final var setClusterStateTaskQueue = createSetClusterStateTaskQueue(clusterService);
        final ClusterState clusterStateOfTerm1 = incrementTerm(initialState);
        setClusterStateTaskQueue.submitTask(randomAlphaOfLength(5), new SetClusterStateTask(clusterStateOfTerm1), null);
        assertThat(deterministicTaskQueue.hasRunnableTasks(), is(true));
        deterministicTaskQueue.runAllRunnableTasks();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(1));
        final var pendingStateRecoveryOfTerm1 = gatewayService.currentPendingStateRecovery;
        assertThat(pendingStateRecoveryOfTerm1, notNullValue());

        // Will *not* run recover again for the same term
        setClusterStateTaskQueue.submitTask(randomAlphaOfLength(5), new SetClusterStateTask(clusterStateOfTerm1), null);
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(deterministicTaskQueue.hasAnyTasks(), is(false));
        assertThat(rerouteCount.get(), equalTo(1));
        assertClusterStateBlocks(clusterService, true);
        assertThat(gatewayService.currentPendingStateRecovery, sameInstance(pendingStateRecoveryOfTerm1));

        // Will run recover again for a newer term
        final ClusterState clusterStateOfTerm2 = ClusterState.builder(initialState)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(2).build()).build())
            .build();
        setClusterStateTaskQueue.submitTask(randomAlphaOfLength(5), new SetClusterStateTask(clusterStateOfTerm2), null);
        assertThat(deterministicTaskQueue.hasRunnableTasks(), is(true));
        deterministicTaskQueue.runAllRunnableTasks();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(2));
        assertThat(gatewayService.currentPendingStateRecovery, not(sameInstance(pendingStateRecoveryOfTerm1)));

        // Never ran any scheduled task since recovery is immediate
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(false));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(0L));
    }

    public void testScheduledRecovery() {
        final var hasRecoverAfterTime = randomBoolean();
        final ClusterService clusterService = createServicesTupleForScheduledRecovery(randomIntBetween(2, 5), hasRecoverAfterTime).v1();

        // Recover when the scheduled recovery is ready to run
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(1));
        assertTimeElapsed(TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis());
    }

    public void testScheduledRecoveryCancelledWhenClusterCanRecoverImmediately() {
        final var expectedNumberOfDataNodes = randomIntBetween(2, 5);
        final boolean hasRecoverAfterTime = randomBoolean();
        final var servicesTuple = createServicesTupleForScheduledRecovery(expectedNumberOfDataNodes, hasRecoverAfterTime);
        final ClusterService clusterService = servicesTuple.v1();
        final GatewayService gatewayService = servicesTuple.v2();
        final var pendingStateRecoveryOfTerm1 = gatewayService.currentPendingStateRecovery;

        // The 1st schedule is cancelled when the cluster has enough nodes
        final var setDataNodeCountTaskQueue = createSetDataNodeCountTaskQueue(clusterService);
        setDataNodeCountTaskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(expectedNumberOfDataNodes), null);
        deterministicTaskQueue.runAllRunnableTasks();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(1));
        assertThat(gatewayService.currentPendingStateRecovery, sameInstance(pendingStateRecoveryOfTerm1));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(0L));
        // Cancelled scheduled recovery is a no-op
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(rerouteCount.get(), equalTo(1));
        assertTimeElapsed(TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis());
    }

    public void testScheduledRecoveryNoOpWhenNewTermBegins() {
        final var hasRecoverAfterTime = randomBoolean();
        final var servicesTuple = createServicesTupleForScheduledRecovery(randomIntBetween(2, 5), hasRecoverAfterTime);
        final ClusterService clusterService = servicesTuple.v1();
        final GatewayService gatewayService = servicesTuple.v2();
        final var setClusterStateTaskQueue = createSetClusterStateTaskQueue(clusterService);
        final var pendingStateRecoveryOfTerm1 = gatewayService.currentPendingStateRecovery;

        // The 1st schedule is effectively cancelled if a new term begins
        final TimeValue elapsed = TimeValue.timeValueMinutes(1);
        final ClusterState clusterStateOfTerm2 = incrementTerm(clusterService.state());
        deterministicTaskQueue.scheduleAtAndRunUpTo(
            elapsed.millis(),
            () -> setClusterStateTaskQueue.submitTask(randomAlphaOfLength(5), new SetClusterStateTask(clusterStateOfTerm2), null)
        );
        assertThat(gatewayService.currentPendingStateRecovery, not(sameInstance(pendingStateRecoveryOfTerm1)));
        // The 1st scheduled recovery is now a no-op
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(
            deterministicTaskQueue.getCurrentTimeMillis(),
            equalTo(TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis())
        );
        assertClusterStateBlocks(clusterService, true);
        assertThat(rerouteCount.get(), equalTo(0));
        // The 2nd schedule will perform the recovery
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(1));
        assertTimeElapsed(elapsed.millis() + TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis());
    }

    private Tuple<ClusterService, GatewayService> createServicesTupleForScheduledRecovery(
        int expectedNumberOfDataNodes,
        boolean hasRecoverAfterTime
    ) {
        final Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(EXPECTED_DATA_NODES_SETTING.getKey(), expectedNumberOfDataNodes);
        if (hasRecoverAfterTime) {
            settingsBuilder.put(RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(10));
        }
        final ClusterState initialState = buildClusterState(1, 0);
        final ClusterService clusterService = createClusterService(settingsBuilder, initialState);
        final GatewayService gatewayService = createGatewayService(clusterService);
        assertClusterStateBlocks(clusterService, true);

        final ClusterState clusterStateOfTerm1 = incrementTerm(initialState);
        final var setClusterStateTaskQueue = createSetClusterStateTaskQueue(clusterService);
        setClusterStateTaskQueue.submitTask(randomAlphaOfLength(5), new SetClusterStateTask(clusterStateOfTerm1), null);
        deterministicTaskQueue.runAllRunnableTasks(); // publish cluster state term change
        // recovery is scheduled but has not run yet
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(true));
        assertClusterStateBlocks(clusterService, true);
        assertThat(rerouteCount.get(), equalTo(0));
        final GatewayService.PendingStateRecovery pendingStateRecoveryOfInitialTerm = gatewayService.currentPendingStateRecovery;
        assertThat(pendingStateRecoveryOfInitialTerm, notNullValue());
        return new Tuple<>(clusterService, gatewayService);
    }

    public void testScheduledRecoveryWithRecoverAfterNodes() {
        final Settings.Builder settingsBuilder = Settings.builder();
        final int expectedNumberOfDataNodes = randomIntBetween(4, 6);
        final boolean hasRecoverAfterTime = randomBoolean();
        if (hasRecoverAfterTime) {
            settingsBuilder.put(RECOVER_AFTER_TIME_SETTING.getKey(), TimeValue.timeValueMinutes(10));
        } else {
            settingsBuilder.put(EXPECTED_DATA_NODES_SETTING.getKey(), expectedNumberOfDataNodes);
        }
        final int recoverAfterNodes = expectedNumberOfDataNodes - 1;
        settingsBuilder.put(RECOVER_AFTER_DATA_NODES_SETTING.getKey(), recoverAfterNodes);

        final ClusterState initialState = buildClusterState(1, 1);
        final ClusterService clusterService = createClusterService(settingsBuilder, initialState);
        final GatewayService gatewayService = createGatewayService(clusterService);
        assertClusterStateBlocks(clusterService, true);

        // Not recover because recoverAfterDataNodes not met
        final var setDataNodeCountTaskQueue = createSetDataNodeCountTaskQueue(clusterService);
        setDataNodeCountTaskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(recoverAfterNodes - 1), null);
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(0L));
        assertClusterStateBlocks(clusterService, true);
        final var pendingStateRecoveryOfInitialTerm = gatewayService.currentPendingStateRecovery;
        assertThat(pendingStateRecoveryOfInitialTerm, notNullValue());

        // The 1st scheduled recovery when recoverAfterDataNodes is met
        setDataNodeCountTaskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(recoverAfterNodes), null);
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(deterministicTaskQueue.hasDeferredTasks(), is(true));
        assertThat(gatewayService.currentPendingStateRecovery, sameInstance(pendingStateRecoveryOfInitialTerm));

        // The 1st schedule is cancelled when data nodes drop below recoverAfterDataNodes
        final TimeValue elapsed = TimeValue.timeValueMinutes(1);
        deterministicTaskQueue.scheduleAtAndRunUpTo(
            elapsed.millis(),
            () -> setDataNodeCountTaskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(recoverAfterNodes - 1), null)
        );

        // The 2nd scheduled recovery when data nodes are above recoverAfterDataNodes again
        deterministicTaskQueue.scheduleAtAndRunUpTo(
            elapsed.millis() * 2,
            () -> setDataNodeCountTaskQueue.submitTask(randomAlphaOfLength(5), new SetDataNodeCountTask(recoverAfterNodes), null)
        );

        assertThat(gatewayService.currentPendingStateRecovery, sameInstance(pendingStateRecoveryOfInitialTerm));
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(elapsed.millis() * 2));

        // The 1st scheduled recovery is now a no-op since it is cancelled
        deterministicTaskQueue.advanceTime();
        deterministicTaskQueue.runAllRunnableTasks();
        assertThat(
            deterministicTaskQueue.getCurrentTimeMillis(),
            equalTo(TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis())
        );
        assertClusterStateBlocks(clusterService, true);
        assertThat(rerouteCount.get(), equalTo(0));

        // The 2nd scheduled recovery will recover the state
        deterministicTaskQueue.runAllTasksInTimeOrder();
        assertClusterStateBlocks(clusterService, false);
        assertThat(rerouteCount.get(), equalTo(1));
        assertTimeElapsed(elapsed.millis() * 2 + TimeValue.timeValueMinutes(hasRecoverAfterTime ? 10 : 5).millis());
    }

    private void assertClusterStateBlocks(ClusterService clusterService, boolean isBlocked) {
        assertThat(clusterService.state().blocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK), is(isBlocked));
    }

    private void assertTimeElapsed(long elapsedInMillis) {
        assertThat(deterministicTaskQueue.getCurrentTimeMillis(), equalTo(elapsedInMillis));
    }

    private ClusterState buildClusterState(int numberOfNodes, long term) {
        assert numberOfNodes >= 1;
        final String localNodeId = dataNodeIdPrefix + "0";
        final DiscoveryNodes.Builder discoveryNodesBuilder = DiscoveryNodes.builder()
            .localNodeId(localNodeId)
            .masterNodeId(localNodeId)
            .add(DiscoveryNodeUtils.create(localNodeId));
        for (int i = 1; i < numberOfNodes; i++) {
            discoveryNodesBuilder.add(DiscoveryNodeUtils.create(dataNodeIdPrefix + i));
        }

        final ClusterState stateWithBlock = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(discoveryNodesBuilder.build())
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(term).build()).build())
            .blocks(ClusterBlocks.builder().addGlobalBlock(STATE_NOT_RECOVERED_BLOCK).build())
            .build();
        return stateWithBlock;
    }

    private static ClusterState incrementTerm(ClusterState initialState) {
        return ClusterState.builder(initialState)
            .metadata(Metadata.builder().coordinationMetadata(CoordinationMetadata.builder().term(initialState.term() + 1).build()).build())
            .build();
    }

    record SetDataNodeCountTask(int dataNodeCount) implements ClusterStateTaskListener {
        SetDataNodeCountTask(int dataNodeCount) {
            assertThat(dataNodeCount, greaterThanOrEqualTo(1));
            this.dataNodeCount = dataNodeCount;
        }

        @Override
        public void onFailure(Exception e) {
            fail(e, "unexpected failure");
        }
    }

    private MasterServiceTaskQueue<SetDataNodeCountTask> createSetDataNodeCountTaskQueue(ClusterService clusterService) {
        return clusterService.createTaskQueue("set-data-node-count", Priority.NORMAL, batchExecutionContext -> {
            final ClusterState initialState = batchExecutionContext.initialState();
            final DiscoveryNodes initialNodes = initialState.nodes();
            final int initialDataNodeCount = initialNodes.getDataNodes().size();
            int targetDataNodeCount = initialDataNodeCount;
            for (var taskContext : batchExecutionContext.taskContexts()) {
                targetDataNodeCount = taskContext.getTask().dataNodeCount();
                taskContext.success(() -> {});
            }
            if (targetDataNodeCount == initialDataNodeCount) {
                return initialState;
            }

            final DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(initialNodes);
            for (int i = initialDataNodeCount; i < targetDataNodeCount; i++) {
                nodesBuilder.add(DiscoveryNodeUtils.create(dataNodeIdPrefix + i));
            }
            for (int i = targetDataNodeCount; i < initialDataNodeCount; i++) {
                nodesBuilder.remove(dataNodeIdPrefix + i);
            }
            final DiscoveryNodes targetNodes = nodesBuilder.build();
            assertThat(targetNodes.getDataNodes().size(), equalTo(targetDataNodeCount));
            return ClusterState.builder(initialState).nodes(targetNodes).build();
        });
    }

    record SetClusterStateTask(ClusterState clusterState) implements ClusterStateTaskListener {
        @Override
        public void onFailure(Exception e) {
            fail(e, "unexpected failure");
        }
    }

    private MasterServiceTaskQueue<SetClusterStateTask> createSetClusterStateTaskQueue(ClusterService clusterService) {
        return clusterService.createTaskQueue("set-cluster-state", Priority.NORMAL, batchExecutionContext -> {
            final var initialState = batchExecutionContext.initialState();
            var targetState = initialState;
            for (var taskContext : batchExecutionContext.taskContexts()) {
                targetState = taskContext.getTask().clusterState();
                taskContext.success(() -> {});
            }
            // fix up the version numbers
            return ClusterState.builder(targetState)
                .version(initialState.version())
                .metadata(Metadata.builder(targetState.metadata()).version(initialState.metadata().version()))
                .build();
        });
    }
}
