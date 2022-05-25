/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsAction;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.IndicesAdminClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.ImmutableOpenMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.ml.MlMetadata;
import org.junit.Before;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class MlInitializationServiceTests extends ESTestCase {

    private static final ClusterName CLUSTER_NAME = new ClusterName("my_cluster");

    private ThreadPool threadPool;
    private ExecutorService executorService;
    private ClusterService clusterService;
    private Client client;
    private MlAssignmentNotifier mlAssignmentNotifier;

    @Before
    public void setUpMocks() {
        threadPool = mock(ThreadPool.class);
        executorService = mock(ExecutorService.class);
        clusterService = mock(ClusterService.class);
        client = mock(Client.class);
        mlAssignmentNotifier = mock(MlAssignmentNotifier.class);

        doAnswer(invocation -> {
            ((Runnable) invocation.getArguments()[0]).run();
            return null;
        }).when(executorService).execute(any(Runnable.class));
        when(threadPool.executor(ThreadPool.Names.GENERIC)).thenReturn(executorService);
        when(threadPool.executor(MachineLearning.UTILITY_THREAD_POOL_NAME)).thenReturn(executorService);

        Scheduler.ScheduledCancellable scheduledCancellable = mock(Scheduler.ScheduledCancellable.class);
        when(threadPool.schedule(any(), any(), any())).thenReturn(scheduledCancellable);

        when(clusterService.getClusterName()).thenReturn(CLUSTER_NAME);

        @SuppressWarnings("unchecked")
        ActionFuture<GetSettingsResponse> getSettingsResponseActionFuture = mock(ActionFuture.class);
        when(getSettingsResponseActionFuture.actionGet()).thenReturn(new GetSettingsResponse(ImmutableOpenMap.of(), ImmutableOpenMap.of()));
        IndicesAdminClient indicesAdminClient = mock(IndicesAdminClient.class);
        when(indicesAdminClient.getSettings(any())).thenReturn(getSettingsResponseActionFuture);
        AdminClient adminClient = mock(AdminClient.class);
        when(adminClient.indices()).thenReturn(indicesAdminClient);
        when(client.admin()).thenReturn(adminClient);
        @SuppressWarnings("unchecked")
        ActionFuture<GetSettingsResponse> actionFuture = mock(ActionFuture.class);
        when(actionFuture.actionGet()).thenReturn(new GetSettingsResponse(ImmutableOpenMap.of(), ImmutableOpenMap.of()));
        when(client.execute(eq(GetSettingsAction.INSTANCE), any())).thenReturn(actionFuture);
    }

    public void testInitialize() {
        MlInitializationService initializationService = new MlInitializationService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            client,
            mlAssignmentNotifier
        );
        initializationService.onMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(true));
    }

    public void testInitialize_noMasterNode() {
        MlInitializationService initializationService = new MlInitializationService(
            Settings.EMPTY,
            threadPool,
            clusterService,
            client,
            mlAssignmentNotifier
        );
        initializationService.offMaster();
        assertThat(initializationService.getDailyMaintenanceService().isStarted(), is(false));
    }

    public void testNodeGoesFromMasterToNonMasterAndBack() {
        MlDailyMaintenanceService initialDailyMaintenanceService = mock(MlDailyMaintenanceService.class);

        MlInitializationService initializationService = new MlInitializationService(
            client,
            threadPool,
            initialDailyMaintenanceService,
            clusterService
        );
        initializationService.offMaster();
        verify(initialDailyMaintenanceService).stop();

        initializationService.onMaster();
        verify(initialDailyMaintenanceService).start();
    }

    public void testRatioUpdateIfNecessaryWhenRatioIsKnown() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(ByteSizeValue.ofGb(8).getBytes()),
            MachineLearning.CPU_RATIO_NODE_ATTR,
            "0.5",
            MachineLearning.AVAILABLE_PROCESSORS_NODE_ATTR,
            "4"
        );
        List<DiscoveryNode> nodes = buildNodesWithAttr(nodeAttr);
        assertThat(MlInitializationService.ratioUpdateIfNecessary(nodes, MlMetadataTests.randomInstance()).isEmpty(), is(true));
    }

    public void testRatioUpdateEmptyNodes() {
        assertThat(MlInitializationService.ratioUpdateIfNecessary(List.of(), MlMetadataTests.randomInstance()).isEmpty(), is(true));
    }

    public void testRatioUpdateWhenNewNodeSizeIsBigger() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(ByteSizeValue.ofGb(8).getBytes()),
            MachineLearning.AVAILABLE_PROCESSORS_NODE_ATTR,
            "4"
        );
        List<DiscoveryNode> nodes = buildNodesWithAttr(nodeAttr);
        Optional<MlInitializationService.UpdateCpuRatioTask> newRatio = MlInitializationService.ratioUpdateIfNecessary(
            nodes,
            MlMetadata.Builder.from(MlMetadataTests.randomInstance())
                .setMaxMlNodeSeen(randomLongBetween(0, ByteSizeValue.ofGb(8).getBytes() - 1))
                .build()
        );
        assertThat(newRatio.isPresent(), is(true));
        assertThat(newRatio.get().maxNodeSeen(), equalTo(ByteSizeValue.ofGb(8).getBytes()));
        assertThat(newRatio.get().cpuRatio(), closeTo(2.0, 1e-15));
    }

    public void testRatioUpdateWhenNewNodeSizeIsSmaller() {
        Map<String, String> nodeAttr = Map.of(
            MachineLearning.MACHINE_MEMORY_NODE_ATTR,
            Long.toString(ByteSizeValue.ofGb(8).getBytes()),
            MachineLearning.AVAILABLE_PROCESSORS_NODE_ATTR,
            "4"
        );
        List<DiscoveryNode> nodes = buildNodesWithAttr(nodeAttr);
        Optional<MlInitializationService.UpdateCpuRatioTask> newRatio = MlInitializationService.ratioUpdateIfNecessary(
            nodes,
            MlMetadata.Builder.from(MlMetadataTests.randomInstance())
                .setMaxMlNodeSeen(randomLongBetween(ByteSizeValue.ofGb(8).getBytes() + 1, Long.MAX_VALUE / 2))
                .setCpuRatio(4.0)
                .build()
        );
        assertThat(newRatio.isPresent(), is(false));
    }

    public void testUpdateRatioClusterStateExecutor() {
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        MlMetadata.TYPE,
                        new MlMetadata.Builder(MlMetadataTests.randomInstance()).setMaxMlNodeSeen(
                            randomLongBetween(0, ByteSizeValue.ofGb(2).getBytes())
                        ).build()
                    )
            )
            .build();
        ClusterState newState = MlInitializationService.updateRatioClusterStateExecutor(
            currentState,
            List.of(
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(2).getBytes(), 0.25)),
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(4).getBytes(), 0.4)),
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(8).getBytes(), 0.5))
            )
        );
        assertThat(MlMetadata.getMlMetadata(newState).getCpuRatio(), equalTo(0.5));
        assertThat(MlMetadata.getMlMetadata(newState).getMaxMlNodeSeen(), equalTo(ByteSizeValue.ofGb(8).getBytes()));
    }

    public void testUpdateRatioClusterStateExecutorAllTooSmallorEmpty() {
        ClusterState currentState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .putCustom(
                        MlMetadata.TYPE,
                        new MlMetadata.Builder(MlMetadataTests.randomInstance()).setMaxMlNodeSeen(
                            randomLongBetween(ByteSizeValue.ofGb(8).getBytes(), Long.MAX_VALUE / 2)
                        ).setCpuRatio(0.5).build()
                    )
            )
            .build();
        ClusterState newState = MlInitializationService.updateRatioClusterStateExecutor(
            currentState,
            List.of(
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(2).getBytes(), 0.25)),
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(4).getBytes(), 0.4)),
                new TestTaskContext(new MlInitializationService.UpdateCpuRatioTask(ByteSizeValue.ofGb(8).getBytes(), 0.5))
            )
        );
        assertThat(newState, is(currentState));

        newState = MlInitializationService.updateRatioClusterStateExecutor(currentState, List.of());
        assertThat(newState, is(currentState));
    }

    private static List<DiscoveryNode> buildNodesWithAttr(Map<String, String> nodeAttr) {
        return IntStream.range(1, randomIntBetween(1, 10))
            .mapToObj(
                i -> new DiscoveryNode(
                    "ml-node-" + i,
                    ESTestCase.buildNewFakeTransportAddress(),
                    nodeAttr,
                    Set.of(
                        DiscoveryNodeRole.DATA_ROLE,
                        DiscoveryNodeRole.MASTER_ROLE,
                        DiscoveryNodeRole.INGEST_ROLE,
                        DiscoveryNodeRole.ML_ROLE
                    ),
                    Version.CURRENT
                )
            )
            .toList();
    }

    private record TestTaskContext(MlInitializationService.UpdateCpuRatioTask task)
        implements
            ClusterStateTaskExecutor.TaskContext<MlInitializationService.UpdateCpuRatioTask> {
        @Override
        public MlInitializationService.UpdateCpuRatioTask getTask() {
            return task;
        }

        @Override
        public void success(ActionListener<ClusterState> publishListener) {}

        @Override
        public void success(ActionListener<ClusterState> publishListener, ClusterStateAckListener clusterStateAckListener) {}

        @Override
        public void onFailure(Exception failure) {}
    }
}
