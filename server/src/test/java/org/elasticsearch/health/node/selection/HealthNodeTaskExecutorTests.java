/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HealthNodeTaskExecutorTests extends ESTestCase {

    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private String localNodeId;
    private ClusterSettings clusterSettings;
    private Settings settings;

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(HealthNodeTaskExecutorTests.class.getSimpleName());
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        clusterService = createClusterService(threadPool);
        localNodeId = clusterService.localNode().getId();
        persistentTasksService = mock(PersistentTasksService.class);
        settings = Settings.builder().build();
        clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(threadPool);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        clusterService.close();
    }

    public void testTaskCreation() throws Exception {
        HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, ActionListener.noop());
        // Ensure that if the task is gone, it will be recreated.
        clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, ActionListener.noop());
        assertBusy(
            () -> verify(persistentTasksService, times(2)).sendStartRequest(
                eq("health-node"),
                eq("health-node"),
                eq(new HealthNodeTaskParams()),
                isNotNull(),
                any()
            )
        );
    }

    public void testSkippingTaskCreationIfItExists() {
        HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        executor.startTask(new ClusterChangedEvent("", stateWithHealthNodeSelectorTask(initialState()), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, never()).sendStartRequest(
            eq("health-node"),
            eq("health-node"),
            eq(new HealthNodeTaskParams()),
            isNotNull(),
            any()
        );
    }

    public void testStopTaskOnDisable() {
        HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        HealthNode task = mock(HealthNode.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, new HealthNodeTaskParams(), state);
        clusterSettings.applySettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false).build());
        verify(task, times(1)).markAsCompleted();
        verify(task, never()).markAsLocallyAborted(anyString());
    }

    public void testDoNothingIfNodeShuttingDownButNotYetReassigned() {
        final HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(
            clusterService,
            persistentTasksService,
            settings,
            clusterSettings
        );
        final HealthNode task = mock(HealthNode.class);
        executor.nodeOperation(task, new HealthNodeTaskParams(), mock(PersistentTaskState.class));

        // Local node is now marked for shutdown but the health task is still assigned to it.
        final SingleNodeShutdownMetadata.Type shutdownType = randomFrom(
            SingleNodeShutdownMetadata.Type.REMOVE,
            SingleNodeShutdownMetadata.Type.RESTART,
            SIGTERM
        );
        final var assignedToLocal = new PersistentTasksCustomMetadata.Assignment(localNodeId, "");
        final ClusterState shutdownState = stateWithNodeShuttingDown(
            stateWithHealthNodeSelectorTask(initialState(), assignedToLocal),
            shutdownType
        );
        HealthNodeTaskExecutorTests.<Void>safeAwait(
            listener -> clusterService.getClusterApplierService().onNewClusterState("node shutdown applied", () -> shutdownState, listener)
        );
        // The executor must not abort the task on its own.
        verify(task, never()).markAsLocallyAborted(anyString());
        verify(task, never()).markAsCompleted();
    }

    private ClusterState initialState() {
        Metadata.Builder metadata = Metadata.builder();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNodeUtils.create(localNodeId));
        nodes.localNodeId(localNodeId);
        nodes.masterNodeId(localNodeId);

        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).build();
    }

    private ClusterState stateWithNodeShuttingDown(ClusterState clusterState, SingleNodeShutdownMetadata.Type type) {
        NodesShutdownMetadata nodesShutdownMetadata = new NodesShutdownMetadata(
            Collections.singletonMap(
                localNodeId,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(localNodeId)
                    .setNodeEphemeralId(localNodeId)
                    .setReason("shutdown for a unit test")
                    .setType(type)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setGracePeriod(type == SIGTERM ? randomTimeValue() : null)
                    .build()
            )
        );

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    private ClusterState stateWithHealthNodeSelectorTask(ClusterState clusterState) {
        return stateWithHealthNodeSelectorTask(clusterState, NO_NODE_FOUND);
    }

    private ClusterState stateWithHealthNodeSelectorTask(ClusterState clusterState, PersistentTasksCustomMetadata.Assignment assignment) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        ClusterPersistentTasksCustomMetadata.Builder tasks = ClusterPersistentTasksCustomMetadata.builder();
        tasks.addTask(HealthNode.TASK_NAME, HealthNode.TASK_NAME, new HealthNodeTaskParams(), assignment);

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata())
            .putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }
}
