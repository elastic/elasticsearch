/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.health.node.selection;

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
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
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

    public void testMasterCreatesTask() {
        HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        HealthNodeTaskExecutorTests.<Void>safeAwait(
            listener -> clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener)
        );
        // Ensure that if the task is gone, it will be recreated.
        HealthNodeTaskExecutorTests.<Void>safeAwait(
            listener -> clusterService.getClusterApplierService().onNewClusterState("initialization", this::initialState, listener)
        );
        verify(persistentTasksService, times(2)).sendClusterStartRequest(
            eq("health-node"),
            eq("health-node"),
            eq(new HealthNodeTaskParams()),
            isNotNull(),
            any()
        );
    }

    public void testTaskCreationSkippedIfAlreadyExists() {
        HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        HealthNodeTaskExecutorTests.<Void>safeAwait(
            listener -> clusterService.getClusterApplierService()
                .onNewClusterState("task already exists", () -> stateWithHealthNodeSelectorTask(initialState()), listener)
        );
        verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
    }

    public void testNonMasterDoesNotRequestStartTask() {
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(localNodeId))
                    .add(DiscoveryNodeUtils.create("master"))
                    .localNodeId(localNodeId)
                    .masterNodeId("master")
            )
            .metadata(Metadata.builder().build())
            .build();

        clusterSettings.applySettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), true).build());
        try (ClusterService nonMasterClusterService = createClusterService(state, threadPool, clusterSettings)) {
            HealthNodeTaskExecutor.create(nonMasterClusterService, persistentTasksService, settings, clusterSettings);
            setState(nonMasterClusterService, state);
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
        }
    }

    public void testMasterEnableAndDisable() {
        HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        final var assignedToLocal = new PersistentTasksCustomMetadata.Assignment(localNodeId, "");

        boolean prevEnabled = true;
        int expectedStartRequests = 0;
        int expectedRemoveRequests = 0;

        final int cycles = randomIntBetween(5, 10);
        for (int i = 0; i < cycles; i++) {
            boolean taskExists = randomBoolean();
            if (taskExists) {
                setState(clusterService, stateWithHealthNodeSelectorTask(initialState(), assignedToLocal));
            } else {
                setState(clusterService, initialState());
            }
            // setState fires the taskStarter listener when it is registered
            if (prevEnabled && taskExists == false) {
                expectedStartRequests++;
            }
            boolean enabled = randomBoolean();
            boolean transitionsToDisabled = prevEnabled && enabled == false;
            if (transitionsToDisabled && taskExists) {
                expectedRemoveRequests++;
            }
            clusterSettings.applySettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), enabled).build());
            prevEnabled = enabled;
        }
        verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(
            eq("health-node"),
            eq("health-node"),
            eq(new HealthNodeTaskParams()),
            isNotNull(),
            any()
        );
        verify(persistentTasksService, times(expectedRemoveRequests)).sendClusterRemoveRequest(
            eq(HealthNode.TASK_NAME),
            eq(timeValueSeconds(30)),
            any()
        );
    }

    public void testNonMasterDoesNotRemoveTaskOnDisable() {
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create(localNodeId))
                    .add(DiscoveryNodeUtils.create("master"))
                    .localNodeId(localNodeId)
                    .masterNodeId("master")
            )
            .metadata(Metadata.builder().build())
            .build();

        try (ClusterService nonMasterClusterService = createClusterService(state, threadPool, clusterSettings)) {
            HealthNodeTaskExecutor.create(nonMasterClusterService, persistentTasksService, settings, clusterSettings);
            clusterSettings.applySettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false).build());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
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
