/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.health.node.selection;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
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
import java.util.List;

import static org.elasticsearch.persistent.PersistentTasksExecutor.NO_NODE_FOUND;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class HealthNodeExecutorTests extends ESTestCase {

    /** Needed by {@link ClusterService} **/
    private static ThreadPool threadPool;

    private ClusterService clusterService;
    private PersistentTasksService persistentTasksService;
    private String localNodeId;
    private ClusterSettings clusterSettings;
    private Settings settings;

    private static final List<SingleNodeShutdownMetadata.Type> REMOVE_SHUTDOWN_TYPES = List.of(
        SingleNodeShutdownMetadata.Type.RESTART,
        SingleNodeShutdownMetadata.Type.REMOVE,
        SingleNodeShutdownMetadata.Type.SIGTERM
    );

    @BeforeClass
    public static void setUpThreadPool() {
        threadPool = new TestThreadPool(HealthNodeExecutorTests.class.getSimpleName());
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

    public void testTaskCreation() {
        HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        executor.startTask(new ClusterChangedEvent("", initialState(), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, times(1)).sendStartRequest(
            eq("health-node"),
            eq("health-node"),
            eq(new HealthNodeTaskParams()),
            any()
        );
    }

    public void testSkippingTaskCreationIfItExists() {
        HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        executor.startTask(new ClusterChangedEvent("", stateWithHealthNodeSelectorTask(initialState()), ClusterState.EMPTY_STATE));
        verify(persistentTasksService, never()).sendStartRequest(
            eq("health-node"),
            eq("health-node"),
            eq(new HealthNodeTaskParams()),
            any()
        );
    }

    public void testDoNothingIfAlreadyShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(
                clusterService,
                persistentTasksService,
                settings,
                clusterSettings
            );
            HealthNode task = mock(HealthNode.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new HealthNodeTaskParams(), state);
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState(), type);
            executor.shuttingDown(new ClusterChangedEvent("unchanged", withShutdown, withShutdown));
            verify(task, never()).markAsLocallyAborted(anyString());
        }
    }

    public void testAbortOnShutdown() {
        for (SingleNodeShutdownMetadata.Type type : REMOVE_SHUTDOWN_TYPES) {
            HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(
                clusterService,
                persistentTasksService,
                settings,
                clusterSettings
            );
            HealthNode task = mock(HealthNode.class);
            PersistentTaskState state = mock(PersistentTaskState.class);
            executor.nodeOperation(task, new HealthNodeTaskParams(), state);
            ClusterState initialState = initialState();
            ClusterState withShutdown = stateWithNodeShuttingDown(initialState, type);
            executor.shuttingDown(new ClusterChangedEvent("shutdown node", withShutdown, initialState));
            verify(task, times(1)).markAsLocallyAborted(anyString());
        }
    }

    public void testAbortOnDisable() {
        HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(clusterService, persistentTasksService, settings, clusterSettings);
        HealthNode task = mock(HealthNode.class);
        PersistentTaskState state = mock(PersistentTaskState.class);
        executor.nodeOperation(task, new HealthNodeTaskParams(), state);
        clusterSettings.applySettings(Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), false).build());
        verify(task, times(1)).markAsLocallyAborted(anyString());
    }

    private ClusterState initialState() {
        Metadata.Builder metadata = Metadata.builder();

        DiscoveryNodes.Builder nodes = DiscoveryNodes.builder();
        nodes.add(DiscoveryNode.createLocal(Settings.EMPTY, buildNewFakeTransportAddress(), localNodeId));
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
                    .setReason("shutdown for a unit test")
                    .setType(type)
                    .setStartedAtMillis(randomNonNegativeLong())
                    .setGracePeriod(
                        type == SingleNodeShutdownMetadata.Type.SIGTERM
                            ? TimeValue.parseTimeValue(randomTimeValue(), this.getTestName())
                            : null
                    )
                    .build()
            )
        );

        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    private ClusterState stateWithHealthNodeSelectorTask(ClusterState clusterState) {
        ClusterState.Builder builder = ClusterState.builder(clusterState);
        PersistentTasksCustomMetadata.Builder tasks = PersistentTasksCustomMetadata.builder();
        tasks.addTask(HealthNode.TASK_NAME, HealthNode.TASK_NAME, new HealthNodeTaskParams(), NO_NODE_FOUND);

        Metadata.Builder metadata = Metadata.builder(clusterState.metadata()).putCustom(PersistentTasksCustomMetadata.TYPE, tasks.build());
        return builder.metadata(metadata).build();
    }
}
