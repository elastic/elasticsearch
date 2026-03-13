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
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.core.TimeValue.timeValueSeconds;
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
    private static final String LOCAL_NODE_ID = "local";
    private static final PersistentTasksCustomMetadata.Assignment LOCAL_ASSIGNMENT = new PersistentTasksCustomMetadata.Assignment(
        LOCAL_NODE_ID,
        ""
    );

    private PersistentTasksService persistentTasksService;
    private ThreadPool threadPool;

    @Before
    public void setup() throws Exception {
        persistentTasksService = mock(PersistentTasksService.class);
        threadPool = new TestThreadPool(HealthNodeTaskExecutorTests.class.getSimpleName());
    }

    @After
    public void cleanup() throws Exception {
        terminate(threadPool);
    }

    public void testMasterTaskReconciliation() {
        final boolean localEnabled = randomBoolean();
        final var nodeSettings = Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), localEnabled).build();
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var initialState = initialState(true);

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool, clusterSettings)) {
            HealthNodeTaskExecutor.create(clusterService, persistentTasksService, nodeSettings, clusterSettings);

            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                boolean enabled = localEnabled;

                final var baseState = taskExists ? stateWithHealthNodeSelectorTask(initialState) : initialState;
                if (randomBoolean()) {
                    enabled = randomBoolean();
                    setState(clusterService, stateWithEnabledHealthSetting(baseState, enabled));
                } else {
                    // Simulates the setting having never been recorded in the cluster state or
                    // a PUT _cluster/settings {"persistent": {"health.node.enabled": null}} call.
                    // Falls back to the node-level value (localEnabled).
                    setState(clusterService, baseState);
                }
                if (enabled && taskExists == false) {
                    expectedStartRequests++;
                }
                if (enabled == false && taskExists) {
                    expectedRemoveRequests++;
                }
            }
            verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(
                eq(HealthNode.TASK_NAME),
                eq(HealthNode.TASK_NAME),
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
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        Settings nodeSettings = Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), randomBoolean()).build();
        ClusterSettings clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var initialState = initialState(false);
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool, clusterSettings)) {
            var state = initialState;
            if (randomBoolean()) {
                state = stateWithEnabledHealthSetting(state, randomBoolean());
            }
            if (randomBoolean()) {
                state = stateWithHealthNodeSelectorTask(state);
            }
            HealthNodeTaskExecutor.create(clusterService, persistentTasksService, nodeSettings, clusterSettings);
            setState(clusterService, state);
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
    }

    public void testDoesNothingIfNodeShuttingDownButNotYetReassigned() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var state = initialState(randomBoolean());
        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(state, threadPool, clusterSettings)) {
            final HealthNodeTaskExecutor executor = HealthNodeTaskExecutor.create(
                clusterService,
                persistentTasksService,
                Settings.EMPTY,
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
            final ClusterState shutdownState = stateWithNodeShuttingDown(stateWithHealthNodeSelectorTask(state), shutdownType);
            HealthNodeTaskExecutorTests.<Void>safeAwait(
                listener -> clusterService.getClusterApplierService()
                    .onNewClusterState("node shutdown applied", () -> shutdownState, listener)
            );
            // The executor must not abort the task on its own.
            verify(task, never()).markAsLocallyAborted(anyString());
            verify(task, never()).markAsCompleted();
        }
    }

    private ClusterState initialState(boolean localNodeIsMaster) {
        final var nodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(LOCAL_NODE_ID)).localNodeId(LOCAL_NODE_ID);
        if (localNodeIsMaster) {
            nodes.masterNodeId(LOCAL_NODE_ID);
        } else {
            nodes.add(DiscoveryNodeUtils.create("another-node"));
            nodes.masterNodeId("another-node");
        }
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder()).build();
    }

    private ClusterState stateWithNodeShuttingDown(ClusterState clusterState, SingleNodeShutdownMetadata.Type type) {
        final var nodesShutdownMetadata = new NodesShutdownMetadata(
            Collections.singletonMap(
                LOCAL_NODE_ID,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(LOCAL_NODE_ID)
                    .setNodeEphemeralId(LOCAL_NODE_ID)
                    .setReason("test related shutdown")
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
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(HealthNode.TASK_NAME, HealthNode.TASK_NAME, new HealthNodeTaskParams(), LOCAL_ASSIGNMENT)
            .build();
        final var metadata = Metadata.builder(clusterState.metadata()).putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks);
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    private ClusterState stateWithEnabledHealthSetting(ClusterState clusterState, boolean enabled) {
        final var persistentSettings = Settings.builder().put(HealthNodeTaskExecutor.ENABLED_SETTING.getKey(), enabled).build();
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).persistentSettings(persistentSettings))
            .build();
    }
}
