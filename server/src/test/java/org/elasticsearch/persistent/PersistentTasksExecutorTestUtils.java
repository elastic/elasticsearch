/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.function.UnaryOperator;

import static org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type.SIGTERM;
import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.elasticsearch.test.ESTestCase.randomBoolean;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class PersistentTasksExecutorTestUtils {

    public static final String LOCAL_NODE_ID = "local";
    public static final PersistentTasksCustomMetadata.Assignment LOCAL_ASSIGNMENT = new PersistentTasksCustomMetadata.Assignment(
        LOCAL_NODE_ID,
        ""
    );

    private PersistentTasksExecutorTestUtils() {}

    @FunctionalInterface
    public interface ExecutorFactory {
        void create(ClusterService clusterService, Settings nodeSettings, ClusterSettings clusterSettings);
    }

    /// Asserts that a settings-enabled master-node executor correctly reconciles task start and stop requests.
    ///
    /// Repeatedly applies cluster states with random combinations of task existence and enabled/disabled settings
    /// and verifies the expected number of start and stop requests against `persistentTasksService`.
    public static void assertToggeableMasterTaskReconciliation(
        String taskName,
        PersistentTaskParams params,
        Setting<Boolean> toggeableSetting,
        Collection<Setting<?>> extraClusterSettings,
        PersistentTasksService persistentTasksService,
        ThreadPool threadPool,
        PersistentTasksExecutor.Scope scope,
        @Nullable ProjectId projectId,
        ExecutorFactory executorFactory
    ) {
        final boolean localEnabled = randomBoolean();
        final var nodeSettings = Settings.builder().put(toggeableSetting.getKey(), localEnabled).build();
        final var allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(toggeableSetting);
        allSettings.addAll(extraClusterSettings);
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var initialState = stateWithLocalNode(true);

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool, clusterSettings)) {
            executorFactory.create(clusterService, nodeSettings, clusterSettings);
            int expectedStartRequests = 0;
            int expectedStopRequests = 0;

            final int cycles = ESTestCase.randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                boolean enabled = localEnabled;

                final var baseState = taskExists ? stateWithLocallyAssignedTask(initialState, taskName, params, scope, projectId) : initialState;
                if (randomBoolean()) {
                    enabled = randomBoolean();
                    setState(clusterService, stateWithEnabledSetting(baseState, toggeableSetting, enabled));
                } else {
                    // Simulates the setting having never been recorded in the cluster state or being reset to null.
                    // Falls back to the node-level default (localEnabled).
                    setState(clusterService, baseState);
                }
                if (enabled && taskExists == false) {
                    expectedStartRequests++;
                }
                if (enabled == false && taskExists) {
                    expectedStopRequests++;
                }
            }
            switch (scope) {
                case CLUSTER -> {
                    verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(
                        eq(taskName),
                        eq(taskName),
                        eq(params),
                        isNotNull(),
                        any()
                    );
                    verify(persistentTasksService, times(expectedStopRequests)).sendClusterRemoveRequest(
                        eq(taskName),
                        isNotNull(),
                        any()
                    );
                }
                case PROJECT -> {
                    verify(persistentTasksService, times(expectedStartRequests)).sendProjectStartRequest(
                        eq(projectId),
                        eq(taskName),
                        eq(taskName),
                        eq(params),
                        isNotNull(),
                        any()
                    );
                    verify(persistentTasksService, times(expectedStopRequests)).sendProjectRemoveRequest(
                        eq(projectId),
                        eq(taskName),
                        isNotNull(),
                        any()
                    );
                }
            }
        }
    }

    /// Asserts that a settings-enabled master-node executor never starts or stops its task when the local node is
    /// not master, regardless of what value the enabled setting has or whether the task is present.
    public static void assertNonMasterIgnoresToggeableTask(
        String taskName,
        PersistentTaskParams params,
        Setting<Boolean> toggeableSetting,
        Collection<Setting<?>> extraClusterSettings,
        PersistentTasksService persistentTasksService,
        ThreadPool threadPool,
        PersistentTasksExecutor.Scope scope,
        @Nullable ProjectId projectId,
        ExecutorFactory executorFactory
    ) {
        final var nodeSettings = Settings.builder().put(toggeableSetting.getKey(), randomBoolean()).build();
        final var allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(toggeableSetting);
        allSettings.addAll(extraClusterSettings);
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var initialState = stateWithLocalNode(false);

        try (ClusterService clusterService = ClusterServiceUtils.createClusterService(initialState, threadPool, clusterSettings)) {
            executorFactory.create(clusterService, nodeSettings, clusterSettings);
            var state = initialState;
            if (randomBoolean()) {
                state = stateWithEnabledSetting(state, toggeableSetting, randomBoolean());
            }
            if (randomBoolean()) {
                state = stateWithLocallyAssignedTask(state, taskName, params, scope, projectId);
            }
            setState(clusterService, state);
            verifyNoMoreInteractions(persistentTasksService);
        }
    }

    private static ClusterState stateWithLocallyAssignedTask(
            ClusterState state,
            String taskName,
            PersistentTaskParams params,
            PersistentTasksExecutor.Scope scope,
            @Nullable ProjectId projectId
    ) {
        return switch (scope) {
            case CLUSTER -> stateWithLocallyAssignedClusterTask(state, taskName, params);
            case PROJECT -> stateWithLocallyAssignedProjectTask(state, projectId, taskName, params);
        };
    }

    public static ClusterState stateWithLocallyAssignedClusterTask(ClusterState clusterState, String taskName, PersistentTaskParams params) {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(taskName, taskName, params, LOCAL_ASSIGNMENT)
            .build();
        final var metadata = Metadata.builder(clusterState.metadata()).putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks);
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    public static ClusterState stateWithLocallyAssignedProjectTask(
        ClusterState clusterState,
        ProjectId projectId,
        String taskName,
        PersistentTaskParams params
    ) {
        final var existingProject = clusterState.metadata().getProject(projectId);
        final var tasks = PersistentTasksCustomMetadata.builder()
            .addTask(taskName, taskName, params, LOCAL_ASSIGNMENT)
            .build();
        final var newProject = ProjectMetadata.builder(existingProject).putCustom(PersistentTasksCustomMetadata.TYPE, tasks).build();
        return ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata()).put(newProject)).build();
    }

    public static ClusterState stateWithLocalNode(boolean localNodeIsMaster) {
        final var nodes = DiscoveryNodes.builder().add(DiscoveryNodeUtils.create(LOCAL_NODE_ID)).localNodeId(LOCAL_NODE_ID);
        if (localNodeIsMaster) {
            nodes.masterNodeId(LOCAL_NODE_ID);
        } else {
            nodes.add(DiscoveryNodeUtils.create("another-node"));
            nodes.masterNodeId("another-node");
        }
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder()).build();
    }

    public static ClusterState stateWithNodeShuttingDown(ClusterState clusterState, SingleNodeShutdownMetadata.Type type) {
        final var nodesShutdownMetadata = new NodesShutdownMetadata(
            Collections.singletonMap(
                LOCAL_NODE_ID,
                SingleNodeShutdownMetadata.builder()
                    .setNodeId(LOCAL_NODE_ID)
                    .setNodeEphemeralId(LOCAL_NODE_ID)
                    .setReason("test related shutdown")
                    .setType(type)
                    .setStartedAtMillis(ESTestCase.randomNonNegativeLong())
                    .setGracePeriod(type == SIGTERM ? ESTestCase.randomTimeValue() : null)
                    .build()
            )
        );
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodesShutdownMetadata).build())
            .build();
    }

    private static ClusterState stateWithEnabledSetting(ClusterState clusterState, Setting<Boolean> enabledSetting, boolean enabled) {
        final var persistentSettings = Settings.builder().put(enabledSetting.getKey(), enabled).build();
        return ClusterState.builder(clusterState)
            .metadata(Metadata.builder(clusterState.metadata()).persistentSettings(persistentSettings))
            .build();
    }

}
