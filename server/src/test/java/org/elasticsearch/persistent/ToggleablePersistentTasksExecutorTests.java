/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.persistent;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.persistent.PersistentTasksExecutor.Scope;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class ToggleablePersistentTasksExecutorTests extends ESTestCase {
    private static final String LOCAL_NODE_ID = "local";
    private static final String TASK_NAME = "test_toggleable_task";

    private PersistentTasksService persistentTasksService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        persistentTasksService = mock(PersistentTasksService.class);
        threadPool = new TestThreadPool(ToggleablePersistentTasksExecutorTests.class.getSimpleName());
    }

    @After
    public void cleanup() throws Exception {
        terminate(threadPool);
    }

    public void testClusterTaskReconciliation() {
        final boolean initiallyEnabled = randomBoolean();
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        final var initialState = masterState();

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            setState(clusterService, initialState);
            final var executor = new TestToggleableExecutor(clusterService, persistentTasksService, Scope.CLUSTER, initiallyEnabled);
            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            final int cycles = randomIntBetween(5, 10);
            var enabled = initiallyEnabled;
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                final var state = taskExists ? stateWithClusterTask(initialState) : initialState;
                if (randomBoolean()) {
                    enabled = randomBoolean();
                    executor.setEnabled(enabled, null);
                }
                setState(clusterService, state);
                if (enabled && taskExists == false) {
                    expectedStartRequests++;
                }
                if (enabled == false && taskExists) {
                    expectedRemoveRequests++;
                }
            }
            verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests)).sendClusterRemoveRequest(eq(TASK_NAME), isNotNull(), any());
            verify(persistentTasksService, never()).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(any(), any(), any(), any());
        }
    }

    public void testProjectTaskReconciliation() {
        final boolean initiallyEnabled = randomBoolean();
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        final var projectId1 = randomUniqueProjectId();
        final var projectId2 = randomValueOtherThan(projectId1, ESTestCase::randomUniqueProjectId);
        final var initialState = masterStateWithProjects(Set.of(projectId1, projectId2));

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            setState(clusterService, initialState);
            final var executor = new TestToggleableExecutor(clusterService, persistentTasksService, Scope.PROJECT, initiallyEnabled);
            final var taskId1 = executor.getProjectTaskId(projectId1);
            final var taskId2 = executor.getProjectTaskId(projectId2);

            final var expectedStartRequests = new HashMap<ProjectId, Integer>();
            final var expectedRemoveRequests = new HashMap<ProjectId, Integer>();

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean task1Exists = randomBoolean();
                final boolean task2Exists = randomBoolean();

                var state = task1Exists ? stateWithProjectTask(initialState, projectId1, taskId1) : initialState;
                state = task2Exists ? stateWithProjectTask(state, projectId2, taskId2) : state;

                var task1Enabled = initiallyEnabled;
                var task2Enabled = initiallyEnabled;

                if (randomBoolean()) {
                    task1Enabled = randomBoolean();
                    executor.setEnabled(projectId1, task1Enabled);
                }
                if (randomBoolean()) {
                    task2Enabled = randomBoolean();
                    executor.setEnabled(projectId2, task2Enabled);
                }
                setState(clusterService, state);
                if (task1Enabled && task1Exists == false) {
                    expectedStartRequests.merge(projectId1, 1, Integer::sum);
                }
                if (task2Enabled && task2Exists == false) {
                    expectedStartRequests.merge(projectId2, 1, Integer::sum);
                }
                if (task1Enabled == false && task1Exists) {
                    expectedRemoveRequests.merge(projectId1, 1, Integer::sum);
                }
                if (task2Enabled == false && task2Exists) {
                    expectedRemoveRequests.merge(projectId2, 1, Integer::sum);
                }
                executor.wipeProjectsEnabled();
            }
            verify(persistentTasksService, times(expectedStartRequests.getOrDefault(projectId1, 0))).sendProjectStartRequest(
                eq(projectId1),
                eq(taskId1),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedStartRequests.getOrDefault(projectId2, 0))).sendProjectStartRequest(
                eq(projectId2),
                eq(taskId2),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests.getOrDefault(projectId1, 0))).sendProjectRemoveRequest(
                eq(projectId1),
                eq(taskId1),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests.getOrDefault(projectId2, 0))).sendProjectRemoveRequest(
                eq(projectId2),
                eq(taskId2),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        final var nodeSettings = Settings.builder().build();
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        final var scope = randomBoolean() ? Scope.PROJECT : Scope.CLUSTER;
        final var initialState = switch (scope) {
            case Scope.CLUSTER -> nonMasterState();
            case Scope.PROJECT -> nonMasterStateWithProjects(Set.of(ProjectId.DEFAULT));
        };
        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            final var executor = new TestToggleableExecutor(clusterService, persistentTasksService, Scope.PROJECT, randomBoolean());
            var state = initialState;
            if (randomBoolean()) {
                final var enabled = randomBoolean();
                switch (scope) {
                    case Scope.CLUSTER -> executor.setEnabled(enabled, null);
                    case Scope.PROJECT -> executor.setEnabled(enabled, ProjectId.DEFAULT);
                }
            }
            if (randomBoolean()) {
                switch (scope) {
                    case Scope.CLUSTER -> state = stateWithClusterTask(state);
                    case Scope.PROJECT -> state = stateWithProjectTask(
                        state,
                        ProjectId.DEFAULT,
                        executor.getProjectTaskId(ProjectId.DEFAULT)
                    );
                }
            }
            setState(clusterService, state);
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
            verify(persistentTasksService, never()).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(any(), any(), any(), any());
        }
    }

    private static ClusterState masterState() {
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(LOCAL_NODE_ID))
            .localNodeId(LOCAL_NODE_ID)
            .masterNodeId(LOCAL_NODE_ID);
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder()).build();
    }

    private static ClusterState masterStateWithProjects(Set<ProjectId> projectIds) {
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(LOCAL_NODE_ID))
            .localNodeId(LOCAL_NODE_ID)
            .masterNodeId(LOCAL_NODE_ID);
        final var metadata = Metadata.builder();
        for (var projectId : projectIds) {
            metadata.put(ProjectMetadata.builder(projectId));
        }
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).build();
    }

    private static ClusterState nonMasterState() {
        return nonMasterStateWithProjects(Set.of());
    }

    private static ClusterState nonMasterStateWithProjects(Set<ProjectId> projectIds) {
        final var nodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create(LOCAL_NODE_ID))
            .localNodeId(LOCAL_NODE_ID)
            .add(DiscoveryNodeUtils.create("another-node"))
            .masterNodeId("another-node");
        final var metadata = Metadata.builder();
        for (var projectId : projectIds) {
            metadata.put(ProjectMetadata.builder(projectId));
        }
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(metadata).build();
    }

    private static ClusterState stateWithClusterTask(ClusterState clusterState) {
        final var tasks = ClusterPersistentTasksCustomMetadata.builder()
            .addTask(TASK_NAME, TASK_NAME, TestParams.INSTANCE, new PersistentTasksCustomMetadata.Assignment(LOCAL_NODE_ID, ""))
            .build();
        final var metadata = Metadata.builder(clusterState.metadata()).putCustom(ClusterPersistentTasksCustomMetadata.TYPE, tasks);
        return ClusterState.builder(clusterState).metadata(metadata).build();
    }

    private static ClusterState stateWithProjectTask(ClusterState clusterState, ProjectId projectId, String taskId) {
        final var tasks = PersistentTasksCustomMetadata.builder()
            .addTask(taskId, TASK_NAME, TestParams.INSTANCE, new PersistentTasksCustomMetadata.Assignment(LOCAL_NODE_ID, ""))
            .build();
        final var projectMetadata = ProjectMetadata.builder(clusterState.metadata().getProject(projectId))
            .putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        return ClusterState.builder(clusterState).metadata(Metadata.builder(clusterState.metadata()).put(projectMetadata)).build();
    }

    static class TestToggleableExecutor extends ToggleablePersistentTasksExecutor<TestParams> {
        private final Scope scope;

        TestToggleableExecutor(
            ClusterService clusterService,
            PersistentTasksService persistentTasksService,
            Scope scope,
            boolean initialEnabled
        ) {
            super(
                TASK_NAME,
                clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT),
                clusterService,
                persistentTasksService,
                initialEnabled,
                () -> TestParams.INSTANCE
            );
            this.scope = scope;
        }

        @Override
        public Scope scope() {
            return scope;
        }

        void setEnabled(boolean enabled, @Nullable ProjectId projectId) {
            if (projectId != null) {
                setEnabled(projectId, enabled);
            } else {
                setEnabled(enabled);
            }
        }

        void wipeProjectsEnabled() {
            cleanupObsoleteProjectTasks(masterState());
        }

        @Override
        protected void nodeOperation(AllocatedPersistentTask task, TestParams params, PersistentTaskState state) {}
    }

    static class TestParams implements PersistentTaskParams {
        static final TestParams INSTANCE = new TestParams();

        TestParams() {}

        @Override
        public String getWriteableName() {
            return TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return TransportVersion.minimumCompatible();
        }

        @Override
        public void writeTo(StreamOutput out) {}

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }
    }
}
