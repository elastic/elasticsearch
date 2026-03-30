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
import org.elasticsearch.action.support.master.MasterNodeRequest;
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
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PersistentTaskLifecycleManagerTests extends ESTestCase {
    private static final String LOCAL_NODE_ID = "local";
    private static final String TASK_NAME = "test_lifecycle_task";
    private static final Setting<Boolean> TASK_ENABLED_SETTING = Setting.boolSetting(
        "test.task.enabled",
        true,
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    private PersistentTasksService persistentTasksService;
    private ThreadPool threadPool;

    @Before
    public void setup() {
        persistentTasksService = mock(PersistentTasksService.class);
        threadPool = new TestThreadPool(PersistentTaskLifecycleManagerTests.class.getSimpleName());
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
            final var enabled = new AtomicBoolean(initiallyEnabled);
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, enabled::get, () -> TestParams.INSTANCE, MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT);

            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                final var state = taskExists ? stateWithClusterTask(initialState) : initialState;
                if (randomBoolean()) {
                    enabled.set(randomBoolean());
                }
                setState(clusterService, state);
                if (enabled.get() && taskExists == false) {
                    expectedStartRequests++;
                }
                if (enabled.get() == false && taskExists) {
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
            final var enabled = new AtomicBoolean(initiallyEnabled);
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(
                TASK_NAME,
                projectId -> TASK_NAME,
                enabled::get,
                () -> TestParams.INSTANCE,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
            );

            final var expectedStartRequests = new HashMap<ProjectId, Integer>();
            final var expectedRemoveRequests = new HashMap<ProjectId, Integer>();

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean task1Exists = randomBoolean();
                final boolean task2Exists = randomBoolean();

                var state = task1Exists ? stateWithProjectTask(initialState, projectId1, TASK_NAME) : initialState;
                state = task2Exists ? stateWithProjectTask(state, projectId2, TASK_NAME) : state;

                if (randomBoolean()) {
                    enabled.set(randomBoolean());
                }
                setState(clusterService, state);
                if (enabled.get() && task1Exists == false) {
                    expectedStartRequests.merge(projectId1, 1, Integer::sum);
                }
                if (enabled.get() && task2Exists == false) {
                    expectedStartRequests.merge(projectId2, 1, Integer::sum);
                }
                if (enabled.get() == false && task1Exists) {
                    expectedRemoveRequests.merge(projectId1, 1, Integer::sum);
                }
                if (enabled.get() == false && task2Exists) {
                    expectedRemoveRequests.merge(projectId2, 1, Integer::sum);
                }
            }
            verify(persistentTasksService, times(expectedStartRequests.getOrDefault(projectId1, 0))).sendProjectStartRequest(
                eq(projectId1),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedStartRequests.getOrDefault(projectId2, 0))).sendProjectStartRequest(
                eq(projectId2),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests.getOrDefault(projectId1, 0))).sendProjectRemoveRequest(
                eq(projectId1),
                eq(TASK_NAME),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests.getOrDefault(projectId2, 0))).sendProjectRemoveRequest(
                eq(projectId2),
                eq(TASK_NAME),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
    }

    public void testClusterTaskReconciliationFromSetting() {
        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), false).build();
        final var allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(TASK_ENABLED_SETTING);
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(
                TASK_NAME,
                TASK_ENABLED_SETTING,
                () -> TestParams.INSTANCE,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
            );

            setState(clusterService, masterState());
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());

            setState(clusterService, stateWithPersistentSetting(masterState(), TASK_ENABLED_SETTING.getKey(), true));
            verify(persistentTasksService, times(1)).sendClusterStartRequest(
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );

            setState(clusterService, stateWithPersistentSetting(stateWithClusterTask(masterState()), TASK_ENABLED_SETTING.getKey(), false));
            verify(persistentTasksService, times(1)).sendClusterRemoveRequest(eq(TASK_NAME), isNotNull(), any());
        }
    }

    public void testProjectTaskReconciliationFromSetting() {
        final var projectId = randomUniqueProjectId();
        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), true).build();
        final var allSettings = new HashSet<>(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        allSettings.add(TASK_ENABLED_SETTING);
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        final var stateWithProject = masterStateWithProjects(Set.of(projectId));

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(
                TASK_NAME,
                p -> TASK_NAME,
                TASK_ENABLED_SETTING,
                () -> TestParams.INSTANCE,
                MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
            );

            setState(clusterService, stateWithProject);
            verify(persistentTasksService, times(1)).sendProjectStartRequest(
                eq(projectId),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );

            setState(
                clusterService,
                stateWithPersistentSetting(
                    stateWithProjectTask(stateWithProject, projectId, TASK_NAME),
                    TASK_ENABLED_SETTING.getKey(),
                    false
                )
            );
            verify(persistentTasksService, times(1)).sendProjectRemoveRequest(eq(projectId), eq(TASK_NAME), isNotNull(), any());

            setState(clusterService, stateWithPersistentSetting(stateWithProject, TASK_ENABLED_SETTING.getKey(), true));
            verify(persistentTasksService, times(2)).sendProjectStartRequest(
                eq(projectId),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
        }
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        final var nodeSettings = Settings.builder().build();
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
        final boolean projectScoped = randomBoolean();
        final var initialState = projectScoped ? nonMasterStateWithProjects(Set.of(ProjectId.DEFAULT)) : nonMasterState();

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            if (projectScoped) {
                manager.registerProjectTask(
                    TASK_NAME,
                    unused -> TASK_NAME,
                    () -> randomBoolean(),
                    () -> TestParams.INSTANCE,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
                );
            } else {
                manager.registerClusterTask(
                    TASK_NAME,
                    () -> randomBoolean(),
                    () -> TestParams.INSTANCE,
                    MasterNodeRequest.INFINITE_MASTER_NODE_TIMEOUT
                );
            }

            var state = initialState;
            if (randomBoolean()) {
                if (projectScoped) {
                    state = stateWithProjectTask(state, ProjectId.DEFAULT, TASK_NAME);
                } else {
                    state = stateWithClusterTask(state);
                }
            }
            setState(clusterService, state);
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
            verify(persistentTasksService, never()).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(any(), any(), any(), any());
        }
    }

    private static ClusterState stateWithPersistentSetting(ClusterState state, String key, boolean value) {
        return ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).persistentSettings(Settings.builder().put(key, value).build()))
            .build();
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
