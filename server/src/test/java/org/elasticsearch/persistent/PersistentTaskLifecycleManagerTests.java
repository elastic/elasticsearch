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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentBuilder;
import org.junit.After;
import org.junit.Before;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PersistentTaskLifecycleManagerTests extends ESTestCase {
    private static final String LOCAL_NODE_ID = "local";
    private static final String ANOTHER_NODE_ID = "another-node";
    private static final DiscoveryNode LOCAL_NODE = DiscoveryNodeUtils.create(LOCAL_NODE_ID);
    private static final DiscoveryNode ANOTHER_NODE = DiscoveryNodeUtils.create(ANOTHER_NODE_ID);

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

    public void testClusterTaskReconciliationNoInFlightRequests() {
        // Each request completes immediately so inFlight should always be NONE between reconcile cycles.
        doAnswer(completesImmediately(4)).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(completesImmediately(2)).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = LOCAL_NODE;
        final var enabled = new AtomicBoolean(randomBoolean());

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)
        ) {
            setState(clusterService, masterState());
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, enabled::get, () -> TestParams.INSTANCE);

            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                final var state = taskExists ? stateWithClusterTask(masterState()) : masterState();
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

    public void testSettingsCorrectlyPropagatedToClusterTaskReconciliation() {
        doAnswer(completesImmediately(4)).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(completesImmediately(2)).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), false).build();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var localNode = LOCAL_NODE;

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);

            setState(clusterService, masterState());
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());

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

    public void testClusterTaskReconciliationDeduplicatesInFlightRequests() {
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assert listener.get() == null : "unexpected duplicate in flight responses";
            listener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assert listener.get() == null : "unexpected duplicate in flight responses";
            listener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final var enabled = new AtomicBoolean();
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = LOCAL_NODE;

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, enabled::get, () -> TestParams.INSTANCE);

            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            for (int cycle = 0; cycle < 10; cycle++) {
                final boolean sendStart = randomBoolean();
                enabled.set(sendStart);
                final var state = sendStart ? masterState() : stateWithClusterTask(masterState());
                setState(clusterService, state); // triggers the first request

                if (sendStart) expectedStartRequests++;
                else expectedRemoveRequests++;

                // A request is still in flight -> no new requests should be sent.
                for (int j = 0; j < 10; j++) {
                    enabled.set(randomBoolean());
                    setState(clusterService, randomBoolean() ? masterState() : stateWithClusterTask(masterState()));
                }

                verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(any(), any(), any(), any(), any());
                verify(persistentTasksService, times(expectedRemoveRequests)).sendClusterRemoveRequest(any(), any(), any());

                final boolean postCompletionEnabled = randomBoolean();
                enabled.set(postCompletionEnabled);
                listener.getAndSet(null).onResponse(null); // clears inFlight

                if (postCompletionEnabled != sendStart) {
                    // The opposite request should have been sent immediately on completion.
                    if (sendStart) expectedRemoveRequests++;
                    else expectedStartRequests++;
                    verify(persistentTasksService, times(expectedStartRequests)).sendClusterStartRequest(any(), any(), any(), any(), any());
                    verify(persistentTasksService, times(expectedRemoveRequests)).sendClusterRemoveRequest(any(), any(), any());
                    listener.getAndSet(null).onResponse(null);
                }
            }
        }
    }

    public void testProjectTaskReconciliationNoInFlightRequests() {
        // Each request completes immediately so inFlight is always NONE between reconcile cycles.
        doAnswer(completesImmediately(5)).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(completesImmediately(3)).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final var projectId1 = randomUniqueProjectId();
        final var projectId2 = randomValueOtherThan(projectId1, ESTestCase::randomUniqueProjectId);
        final var enabled = new AtomicBoolean(randomBoolean());
        final var baseState = masterStateWithProjects(Set.of(projectId1, projectId2));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = LOCAL_NODE;

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            setState(clusterService, baseState);
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, enabled::get, () -> TestParams.INSTANCE);

            int expectedStartRequests1 = 0;
            int expectedRemoveRequests1 = 0;
            int expectedStartRequests2 = 0;
            int expectedRemoveRequests2 = 0;

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists1 = randomBoolean();
                final boolean taskExists2 = randomBoolean();
                if (randomBoolean()) {
                    enabled.set(randomBoolean());
                }
                var state = masterStateWithProjects(Set.of(projectId1, projectId2));
                state = taskExists1 ? stateWithProjectTask(state, projectId1, TASK_NAME) : state;
                state = taskExists2 ? stateWithProjectTask(state, projectId2, TASK_NAME) : state;
                setState(clusterService, state);
                if (enabled.get() && taskExists1 == false) expectedStartRequests1++;
                if (enabled.get() == false && taskExists1) expectedRemoveRequests1++;
                if (enabled.get() && taskExists2 == false) expectedStartRequests2++;
                if (enabled.get() == false && taskExists2) expectedRemoveRequests2++;
            }

            verify(persistentTasksService, times(expectedStartRequests1)).sendProjectStartRequest(
                eq(projectId1),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests1)).sendProjectRemoveRequest(
                eq(projectId1),
                eq(TASK_NAME),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedStartRequests2)).sendProjectStartRequest(
                eq(projectId2),
                eq(TASK_NAME),
                eq(TASK_NAME),
                eq(TestParams.INSTANCE),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, times(expectedRemoveRequests2)).sendProjectRemoveRequest(
                eq(projectId2),
                eq(TASK_NAME),
                isNotNull(),
                any()
            );
            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
    }

    public void testSettingsCorrectlyPropagatedToProjectTaskReconciliation() {
        doAnswer(completesImmediately(5)).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(completesImmediately(3)).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final var projectId = randomUniqueProjectId();
        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), true).build();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var localNode = LOCAL_NODE;
        final var stateWithProject = masterStateWithProjects(Set.of(projectId));

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);

            setState(clusterService, masterState());
            verify(persistentTasksService, never()).sendProjectStartRequest(eq(projectId), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(eq(projectId), any(), any(), any());

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
        }
    }

    public void testProjectTaskReconciliationDeduplicatesInFlightRequests() {
        final var projectId = randomUniqueProjectId();
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assert listener.get() == null : "unexpected duplicate in flight responses";
            listener.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assert listener.get() == null : "unexpected duplicate in flight responses";
            listener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final var enabled = new AtomicBoolean();
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = LOCAL_NODE;

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, enabled::get, () -> TestParams.INSTANCE);

            int expectedStartRequests = 0;
            int expectedRemoveRequests = 0;

            for (int cycle = 0; cycle < 10; cycle++) {
                final boolean sendStart = randomBoolean();
                enabled.set(sendStart);
                final var initialState = masterStateWithProjects(Set.of(projectId));
                setState(clusterService, sendStart ? initialState : stateWithProjectTask(initialState, projectId, TASK_NAME));

                if (sendStart) expectedStartRequests++;
                else expectedRemoveRequests++;

                // A request is still in flight -> no new requests should be sent.
                for (int j = 0; j < 10; j++) {
                    enabled.set(randomBoolean());
                    final var newState = masterStateWithProjects(Set.of(projectId));
                    setState(clusterService, randomBoolean() ? newState : stateWithProjectTask(newState, projectId, TASK_NAME));
                }

                verify(persistentTasksService, times(expectedStartRequests)).sendProjectStartRequest(
                    eq(projectId),
                    any(),
                    any(),
                    any(),
                    any(),
                    any()
                );
                verify(persistentTasksService, times(expectedRemoveRequests)).sendProjectRemoveRequest(eq(projectId), any(), any(), any());

                final boolean postCompletionEnabled = randomBoolean();
                enabled.set(postCompletionEnabled);
                listener.getAndSet(null).onResponse(null); // clears inFlight

                if (postCompletionEnabled != sendStart) {
                    // The opposite request should have been sent immediately on completion.
                    if (sendStart) expectedRemoveRequests++;
                    else expectedStartRequests++;
                    verify(persistentTasksService, times(expectedStartRequests)).sendProjectStartRequest(
                        eq(projectId),
                        any(),
                        any(),
                        any(),
                        any(),
                        any()
                    );
                    verify(persistentTasksService, times(expectedRemoveRequests)).sendProjectRemoveRequest(
                        eq(projectId),
                        any(),
                        any(),
                        any()
                    );
                    listener.getAndSet(null).onResponse(null);
                }
            }
        }
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final var localNode = LOCAL_NODE;
        final boolean projectScoped = randomBoolean();
        final var initialState = projectScoped ? nonMasterStateWithProjects(Set.of(ProjectId.DEFAULT)) : nonMasterState();

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, localNode, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            if (projectScoped) {
                manager.registerProjectTask(TASK_NAME, projectId -> TASK_NAME, () -> randomBoolean(), () -> TestParams.INSTANCE);
            } else {
                manager.registerClusterTask(TASK_NAME, () -> randomBoolean(), () -> TestParams.INSTANCE);
            }

            var state = initialState;
            if (randomBoolean()) {
                state = projectScoped ? stateWithProjectTask(state, ProjectId.DEFAULT, TASK_NAME) : stateWithClusterTask(state);
            }
            setState(clusterService, state);

            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
            verify(persistentTasksService, never()).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(any(), any(), any(), any());
        }
    }

    @SuppressWarnings("unchecked")
    private static Answer<Void> completesImmediately(int listenerArgIndex) {
        return inv -> {
            ((ActionListener<Object>) inv.getArgument(listenerArgIndex)).onResponse(null);
            return null;
        };
    }

    private static ClusterState stateWithPersistentSetting(ClusterState state, String key, boolean value) {
        return ClusterState.builder(state)
            .metadata(Metadata.builder(state.metadata()).persistentSettings(Settings.builder().put(key, value).build()))
            .build();
    }

    private static ClusterState masterState() {
        final var nodes = DiscoveryNodes.builder().add(LOCAL_NODE).localNodeId(LOCAL_NODE_ID).masterNodeId(LOCAL_NODE_ID);
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).metadata(Metadata.builder()).build();
    }

    private static ClusterState masterStateWithProjects(Set<ProjectId> projectIds) {
        final var nodes = DiscoveryNodes.builder().add(LOCAL_NODE).localNodeId(LOCAL_NODE_ID).masterNodeId(LOCAL_NODE_ID);
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
            .add(LOCAL_NODE)
            .localNodeId(LOCAL_NODE_ID)
            .add(ANOTHER_NODE)
            .masterNodeId(ANOTHER_NODE_ID);
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
