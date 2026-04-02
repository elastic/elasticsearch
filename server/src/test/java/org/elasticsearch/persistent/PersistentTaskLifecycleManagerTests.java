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

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.ClusterServiceUtils.setState;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNotNull;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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
        final var startListener = new AtomicReference<ActionListener<?>>();
        final var removeListener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task start request", startListener.get(), nullValue());
            startListener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(eq(TASK_NAME), eq(TASK_NAME), eq(TestParams.INSTANCE), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task remove request", removeListener.get(), nullValue());
            removeListener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(eq(TASK_NAME), isNotNull(), any());

        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        boolean enabled = randomBoolean();

        try (
            ClusterService clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)
        ) {
            setState(clusterService, masterState());
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists = randomBoolean();
                if (randomBoolean()) {
                    enabled = randomBoolean();
                }
                final var state = taskExists ? stateWithClusterTask(masterState()) : masterState();
                setState(clusterService, stateWithPersistentSetting(state, TASK_ENABLED_SETTING.getKey(), enabled));

                if (enabled && taskExists == false) {
                    setState(clusterService, stateWithClusterTask(clusterService.state()));
                    assertThat("expected in flight cluster task start request", startListener.get(), notNullValue());
                    startListener.getAndSet(null).onResponse(null);
                }
                if (enabled == false && taskExists) {
                    assertThat("expected in flight cluster task remove request", removeListener.get(), notNullValue());
                    setState(clusterService, stateWithoutClusterTask(clusterService.state()));
                    removeListener.getAndSet(null).onResponse(null);
                }
                assertThat("unexpected in flight cluster task start request", startListener.get(), nullValue());
                assertThat("unexpected in flight cluster task remove request", removeListener.get(), nullValue());
            }
            verify(persistentTasksService, never()).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendProjectRemoveRequest(any(), any(), any(), any());
        }
    }

    public void testSettingsCorrectlyPropagatedToClusterTaskReconciliation() {
        final var startListener = new AtomicReference<ActionListener<?>>();
        final var removeListener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task start request", startListener.get(), nullValue());
            startListener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(eq(TASK_NAME), eq(TASK_NAME), eq(TestParams.INSTANCE), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task remove request", removeListener.get(), nullValue());
            removeListener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(eq(TASK_NAME), isNotNull(), any());

        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), false).build();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            setState(clusterService, masterState());
            assertThat("unexpected in flight cluster task start request", startListener.get(), nullValue());
            assertThat("unexpected in flight cluster task remove request", removeListener.get(), nullValue());

            setState(clusterService, stateWithPersistentSetting(masterState(), TASK_ENABLED_SETTING.getKey(), true));
            assertThat("expected in flight cluster task start request", startListener.get(), notNullValue());
            assertThat("unexpected in flight cluster task remove request", removeListener.get(), nullValue());
            setState(clusterService, stateWithClusterTask(clusterService.state()));
            startListener.getAndSet(null).onResponse(null);

            setState(clusterService, stateWithPersistentSetting(stateWithClusterTask(masterState()), TASK_ENABLED_SETTING.getKey(), false));
            assertThat("expected in flight cluster task remove request", removeListener.get(), notNullValue());
            assertThat("unexpected in flight cluster task start request", startListener.get(), nullValue());
            removeListener.getAndSet(null).onResponse(null);
        }
    }

    public void testClusterTaskReconciliationDeduplicatesInFlightRequests() {
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task start request", listener.get(), nullValue());
            listener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task remove request", listener.get(), nullValue());
            listener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            for (int cycle = 0; cycle < 10; cycle++) {
                final boolean sendStart = randomBoolean();
                setState(
                    clusterService,
                    stateWithPersistentSetting(
                        sendStart ? masterState() : stateWithClusterTask(masterState()),
                        TASK_ENABLED_SETTING.getKey(),
                        sendStart
                    )
                );
                assertThat("expected in flight cluster task start/stop request", listener.get(), notNullValue());

                // A request is still in flight -> no new requests should be sent. The doAnswer guard ensures this.
                for (int j = 0; j < 10; j++) {
                    setState(clusterService, randomBoolean() ? masterState() : stateWithClusterTask(masterState()));
                }

                final boolean postCompletionEnabled = randomBoolean();
                setState(
                    clusterService,
                    stateWithPersistentSetting(
                        sendStart ? stateWithClusterTask(masterState()) : masterState(),
                        TASK_ENABLED_SETTING.getKey(),
                        postCompletionEnabled
                    )
                );
                listener.getAndSet(null).onResponse(null);

                if (postCompletionEnabled != sendStart) {
                    // The opposite request should have been sent immediately on completion.
                    assertThat("expected opposite cluster task in flight request", listener.get(), notNullValue());
                    setState(
                        clusterService,
                        stateWithPersistentSetting(
                            sendStart ? masterState() : stateWithClusterTask(masterState()),
                            TASK_ENABLED_SETTING.getKey(),
                            postCompletionEnabled
                        )
                    );
                    listener.getAndSet(null).onResponse(null);
                } else {
                    assertThat("unexpected in flight cluster task request after completion", listener.get(), nullValue());
                }
            }
        }
    }

    public void testProjectTaskReconciliationNoInFlightRequests() {
        final var projectId1 = randomUniqueProjectId();
        final var projectId2 = randomUniqueProjectId();
        boolean enabled = randomBoolean();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        final var startListener1 = new AtomicReference<ActionListener<?>>();
        final var startListener2 = new AtomicReference<ActionListener<?>>();
        final var removeListener1 = new AtomicReference<ActionListener<?>>();
        final var removeListener2 = new AtomicReference<ActionListener<?>>();

        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project1 task start request", startListener1.get(), nullValue());
            startListener1.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService)
            .sendProjectStartRequest(eq(projectId1), eq(TASK_NAME), eq(TASK_NAME), eq(TestParams.INSTANCE), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project2 task start request", startListener2.get(), nullValue());
            startListener2.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService)
            .sendProjectStartRequest(eq(projectId2), eq(TASK_NAME), eq(TASK_NAME), eq(TestParams.INSTANCE), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project1 task remove request", removeListener1.get(), nullValue());
            removeListener1.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(eq(projectId1), eq(TASK_NAME), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project2 task remove request", removeListener2.get(), nullValue());
            removeListener2.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(eq(projectId2), eq(TASK_NAME), isNotNull(), any());

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            setState(clusterService, masterStateWithProjects(Set.of(projectId1, projectId2)));
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            final int cycles = randomIntBetween(5, 10);
            for (int i = 0; i < cycles; i++) {
                final boolean taskExists1 = randomBoolean();
                final boolean taskExists2 = randomBoolean();
                if (randomBoolean()) {
                    enabled = randomBoolean();
                }
                var state = masterStateWithProjects(Set.of(projectId1, projectId2));
                if (taskExists1) state = stateWithProjectTask(state, projectId1, TASK_NAME);
                if (taskExists2) state = stateWithProjectTask(state, projectId2, TASK_NAME);
                setState(clusterService, stateWithPersistentSetting(state, TASK_ENABLED_SETTING.getKey(), enabled));

                if (enabled && taskExists1 == false) {
                    assertThat("expected in flight start request for project1", startListener1.get(), notNullValue());
                    setState(clusterService, stateWithProjectTask(clusterService.state(), projectId1, TASK_NAME));
                    startListener1.getAndSet(null).onResponse(null);
                } else if (enabled == false && taskExists1) {
                    assertThat("expected in flight remove request for project1", removeListener1.get(), notNullValue());
                    setState(clusterService, stateWithoutProjectTask(clusterService.state(), projectId1, TASK_NAME));
                    removeListener1.getAndSet(null).onResponse(null);
                }
                assertThat("unexpected in flight start request for project1", startListener1.get(), nullValue());
                assertThat("unexpected in flight remove request for project1", removeListener1.get(), nullValue());

                if (enabled && taskExists2 == false) {
                    assertThat("expected in flight start request for project2", startListener2.get(), notNullValue());
                    setState(clusterService, stateWithProjectTask(clusterService.state(), projectId2, TASK_NAME));
                    startListener2.getAndSet(null).onResponse(null);
                } else if (enabled == false && taskExists2) {
                    assertThat("expected in flight remove request for project2", removeListener2.get(), notNullValue());
                    setState(clusterService, stateWithoutProjectTask(clusterService.state(), projectId2, TASK_NAME));
                    removeListener2.getAndSet(null).onResponse(null);
                }
                assertThat("unexpected in flight start request for project2", startListener2.get(), nullValue());
                assertThat("unexpected in flight remove request for project2", removeListener2.get(), nullValue());
            }

            verify(persistentTasksService, never()).sendClusterStartRequest(any(), any(), any(), any(), any());
            verify(persistentTasksService, never()).sendClusterRemoveRequest(any(), any(), any());
        }
    }

    public void testSettingsCorrectlyPropagatedToProjectTaskReconciliation() {
        final var projectId = randomUniqueProjectId();
        final var startListener = new AtomicReference<ActionListener<?>>();
        final var removeListener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task start request", startListener.get(), nullValue());
            startListener.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService)
            .sendProjectStartRequest(eq(projectId), eq(TASK_NAME), eq(TASK_NAME), eq(TestParams.INSTANCE), isNotNull(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task remove request", removeListener.get(), nullValue());
            removeListener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(eq(projectId), eq(TASK_NAME), isNotNull(), any());

        final var nodeSettings = Settings.builder().put(TASK_ENABLED_SETTING.getKey(), true).build();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final var stateWithProject = masterStateWithProjects(Set.of(projectId));

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            setState(clusterService, masterState());
            assertThat("unexpected in flight project task start request", startListener.get(), nullValue());
            assertThat("unexpected in flight project task remove request", removeListener.get(), nullValue());

            setState(clusterService, stateWithProject);
            assertThat("expected in flight project task start request", startListener.get(), notNullValue());
            setState(clusterService, stateWithProjectTask(clusterService.state(), projectId, TASK_NAME));
            startListener.getAndSet(null).onResponse(null);

            setState(
                clusterService,
                stateWithPersistentSetting(
                    stateWithProjectTask(stateWithProject, projectId, TASK_NAME),
                    TASK_ENABLED_SETTING.getKey(),
                    false
                )
            );
            assertThat("expected in flight project task remove request", removeListener.get(), notNullValue());
            removeListener.getAndSet(null).onResponse(null);
        }
    }

    public void testProjectTaskReconciliationDeduplicatesInFlightRequests() {
        final var projectId = randomUniqueProjectId();
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task start request", listener.get(), nullValue());
            listener.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task remove request", listener.get(), nullValue());
            listener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            for (int cycle = 0; cycle < 10; cycle++) {
                final boolean sendStart = randomBoolean();
                final var initialState = masterStateWithProjects(Set.of(projectId));
                setState(
                    clusterService,
                    stateWithPersistentSetting(
                        sendStart ? initialState : stateWithProjectTask(initialState, projectId, TASK_NAME),
                        TASK_ENABLED_SETTING.getKey(),
                        sendStart
                    )
                );
                assertThat("expected in flight request", listener.get(), notNullValue());

                // A request is still in flight -> no new requests should be sent. The doAnswer guard ensures this.
                for (int j = 0; j < 10; j++) {
                    final var newState = masterStateWithProjects(Set.of(projectId));
                    setState(clusterService, randomBoolean() ? newState : stateWithProjectTask(newState, projectId, TASK_NAME));
                }
                assertThat("no new project request should have been sent while one was in flight", listener.get(), notNullValue());

                final boolean postCompletionEnabled = randomBoolean();
                final var baseProjectState = masterStateWithProjects(Set.of(projectId));
                setState(
                    clusterService,
                    stateWithPersistentSetting(
                        sendStart ? stateWithProjectTask(baseProjectState, projectId, TASK_NAME) : baseProjectState,
                        TASK_ENABLED_SETTING.getKey(),
                        postCompletionEnabled
                    )
                );
                listener.getAndSet(null).onResponse(null);

                if (postCompletionEnabled != sendStart) {
                    // The opposite request should have been sent immediately on completion.
                    assertThat("expected opposite project task in flight request", listener.get(), notNullValue());
                    setState(
                        clusterService,
                        stateWithPersistentSetting(
                            sendStart ? baseProjectState : stateWithProjectTask(baseProjectState, projectId, TASK_NAME),
                            TASK_ENABLED_SETTING.getKey(),
                            postCompletionEnabled
                        )
                    );
                    listener.getAndSet(null).onResponse(null);
                } else {
                    assertThat("unexpected in flight project task request after completion", listener.get(), nullValue());
                }
            }
        }
    }

    public void testClusterTaskStartStopRetriesOnFailure() {
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task request", listener.get(), nullValue());
            listener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight cluster task request", listener.get(), nullValue());
            listener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final boolean sendStart = randomBoolean();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? masterState() : stateWithClusterTask(masterState()),
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            assertThat("expected in flight cluster task request", listener.get(), notNullValue());
            listener.getAndSet(null).onFailure(new RuntimeException("transient"));
            assertThat("expected retry cluster task request after failure", listener.get(), notNullValue());
            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? stateWithClusterTask(masterState()) : masterState(),
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            listener.getAndSet(null).onResponse(null);
        }
    }

    public void testProjectTaskStartStopRetriesOnFailure() {
        final var projectId = randomUniqueProjectId();
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task request", listener.get(), nullValue());
            listener.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            assertThat("unexpected duplicate in flight project task request", listener.get(), nullValue());
            listener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final boolean sendStart = randomBoolean();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            final var baseState = masterStateWithProjects(Set.of(projectId));
            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? baseState : stateWithProjectTask(baseState, projectId, TASK_NAME),
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            assertThat("expected in flight project task request", listener.get(), notNullValue());
            listener.getAndSet(null).onFailure(new RuntimeException("transient"));
            assertThat("expected retry project task request after failure", listener.get(), notNullValue());
            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? stateWithProjectTask(baseState, projectId, TASK_NAME) : baseState,
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            listener.getAndSet(null).onResponse(null);
        }
    }

    public void testClusterTaskDoesNotRetryAfterNodeShutdown() {
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            listener.set(inv.getArgument(4));
            return null;
        }).when(persistentTasksService).sendClusterStartRequest(any(), any(), any(), any(), any());
        doAnswer(inv -> {
            listener.set(inv.getArgument(2));
            return null;
        }).when(persistentTasksService).sendClusterRemoveRequest(any(), any(), any());

        final boolean sendStart = randomBoolean();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? masterState() : stateWithClusterTask(masterState()),
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            assertThat("expected in flight cluster task request", listener.get(), notNullValue());

            manager.stop();
            listener.getAndSet(null).onFailure(new RuntimeException("transient"));

            assertThat("unexpected retry cluster task request after manager stopped", listener.get(), nullValue());
        }
    }

    public void testProjectTaskDoesNotRetryAfterNodeShutdown() {
        final var projectId = randomUniqueProjectId();
        final var listener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            listener.set(inv.getArgument(5));
            return null;
        }).when(persistentTasksService).sendProjectStartRequest(any(), any(), any(), any(), any(), any());
        doAnswer(inv -> {
            listener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final boolean sendStart = randomBoolean();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            manager.start();

            final var baseState = masterStateWithProjects(Set.of(projectId));
            setState(
                clusterService,
                stateWithPersistentSetting(
                    sendStart ? baseState : stateWithProjectTask(baseState, projectId, TASK_NAME),
                    TASK_ENABLED_SETTING.getKey(),
                    sendStart
                )
            );
            assertThat("expected in flight project task request", listener.get(), notNullValue());

            manager.stop();
            listener.getAndSet(null).onFailure(new RuntimeException("transient"));

            assertThat("unexpected retry project task request after manager stopped", listener.get(), nullValue());
        }
    }

    public void testProjectTaskOnRemoveCallbackIsInvoked() {
        final var projectId = randomUniqueProjectId();
        final var removeListener = new AtomicReference<ActionListener<?>>();
        doAnswer(inv -> {
            removeListener.set(inv.getArgument(3));
            return null;
        }).when(persistentTasksService).sendProjectRemoveRequest(any(), any(), any(), any());

        final var onRemove = new AtomicReference<ProjectId>();
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            manager.registerProjectTask(TASK_NAME, p -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE, onRemove::set);
            manager.start();

            final var state = stateWithProjectTask(masterStateWithProjects(Set.of(projectId)), projectId, TASK_NAME);
            setState(clusterService, stateWithPersistentSetting(state, TASK_ENABLED_SETTING.getKey(), false));
            assertThat("expected in flight project task remove request", removeListener.get(), notNullValue());
            assertThat("onRemove should not be called before request completes", onRemove.get(), nullValue());

            removeListener.getAndSet(null).onResponse(null);
            assertThat("onRemove should be called after successful remove", onRemove.get(), notNullValue());
        }
    }

    public void testNonMasterNeverStartsOrStopsTask() {
        final var allSettings = Sets.union(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS, Set.of(TASK_ENABLED_SETTING));
        final var nodeSettings = Settings.EMPTY;
        final var clusterSettings = new ClusterSettings(nodeSettings, allSettings);
        final boolean projectScoped = randomBoolean();
        final var initialState = projectScoped ? nonMasterStateWithProjects(Set.of(ProjectId.DEFAULT)) : nonMasterState();

        try (var clusterService = ClusterServiceUtils.createClusterService(threadPool, LOCAL_NODE, nodeSettings, clusterSettings)) {
            final var manager = new PersistentTaskLifecycleManager(persistentTasksService, clusterService);
            if (projectScoped) {
                manager.registerProjectTask(TASK_NAME, projectId -> TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE, p -> {});
            } else {
                manager.registerClusterTask(TASK_NAME, TASK_ENABLED_SETTING, () -> TestParams.INSTANCE);
            }
            manager.start();

            var state = initialState;
            if (randomBoolean()) {
                state = projectScoped ? stateWithProjectTask(state, ProjectId.DEFAULT, TASK_NAME) : stateWithClusterTask(state);
            }
            setState(clusterService, state);

            verifyNoInteractions(persistentTasksService);
        }
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

    private static ClusterState stateWithoutClusterTask(ClusterState state) {
        final var metadata = Metadata.builder(state.metadata()).removeCustom(ClusterPersistentTasksCustomMetadata.TYPE);
        return ClusterState.builder(state).metadata(metadata).build();
    }

    private static ClusterState stateWithoutProjectTask(ClusterState state, ProjectId projectId, String taskId) {
        final var project = state.metadata().getProject(projectId);
        final var existingTasks = (PersistentTasksCustomMetadata) project.custom(PersistentTasksCustomMetadata.TYPE);
        if (existingTasks == null) {
            return state;
        }
        final var tasks = PersistentTasksCustomMetadata.builder(existingTasks).removeTask(taskId).build();
        final var projectMetadata = ProjectMetadata.builder(project).putCustom(PersistentTasksCustomMetadata.TYPE, tasks);
        return ClusterState.builder(state).metadata(Metadata.builder(state.metadata()).put(projectMetadata)).build();
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
