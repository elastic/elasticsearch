/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata.PersistentTask;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpointingInfo;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformIndexerPosition;
import org.elasticsearch.xpack.core.transform.transforms.TransformProgress;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.DefaultTransformExtension;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.After;
import org.junit.Before;
import org.mockito.verification.VerificationMode;

import java.time.Clock;
import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class TransformTaskTests extends ESTestCase {

    private TestThreadPool threadPool;
    private Client client;

    @Before
    public void setupClient() {
        if (threadPool != null) {
            threadPool.close();
        }
        threadPool = createThreadPool();
        client = new NoOpClient(threadPool);
    }

    @After
    public void tearDownClient() {
        threadPool.close();
    }

    // see https://github.com/elastic/elasticsearch/issues/48957
    public void testStopOnFailedTaskWithStoppedIndexer() {
        Clock clock = Clock.systemUTC();
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        TransformConfig transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        TransformAuditor auditor = MockTransformAuditor.createMockAuditor();

        TransformState transformState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0L,
            "because",
            null,
            null,
            false,
            null
        );

        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(clock, threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            mockTransformNode()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);

        transformTask.initializeIndexer(indexerBuilder(transformConfig, transformServices(clock, auditor, threadPool)));
        TransformState state = transformTask.getState();
        assertEquals(TransformTaskState.FAILED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertThat(state.getReason(), equalTo("because"));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> transformTask.stop(false, false));

        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(
            e.getMessage(),
            equalTo(
                "Unable to stop transform ["
                    + transformConfig.getId()
                    + "] as it is in a failed state. Use force stop to stop the transform. More details: [because]"
            )
        );

        // verify that shutdown has not been called
        verify(taskManager, times(0)).unregister(any());

        state = transformTask.getState();
        assertEquals(TransformTaskState.FAILED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertThat(state.getReason(), equalTo("because"));

        transformTask.stop(true, false);

        // verify shutdown has been called
        verify(taskManager).unregister(any());

        state = transformTask.getState();
        assertEquals(TransformTaskState.STARTED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertEquals(state.getReason(), null);
    }

    private TransformNode mockTransformNode() {
        var transformNode = mock(TransformNode.class);
        when(transformNode.isShuttingDown()).thenReturn(randomBoolean() ? Optional.of(false) : Optional.<Boolean>empty());
        return transformNode;
    }

    private TransformServices transformServices(Clock clock, TransformAuditor auditor, ThreadPool threadPool) {
        var transformsConfigManager = new InMemoryTransformConfigManager();
        var transformsCheckpointService = new TransformCheckpointService(
            clock,
            Settings.EMPTY,
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                threadPool,
                null
            ),
            transformsConfigManager,
            auditor
        );
        return new TransformServices(
            transformsConfigManager,
            transformsCheckpointService,
            auditor,
            new TransformScheduler(clock, threadPool, Settings.EMPTY, TimeValue.ZERO),
            mock(TransformNode.class)
        );
    }

    private ClientTransformIndexerBuilder indexerBuilder(TransformConfig transformConfig, TransformServices transformServices) {
        return new ClientTransformIndexerBuilder().setClient(new ParentTaskAssigningClient(client, TaskId.EMPTY_TASK_ID))
            .setClusterService(mock(ClusterService.class))
            .setIndexNameExpressionResolver(mock(IndexNameExpressionResolver.class))
            .setTransformExtension(new DefaultTransformExtension())
            .setTransformConfig(transformConfig)
            .setTransformServices(transformServices);
    }

    public void testStopOnFailedTaskWithoutIndexer() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        TransformConfig transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        TransformAuditor auditor = MockTransformAuditor.createMockAuditor();

        TransformState transformState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0L,
            "because",
            null,
            null,
            false,
            null
        );

        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            mockTransformNode()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        transformTask.fail(null, "because", ActionTestUtils.assertNoFailureListener(r -> { listenerCalled.compareAndSet(false, true); }));

        TransformState state = transformTask.getState();
        assertEquals(TransformTaskState.FAILED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertThat(state.getReason(), equalTo("because"));

        ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class, () -> transformTask.stop(false, false));

        assertThat(e.status(), equalTo(RestStatus.CONFLICT));
        assertThat(
            e.getMessage(),
            equalTo(
                "Unable to stop transform ["
                    + transformConfig.getId()
                    + "] as it is in a failed state. Use force stop to stop the transform. More details: [because]"
            )
        );

        // verify that shutdown has not been called
        verify(taskManager, times(0)).unregister(any());

        state = transformTask.getState();
        assertEquals(TransformTaskState.FAILED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertThat(state.getReason(), equalTo("because"));

        transformTask.stop(true, false);

        // verify shutdown has been called
        verify(taskManager).unregister(any());

        state = transformTask.getState();
        assertEquals(TransformTaskState.STARTED, state.getTaskState());
        assertEquals(IndexerState.STOPPED, state.getIndexerState());
        assertEquals(state.getReason(), null);
    }

    public void testFailWhenNodeIsShuttingDown() {
        var threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        var transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        var auditor = MockTransformAuditor.createMockAuditor();

        var transformState = new TransformState(
            TransformTaskState.STARTED,
            IndexerState.INDEXING,
            null,
            0L,
            "because",
            null,
            null,
            false,
            null
        );

        var node = mock(TransformNode.class);
        when(node.isShuttingDown()).thenReturn(Optional.of(true));
        when(node.nodeId()).thenReturn("node");

        var transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            node
        );

        var taskManager = mock(TaskManager.class);
        var persistentTasksService = mock(PersistentTasksService.class);
        transformTask.init(persistentTasksService, taskManager, "task-id", 42);
        var listenerCalled = new AtomicBoolean(false);
        transformTask.fail(null, "because", ActionTestUtils.assertNoFailureListener(r -> { listenerCalled.compareAndSet(false, true); }));

        var state = transformTask.getState();
        assertEquals(TransformTaskState.STARTED, state.getTaskState());
        assertEquals(IndexerState.STARTED, state.getIndexerState());

        assertTrue(listenerCalled.get());
        // verify shutdown has been called
        verify(taskManager, times(1)).unregister(any());
        verify(persistentTasksService, times(1)).sendCompletionRequest(
            eq("task-id"),
            eq(42L),
            isNull(),
            eq("Node is shutting down."),
            isNull(),
            any()
        );
    }

    public void testGetTransformTask() {
        {
            ClusterState clusterState = ClusterState.EMPTY_STATE;
            assertThat(TransformTask.getTransformTask("transform-1", clusterState), is(nullValue()));
            assertThat(TransformTask.getTransformTask("other-1", clusterState), is(nullValue()));
        }
        {
            ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            PersistentTasksCustomMetadata.TYPE,
                            PersistentTasksCustomMetadata.builder()
                                .addTask("other-1", "other", null, null)
                                .addTask("other-2", "other", null, null)
                                .addTask("other-3", "other", null, null)
                                .build()
                        )
                )
                .build();
            assertThat(TransformTask.getTransformTask("transform-1", clusterState), is(nullValue()));
            assertThat(TransformTask.getTransformTask("other-1", clusterState), is(nullValue()));
        }
        {
            TransformTaskParams transformTaskParams = createTransformTaskParams("transform-1");
            PersistentTaskParams otherTaskParams = mock(PersistentTaskParams.class);
            when(otherTaskParams.getWriteableName()).thenReturn(TransformTaskParams.NAME);
            ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            PersistentTasksCustomMetadata.TYPE,
                            PersistentTasksCustomMetadata.builder()
                                .addTask("transform-1", TransformTaskParams.NAME, transformTaskParams, null)
                                .addTask("other-1", "other", null, null)
                                .addTask("transform-2", TransformTaskParams.NAME, otherTaskParams, null)
                                .addTask("other-2", "other", null, null)
                                .addTask("transform-3", TransformTaskParams.NAME, null, null)
                                .addTask("other-3", "other", null, null)
                                .build()
                        )
                )
                .build();
            assertThat(TransformTask.getTransformTask("transform-1", clusterState).getId(), is(equalTo("transform-1")));
            ElasticsearchStatusException e = expectThrows(
                ElasticsearchStatusException.class,
                () -> TransformTask.getTransformTask("transform-2", clusterState).getId()
            );
            assertThat(e.getMessage(), is(equalTo("Found transform persistent task [transform-2] with incorrect params")));
            e = expectThrows(ElasticsearchStatusException.class, () -> TransformTask.getTransformTask("transform-3", clusterState).getId());
            assertThat(e.getMessage(), is(equalTo("Found transform persistent task [transform-3] with incorrect params")));
            assertThat(TransformTask.getTransformTask("other-1", clusterState), is(nullValue()));
            assertThat(TransformTask.getTransformTask("other-2", clusterState), is(nullValue()));
            assertThat(TransformTask.getTransformTask("other-3", clusterState), is(nullValue()));
        }
    }

    public void testFindAllTransformTasks() {
        {
            ClusterState clusterState = ClusterState.EMPTY_STATE;
            assertThat(TransformTask.findAllTransformTasks(clusterState), is(empty()));
        }
        {
            ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            PersistentTasksCustomMetadata.TYPE,
                            PersistentTasksCustomMetadata.builder()
                                .addTask("other-1", "other", null, null)
                                .addTask("other-2", "other", null, null)
                                .addTask("other-3", "other", null, null)
                                .build()
                        )
                )
                .build();
            assertThat(TransformTask.findAllTransformTasks(clusterState), is(empty()));
        }
        {
            ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
                .metadata(
                    Metadata.builder()
                        .putCustom(
                            PersistentTasksCustomMetadata.TYPE,
                            PersistentTasksCustomMetadata.builder()
                                .addTask("transform-1", TransformTaskParams.NAME, null, null)
                                .addTask("other-1", "other", null, null)
                                .addTask("transform-2", TransformTaskParams.NAME, null, null)
                                .addTask("other-2", "other", null, null)
                                .addTask("transform-3", TransformTaskParams.NAME, null, null)
                                .addTask("other-3", "other", null, null)
                                .build()
                        )
                )
                .build();
            assertThat(
                TransformTask.findAllTransformTasks(clusterState).stream().map(PersistentTask::getId).collect(toList()),
                containsInAnyOrder("transform-1", "transform-2", "transform-3")
            );
        }
    }

    public void testFindTransformTasks() {
        ClusterState clusterState = ClusterState.builder(new ClusterName("some-cluster"))
            .metadata(
                Metadata.builder()
                    .putCustom(
                        PersistentTasksCustomMetadata.TYPE,
                        PersistentTasksCustomMetadata.builder()
                            .addTask("transform-1", TransformTaskParams.NAME, createTransformTaskParams("transform-1"), null)
                            .addTask("other-1", "other", null, null)
                            .addTask("transform-2", TransformTaskParams.NAME, createTransformTaskParams("transform-2"), null)
                            .addTask("other-2", "other", null, null)
                            .addTask("transform-3", TransformTaskParams.NAME, createTransformTaskParams("transform-3"), null)
                            .addTask("other-3", "other", null, null)
                            .build()
                    )
            )
            .build();
        assertThat(
            TransformTask.findTransformTasks("trans*-*", clusterState).stream().map(PersistentTask::getId).collect(toList()),
            containsInAnyOrder("transform-1", "transform-2", "transform-3")
        );
        assertThat(
            TransformTask.findTransformTasks("*-2", clusterState).stream().map(PersistentTask::getId).collect(toList()),
            contains("transform-2")
        );
        assertThat(TransformTask.findTransformTasks("*-4", clusterState), is(empty()));
        assertThat(
            TransformTask.findTransformTasks(Set.of("transform-1", "transform-2", "transform-3", "transform-4"), clusterState)
                .stream()
                .map(PersistentTask::getId)
                .collect(toList()),
            containsInAnyOrder("transform-1", "transform-2", "transform-3")
        );
        assertThat(
            TransformTask.findTransformTasks(Set.of("transform-1", "transform-2"), clusterState)
                .stream()
                .map(PersistentTask::getId)
                .collect(toList()),
            containsInAnyOrder("transform-1", "transform-2")
        );
        assertThat(TransformTask.findTransformTasks(Set.of("transform-4", "transform-5"), clusterState), is(empty()));
    }

    public void testApplyNewAuthState() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        TransformConfig transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        TransformAuditor auditor = MockTransformAuditor.createMockAuditor();

        TransformState transformState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0L,
            "because",
            null,
            null,
            false,
            AuthorizationState.green()
        );

        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            mockTransformNode()
        );
        assertThat(transformTask.getContext().getAuthState().getStatus(), is(equalTo(HealthStatus.GREEN)));

        transformTask.applyNewAuthState(AuthorizationState.red(new ElasticsearchSecurityException("missing permissions")));
        assertThat(transformTask.getContext().getAuthState().getStatus(), is(equalTo(HealthStatus.RED)));
        assertThat(transformTask.getContext().getAuthState().getLastAuthError(), is(equalTo("missing permissions")));

        transformTask.applyNewAuthState(AuthorizationState.green());
        assertThat(transformTask.getContext().getAuthState().getStatus(), is(equalTo(HealthStatus.GREEN)));

        transformTask.applyNewAuthState(null);
        assertThat(transformTask.getContext().getAuthState(), is(nullValue()));
    }

    public void testDeriveBasicCheckpointingInfoWithNoIndexer() {
        var transformTask = createTransformTask(
            TransformConfigTests.randomTransformConfigWithoutHeaders(),
            MockTransformAuditor.createMockAuditor()
        );
        var checkpointingInfo = transformTask.deriveBasicCheckpointingInfo();
        assertThat(checkpointingInfo, sameInstance(TransformCheckpointingInfo.EMPTY));
    }

    private TransformTask createTransformTask(TransformConfig transformConfig, MockTransformAuditor auditor) {
        var threadPool = mock(ThreadPool.class);

        var transformState = new TransformState(
            TransformTaskState.STARTED,
            IndexerState.STARTED,
            null,
            0L,
            "because",
            null,
            null,
            false,
            null
        );

        return new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            mockTransformNode()
        );
    }

    public void testDeriveBasicCheckpointingInfoWithIndexer() {
        var lastCheckpoint = mock(TransformCheckpoint.class);
        when(lastCheckpoint.getCheckpoint()).thenReturn(5L);
        var nextCheckpoint = mock(TransformCheckpoint.class);
        when(nextCheckpoint.getCheckpoint()).thenReturn(6L);
        var position = mock(TransformIndexerPosition.class);
        var progress = mock(TransformProgress.class);

        var transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        var auditor = MockTransformAuditor.createMockAuditor();
        var transformTask = createTransformTask(transformConfig, auditor);

        transformTask.initializeIndexer(
            indexerBuilder(transformConfig, transformServices(Clock.systemUTC(), auditor, threadPool)).setLastCheckpoint(lastCheckpoint)
                .setNextCheckpoint(nextCheckpoint)
                .setInitialPosition(position)
                .setProgress(progress)
        );

        var checkpointingInfo = transformTask.deriveBasicCheckpointingInfo();
        assertThat(checkpointingInfo, not(sameInstance(TransformCheckpointingInfo.EMPTY)));
        assertThat(checkpointingInfo.getLast().getCheckpoint(), equalTo(5L));
        assertThat(checkpointingInfo.getNext().getCheckpoint(), equalTo(6L));
        assertThat(checkpointingInfo.getNext().getPosition(), sameInstance(position));
        assertThat(checkpointingInfo.getNext().getCheckpointProgress(), sameInstance(progress));
    }

    public void testInitializeIndexerWhenAlreadyInitialized() {
        var transformTask = createTransformTask(
            TransformConfigTests.randomTransformConfigWithoutHeaders(),
            MockTransformAuditor.createMockAuditor()
        );
        transformTask.initializeIndexer(mock(ClientTransformIndexerBuilder.class));
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> transformTask.initializeIndexer(mock(ClientTransformIndexerBuilder.class))
        );
        assertThat(e.getMessage(), containsString("The object cannot be set twice!"));
    }

    public void testTriggeredIsNoOpWhenTransformIdMismatch() {
        var transformId = randomAlphaOfLengthBetween(1, 10);
        var transformTask = createTransformTask(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            MockTransformAuditor.createMockAuditor()
        );
        var indexer = mock(ClientTransformIndexer.class);
        transformTask.initializeIndexer(indexer);
        transformTask.triggered(new TransformScheduler.Event("not-" + transformId, randomNonNegativeLong(), randomNonNegativeLong()));
        verifyNoInteractions(indexer);
    }

    public void testTriggeredIsNoOpWhenIndexerIsUninitialized() {
        var transformId = randomAlphaOfLengthBetween(1, 10);
        var transformTask = createTransformTask(
            TransformConfigTests.randomTransformConfigWithoutHeaders(transformId),
            MockTransformAuditor.createMockAuditor()
        );
        transformTask.triggered(new TransformScheduler.Event(transformId, randomNonNegativeLong(), randomNonNegativeLong()));
    }

    public void testTriggeredIsNoOpWhenStateIsWrong() {
        testTriggered(TransformTaskState.STOPPED, IndexerState.INDEXING, never());
        testTriggered(TransformTaskState.STOPPED, IndexerState.STOPPING, never());
        testTriggered(TransformTaskState.STOPPED, IndexerState.STOPPED, never());
        testTriggered(TransformTaskState.STOPPED, IndexerState.ABORTING, never());
        testTriggered(TransformTaskState.STOPPED, IndexerState.STARTED, never());
        testTriggered(TransformTaskState.FAILED, IndexerState.INDEXING, never());
        testTriggered(TransformTaskState.FAILED, IndexerState.STOPPING, never());
        testTriggered(TransformTaskState.FAILED, IndexerState.STOPPED, never());
        testTriggered(TransformTaskState.FAILED, IndexerState.ABORTING, never());
        testTriggered(TransformTaskState.FAILED, IndexerState.STARTED, never());
        testTriggered(TransformTaskState.STARTED, IndexerState.INDEXING, never());
        testTriggered(TransformTaskState.STARTED, IndexerState.STOPPING, never());
        testTriggered(TransformTaskState.STARTED, IndexerState.STOPPED, never());
        testTriggered(TransformTaskState.STARTED, IndexerState.ABORTING, never());
    }

    public void testTriggeredActuallyTriggersIndexer() {
        testTriggered(TransformTaskState.STARTED, IndexerState.STARTED, times(1));
    }

    private void testTriggered(TransformTaskState taskState, IndexerState indexerState, VerificationMode indexerVerificationMode) {
        String transformId = randomAlphaOfLengthBetween(1, 10);
        TransformState transformState = new TransformState(taskState, indexerState, null, 0L, "because", null, null, false, null);
        ThreadPool threadPool = mock(ThreadPool.class);
        TransformAuditor auditor = mock(TransformAuditor.class);
        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            createTransformTaskParams(transformId),
            transformState,
            new TransformScheduler(mock(Clock.class), threadPool, Settings.EMPTY, TimeValue.ZERO),
            auditor,
            threadPool,
            Collections.emptyMap(),
            mockTransformNode()
        );

        ClientTransformIndexer indexer = mock(ClientTransformIndexer.class);
        when(indexer.getState()).thenReturn(indexerState);
        transformTask.initializeIndexer(indexer);
        transformTask.triggered(new TransformScheduler.Event(transformId, randomNonNegativeLong(), randomNonNegativeLong()));

        verify(indexer, indexerVerificationMode).maybeTriggerAsyncJob(anyLong());
        verifyNoInteractions(auditor, threadPool);
    }

    private static TransformTaskParams createTransformTaskParams(String transformId) {
        return new TransformTaskParams(transformId, TransformConfigVersion.CURRENT, TimeValue.timeValueSeconds(10), false);
    }
}
