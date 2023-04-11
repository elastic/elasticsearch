/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ParentTaskAssigningClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.transforms.AuthorizationState;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.TransformServices;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.elasticsearch.xpack.transform.transforms.scheduling.TransformScheduler;
import org.junit.After;
import org.junit.Before;

import java.time.Clock;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransformTaskTests extends ESTestCase {

    private Client client;

    @Before
    public void setupClient() {
        if (client != null) {
            client.close();
        }
        client = new NoOpClient(getTestName());
    }

    @After
    public void tearDownClient() {
        client.close();
    }

    // see https://github.com/elastic/elasticsearch/issues/48957
    public void testStopOnFailedTaskWithStoppedIndexer() {
        Clock clock = Clock.systemUTC();
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        TransformConfig transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        TransformAuditor auditor = MockTransformAuditor.createMockAuditor();
        TransformConfigManager transformsConfigManager = new InMemoryTransformConfigManager();
        TransformCheckpointService transformsCheckpointService = new TransformCheckpointService(
            clock,
            Settings.EMPTY,
            new ClusterService(
                Settings.EMPTY,
                new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
                null,
                (TaskManager) null
            ),
            transformsConfigManager,
            auditor
        );
        TransformServices transformServices = new TransformServices(
            transformsConfigManager,
            transformsCheckpointService,
            auditor,
            new TransformScheduler(clock, threadPool, Settings.EMPTY)
        );

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
            client,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(clock, threadPool, Settings.EMPTY),
            auditor,
            threadPool,
            Collections.emptyMap()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);

        ClientTransformIndexerBuilder indexerBuilder = new ClientTransformIndexerBuilder();
        indexerBuilder.setClient(new ParentTaskAssigningClient(client, TaskId.EMPTY_TASK_ID))
            .setTransformConfig(transformConfig)
            .setTransformServices(transformServices);

        transformTask.initializeIndexer(indexerBuilder);
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
                    + "] as it is in a failed state with reason [because]. Use force stop to stop the transform."
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
            client,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY),
            auditor,
            threadPool,
            Collections.emptyMap()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        transformTask.fail("because", ActionListener.wrap(r -> { listenerCalled.compareAndSet(false, true); }, e -> {
            fail("setting transform task to failed failed with: " + e);
        }));

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
                    + "] as it is in a failed state with reason [because]. Use force stop to stop the transform."
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
            client,
            createTransformTaskParams(transformConfig.getId()),
            transformState,
            new TransformScheduler(Clock.systemUTC(), threadPool, Settings.EMPTY),
            auditor,
            threadPool,
            Collections.emptyMap()
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

    private static TransformTaskParams createTransformTaskParams(String transformId) {
        return new TransformTaskParams(transformId, Version.CURRENT, TimeValue.timeValueSeconds(10), false);
    }
}
