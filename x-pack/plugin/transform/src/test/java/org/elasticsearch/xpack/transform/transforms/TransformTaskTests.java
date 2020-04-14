/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.ParentTaskAssigningClient;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.scheduler.SchedulerEngine;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfig;
import org.elasticsearch.xpack.core.transform.transforms.TransformConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.elasticsearch.xpack.transform.checkpoint.TransformCheckpointService;
import org.elasticsearch.xpack.transform.notifications.MockTransformAuditor;
import org.elasticsearch.xpack.transform.notifications.TransformAuditor;
import org.elasticsearch.xpack.transform.persistence.InMemoryTransformConfigManager;
import org.elasticsearch.xpack.transform.persistence.TransformConfigManager;
import org.junit.After;
import org.junit.Before;

import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Matchers.any;
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
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.executor("generic")).thenReturn(mock(ExecutorService.class));

        TransformConfig transformConfig = TransformConfigTests.randomTransformConfigWithoutHeaders();
        TransformAuditor auditor = new MockTransformAuditor();
        TransformConfigManager transformsConfigManager = new InMemoryTransformConfigManager();
        TransformCheckpointService transformsCheckpointService = new TransformCheckpointService(
            Settings.EMPTY,
            new ClusterService(Settings.EMPTY, new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS), null),
            transformsConfigManager,
            auditor
        );

        TransformState transformState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0L,
            "because",
            null,
            null,
            false
        );

        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            client,
            new TransformTaskParams(transformConfig.getId(), Version.CURRENT, TimeValue.timeValueSeconds(10), false),
            transformState,
            mock(SchedulerEngine.class),
            auditor,
            threadPool,
            Collections.emptyMap()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);

        ClientTransformIndexerBuilder indexerBuilder = new ClientTransformIndexerBuilder();
        indexerBuilder.setClient(new ParentTaskAssigningClient(client, TaskId.EMPTY_TASK_ID))
            .setTransformConfig(transformConfig)
            .setAuditor(auditor)
            .setTransformsConfigManager(transformsConfigManager)
            .setTransformsCheckpointService(transformsCheckpointService)
            .setFieldMappings(Collections.emptyMap());

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
        TransformAuditor auditor = new MockTransformAuditor();

        TransformState transformState = new TransformState(
            TransformTaskState.FAILED,
            IndexerState.STOPPED,
            null,
            0L,
            "because",
            null,
            null,
            false
        );

        TransformTask transformTask = new TransformTask(
            42,
            "some_type",
            "some_action",
            TaskId.EMPTY_TASK_ID,
            client,
            new TransformTaskParams(transformConfig.getId(), Version.CURRENT, TimeValue.timeValueSeconds(10), false),
            transformState,
            mock(SchedulerEngine.class),
            auditor,
            threadPool,
            Collections.emptyMap()
        );

        TaskManager taskManager = mock(TaskManager.class);

        transformTask.init(mock(PersistentTasksService.class), taskManager, "task-id", 42);
        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        transformTask.fail(
            "because",
            ActionListener.wrap(
                r -> { listenerCalled.compareAndSet(false, true); },
                e -> { fail("setting transform task to failed failed with: " + e); }
            )
        );

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

}
