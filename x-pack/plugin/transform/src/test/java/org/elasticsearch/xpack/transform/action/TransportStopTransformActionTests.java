/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.transform.action;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.persistent.PersistentTaskResponse;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.persistent.RemovePersistentTaskAction;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.core.indexing.IndexerState;
import org.elasticsearch.xpack.core.transform.TransformConfigVersion;
import org.elasticsearch.xpack.core.transform.action.StopTransformAction;
import org.elasticsearch.xpack.core.transform.transforms.TransformState;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskParams;
import org.elasticsearch.xpack.core.transform.transforms.TransformTaskState;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.rest.RestStatus.CONFLICT;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.same;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportStopTransformActionTests extends ESTestCase {

    private Metadata.Builder buildMetadata(PersistentTasksCustomMetadata ptasks) {
        return Metadata.builder().putCustom(PersistentTasksCustomMetadata.TYPE, ptasks);
    }

    public void testTaskStateValidationWithNoTasks() {
        Metadata.Builder metadata = Metadata.builder();
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).metadata(metadata);
        TransportStopTransformAction.validateTaskState(csBuilder.build(), List.of("non-failed-task"), false);

        PersistentTasksCustomMetadata.Builder pTasksBuilder = PersistentTasksCustomMetadata.builder();
        csBuilder = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build()));
        TransportStopTransformAction.validateTaskState(csBuilder.build(), List.of("non-failed-task"), false);
    }

    public void testTaskStateValidationWithTransformTasks() {
        // Test with the task state being null
        PersistentTasksCustomMetadata.Builder pTasksBuilder = PersistentTasksCustomMetadata.builder()
            .addTask(
                "non-failed-task",
                TransformTaskParams.NAME,
                new TransformTaskParams("transform-task-1", TransformConfigVersion.CURRENT, null, false),
                new PersistentTasksCustomMetadata.Assignment("current-data-node-with-1-tasks", "")
            );
        ClusterState.Builder csBuilder = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build()));

        TransportStopTransformAction.validateTaskState(csBuilder.build(), List.of("non-failed-task"), false);

        // test again with a non failed task but this time it has internal state
        pTasksBuilder.updateTaskState(
            "non-failed-task",
            new TransformState(TransformTaskState.STOPPED, IndexerState.STOPPED, null, 0L, null, null, null, false, null)
        );
        csBuilder = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build()));

        TransportStopTransformAction.validateTaskState(csBuilder.build(), List.of("non-failed-task"), false);

        // test again with one failed task
        pTasksBuilder.addTask(
            "failed-task",
            TransformTaskParams.NAME,
            new TransformTaskParams("transform-task-1", TransformConfigVersion.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("current-data-node-with-1-tasks", "")
        )
            .updateTaskState(
                "failed-task",
                new TransformState(TransformTaskState.FAILED, IndexerState.STOPPED, null, 0L, "task has failed", null, null, false, null)
            );
        final ClusterState cs = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build())).build();

        TransportStopTransformAction.validateTaskState(cs, List.of("non-failed-task", "failed-task"), true);

        TransportStopTransformAction.validateTaskState(cs, List.of("non-failed-task"), false);

        ClusterState.Builder csBuilderFinal = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build()));
        ElasticsearchStatusException ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportStopTransformAction.validateTaskState(csBuilderFinal.build(), List.of("failed-task"), false)
        );

        assertThat(ex.status(), equalTo(CONFLICT));
        assertThat(
            ex.getMessage(),
            equalTo(
                "Unable to stop transform [failed-task] as it is in a failed state. Use force stop to stop the transform. "
                    + "More details: [task has failed]"
            )
        );

        // test again with two failed tasks
        pTasksBuilder.addTask(
            "failed-task-2",
            TransformTaskParams.NAME,
            new TransformTaskParams("transform-task-2", TransformConfigVersion.CURRENT, null, false),
            new PersistentTasksCustomMetadata.Assignment("current-data-node-with-2-tasks", "")
        )
            .updateTaskState(
                "failed-task-2",
                new TransformState(
                    TransformTaskState.FAILED,
                    IndexerState.STOPPED,
                    null,
                    0L,
                    "task has also failed",
                    null,
                    null,
                    false,
                    null
                )
            );

        var csBuilderMultiTask = ClusterState.builder(new ClusterName("_name")).metadata(buildMetadata(pTasksBuilder.build()));
        ex = expectThrows(
            ElasticsearchStatusException.class,
            () -> TransportStopTransformAction.validateTaskState(csBuilderMultiTask.build(), List.of("failed-task", "failed-task-2"), false)
        );

        assertThat(ex.status(), equalTo(CONFLICT));
        assertThat(
            ex.getMessage(),
            equalTo(
                "Unable to stop transforms. The following transforms are in a failed state [failed-task, failed-task-2]. Use force "
                    + "stop to stop the transforms. More details: [task has failed, task has also failed]"
            )
        );
    }

    public void testFirstNotOKStatus() {
        List<ElasticsearchException> nodeFailures = new ArrayList<>();
        List<TaskOperationFailure> taskOperationFailures = new ArrayList<>();

        nodeFailures.add(
            new ElasticsearchException("nodefailure", new ElasticsearchStatusException("failure", RestStatus.UNPROCESSABLE_ENTITY))
        );
        taskOperationFailures.add(new TaskOperationFailure("node", 1, new ElasticsearchStatusException("failure", RestStatus.BAD_REQUEST)));

        assertThat(TransportStopTransformAction.firstNotOKStatus(List.of(), List.of()), equalTo(RestStatus.INTERNAL_SERVER_ERROR));

        assertThat(TransportStopTransformAction.firstNotOKStatus(taskOperationFailures, List.of()), equalTo(RestStatus.BAD_REQUEST));
        assertThat(TransportStopTransformAction.firstNotOKStatus(taskOperationFailures, nodeFailures), equalTo(RestStatus.BAD_REQUEST));
        assertThat(
            TransportStopTransformAction.firstNotOKStatus(
                taskOperationFailures,
                List.of(new ElasticsearchException(new ElasticsearchStatusException("not failure", RestStatus.OK)))
            ),
            equalTo(RestStatus.BAD_REQUEST)
        );

        assertThat(
            TransportStopTransformAction.firstNotOKStatus(
                List.of(new TaskOperationFailure("node", 1, new ElasticsearchStatusException("not failure", RestStatus.OK))),
                nodeFailures
            ),
            equalTo(RestStatus.INTERNAL_SERVER_ERROR)
        );

        assertThat(TransportStopTransformAction.firstNotOKStatus(List.of(), nodeFailures), equalTo(RestStatus.INTERNAL_SERVER_ERROR));
    }

    public void testBuildException() {
        List<ElasticsearchException> nodeFailures = new ArrayList<>();
        List<TaskOperationFailure> taskOperationFailures = new ArrayList<>();

        nodeFailures.add(new ElasticsearchException("node failure"));
        taskOperationFailures.add(
            new TaskOperationFailure("node", 1, new ElasticsearchStatusException("task failure", RestStatus.BAD_REQUEST))
        );

        RestStatus status = CONFLICT;
        ElasticsearchStatusException statusException = TransportStopTransformAction.buildException(
            taskOperationFailures,
            nodeFailures,
            status
        );

        assertThat(statusException.status(), equalTo(status));
        assertThat(statusException.getMessage(), equalTo(taskOperationFailures.get(0).getCause().getMessage()));
        assertThat(statusException.getSuppressed().length, equalTo(1));

        statusException = TransportStopTransformAction.buildException(List.of(), nodeFailures, status);
        assertThat(statusException.status(), equalTo(status));
        assertThat(statusException.getMessage(), equalTo(nodeFailures.get(0).getMessage()));
        assertThat(statusException.getSuppressed().length, equalTo(0));

        statusException = TransportStopTransformAction.buildException(taskOperationFailures, List.of(), status);
        assertThat(statusException.status(), equalTo(status));
        assertThat(statusException.getMessage(), equalTo(taskOperationFailures.get(0).getCause().getMessage()));
        assertThat(statusException.getSuppressed().length, equalTo(0));
    }

    public void testCancelTransformTasksListener_NoTasks() {
        StopTransformAction.Response responseTrue = new StopTransformAction.Response(true);

        PersistentTasksService persistentTasksService = mock(PersistentTasksService.class);
        Set<String> transformTasks = Set.of();
        ActionListener<StopTransformAction.Response> listener = Mockito.<ActionListener<StopTransformAction.Response>>mock();

        ActionListener<StopTransformAction.Response> cancelTransformTasksListener = TransportStopTransformAction
            .cancelTransformTasksListener(persistentTasksService, transformTasks, listener);
        cancelTransformTasksListener.onResponse(responseTrue);
        verify(listener, times(1)).onResponse(responseTrue);
    }

    public void testCancelTransformTasksListener_ThreeTasksRemovedSuccessfully() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        // We treat NotFound as a successful removal of the task
        doAnswer(randomBoolean() ? withResponse() : withException(new ResourceNotFoundException("task not found"))).when(client)
            .execute(same(RemovePersistentTaskAction.INSTANCE), any(), any());

        PersistentTasksService persistentTasksService = new PersistentTasksService(mock(ClusterService.class), threadPool, client);
        Set<String> transformTasks = Set.of("task-A", "task-B", "task-C");
        ActionListener<StopTransformAction.Response> listener = Mockito.<ActionListener<StopTransformAction.Response>>mock();

        StopTransformAction.Response responseTrue = new StopTransformAction.Response(true);
        ActionListener<StopTransformAction.Response> cancelTransformTasksListener = TransportStopTransformAction
            .cancelTransformTasksListener(persistentTasksService, transformTasks, listener);
        cancelTransformTasksListener.onResponse(responseTrue);

        verify(listener).onResponse(responseTrue);
    }

    public void testCancelTransformTasksListener_OneTaskCouldNotBeRemoved() {
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        Client client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);

        doAnswer(randomBoolean() ? withResponse() : withException(new ResourceNotFoundException("task not found"))).when(client)
            .execute(
                same(RemovePersistentTaskAction.INSTANCE),
                eq(new RemovePersistentTaskAction.Request(TEST_REQUEST_TIMEOUT, "task-A")),
                any()
            );
        doAnswer(randomBoolean() ? withResponse() : withException(new ResourceNotFoundException("task not found"))).when(client)
            .execute(
                same(RemovePersistentTaskAction.INSTANCE),
                eq(new RemovePersistentTaskAction.Request(TEST_REQUEST_TIMEOUT, "task-B")),
                any()
            );
        doAnswer(withException(new IllegalStateException("real issue while removing task"))).when(client)
            .execute(
                same(RemovePersistentTaskAction.INSTANCE),
                eq(new RemovePersistentTaskAction.Request(TEST_REQUEST_TIMEOUT, "task-C")),
                any()
            );

        PersistentTasksService persistentTasksService = new PersistentTasksService(mock(ClusterService.class), threadPool, client);
        Set<String> transformTasks = Set.of("task-A", "task-B", "task-C");
        ActionListener<StopTransformAction.Response> listener = Mockito.<ActionListener<StopTransformAction.Response>>mock();

        StopTransformAction.Response responseTrue = new StopTransformAction.Response(true);
        ActionListener<StopTransformAction.Response> cancelTransformTasksListener = TransportStopTransformAction
            .cancelTransformTasksListener(persistentTasksService, transformTasks, listener);
        cancelTransformTasksListener.onResponse(responseTrue);

        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(listener, times(1)).onFailure(exceptionArgumentCaptor.capture());
        Exception actualException = exceptionArgumentCaptor.getValue();
        assertThat(actualException.getMessage(), containsString("real issue while removing task"));
        assertThat(actualException.getSuppressed(), is(emptyArray()));
    }

    private static Answer<?> withResponse() {
        return invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var l = (ActionListener<PersistentTaskResponse>) invocationOnMock.getArguments()[2];
            l.onResponse(new PersistentTaskResponse((PersistentTasksCustomMetadata.PersistentTask<?>) null));
            return null;
        };
    }

    private static Answer<?> withException(Exception e) {
        return invocationOnMock -> {
            @SuppressWarnings("unchecked")
            var l = (ActionListener<PersistentTaskResponse>) invocationOnMock.getArguments()[2];
            l.onFailure(e);
            return null;
        };
    }
}
