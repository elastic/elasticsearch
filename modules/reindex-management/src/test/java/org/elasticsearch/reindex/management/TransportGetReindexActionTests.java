/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.client.internal.AdminClient;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.ClusterAdminClient;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.index.reindex.TaskRelocatedException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.reindex.management.TransportGetReindexAction.notFoundException;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportGetReindexActionTests extends ESTestCase {

    private TaskId taskId;
    private TimeValue timeout;
    private TransportGetReindexAction action;
    private ClusterAdminClient client;
    private AtomicReference<GetReindexResponse> responseRef;
    private AtomicReference<Exception> failureRef;
    private ActionListener<GetReindexResponse> listener;

    @Before
    public void setup() {
        timeout = TimeValue.timeValueSeconds(randomIntBetween(1, 600));
        taskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        client = mock();
        action = new TransportGetReindexAction(mock(), mock(), setupMockClient(client));
        responseRef = new AtomicReference<>();
        failureRef = new AtomicReference<>();
        listener = ActionListener.wrap(responseRef::set, failureRef::set);
    }

    public void testReindexFound() {
        TaskResult probeResult = new TaskResult(false, createReindexTaskInfo());
        TaskResult fetchResult = new TaskResult(true, createReindexTaskInfo());
        GetReindexRequest request = createGetReindexRequest(false);

        mockGetTaskSequence(probeResult, fetchResult);
        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertEquals(new GetReindexResponse(fetchResult), responseRef.get());
        assertProbeAndFetch(false);
    }

    public void testReindexFoundWithWait() {
        TaskResult probeResult = new TaskResult(false, createReindexTaskInfo());
        TaskResult fetchResult = new TaskResult(true, createReindexTaskInfo());
        GetReindexRequest request = createGetReindexRequest(true);

        mockGetTaskSequence(probeResult, fetchResult);
        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertEquals(new GetReindexResponse(fetchResult), responseRef.get());
        assertProbeAndFetch(true);
    }

    public void testWaitForAlreadyCompletedTask() {
        TaskResult completedResult = new TaskResult(true, createReindexTaskInfo());
        GetReindexRequest request = createGetReindexRequest(true);

        mockGetTaskSequence(completedResult, completedResult);
        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertEquals(new GetReindexResponse(completedResult), responseRef.get());
        assertProbeAndFetch(true);
    }

    public void testTaskNotFound() {
        GetReindexRequest request = createGetReindexRequest(false);

        mockGetTaskFailure(new ResourceNotFoundException("task not found"));
        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
    }

    public void testNonReindexTaskTreatedAsNotFound() {
        TaskResult probeResult = new TaskResult(true, createTaskInfo(taskId, "other_action"));
        GetReindexRequest request = createGetReindexRequest(randomBoolean());

        mockGetTask(probeResult);
        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertEquals(notFoundException(taskId).getMessage(), failureRef.get().getMessage());
        assertSingleProbe();
    }

    public void testSubtaskTreatedAsNotFound() {
        TaskId parentTaskId = new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000));
        TaskInfo subtaskInfo = new TaskInfo(
            taskId,
            "test",
            taskId.getNodeId(),
            ReindexAction.NAME,
            "test",
            null,
            0,
            0,
            false,
            false,
            parentTaskId,
            Collections.emptyMap()
        );
        GetReindexRequest request = createGetReindexRequest(randomBoolean());

        mockGetTask(new TaskResult(false, subtaskInfo));
        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertThat(failureRef.get(), instanceOf(ResourceNotFoundException.class));
        assertSingleProbe();
    }

    public void testOtherExceptionPropagated() {
        GetReindexRequest request = createGetReindexRequest(false);

        RuntimeException otherException = new RuntimeException("some other error");
        mockGetTaskFailure(otherException);
        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertSame(otherException, failureRef.get());
    }

    public void testExceptionInFetchPropagated() {
        TaskResult probeResult = new TaskResult(false, createReindexTaskInfo());
        GetReindexRequest request = createGetReindexRequest(true);

        RuntimeException fetchException = new RuntimeException("random error");
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onResponse(new GetTaskResponse(probeResult));
            return null;
        }).doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onFailure(fetchException);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());

        action.doExecute(mock(), request, listener);

        assertNull(responseRef.get());
        assertSame(fetchException, failureRef.get());
    }

    public void testMergedRelocatedResultAccepted() {
        TaskId relocatedTaskId = randomValueOtherThan(taskId, () -> new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000)));
        TaskInfo mergedInfo = new TaskInfo(
            relocatedTaskId,
            "test",
            relocatedTaskId.getNodeId(),
            ReindexAction.NAME,
            "test",
            null,
            100,
            5000,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap(),
            taskId,
            100
        );
        TaskResult probeResult = new TaskResult(false, createReindexTaskInfo());
        TaskResult mergedResult = new TaskResult(false, mergedInfo);
        GetReindexRequest request = createGetReindexRequest(false);

        mockGetTaskSequence(probeResult, mergedResult);
        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertEquals(mergedResult, responseRef.get().getTaskResult());
        assertThat(responseRef.get().getTaskResult().getTask().originalTaskId(), equalTo(taskId));
    }

    public void testUnresolvedRelocationReturnedAsIs() throws IOException {
        TaskId relocatedTaskId = randomValueOtherThan(taskId, () -> new TaskId(randomAlphaOfLength(10), randomIntBetween(1, 1000)));
        TaskResult probeResult = new TaskResult(false, createReindexTaskInfo());
        TaskResult relocationResult = new TaskResult(
            createReindexTaskInfo(),
            (Exception) new TaskRelocatedException(taskId, relocatedTaskId)
        );
        GetReindexRequest request = createGetReindexRequest(false);

        mockGetTaskSequence(probeResult, relocationResult);
        action.doExecute(mock(), request, listener);

        assertNull(failureRef.get());
        assertEquals(relocationResult, responseRef.get().getTaskResult());
    }

    // --- helpers ---

    private TaskInfo createReindexTaskInfo() {
        return createTaskInfo(taskId, ReindexAction.NAME);
    }

    private void mockGetTask(TaskResult result) {
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onResponse(new GetTaskResponse(result));
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());
    }

    private void mockGetTaskFailure(Exception e) {
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onFailure(e);
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());
    }

    private void mockGetTaskSequence(TaskResult first, TaskResult second) {
        doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onResponse(new GetTaskResponse(first));
            return null;
        }).doAnswer(inv -> {
            ActionListener<GetTaskResponse> l = inv.getArgument(1);
            l.onResponse(new GetTaskResponse(second));
            return null;
        }).when(client).getTask(any(GetTaskRequest.class), any());
    }

    /** Asserts only the probe was issued (validation rejected before the fetch). */
    private void assertSingleProbe() {
        ArgumentCaptor<GetTaskRequest> captor = ArgumentCaptor.captor();
        verify(client).getTask(captor.capture(), any());
        GetTaskRequest probe = captor.getValue();
        assertFalse(probe.getWaitForCompletion());
        assertFalse(probe.getFollowRelocations());
    }

    /** Asserts probe + fetch were issued with the expected wait_for_completion. */
    private void assertProbeAndFetch(boolean expectWait) {
        ArgumentCaptor<GetTaskRequest> captor = ArgumentCaptor.captor();
        verify(client, times(2)).getTask(captor.capture(), any());
        List<GetTaskRequest> requests = captor.getAllValues();

        GetTaskRequest probe = requests.get(0);
        assertFalse(probe.getWaitForCompletion());
        assertFalse(probe.getFollowRelocations());

        GetTaskRequest fetch = requests.get(1);
        assertEquals(expectWait, fetch.getWaitForCompletion());
        assertTrue(fetch.getFollowRelocations());
        if (expectWait) {
            assertEquals(timeout, fetch.getTimeout());
        }
    }

    private TaskInfo createTaskInfo(TaskId taskId, String action) {
        return new TaskInfo(
            taskId,
            "test",
            taskId.getNodeId(),
            action,
            "test",
            null,
            0,
            0,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Collections.emptyMap()
        );
    }

    private GetReindexRequest createGetReindexRequest(boolean waitForCompletion) {
        return new GetReindexRequest(taskId, waitForCompletion, timeout);
    }

    private Client setupMockClient(ClusterAdminClient clusterAdminClient) {
        Client client = mock();
        AdminClient adminClient = mock();
        when(client.admin()).thenReturn(adminClient);
        when(adminClient.cluster()).thenReturn(clusterAdminClient);
        return client;
    }
}
