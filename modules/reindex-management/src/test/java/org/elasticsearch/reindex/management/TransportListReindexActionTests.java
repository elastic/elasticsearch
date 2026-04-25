/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.reindex.management;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportListReindexActionTests extends ESTestCase {

    private Client client;
    private TransportListReindexAction action;
    private AtomicReference<ListReindexResponse> responseRef;
    private AtomicReference<Exception> failureRef;
    private ActionListener<ListReindexResponse> listener;

    @Before
    public void setup() {
        client = mock();
        var threadPool = mock(ThreadPool.class);
        when(client.threadPool()).thenReturn(threadPool);
        when(threadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        action = new TransportListReindexAction(mock(), mock(), client);
        responseRef = new AtomicReference<>();
        failureRef = new AtomicReference<>();
        listener = ActionListener.wrap(responseRef::set, failureRef::set);
    }

    public void testNonRelocatedTaskPassedThrough() {
        final TaskId taskId = new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong());
        final boolean cancellable = randomBoolean();
        final TaskInfo info = new TaskInfo(
            taskId,
            randomAlphanumericOfLength(10),
            taskId.getNodeId(),
            ReindexAction.NAME,
            randomAlphanumericOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            cancellable,
            cancellable && randomBoolean(),
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );

        returnGivenTasksOnActionExecution(List.of(info));
        action.doExecute(mock(), new ListReindexRequest(randomBoolean()), listener);

        assertNull(failureRef.get());
        assertThat(responseRef.get().getTasks(), equalTo(List.of(info)));
    }

    public void testRelocatedTaskRewritten() {
        final TaskId originalId = new TaskId("originalNode", randomNonNegativeLong());
        final TaskId relocatedId = new TaskId("relocatedNode", randomNonNegativeLong());
        final long originalStartMillis = randomLongBetween(0, 100);
        final long relocatedStartMillis = randomLongBetween(originalStartMillis, originalStartMillis + randomLongBetween(0, 100));
        final long relocatedRunningNanos = randomNonNegativeLong();
        final boolean cancellable = randomBoolean();

        final TaskInfo info = new TaskInfo(
            relocatedId,
            randomAlphanumericOfLength(10),
            relocatedId.getNodeId(),
            ReindexAction.NAME,
            randomAlphanumericOfLength(10),
            null,
            relocatedStartMillis,
            relocatedRunningNanos,
            cancellable,
            cancellable && randomBoolean(),
            TaskId.EMPTY_TASK_ID,
            Map.of(),
            originalId,
            originalStartMillis
        );

        returnGivenTasksOnActionExecution(List.of(info));
        action.doExecute(mock(), new ListReindexRequest(randomBoolean()), listener);

        assertNull(failureRef.get());
        assertThat(responseRef.get().getTasks(), hasSize(1));
        final TaskInfo rewritten = responseRef.get().getTasks().getFirst();
        assertThat(rewritten.taskId(), equalTo(originalId));
        assertThat(rewritten.taskId(), equalTo(rewritten.originalTaskId()));
        assertThat(rewritten.node(), equalTo(originalId.getNodeId()));
        assertThat(rewritten.startTime(), equalTo(originalStartMillis));
        assertThat(rewritten.startTime(), equalTo(rewritten.originalStartTimeMillis()));
        long expectedRunningNanos = relocatedRunningNanos + TimeUnit.MILLISECONDS.toNanos(relocatedStartMillis - originalStartMillis);
        assertThat(rewritten.runningTimeNanos(), equalTo(expectedRunningNanos));
    }

    public void testChildTasksFiltered() {
        final TaskId parentId = new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong());
        final TaskInfo parentTask = new TaskInfo(
            parentId,
            randomAlphanumericOfLength(10),
            parentId.getNodeId(),
            ReindexAction.NAME,
            randomAlphanumericOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        final TaskId childId = new TaskId(randomAlphanumericOfLength(5), randomNonNegativeLong());
        final TaskInfo childTask = new TaskInfo(
            childId,
            randomAlphanumericOfLength(10),
            childId.getNodeId(),
            ReindexAction.NAME,
            randomAlphanumericOfLength(10),
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomBoolean(),
            false,
            parentId,
            Map.of()
        );

        returnGivenTasksOnActionExecution(List.of(parentTask, childTask));
        action.doExecute(mock(), new ListReindexRequest(randomBoolean()), listener);

        assertNull(failureRef.get());
        assertThat(responseRef.get().getTasks(), equalTo(List.of(parentTask)));
    }

    public void testRequestPassesActionAndDetailed() {
        final boolean detailed = randomBoolean();
        returnGivenTasksOnActionExecution(List.of());
        action.doExecute(mock(), new ListReindexRequest(detailed), listener);

        assertNull(failureRef.get());

        final ArgumentCaptor<ListTasksRequest> captor = ArgumentCaptor.forClass(ListTasksRequest.class);
        verify(client).execute(eq(TransportListTasksAction.TYPE), captor.capture(), any());
        final ListTasksRequest forwarded = captor.getValue();
        assertThat(forwarded.getActions(), equalTo(new String[] { ReindexAction.NAME }));
        assertThat(forwarded.getDetailed(), equalTo(detailed));
    }

    private void returnGivenTasksOnActionExecution(final List<TaskInfo> tasks) {
        doAnswer(inv -> {
            final ActionListener<ListTasksResponse> l = inv.getArgument(2);
            l.onResponse(new ListTasksResponse(tasks, List.of(), List.of()));
            return null;
        }).when(client).execute(eq(TransportListTasksAction.TYPE), any(ListTasksRequest.class), any());
    }
}
