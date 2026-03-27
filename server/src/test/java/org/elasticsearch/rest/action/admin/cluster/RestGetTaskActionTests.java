/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.rest.action.admin.cluster;

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.admin.cluster.node.tasks.get.GetTaskResponse;
import org.elasticsearch.action.admin.cluster.node.tasks.get.TransportGetTaskAction;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.tasks.TaskResult;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.util.Map;

public class RestGetTaskActionTests extends RestActionTestCase {

    @Before
    public void resetClient() {
        verifyingClient.reset();
    }

    /**
     * {@code GET /_tasks} for a parent reindex task must emit the soft deprecation warning steering callers to
     * {@code GET /_reindex/<task_id>}.
     */
    public void testGetTaskActionLogsDeprecationWarningForReindexingTask() throws Exception {
        RestGetTaskAction action = new RestGetTaskAction(f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber = randomLongBetween(1L, Long.MAX_VALUE);
        TaskId taskId = new TaskId(nodeId, taskNumber);
        TaskInfo taskInfo = new TaskInfo(
            taskId,
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        GetTaskResponse response = new GetTaskResponse(new TaskResult(false, taskInfo));
        verifyingClient.setExecuteVerifier((a, r) -> {
            Assert.assertEquals(TransportGetTaskAction.TYPE.name(), a.name());
            return response;
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);

        assertWarnings(
            "Using the task management APIs to get reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, GET /_reindex/<task_id>."
        );
    }

    /**
     * {@code GET /_tasks} for a reindex slice subtask must not emit a deprecation warning. This prevents a single reindexing call that
     * uses slices to receive many deprecation warnings. One is enough.
     */
    public void testGetTaskActionDoesNotLogDeprecationWarningForReindexingSlicedSubtask() throws Exception {
        RestGetTaskAction action = new RestGetTaskAction(f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long parentTaskNumber = randomLongBetween(1L, Long.MAX_VALUE);
        long childTaskNumber = randomValueOtherThan(parentTaskNumber, () -> randomLongBetween(1L, Long.MAX_VALUE));
        TaskId taskId = new TaskId(nodeId, childTaskNumber);
        TaskId parentId = new TaskId(nodeId, parentTaskNumber);
        TaskInfo taskInfo = new TaskInfo(
            taskId,
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            parentId,
            Map.of()
        );
        GetTaskResponse response = new GetTaskResponse(new TaskResult(false, taskInfo));
        verifyingClient.setExecuteVerifier((a, r) -> response);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tasks/" + nodeId + ":" + childTaskNumber)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
    }

    /**
     * {@code GET /_tasks} for a non-reindex task must not emit the reindex-specific deprecation.
     */
    public void testGetTaskActionDoesNotLogDeprecationWarningForNonReindexTask() throws Exception {
        RestGetTaskAction action = new RestGetTaskAction(f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber = randomLongBetween(1L, Long.MAX_VALUE);
        TaskId taskId = new TaskId(nodeId, taskNumber);
        TaskInfo taskInfo = new TaskInfo(
            taskId,
            "bulk",
            nodeId,
            TransportBulkAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        GetTaskResponse response = new GetTaskResponse(new TaskResult(false, taskInfo));
        verifyingClient.setExecuteVerifier((a, r) -> {
            Assert.assertEquals(TransportGetTaskAction.TYPE.name(), a.name());
            return response;
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
    }

    /**
     * When {@link TransportGetTaskAction} fails, the handler must not emit any deprecation logging
     */
    public void testGetTaskActionDoesNotLogDeprecationWarningWhenGetTaskFails() throws Exception {
        RestGetTaskAction action = new RestGetTaskAction(f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber = randomLongBetween(1L, Long.MAX_VALUE);
        TaskId taskId = new TaskId(nodeId, taskNumber);

        NodeClient failingClient = new NoOpNodeClient(verifyingClient.threadPool()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request request,
                ActionListener<Response> listener
            ) {
                Assert.assertEquals(TransportGetTaskAction.TYPE.name(), actionType.name());
                listener.onFailure(
                    new ResourceNotFoundException("task [{}] isn't running and hasn't stored its results", taskId.toString())
                );
            }
        };

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), failingClient);
    }

    /**
     * When the feature gate does not pass, retrieving a parent reindex task via the tasks API must not log a deprecation message.
     */
    public void testGetTaskActionDoesNotLogDeprecationWarningWhenFeatureIsNotSupported() throws Exception {
        RestGetTaskAction action = new RestGetTaskAction(f -> false);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber = randomLongBetween(1L, Long.MAX_VALUE);
        TaskId taskId = new TaskId(nodeId, taskNumber);
        TaskInfo taskInfo = new TaskInfo(
            taskId,
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            false,
            false,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        GetTaskResponse response = new GetTaskResponse(new TaskResult(false, taskInfo));
        verifyingClient.setExecuteVerifier((a, r) -> response);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.GET)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber)
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
    }
}
