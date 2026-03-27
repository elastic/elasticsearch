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
import org.elasticsearch.action.admin.cluster.node.tasks.cancel.TransportCancelTasksAction;
import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksResponse;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.index.reindex.ReindexAction;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.test.rest.FakeRestChannel;
import org.elasticsearch.test.rest.FakeRestRequest;
import org.elasticsearch.test.rest.RestActionTestCase;
import org.junit.Assert;
import org.junit.Before;

import java.util.List;
import java.util.Map;

public class RestCancelTasksActionTests extends RestActionTestCase {

    @Before
    public void resetClient() {
        verifyingClient.reset();
    }

    /**
     * {@code POST /_tasks/.../_cancel} on a parent reindex task must emit the soft deprecation warning steering callers to
     * {@code POST /_reindex/<task_id>/_cancel}.
     */
    public void testCancelTaskActionLogsDeprecationWarningForReindexingTask() throws Exception {
        RestCancelTasksAction action = new RestCancelTasksAction(() -> DiscoveryNodes.EMPTY_NODES, f -> true);
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
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        ListTasksResponse response = new ListTasksResponse(List.of(taskInfo), List.of(), List.of());
        verifyingClient.setExecuteVerifier((a, r) -> {
            Assert.assertEquals(TransportCancelTasksAction.TYPE.name(), a.name());
            return response;
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber + "/_cancel")
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);

        assertWarnings(
            "Using the task management APIs to cancel reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
        );
    }

    /**
     * A single HTTP cancel response listing multiple parent reindexing tasks must produce at most one deprecation warning.
     */
    public void testCancelTaskActionLogsDeprecationWarningAtMostOnceWithMultipleReindexTasks() throws Exception {
        RestCancelTasksAction action = new RestCancelTasksAction(() -> DiscoveryNodes.EMPTY_NODES, f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber1 = randomLongBetween(1L, Long.MAX_VALUE);
        long taskNumber2 = randomValueOtherThan(taskNumber1, () -> randomLongBetween(1L, Long.MAX_VALUE));
        TaskInfo t1 = new TaskInfo(
            new TaskId(nodeId, taskNumber1),
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        TaskInfo t2 = new TaskInfo(
            new TaskId(nodeId, taskNumber2),
            "reindex",
            nodeId,
            ReindexAction.NAME,
            null,
            null,
            0L,
            0L,
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        ListTasksResponse response = new ListTasksResponse(List.of(t1, t2), List.of(), List.of());
        verifyingClient.setExecuteVerifier((a, r) -> response);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tasks/_cancel")
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);

        assertWarnings(
            "Using the task management APIs to cancel reindex tasks is deprecated. "
                + "Use the dedicated reindex API instead, POST /_reindex/<task_id>/_cancel."
        );
    }

    /**
     * Cancelling a non-reindex task must not emit the reindex-specific deprecation.
     */
    public void testCancelTaskActionDoesNotLogDeprecationWarningForNonReindexTask() throws Exception {
        RestCancelTasksAction action = new RestCancelTasksAction(() -> DiscoveryNodes.EMPTY_NODES, f -> true);
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
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        ListTasksResponse response = new ListTasksResponse(List.of(taskInfo), List.of(), List.of());
        verifyingClient.setExecuteVerifier((a, r) -> {
            Assert.assertEquals(TransportCancelTasksAction.TYPE.name(), a.name());
            return response;
        });

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber + "/_cancel")
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
    }

    /**
     * When {@link TransportCancelTasksAction} fails, the handler must not emit the reindex deprecation
     */
    public void testCancelTaskActionDoesNotLogDeprecationWarningWhenCancelTasksFails() throws Exception {
        RestCancelTasksAction action = new RestCancelTasksAction(() -> DiscoveryNodes.EMPTY_NODES, f -> true);
        String nodeId = randomAlphaOfLengthBetween(4, 12);
        long taskNumber = randomLongBetween(1L, Long.MAX_VALUE);

        NodeClient failingClient = new NoOpNodeClient(verifyingClient.threadPool()) {
            @Override
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> actionType,
                Request request,
                ActionListener<Response> listener
            ) {
                Assert.assertEquals(TransportCancelTasksAction.TYPE.name(), actionType.name());
                listener.onFailure(new ResourceNotFoundException("simulated cancel failure"));
            }
        };

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber + "/_cancel")
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), failingClient);
    }

    /**
     * When the feature gate does not pass, cancelling a parent reindex task via the tasks API must not log a deprecation.
     */
    public void testCancelTaskActionDoesNotLogDeprecationWarningWhenFeatureIsNotSupported() throws Exception {
        RestCancelTasksAction action = new RestCancelTasksAction(() -> DiscoveryNodes.EMPTY_NODES, f -> false);
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
            true,
            true,
            TaskId.EMPTY_TASK_ID,
            Map.of()
        );
        ListTasksResponse response = new ListTasksResponse(List.of(taskInfo), List.of(), List.of());
        verifyingClient.setExecuteVerifier((a, r) -> response);

        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withMethod(RestRequest.Method.POST)
            .withPath("/_tasks/" + nodeId + ":" + taskNumber + "/_cancel")
            .build();

        action.handleRequest(request, new FakeRestChannel(request, true), verifyingClient);
    }
}
