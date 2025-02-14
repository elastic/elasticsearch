/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support.tasks;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.action.admin.cluster.node.tasks.list.TransportListTasksAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Cancellable;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.http.HttpSmokeTestCase;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.transport.TransportService;

import java.util.ArrayList;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.support.ActionTestUtils.wrapAsRestResponseListener;
import static org.elasticsearch.test.TaskAssertions.awaitTaskWithPrefix;

public class RestListTasksCancellationIT extends HttpSmokeTestCase {

    public void testListTasksCancellation() throws Exception {
        final Request clusterStateRequest = new Request(HttpGet.METHOD_NAME, "/_cluster/state");
        clusterStateRequest.addParameter("wait_for_metadata_version", Long.toString(Long.MAX_VALUE));
        clusterStateRequest.addParameter("wait_for_timeout", "1h");

        final PlainActionFuture<Response> clusterStateFuture = new PlainActionFuture<>();
        final Cancellable clusterStateCancellable = getRestClient().performRequestAsync(
            clusterStateRequest,
            wrapAsRestResponseListener(clusterStateFuture)
        );

        awaitTaskWithPrefix(ClusterStateAction.NAME);

        final Request tasksRequest = new Request(HttpGet.METHOD_NAME, "/_tasks");
        tasksRequest.addParameter("actions", ClusterStateAction.NAME);
        tasksRequest.addParameter("wait_for_completion", Boolean.toString(true));
        tasksRequest.addParameter("timeout", "1h");

        final PlainActionFuture<Response> tasksFuture = new PlainActionFuture<>();
        final Cancellable tasksCancellable = getRestClient().performRequestAsync(tasksRequest, wrapAsRestResponseListener(tasksFuture));

        awaitTaskWithPrefix(TransportListTasksAction.TYPE.name() + "[n]");

        tasksCancellable.cancel();

        final var taskManagers = new ArrayList<TaskManager>(internalCluster().getNodeNames().length);
        for (final var transportService : internalCluster().getInstances(TransportService.class)) {
            taskManagers.add(transportService.getTaskManager());
        }
        assertBusy(
            () -> assertFalse(
                taskManagers.stream()
                    .flatMap(taskManager -> taskManager.getCancellableTasks().values().stream())
                    .anyMatch(t -> t.getAction().startsWith(TransportListTasksAction.TYPE.name()))
            )
        );

        expectThrows(CancellationException.class, () -> tasksFuture.actionGet(10, TimeUnit.SECONDS));
        clusterStateCancellable.cancel();
    }

}
