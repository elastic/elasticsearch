/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.internal;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.RemoteClusterActionType;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.transport.RemoteClusterService;
import org.elasticsearch.transport.TransportResponse;

import java.util.concurrent.Executor;

public class ParentTaskAssigningClientTests extends ESTestCase {
    public void testSetsParentId() {
        TaskId[] parentTaskId = new TaskId[] { new TaskId(randomAlphaOfLength(3), randomLong()) };

        try (var threadPool = createThreadPool()) {
            // This mock will do nothing but verify that parentTaskId is set on all requests sent to it.
            final var mock = new NoOpClient(threadPool) {
                @Override
                protected <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                    ActionType<Response> action,
                    Request request,
                    ActionListener<Response> listener
                ) {
                    assertEquals(parentTaskId[0], request.getParentTask());
                }
            };

            final var client = new ParentTaskAssigningClient(mock, parentTaskId[0]);
            assertEquals(parentTaskId[0], client.getParentTask());

            // All of these should have the parentTaskId set
            client.bulk(new BulkRequest());
            client.search(new SearchRequest());
            client.clearScroll(new ClearScrollRequest());

            // Now lets verify that unwrapped calls don't have the parentTaskId set
            parentTaskId[0] = TaskId.EMPTY_TASK_ID;
            client.unwrap().bulk(new BulkRequest());
            client.unwrap().search(new SearchRequest());
            client.unwrap().clearScroll(new ClearScrollRequest());
        }
    }

    public void testRemoteClientIsAlsoAParentAssigningClient() {
        TaskId parentTaskId = new TaskId(randomAlphaOfLength(3), randomLong());

        try (var threadPool = createThreadPool()) {
            final var mockClient = new NoOpClient(threadPool) {
                @Override
                public RemoteClusterClient getRemoteClusterClient(
                    String clusterAlias,
                    Executor responseExecutor,
                    RemoteClusterService.DisconnectedStrategy disconnectedStrategy
                ) {
                    return new RemoteClusterClient() {
                        @Override
                        public <Request extends ActionRequest, Response extends TransportResponse> void execute(
                            RemoteClusterActionType<Response> action,
                            Request request,
                            ActionListener<Response> listener
                        ) {
                            assertSame(parentTaskId, request.getParentTask());
                            listener.onFailure(new UnsupportedOperationException("fake remote-cluster client"));
                        }
                    };
                }
            };

            final var client = new ParentTaskAssigningClient(mockClient, parentTaskId);
            final var remoteClusterClient = client.getRemoteClusterClient(
                "remote-cluster",
                EsExecutors.DIRECT_EXECUTOR_SERVICE,
                randomFrom(RemoteClusterService.DisconnectedStrategy.values())
            );
            assertEquals(
                "fake remote-cluster client",
                asInstanceOf(
                    UnsupportedOperationException.class,
                    safeAwaitFailure(
                        ClusterStateResponse.class,
                        listener -> remoteClusterClient.execute(ClusterStateAction.REMOTE_TYPE, new ClusterStateRequest(), listener)
                    )
                ).getMessage()
            );
        }
    }
}
