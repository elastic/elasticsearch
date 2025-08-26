/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.transport;

import org.elasticsearch.Build;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.transport.MockTransport;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.hasToString;

public class TransportServiceDeserializationFailureTests extends ESTestCase {

    public void testDeserializationFailureLogIdentifiesListener() {
        final DiscoveryNode localNode = DiscoveryNodeUtils.create("local");
        final DiscoveryNode otherNode = DiscoveryNodeUtils.create("other");

        final DeterministicTaskQueue deterministicTaskQueue = new DeterministicTaskQueue();

        final String testActionName = "internal:test-action";

        final MockTransport transport = new MockTransport() {
            @Override
            protected void onSendRequest(long requestId, String action, TransportRequest request, DiscoveryNode node) {
                if (action.equals(TransportService.HANDSHAKE_ACTION_NAME)) {
                    handleResponse(
                        requestId,
                        new TransportService.HandshakeResponse(Version.CURRENT, Build.current().hash(), otherNode, new ClusterName(""))
                    );
                }
            }
        };
        final TransportService transportService = transport.createTransportService(
            Settings.EMPTY,
            deterministicTaskQueue.getThreadPool(),
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            ignored -> localNode,
            null,
            Collections.emptySet()
        );

        transportService.registerRequestHandler(
            testActionName,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            EmptyRequest::new,
            (request, channel, task) -> channel.sendResponse(ActionResponse.Empty.INSTANCE)
        );

        transportService.start();
        transportService.acceptIncomingRequests();

        final PlainActionFuture<Releasable> connectionFuture = new PlainActionFuture<>();
        transportService.connectToNode(otherNode, connectionFuture);
        assertTrue(connectionFuture.isDone());

        {
            // requests without a parent task are recorded directly in the response context

            transportService.sendRequest(
                otherNode,
                testActionName,
                new EmptyRequest(),
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler<ActionResponse.Empty>() {
                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse(ActionResponse.Empty response) {
                        fail("should not be called");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        fail("should not be called");
                    }

                    @Override
                    public ActionResponse.Empty read(StreamInput in) {
                        throw new AssertionError("should not be called");
                    }

                    @Override
                    public String toString() {
                        return "test handler without parent";
                    }
                }
            );

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts = transport.getResponseHandlers()
                .prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(containsString("test handler without parent")));
        }

        {
            // requests with a parent task get wrapped up by the transport service, including the action name

            final Task parentTask = transportService.getTaskManager().register("test", "test-action", new TaskAwareRequest() {
                @Override
                public void setParentTask(TaskId taskId) {
                    fail("should not be called");
                }

                @Override
                public void setRequestId(long requestId) {
                    fail("should not be called");
                }

                @Override
                public TaskId getParentTask() {
                    return TaskId.EMPTY_TASK_ID;
                }

                @Override
                public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                    return new CancellableTask(id, type, action, "", parentTaskId, headers);
                }
            });

            transportService.sendChildRequest(
                otherNode,
                testActionName,
                new EmptyRequest(),
                parentTask,
                TransportRequestOptions.EMPTY,
                new TransportResponseHandler<ActionResponse.Empty>() {
                    @Override
                    public Executor executor() {
                        return TransportResponseHandler.TRANSPORT_WORKER;
                    }

                    @Override
                    public void handleResponse(ActionResponse.Empty response) {
                        fail("should not be called");
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        fail("should not be called");
                    }

                    @Override
                    public ActionResponse.Empty read(StreamInput in) {
                        throw new AssertionError("should not be called");
                    }

                    @Override
                    public String toString() {
                        return "test handler with parent";
                    }
                }
            );

            final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts = transport.getResponseHandlers()
                .prune(ignored -> true);
            assertThat(responseContexts, hasSize(1));
            final TransportResponseHandler<? extends TransportResponse> handler = responseContexts.get(0).handler();
            assertThat(handler, hasToString(allOf(containsString("test handler with parent"), containsString(testActionName))));
        }
    }

}
