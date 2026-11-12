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
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class TransportServiceResponseReservationTests extends ESTestCase {

    /**
     * {@link TransportService} wraps every response handler, and a child request's once more. Each wrapper must forward
     * {@link TransportResponseHandler#reserveResponseBytes}, or a handler that reserves silently stops.
     */
    public void testWrappedHandlersReserveResponseBytes() {
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
        transportService.start();
        transportService.acceptIncomingRequests();
        final PlainActionFuture<Releasable> connectionFuture = new PlainActionFuture<>();
        transportService.connectToNode(otherNode, connectionFuture);
        assertTrue(connectionFuture.isDone());

        final AtomicLong reserved = new AtomicLong();
        transportService.sendRequest(otherNode, testActionName, new EmptyRequest(), TransportRequestOptions.EMPTY, reserving(reserved));
        assertRegisteredHandlerReserves(transport, reserved);

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
            reserving(reserved)
        );
        assertRegisteredHandlerReserves(transport, reserved);
    }

    private static void assertRegisteredHandlerReserves(MockTransport transport, AtomicLong reserved) {
        final List<Transport.ResponseContext<? extends TransportResponse>> responseContexts = transport.getResponseHandlers()
            .prune(ignored -> true);
        assertThat(responseContexts, hasSize(1));
        final int bytes = between(1, 1000);
        final Releasable reservation = responseContexts.get(0).handler().reserveResponseBytes(bytes);
        assertThat(reserved.get(), equalTo((long) bytes));
        reservation.close();
        assertThat(reserved.get(), equalTo(0L));
    }

    private static TransportResponseHandler<ActionResponse.Empty> reserving(AtomicLong reserved) {
        return new TransportResponseHandler<>() {
            @Override
            public Executor executor() {
                return Runnable::run;
            }

            @Override
            public Releasable reserveResponseBytes(int bytes) {
                reserved.addAndGet(bytes);
                return () -> reserved.addAndGet(-bytes);
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
        };
    }
}
