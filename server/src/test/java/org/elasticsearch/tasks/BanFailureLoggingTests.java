/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.cluster.node.tasks.TaskManagerTestCase;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.test.transport.StubbableTransport;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.AbstractSimpleTransportTestCase;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportResponseHandler;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.instanceOf;

public class BanFailureLoggingTests extends TaskManagerTestCase {

    @TestLogging(reason = "testing logging at DEBUG", value = "org.elasticsearch.tasks.TaskCancellationService:DEBUG")
    public void testLogsAtDebugOnDisconnectionDuringBan() throws Exception {
        runTest((connection, requestId, action, request, options) -> {
            if (action.equals(TaskCancellationService.BAN_PARENT_ACTION_NAME)) {
                connection.close();
            }
            connection.sendRequest(requestId, action, request, options);
        },
            childNode -> List.of(
                new MockLogAppender.SeenEventExpectation(
                    "cannot send ban",
                    TaskCancellationService.class.getName(),
                    Level.DEBUG,
                    "*cannot send ban for tasks*" + childNode.getId() + "*"
                ),
                new MockLogAppender.SeenEventExpectation(
                    "cannot remove ban",
                    TaskCancellationService.class.getName(),
                    Level.DEBUG,
                    "*failed to remove ban for tasks*" + childNode.getId() + "*"
                )
            )
        );
    }

    @TestLogging(reason = "testing logging at DEBUG", value = "org.elasticsearch.tasks.TaskCancellationService:DEBUG")
    public void testLogsAtDebugOnDisconnectionDuringBanRemoval() throws Exception {
        final AtomicInteger banCount = new AtomicInteger();
        runTest((connection, requestId, action, request, options) -> {
            if (action.equals(TaskCancellationService.BAN_PARENT_ACTION_NAME) && banCount.incrementAndGet() >= 2) {
                connection.close();
            }
            connection.sendRequest(requestId, action, request, options);
        },
            childNode -> List.of(
                new MockLogAppender.UnseenEventExpectation(
                    "cannot send ban",
                    TaskCancellationService.class.getName(),
                    Level.DEBUG,
                    "*cannot send ban for tasks*" + childNode.getId() + "*"
                ),
                new MockLogAppender.SeenEventExpectation(
                    "cannot remove ban",
                    TaskCancellationService.class.getName(),
                    Level.DEBUG,
                    "*failed to remove ban for tasks*" + childNode.getId() + "*"
                )
            )
        );
    }

    private void runTest(
        StubbableTransport.SendRequestBehavior sendRequestBehavior,
        Function<DiscoveryNode, List<MockLogAppender.LoggingExpectation>> expectations
    ) throws Exception {

        final ArrayList<Closeable> resources = new ArrayList<>(3);

        try {

            // the child task might not run, but if it does we must wait for it to be cancelled before shutting everything down
            final ReentrantLock childTaskLock = new ReentrantLock();

            final MockTransportService parentTransportService = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool
            );
            resources.add(parentTransportService);
            parentTransportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(parentTransportService));
            parentTransportService.start();
            parentTransportService.acceptIncomingRequests();

            final MockTransportService childTransportService = MockTransportService.createNewService(
                Settings.EMPTY,
                Version.CURRENT,
                threadPool
            );
            resources.add(childTransportService);
            childTransportService.getTaskManager().setTaskCancellationService(new TaskCancellationService(childTransportService));
            childTransportService.registerRequestHandler(
                "internal:testAction[c]",
                ThreadPool.Names.MANAGEMENT, // busy-wait for cancellation but not on a transport thread
                (StreamInput in) -> new TransportRequest.Empty(in) {
                    @Override
                    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
                        return new CancellableTask(id, type, action, "", parentTaskId, headers);
                    }
                },
                (request, channel, task) -> {
                    final CancellableTask cancellableTask = (CancellableTask) task;
                    if (childTaskLock.tryLock()) {
                        try {
                            assertBusy(() -> assertTrue("task " + task.getId() + " should be cancelled", cancellableTask.isCancelled()));
                        } finally {
                            childTaskLock.unlock();
                        }
                    }
                    channel.sendResponse(new TaskCancelledException("task cancelled"));
                }
            );

            childTransportService.start();
            childTransportService.acceptIncomingRequests();

            parentTransportService.addSendBehavior(sendRequestBehavior);

            AbstractSimpleTransportTestCase.connectToNode(parentTransportService, childTransportService.getLocalDiscoNode());

            final CancellableTask parentTask = (CancellableTask) parentTransportService.getTaskManager()
                .register("transport", "internal:testAction", new ParentRequest());

            parentTransportService.sendChildRequest(
                childTransportService.getLocalDiscoNode(),
                "internal:testAction[c]",
                TransportRequest.Empty.INSTANCE,
                parentTask,
                TransportRequestOptions.EMPTY,
                new ChildResponseHandler(() -> parentTransportService.getTaskManager().unregister(parentTask))
            );

            MockLogAppender appender = new MockLogAppender();
            appender.start();
            resources.add(appender::stop);
            Loggers.addAppender(LogManager.getLogger(TaskCancellationService.class), appender);
            resources.add(() -> Loggers.removeAppender(LogManager.getLogger(TaskCancellationService.class), appender));

            for (MockLogAppender.LoggingExpectation expectation : expectations.apply(childTransportService.getLocalDiscoNode())) {
                appender.addExpectation(expectation);
            }

            final PlainActionFuture<Void> cancellationFuture = new PlainActionFuture<>();
            parentTransportService.getTaskManager().cancelTaskAndDescendants(parentTask, "test", true, cancellationFuture);
            try {
                cancellationFuture.actionGet(TimeValue.timeValueSeconds(10));
            } catch (NodeDisconnectedException e) {
                // acceptable; we mostly ignore the result of cancellation anyway
            }

            // assert busy since failure to remove a ban may be logged after cancellation completed
            assertBusy(appender::assertAllExpectationsMatched);

            assertTrue("child tasks did not finish in time", childTaskLock.tryLock(15, TimeUnit.SECONDS));
        } finally {
            Collections.reverse(resources);
            IOUtils.close(resources);
        }
    }

    private static class ParentRequest implements TaskAwareRequest {
        @Override
        public void setParentTask(TaskId taskId) {
            fail("setParentTask should not be called");
        }

        @Override
        public TaskId getParentTask() {
            return TaskId.EMPTY_TASK_ID;
        }

        @Override
        public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
            return new CancellableTask(id, type, action, "", parentTaskId, headers);
        }
    }

    private static class ChildResponseHandler implements TransportResponseHandler<TransportResponse.Empty> {
        private final Runnable onException;

        ChildResponseHandler(Runnable onException) {
            this.onException = onException;
        }

        @Override
        public void handleResponse(TransportResponse.Empty response) {
            fail("should not get successful response");
        }

        @Override
        public void handleException(TransportException exp) {
            assertThat(exp.unwrapCause(), anyOf(instanceOf(TaskCancelledException.class), instanceOf(NodeDisconnectedException.class)));
            onException.run();
        }

        @Override
        public TransportResponse.Empty read(StreamInput in) {
            return TransportResponse.Empty.INSTANCE;
        }
    }

}
