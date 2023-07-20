/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResultDeduplicator;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.CountDownActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.RefCountingRunnable;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.NodeDisconnectedException;
import org.elasticsearch.transport.NodeNotConnectedException;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.core.Strings.format;

public class TaskCancellationService {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    public static final String CANCEL_CHILD_ACTION_NAME = "internal:admin/tasks/cancel_child";
    public static final TransportVersion VERSION_SUPPORTING_CANCEL_CHILD_ACTION = TransportVersion.V_8_8_0;
    private static final Logger logger = LogManager.getLogger(TaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;
    private final ResultDeduplicator<CancelRequest, Void> deduplicator;

    public TaskCancellationService(TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        this.deduplicator = new ResultDeduplicator<>(transportService.getThreadPool().getThreadContext());
        transportService.registerRequestHandler(
            BAN_PARENT_ACTION_NAME,
            ThreadPool.Names.SAME,
            BanParentTaskRequest::new,
            new BanParentRequestHandler()
        );
        transportService.registerRequestHandler(
            CANCEL_CHILD_ACTION_NAME,
            ThreadPool.Names.SAME,
            CancelChildRequest::new,
            new CancelChildRequestHandler()
        );
    }

    private String localNodeId() {
        return transportService.getLocalNode().getId();
    }

    private static class CancelRequest {
        final CancellableTask task;
        final boolean waitForCompletion;

        CancelRequest(CancellableTask task, boolean waitForCompletion) {
            this.task = task;
            this.waitForCompletion = waitForCompletion;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final CancelRequest that = (CancelRequest) o;
            return waitForCompletion == that.waitForCompletion && Objects.equals(task, that.task);
        }

        @Override
        public int hashCode() {
            return Objects.hash(task, waitForCompletion);
        }
    }

    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> finalListener) {
        deduplicator.executeOnce(
            new CancelRequest(task, waitForCompletion),
            finalListener,
            (r, listener) -> doCancelTaskAndDescendants(task, reason, waitForCompletion, listener)
        );
    }

    void doCancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskId taskId = task.taskInfo(localNodeId(), false).taskId();
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            ListenableFuture<Void> completedListener = new ListenableFuture<>();
            ListenableFuture<Void> setBanListener = new ListenableFuture<>();

            Collection<Transport.Connection> childConnections;
            try (var refs = new RefCountingRunnable(() -> setBanListener.addListener(completedListener))) {
                var banChildrenRef = refs.acquire();
                var cancelTaskRef = refs.acquire();

                childConnections = taskManager.startBanOnChildTasks(task.getId(), reason, () -> {
                    logger.trace("child tasks of parent [{}] are completed", taskId);
                    banChildrenRef.close();
                });

                taskManager.cancel(task, reason, () -> {
                    logger.trace("task [{}] is cancelled", taskId);
                    cancelTaskRef.close();
                });
            }
            setBanOnChildConnections(reason, waitForCompletion, task, childConnections, setBanListener);

            // We remove bans after all child tasks are completed although in theory we can do it on a per-connection basis.
            completedListener.addListener(
                ActionListener.running(
                    transportService.getThreadPool()
                        .getThreadContext()
                        // If we start unbanning when the last child task completed and that child task executed with a specific user, then
                        // unban requests are denied because internal requests can't run with a user. We need to remove bans with the
                        // current thread context.
                        .preserveContext(() -> removeBanOnChildConnections(task, childConnections))
                )
            );

            if (waitForCompletion) {
                // Wait until (1) bans are placed on child connections, (2) child tasks are completed or failed, (3) main task is cancelled.
                completedListener.addListener(listener);
            } else {
                // Only wait until bans are placed on child connections
                setBanListener.addListener(listener);
            }
        } else {
            logger.trace("task [{}] doesn't have any children that should be cancelled", taskId);
            if (waitForCompletion) {
                taskManager.cancel(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancel(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    private void setBanOnChildConnections(
        String reason,
        boolean waitForCompletion,
        CancellableTask task,
        Collection<Transport.Connection> childConnections,
        ActionListener<Void> listener
    ) {
        if (childConnections.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final TaskId taskId = new TaskId(localNodeId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child connections {}", taskId, childConnections);
        CountDownActionListener countDownListener = new CountDownActionListener(childConnections.size(), listener);
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (Transport.Connection connection : childConnections) {
            assert TransportService.unwrapConnection(connection) == connection : "Child connection must be unwrapped";
            transportService.sendRequest(
                connection,
                BAN_PARENT_ACTION_NAME,
                banRequest,
                TransportRequestOptions.EMPTY,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        logger.trace("sent ban for tasks with the parent [{}] for connection [{}]", taskId, connection);
                        countDownListener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(exp);
                        assert cause instanceof ElasticsearchSecurityException == false;
                        if (isUnimportantBanFailure(cause)) {
                            logger.debug(
                                () -> format("cannot send ban for tasks with the parent [%s] on connection [%s]", taskId, connection),
                                exp
                            );
                        } else if (logger.isDebugEnabled()) {
                            logger.warn(
                                () -> format("cannot send ban for tasks with the parent [%s] on connection [%s]", taskId, connection),
                                exp
                            );
                        } else {
                            logger.warn(
                                "cannot send ban for tasks with the parent [{}] on connection [{}]: {}",
                                taskId,
                                connection,
                                exp.getMessage()
                            );
                        }

                        countDownListener.onFailure(exp);
                    }
                }
            );
        }
    }

    private void removeBanOnChildConnections(CancellableTask task, Collection<Transport.Connection> childConnections) {
        final BanParentTaskRequest request = BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(localNodeId(), task.getId()));
        for (Transport.Connection connection : childConnections) {
            assert TransportService.unwrapConnection(connection) == connection : "Child connection must be unwrapped";
            logger.trace("Sending remove ban for tasks with the parent [{}] for connection [{}]", request.parentTaskId, connection);
            transportService.sendRequest(
                connection,
                BAN_PARENT_ACTION_NAME,
                request,
                TransportRequestOptions.EMPTY,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        final Throwable cause = ExceptionsHelper.unwrapCause(exp);
                        assert cause instanceof ElasticsearchSecurityException == false;
                        if (isUnimportantBanFailure(cause)) {
                            logger.debug(
                                () -> format(
                                    "failed to remove ban for tasks with the parent [%s] on connection [%s]",
                                    request.parentTaskId,
                                    connection
                                ),
                                exp
                            );
                        } else if (logger.isDebugEnabled()) {
                            logger.warn(
                                () -> format(
                                    "failed to remove ban for tasks with the parent [%s] on connection [%s]",
                                    request.parentTaskId,
                                    connection
                                ),
                                exp
                            );
                        } else {
                            logger.warn(
                                "failed to remove ban for tasks with the parent [{}] on connection [{}]: {}",
                                request.parentTaskId,
                                connection,
                                exp.getMessage()
                            );
                        }
                    }
                }
            );
        }
    }

    private static boolean isUnimportantBanFailure(Throwable cause) {
        return cause instanceof NodeDisconnectedException || cause instanceof NodeNotConnectedException;
    }

    private static class BanParentTaskRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final boolean ban;
        private final boolean waitForCompletion;
        private final String reason;

        static BanParentTaskRequest createSetBanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            return new BanParentTaskRequest(parentTaskId, reason, waitForCompletion);
        }

        static BanParentTaskRequest createRemoveBanParentTaskRequest(TaskId parentTaskId) {
            return new BanParentTaskRequest(parentTaskId);
        }

        private BanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
            this.waitForCompletion = waitForCompletion;
        }

        private BanParentTaskRequest(TaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
            this.waitForCompletion = false;
        }

        private BanParentTaskRequest(StreamInput in) throws IOException {
            super(in);
            parentTaskId = TaskId.readFromStream(in);
            ban = in.readBoolean();
            reason = ban ? in.readString() : null;
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_8_0)) {
                waitForCompletion = in.readBoolean();
            } else {
                waitForCompletion = false;
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
            out.writeBoolean(ban);
            if (ban) {
                out.writeString(reason);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_8_0)) {
                out.writeBoolean(waitForCompletion);
            }
        }
    }

    private class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug(
                    "Received ban for the parent [{}] on the node [{}], reason: [{}]",
                    request.parentTaskId,
                    localNodeId(),
                    request.reason
                );
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason, channel);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    childTasks.size() + 1,
                    new ChannelActionListener<>(channel).map(r -> TransportResponse.Empty.INSTANCE)
                );
                for (CancellableTask childTask : childTasks) {
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                listener.onResponse(null);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId, localNodeId());
                taskManager.removeBan(request.parentTaskId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

    private static class CancelChildRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final long childRequestId;
        private final String reason;

        static CancelChildRequest createCancelChildRequest(TaskId parentTaskId, long childRequestId, String reason) {
            return new CancelChildRequest(parentTaskId, childRequestId, reason);
        }

        private CancelChildRequest(TaskId parentTaskId, long childRequestId, String reason) {
            this.parentTaskId = parentTaskId;
            this.childRequestId = childRequestId;
            this.reason = reason;
        }

        private CancelChildRequest(StreamInput in) throws IOException {
            super(in);
            parentTaskId = TaskId.readFromStream(in);
            childRequestId = in.readLong();
            reason = in.readString();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
            out.writeLong(childRequestId);
            out.writeString(reason);
        }
    }

    private class CancelChildRequestHandler implements TransportRequestHandler<CancelChildRequest> {
        @Override
        public void messageReceived(final CancelChildRequest request, final TransportChannel channel, Task task) throws Exception {
            taskManager.cancelChildLocal(request.parentTaskId, request.childRequestId, request.reason);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    /**
     * Sends an action to cancel a child task, associated with the given request ID and parent task.
     */
    public void cancelChildRemote(TaskId parentTask, long childRequestId, Transport.Connection childConnection, String reason) {
        if (childConnection.getTransportVersion().onOrAfter(VERSION_SUPPORTING_CANCEL_CHILD_ACTION)) {
            DiscoveryNode childNode = childConnection.getNode();
            logger.debug(
                "sending cancellation of child of parent task [{}] with request ID [{}] to node [{}] because of [{}]",
                parentTask,
                childRequestId,
                childNode,
                reason
            );
            final CancelChildRequest request = CancelChildRequest.createCancelChildRequest(parentTask, childRequestId, reason);
            transportService.sendRequest(
                childNode,
                CANCEL_CHILD_ACTION_NAME,
                request,
                TransportRequestOptions.EMPTY,
                EmptyTransportResponseHandler.INSTANCE_SAME
            );
        }
    }

}
