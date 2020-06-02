/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractAsyncTask;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.NodeAndClusterAlias;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.BiConsumer;

public class TaskCancellationService implements Closeable {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    public static final String HEARTBEAT_ACTION_NAME = "internal:admin/tasks/heartbeat";
    private static final Logger logger = LogManager.getLogger(TaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;
    private final SendHeartbeatTask sendHeartbeatTask;

    public TaskCancellationService(TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
        TransportActionProxy.registerProxyAction(transportService, BAN_PARENT_ACTION_NAME, in -> TransportResponse.Empty.INSTANCE);
        transportService.registerRequestHandler(HEARTBEAT_ACTION_NAME, ThreadPool.Names.SAME, HeartbeatRequest::new,
            new HeartbeatRequestHandler());
        TransportActionProxy.registerProxyAction(transportService, HEARTBEAT_ACTION_NAME, in -> TransportResponse.Empty.INSTANCE);
        this.sendHeartbeatTask = new SendHeartbeatTask(logger, transportService.getThreadPool(),
            this::sendHeartbeatRequest, TimeValue.timeValueSeconds(10));
    }

    @Override
    public void close() throws IOException {
        sendHeartbeatTask.close();
    }

    private String localNodeId() {
        return transportService.getLocalNode().getId();
    }

    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskId taskId = task.taskInfo(localNodeId(), false).getTaskId();
        if (task.shouldCancelChildrenOnCancellation()) {
            logger.trace("cancelling task [{}] and its descendants", taskId);
            StepListener<Void> completedListener = new StepListener<>();
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.map(completedListener, r -> null), 3);
            Collection<NodeAndClusterAlias> childrenNodes = taskManager.startBanOnChildrenNodes(task.getId(), () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            taskManager.cancel(task, reason, () -> {
                logger.trace("task [{}] is cancelled", taskId);
                groupedListener.onResponse(null);
            });
            sendHeartbeatTask.register(taskId, childrenNodes);
            final Releasable unregisterTaskHeartbeats = Releasables.releaseOnce(() -> sendHeartbeatTask.unregister(taskId, childrenNodes));
            try {
                StepListener<Void> banOnNodesListener = new StepListener<>();
                setBanOnNodes(reason, waitForCompletion, task, childrenNodes, banOnNodesListener);
                banOnNodesListener.whenComplete(groupedListener::onResponse, groupedListener::onFailure);
                // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
                // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread
                // context.
                final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                    .preserveContext(() -> Releasables.close(unregisterTaskHeartbeats, () -> removeBanOnNodes(task, childrenNodes)));
                // We remove bans after all child tasks are completed although in theory we can do it on a per-node basis.
                completedListener.whenComplete(r -> removeBansRunnable.run(), e -> removeBansRunnable.run());
                // if wait_for_completion is true, then only return when (1) bans are placed on child nodes, (2) child tasks are
                // completed or failed, (3) the main task is cancelled. Otherwise, return after bans are placed on child nodes.
                if (waitForCompletion) {
                    completedListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
                } else {
                    banOnNodesListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
                }
            } catch (Exception e) {
                unregisterTaskHeartbeats.close();
                throw e;
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

    private Transport.Connection getConnection(NodeAndClusterAlias nodeAndClusterAlias) {
        if (nodeAndClusterAlias.getClusterAlias() == null) {
            return transportService.getConnection(nodeAndClusterAlias.getNode());
        } else {
            return transportService.getRemoteClusterService().getConnection(
                nodeAndClusterAlias.getNode(), nodeAndClusterAlias.getClusterAlias());
        }
    }

    private void sendBanRequest(NodeAndClusterAlias nodeAndClusterAlias,
                                BanParentTaskRequest banRequest, EmptyTransportResponseHandler handler) {
        final Transport.Connection connection;
        try {
            connection = getConnection(nodeAndClusterAlias);
        } catch (TransportException e) {
            handler.handleException(e);
            return;
        }
        // We did not register the ban action with the proxy before
        if (connection.proxyNode() != null && connection.proxyNode().getVersion().before(Version.V_8_0_0)) {
            handler.handleResponse(TransportResponse.Empty.INSTANCE);
        } else {
            transportService.sendRequest(connection, BAN_PARENT_ACTION_NAME, banRequest, TransportRequestOptions.EMPTY, handler);
        }
    }

    private void setBanOnNodes(String reason, boolean waitForCompletion, CancellableTask task,
                               Collection<NodeAndClusterAlias> childNodes, ActionListener<Void> listener) {
        if (childNodes.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final TaskId taskId = new TaskId(localNodeId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child nodes {}", taskId, childNodes);
        GroupedActionListener<Void> groupedListener =
            new GroupedActionListener<>(ActionListener.map(listener, r -> null), childNodes.size());
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (NodeAndClusterAlias node : childNodes) {
            sendBanRequest(node, banRequest, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    logger.trace("sent ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                    groupedListener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                    logger.warn("Cannot send ban for tasks with the parent [{}] to the node [{}]", taskId, node);
                    groupedListener.onFailure(exp);
                }
            });
        }
    }

    private void removeBanOnNodes(CancellableTask task, Collection<NodeAndClusterAlias> childNodes) {
        final BanParentTaskRequest request =
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(localNodeId(), task.getId()));
        for (NodeAndClusterAlias node : childNodes) {
            logger.trace("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
            sendBanRequest(node, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleException(TransportException exp) {
                    assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                    logger.info("failed to remove the parent ban for task {} on node {}", request.parentTaskId, node);
                }
            });
        }
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
            if (in.getVersion().onOrAfter(Version.V_7_8_0)) {
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
            if (out.getVersion().onOrAfter(Version.V_7_8_0)) {
                out.writeBoolean(waitForCompletion);
            }
        }
    }

    private class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug("Received ban for the parent [{}] on the node [{}], reason: [{}]", request.parentTaskId,
                    localNodeId(), request.reason);
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.map(
                    new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request), r -> TransportResponse.Empty.INSTANCE),
                    childTasks.size() + 1);
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

    void sendHeartbeatRequest(NodeAndClusterAlias node, HeartbeatRequest request) {
        final Transport.Connection connection;
        try {
            connection = getConnection(node);
        } catch (TransportException ignored) {
            return;
        }
        if (connection.getVersion().onOrAfter(Version.V_8_0_0) && connection.getNode().getVersion().onOrAfter(Version.V_8_0_0)) {
            transportService.sendRequest(connection, HEARTBEAT_ACTION_NAME, request, TransportRequestOptions.EMPTY,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                        logger.debug("failed to send a task heartbeat to node {}", node);
                    }
                });
        }
    }

    static final class HeartbeatRequest extends TransportRequest {
        final Collection<TaskId> taskIds;

        HeartbeatRequest(Set<TaskId> taskIds) {
            this.taskIds = Objects.requireNonNull(taskIds);
        }

        HeartbeatRequest(StreamInput in) throws IOException {
            super(in);
            this.taskIds = in.readList(TaskId::readFromStream);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeCollection(taskIds);
        }
    }

    private class HeartbeatRequestHandler implements TransportRequestHandler<HeartbeatRequest> {
        @Override
        public void messageReceived(HeartbeatRequest request, TransportChannel channel, Task task) throws Exception {
            for (TaskId taskId : request.taskIds) {
                taskManager.updateBanMarkerTimestamp(taskId);
            }
        }
    }

    static final class SendHeartbeatTask extends AbstractAsyncTask {
        private final Map<NodeAndClusterAlias, List<TaskId>> targets = new HashMap<>();
        private final BiConsumer<NodeAndClusterAlias, HeartbeatRequest> sendFunction;
        private final ThreadPool threadPool;

        SendHeartbeatTask(Logger logger, ThreadPool threadPool,
                          BiConsumer<NodeAndClusterAlias, HeartbeatRequest> sendFunction, TimeValue interval) {
            super(logger, threadPool, interval, true);
            this.sendFunction = sendFunction;
            this.threadPool = threadPool;
        }

        @Override
        protected synchronized boolean mustReschedule() {
            return targets.isEmpty() == false;
        }

        synchronized void register(TaskId taskId, Collection<NodeAndClusterAlias> nodes) {
            for (NodeAndClusterAlias node : nodes) {
                targets.computeIfAbsent(node, k -> new ArrayList<>()).add(taskId);
            }
            if (isScheduled() == false) {
                rescheduleIfNecessary();
            }
        }

        synchronized void unregister(TaskId taskId, Collection<NodeAndClusterAlias> nodes) {
            for (NodeAndClusterAlias node : nodes) {
                final List<TaskId> taskIds = targets.get(node);
                assert taskIds != null;
                final boolean removed = taskIds.remove(taskId);
                assert removed;
                if (taskIds.isEmpty()) {
                    targets.remove(node);
                }
            }
            if (targets.isEmpty()) {
                cancel();
            }
        }

        @Override
        protected void runInternal() {
            final Map<NodeAndClusterAlias, HeartbeatRequest> toSend = new HashMap<>();
            synchronized (this) {
                for (Map.Entry<NodeAndClusterAlias, List<TaskId>> e : targets.entrySet()) {
                    final Set<TaskId> taskIds = new HashSet<>(e.getValue());
                    toSend.put(e.getKey(), new HeartbeatRequest(taskIds));
                }
            }
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                for (Map.Entry<NodeAndClusterAlias, HeartbeatRequest> e : toSend.entrySet()) {
                    sendFunction.accept(e.getKey(), e.getValue());
                }
            }
        }
    }
}
