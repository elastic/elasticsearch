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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import com.carrotsearch.hppc.cursors.ObjectLongCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class TaskCancellationService {
    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";
    public static final String BAN_PARENT_HEARTBEAT_ACTION_NAME = "internal:admin/tasks/ban_heartbeat";

    private static final Logger logger = LogManager.getLogger(TaskCancellationService.class);
    private final TransportService transportService;
    private final TaskManager taskManager;
    private final BanParentMarkerHeartbeatSender banParentMarkerHeartbeatSender;

    public TaskCancellationService(Settings settings, TransportService transportService) {
        this.transportService = transportService;
        this.taskManager = transportService.getTaskManager();
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
        transportService.registerRequestHandler(BAN_PARENT_HEARTBEAT_ACTION_NAME, ThreadPool.Names.SAME, BanParentHeartbeatRequest::new,
            new BanParentHeartbeatRequestHandler());
        this.banParentMarkerHeartbeatSender = new BanParentMarkerHeartbeatSender(transportService.getThreadPool(),
            settings.getAsTime(
                TaskManager.BAN_MARKER_SEND_HEARTBEAT_INTERVAL_SETTING, TaskManager.BAN_MARKER_SEND_HEARTBEAT_DEFAULT_INTERVAL),
            taskId -> taskManager.getChildNodes(taskId.getId()), this::sendBanMarkerHeartbeat);
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
            Collection<DiscoveryNode> childrenNodes = taskManager.startBanOnChildrenNodes(task.getId(), () -> {
                logger.trace("child tasks of parent [{}] are completed", taskId);
                groupedListener.onResponse(null);
            });
            taskManager.cancel(task, reason, () -> {
                logger.trace("task [{}] is cancelled", taskId);
                groupedListener.onResponse(null);
            });
            StepListener<Void> banOnNodesListener = new StepListener<>();
            setBanOnNodes(reason, waitForCompletion, task, childrenNodes, banOnNodesListener);
            banOnNodesListener.whenComplete(groupedListener::onResponse, groupedListener::onFailure);
            final Releasable unregisterHeartbeat = banParentMarkerHeartbeatSender.registerCancellingTask(taskId);
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                .preserveContext(() -> Releasables.close(unregisterHeartbeat, () -> removeBanOnNodes(task, childrenNodes)));
            // We remove bans after all child tasks are completed although in theory we can do it on a per-node basis.
            completedListener.whenComplete(r -> removeBansRunnable.run(), e -> removeBansRunnable.run());
            // if wait_for_completion is true, then only return when (1) bans are placed on child nodes, (2) child tasks are
            // completed or failed, (3) the main task is cancelled. Otherwise, return after bans are placed on child nodes.
            if (waitForCompletion) {
                completedListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
            } else {
                banOnNodesListener.whenComplete(r -> listener.onResponse(null), listener::onFailure);
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

    private void setBanOnNodes(String reason, boolean waitForCompletion, CancellableTask task,
                               Collection<DiscoveryNode> childNodes, ActionListener<Void> listener) {
        if (childNodes.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final TaskId taskId = new TaskId(localNodeId(), task.getId());
        logger.trace("cancelling child tasks of [{}] on child nodes {}", taskId, childNodes);
        GroupedActionListener<Void> groupedListener =
            new GroupedActionListener<>(ActionListener.map(listener, r -> null), childNodes.size());
        final BanParentTaskRequest banRequest = BanParentTaskRequest.createSetBanParentTaskRequest(taskId, reason, waitForCompletion);
        for (DiscoveryNode node : childNodes) {
            transportService.sendRequest(node, BAN_PARENT_ACTION_NAME, banRequest,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
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

    private void removeBanOnNodes(CancellableTask task, Collection<DiscoveryNode> childNodes) {
        final BanParentTaskRequest request =
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(localNodeId(), task.getId()));
        for (DiscoveryNode node : childNodes) {
            logger.trace("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
            transportService.sendRequest(node, BAN_PARENT_ACTION_NAME, request, new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
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
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason, channel.getVersion());
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

    private static class BanParentHeartbeatRequest extends TransportRequest {
        final TaskId parentTaskId;

        BanParentHeartbeatRequest(TaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
        }

        BanParentHeartbeatRequest(StreamInput in) throws IOException {
            super(in);
            this.parentTaskId = TaskId.readFromStream(in);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
        }
    }

    private class BanParentHeartbeatRequestHandler implements TransportRequestHandler<BanParentHeartbeatRequest> {
        @Override
        public void messageReceived(BanParentHeartbeatRequest request, TransportChannel channel, Task task) throws Exception {
            taskManager.updateTimestampOfBannedParentMaker(request.parentTaskId);
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

    private void sendBanMarkerHeartbeat(TaskId taskId, DiscoveryNode node) {
        logger.trace("Sending a heartbeat for the ban parent of task [{}] to the node [{}]", taskId, node);
        if (node.getVersion().onOrAfter(Version.V_8_0_0)) {
            final BanParentHeartbeatRequest request = new BanParentHeartbeatRequest(taskId);
            transportService.sendRequest(node, BAN_PARENT_HEARTBEAT_ACTION_NAME, request,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleException(TransportException exp) {
                        assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                        logger.info(new ParameterizedMessage("failed to send heartbeat for the parent ban heartbeat of task {} on node {}",
                            request.parentTaskId, node), exp);
                    }
                });
        }
    }

    static final class BanParentMarkerHeartbeatSender {
        private final ThreadPool threadPool;
        private final TimeValue interval;
        private final Function<TaskId, Collection<DiscoveryNode>> childNodes;
        private final BiConsumer<TaskId, DiscoveryNode> sendHeartbeatFn;
        private final ObjectLongMap<TaskId> cancellingTasks = new ObjectLongHashMap<>();
        private Scheduler.Cancellable sender = null;

        BanParentMarkerHeartbeatSender(ThreadPool threadPool, TimeValue interval, Function<TaskId, Collection<DiscoveryNode>> childNodes,
                                       BiConsumer<TaskId, DiscoveryNode> sendHeartbeatFn) {
            this.threadPool = threadPool;
            this.interval = interval;
            this.childNodes = childNodes;
            this.sendHeartbeatFn = sendHeartbeatFn;
        }

        synchronized Releasable registerCancellingTask(TaskId taskId) {
            cancellingTasks.addTo(taskId, 1L);
            if (sender == null) {
                assert cancellingTasks.size() == 1 : cancellingTasks.size();
                sender = threadPool.scheduleWithFixedDelay(this::sendHeartbeatMessages, interval, ThreadPool.Names.GENERIC);
            }
            return Releasables.releaseOnce(() -> unregisterCancellingTask(taskId));
        }

        private synchronized void unregisterCancellingTask(TaskId taskId) {
            assert isRunningOrScheduled();
            if (cancellingTasks.addTo(taskId, -1L) == 0) {
                cancellingTasks.remove(taskId);
                if (cancellingTasks.isEmpty()) {
                    if (sender != null) {
                        sender.cancel();
                    }
                    sender = null;
                }
            }
        }

        private void sendHeartbeatMessages() {
            final Set<TaskId> taskIds = new HashSet<>();
            synchronized (this) {
                for (ObjectLongCursor<TaskId> e : cancellingTasks) {
                    taskIds.add(e.key);
                }
            }
            final ThreadContext threadContext = threadPool.getThreadContext();
            try (ThreadContext.StoredContext ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                for (TaskId taskId : taskIds) {
                    for (DiscoveryNode childNode : childNodes.apply(taskId)) {
                        sendHeartbeatFn.accept(taskId, childNode);
                    }
                }
            }
        }

        synchronized boolean isRunningOrScheduled() {
            return sender != null && sender.isCancelled() == false;
        }
    }
}
