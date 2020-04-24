/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.action.admin.cluster.node.tasks.cancel;

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.StepListener;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ChannelActionListener;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskInfo;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.EmptyTransportResponseHandler;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportActionProxy;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportRequestOptions;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask}
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, CancelTasksResponse, TaskInfo> {

    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";

    @Inject
    public TransportCancelTasksAction(ClusterService clusterService, TransportService transportService, ActionFilters actionFilters) {
        super(CancelTasksAction.NAME, clusterService, transportService, actionFilters,
            CancelTasksRequest::new, CancelTasksResponse::new, TaskInfo::new, ThreadPool.Names.MANAGEMENT);
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
        TransportActionProxy.registerProxyAction(transportService, BAN_PARENT_ACTION_NAME, in -> TransportResponse.Empty.INSTANCE);
    }

    @Override
    protected CancelTasksResponse newResponse(CancelTasksRequest request, List<TaskInfo> tasks, List<TaskOperationFailure>
        taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new CancelTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    protected void processTasks(CancelTasksRequest request, Consumer<CancellableTask> operation) {
        if (request.getTaskId().isSet()) {
            // we are only checking one task, we can optimize it
            CancellableTask task = taskManager.getCancellableTask(request.getTaskId().getId());
            if (task != null) {
                if (request.match(task)) {
                    operation.accept(task);
                } else {
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support this operation");
                }
            } else {
                if (taskManager.getTask(request.getTaskId().getId()) != null) {
                    // The task exists, but doesn't support cancellation
                    throw new IllegalArgumentException("task [" + request.getTaskId() + "] doesn't support cancellation");
                } else {
                    throw new ResourceNotFoundException("task [{}] is not found", request.getTaskId());
                }
            }
        } else {
            for (CancellableTask task : taskManager.getCancellableTasks().values()) {
                if (request.match(task)) {
                    operation.accept(task);
                }
            }
        }
    }

    @Override
    protected void taskOperation(CancelTasksRequest request, CancellableTask cancellableTask, ActionListener<TaskInfo> listener) {
        String nodeId = clusterService.localNode().getId();
        cancelTaskAndDescendants(cancellableTask, request.getReason(), request.waitForCompletion(),
            ActionListener.map(listener, r -> cancellableTask.taskInfo(nodeId, false)));
    }

    void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        if (task.shouldCancelChildrenOnCancellation()) {
            StepListener<Void> completedListener = new StepListener<>();
            GroupedActionListener<Void> groupedListener = new GroupedActionListener<>(ActionListener.map(completedListener, r -> null), 3);
            Map<String, List<DiscoveryNode>> childConnections =
                taskManager.startBanOnChildrenNodes(task.getId(), () -> groupedListener.onResponse(null));
            taskManager.cancel(task, reason, () -> groupedListener.onResponse(null));

            StepListener<Void> banOnNodesListener = new StepListener<>();
            setBanOnNodes(reason, waitForCompletion, task, childConnections, banOnNodesListener);
            banOnNodesListener.whenComplete(groupedListener::onResponse, groupedListener::onFailure);
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                .preserveContext(() -> removeBanOnNodes(task, childConnections));
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
            logger.trace("task {} doesn't have any children that should be cancelled", task.getId());
            if (waitForCompletion) {
                taskManager.cancel(task, reason, () -> listener.onResponse(null));
            } else {
                taskManager.cancel(task, reason, () -> {});
                listener.onResponse(null);
            }
        }
    }

    private void setBanOnNodes(String reason, boolean waitForCompletion, CancellableTask task,
                               Map<String, List<DiscoveryNode>> childConnections, ActionListener<Void> listener) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        sendBanParentRequests(childConnections, listener,
            subNodes -> new BanParentTaskRequest(parentTaskId, reason, waitForCompletion, subNodes));
    }

    private void removeBanOnNodes(CancellableTask task, Map<String, List<DiscoveryNode>> childConnections) {
        final TaskId parentTaskId = new TaskId(clusterService.localNode().getId(), task.getId());
        sendBanParentRequests(childConnections, ActionListener.wrap(() -> {}),
            subNodes -> new BanParentTaskRequest(parentTaskId, subNodes));
    }

    private void sendBanParentRequests(Map<String, List<DiscoveryNode>> childConnections, ActionListener<Void> listener,
                                       Function<List<DiscoveryNode>, BanParentTaskRequest> requestGenerator) {
        if (childConnections.isEmpty()) {
            listener.onResponse(null);
            return;
        }
        final int groupSize = childConnections.entrySet().stream()
            .mapToInt(e -> e.getKey().equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY) ? e.getValue().size() : 1)
            .sum();
        final GroupedActionListener<Void> groupedListener =
            new GroupedActionListener<>(ActionListener.map(listener, r -> null), groupSize);
        for (Map.Entry<String, List<DiscoveryNode>> entry : childConnections.entrySet()) {
            final String clusterAlias = entry.getKey();
            if (clusterAlias.equals(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                final BanParentTaskRequest request = requestGenerator.apply(List.of());
                for (DiscoveryNode node : entry.getValue()) {
                    sendBanParentRequest(() -> transportService.getConnection(node), request, groupedListener);
                }
            } else {
                final ArrayList<DiscoveryNode> subNodes = new ArrayList<>(entry.getValue());
                final DiscoveryNode targetNode = subNodes.remove(0);
                if (targetNode.getVersion().onOrAfter(Version.V_8_0_0)) {
                    BanParentTaskRequest request = requestGenerator.apply(subNodes);
                    sendBanParentRequest(() -> transportService.getRemoteClusterService().getConnection(targetNode, clusterAlias),
                        request, groupedListener);
                } else {
                    groupedListener.onResponse(null); // old versions do not support cancellation cross clusters
                }
            }
        }
    }

    private void sendBanParentRequest(Supplier<Transport.Connection> connectionSupplier,
                                      BanParentTaskRequest request, ActionListener<Void> listener) {
        final Transport.Connection connection;
        try {
            connection = connectionSupplier.get();
        } catch (Exception e) {
            listener.onFailure(e);
            return;
        }
        transportService.sendRequest(connection, BAN_PARENT_ACTION_NAME, request, TransportRequestOptions.EMPTY,
            new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                @Override
                public void handleResponse(TransportResponse.Empty response) {
                    listener.onResponse(null);
                }

                @Override
                public void handleException(TransportException exp) {
                    assert ExceptionsHelper.unwrapCause(exp) instanceof ElasticsearchSecurityException == false;
                    logger.warn(new ParameterizedMessage("failed to send {} request for task {} to node {}",
                        request.ban ? "ban" : "unban", request.parentTaskId, connection.getNode()), exp);
                    listener.onFailure(exp);
                }
            });
    }

    private static class BanParentTaskRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final boolean ban;
        private final boolean waitForCompletion;
        private final String reason;
        private final List<DiscoveryNode> subNodes; // forward the request to these sub nodes

        private BanParentTaskRequest(TaskId parentTaskId, String reason, boolean waitForCompletion, List<DiscoveryNode> subNodes) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
            this.waitForCompletion = waitForCompletion;
            this.subNodes = Objects.requireNonNull(subNodes);
        }

        private BanParentTaskRequest(TaskId parentTaskId, List<DiscoveryNode> subNodes) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
            this.waitForCompletion = false;
            this.subNodes = Objects.requireNonNull(subNodes);
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
            if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
                subNodes = in.readList(DiscoveryNode::new);
            } else {
                subNodes = Collections.emptyList();
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
            if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
                out.writeList(subNodes);
            }
        }
    }

    class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug("Received ban for the parent [{}] on the node [{}], reason: [{}]", request.parentTaskId,
                    clusterService.localNode().getId(), request.reason);
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason);
                final int groupSize = childTasks.size() + request.subNodes.size() + 1;
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.map(
                    new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request), r -> TransportResponse.Empty.INSTANCE),
                    groupSize);
                for (CancellableTask childTask : childTasks) {
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                final BanParentTaskRequest subRequest = new BanParentTaskRequest(
                    request.parentTaskId, request.reason, request.waitForCompletion, List.of());
                for (DiscoveryNode subNode : request.subNodes) {
                    sendBanParentRequest(() -> transportService.getConnection(subNode), subRequest, listener);
                }
                listener.onResponse(null);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId, clusterService.localNode().getId());
                taskManager.removeBan(request.parentTaskId);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(
                    ActionListener.map(
                        new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request), r -> TransportResponse.Empty.INSTANCE),
                    request.subNodes.size() + 1);
                final BanParentTaskRequest subRequest = new BanParentTaskRequest(request.parentTaskId, List.of());
                for (DiscoveryNode subNode : request.subNodes) {
                    sendBanParentRequest(() -> transportService.getConnection(subNode), subRequest, listener);
                }
                listener.onResponse(null);
            }
        }
    }

}
