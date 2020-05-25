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
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportException;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.function.Consumer;

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
        final TaskId taskId = task.taskInfo(clusterService.localNode().getId(), false).getTaskId();
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
            // If we start unbanning when the last child task completed and that child task executed with a specific user, then unban
            // requests are denied because internal requests can't run with a user. We need to remove bans with the current thread context.
            final Runnable removeBansRunnable = transportService.getThreadPool().getThreadContext()
                .preserveContext(() -> removeBanOnNodes(task, childrenNodes));
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
        final TaskId taskId = new TaskId(clusterService.localNode().getId(), task.getId());
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
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId()));
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

    class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug("Received ban for the parent [{}] on the node [{}], reason: [{}]", request.parentTaskId,
                    clusterService.localNode().getId(), request.reason);
                final List<CancellableTask> childTasks = taskManager.setBan(request.parentTaskId, request.reason);
                final GroupedActionListener<Void> listener = new GroupedActionListener<>(ActionListener.map(
                    new ChannelActionListener<>(channel, BAN_PARENT_ACTION_NAME, request), r -> TransportResponse.Empty.INSTANCE),
                    childTasks.size() + 1);
                for (CancellableTask childTask : childTasks) {
                    cancelTaskAndDescendants(childTask, request.reason, request.waitForCompletion, listener);
                }
                listener.onResponse(null);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId,
                    clusterService.localNode().getId());
                taskManager.removeBan(request.parentTaskId);
                channel.sendResponse(TransportResponse.Empty.INSTANCE);
            }
        }
    }

}
