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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
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
                    throw new ResourceNotFoundException("task [{}] doesn't support cancellation", request.getTaskId());
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
    protected synchronized void taskOperation(CancelTasksRequest request, CancellableTask cancellableTask,
            ActionListener<TaskInfo> listener) {
        String nodeId = clusterService.localNode().getId();
        final boolean canceled;
        if (cancellableTask.shouldCancelChildrenOnCancellation()) {
            DiscoveryNodes childNodes = clusterService.state().nodes();
            final BanLock banLock = new BanLock(childNodes.getSize(), () -> removeBanOnNodes(cancellableTask, childNodes));
            canceled = taskManager.cancel(cancellableTask, request.getReason(), banLock::onTaskFinished);
            if (canceled) {
                // /In case the task has some child tasks, we need to wait for until ban is set on all nodes
                logger.trace("cancelling task {} on child nodes", cancellableTask.getId());
                AtomicInteger responses = new AtomicInteger(childNodes.getSize());
                List<Exception> failures = new ArrayList<>();
                setBanOnNodes(request.getReason(), cancellableTask, childNodes, new ActionListener<Void>() {
                    @Override
                    public void onResponse(Void aVoid) {
                        processResponse();
                    }

                    @Override
                    public void onFailure(Exception e) {
                        synchronized (failures) {
                            failures.add(e);
                        }
                        processResponse();
                    }

                    private void processResponse() {
                        banLock.onBanSet();
                        if (responses.decrementAndGet() == 0) {
                            if (failures.isEmpty() == false) {
                                IllegalStateException exception = new IllegalStateException("failed to cancel children of the task [" +
                                    cancellableTask.getId() + "]");
                                failures.forEach(exception::addSuppressed);
                                listener.onFailure(exception);
                            } else {
                                listener.onResponse(cancellableTask.taskInfo(nodeId, false));
                            }
                        }
                    }
                });
            }
        }  else {
            canceled = taskManager.cancel(cancellableTask, request.getReason(),
                () -> listener.onResponse(cancellableTask.taskInfo(nodeId, false)));
            if (canceled) {
                logger.trace("task {} doesn't have any children that should be cancelled", cancellableTask.getId());
            }
        }
        if (canceled == false) {
            logger.trace("task {} is already cancelled", cancellableTask.getId());
            throw new IllegalStateException("task with id " + cancellableTask.getId() + " is already cancelled");
        }
    }

    private void setBanOnNodes(String reason, CancellableTask task, DiscoveryNodes nodes, ActionListener<Void> listener) {
        sendSetBanRequest(nodes,
            BanParentTaskRequest.createSetBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId()), reason),
            listener);
    }

    private void removeBanOnNodes(CancellableTask task, DiscoveryNodes nodes) {
        sendRemoveBanRequest(nodes,
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId())));
    }

    private void sendSetBanRequest(DiscoveryNodes nodes, BanParentTaskRequest request, ActionListener<Void> listener) {
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getNodes()) {
            logger.trace("Sending ban for tasks with the parent [{}] to the node [{}], ban [{}]", request.parentTaskId, node.key,
                request.ban);
            transportService.sendRequest(node.value, BAN_PARENT_ACTION_NAME, request,
                new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                    @Override
                    public void handleResponse(TransportResponse.Empty response) {
                        listener.onResponse(null);
                    }

                    @Override
                    public void handleException(TransportException exp) {
                        logger.warn("Cannot send ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node.key);
                        listener.onFailure(exp);
                    }
                });
        }
    }

    private void sendRemoveBanRequest(DiscoveryNodes nodes, BanParentTaskRequest request) {
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getNodes()) {
            logger.debug("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node.key);
            transportService.sendRequest(node.value, BAN_PARENT_ACTION_NAME, request, EmptyTransportResponseHandler
                .INSTANCE_SAME);
        }
    }

    private static class BanLock {
        private final Runnable finish;
        private final AtomicInteger counter;
        private final int nodesSize;

        BanLock(int nodesSize, Runnable finish) {
            counter = new AtomicInteger(0);
            this.finish = finish;
            this.nodesSize = nodesSize;
        }

        public void onBanSet() {
            if (counter.decrementAndGet() == 0) {
                finish();
            }
        }

        public void onTaskFinished() {
            if (counter.addAndGet(nodesSize) == 0) {
                finish();
            }
        }

        public void finish() {
            finish.run();
        }

    }

    private static class BanParentTaskRequest extends TransportRequest {

        private final TaskId parentTaskId;
        private final boolean ban;
        private final String reason;

        static BanParentTaskRequest createSetBanParentTaskRequest(TaskId parentTaskId, String reason) {
            return new BanParentTaskRequest(parentTaskId, reason);
        }

        static BanParentTaskRequest createRemoveBanParentTaskRequest(TaskId parentTaskId) {
            return new BanParentTaskRequest(parentTaskId);
        }

        private BanParentTaskRequest(TaskId parentTaskId, String reason) {
            this.parentTaskId = parentTaskId;
            this.ban = true;
            this.reason = reason;
        }

        private BanParentTaskRequest(TaskId parentTaskId) {
            this.parentTaskId = parentTaskId;
            this.ban = false;
            this.reason = null;
        }

        private BanParentTaskRequest(StreamInput in) throws IOException {
            super(in);
            parentTaskId = TaskId.readFromStream(in);
            ban = in.readBoolean();
            reason = ban ? in.readString() : null;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            parentTaskId.writeTo(out);
            out.writeBoolean(ban);
            if (ban) {
                out.writeString(reason);
            }
        }
    }

    class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel, Task task) throws Exception {
            if (request.ban) {
                logger.debug("Received ban for the parent [{}] on the node [{}], reason: [{}]", request.parentTaskId,
                    clusterService.localNode().getId(), request.reason);
                taskManager.setBan(request.parentTaskId, request.reason);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId,
                    clusterService.localNode().getId());
                taskManager.removeBan(request.parentTaskId);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }

}
