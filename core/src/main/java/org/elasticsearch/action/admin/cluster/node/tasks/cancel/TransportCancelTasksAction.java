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

import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.FailedNodeException;
import org.elasticsearch.action.TaskOperationFailure;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.tasks.TransportTasksAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.tasks.CancellableTask;
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
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

/**
 * Transport action that can be used to cancel currently running cancellable tasks.
 * <p>
 * For a task to be cancellable it has to return an instance of
 * {@link CancellableTask} from {@link TransportRequest#createTask(long, String, String, TaskId)}
 */
public class TransportCancelTasksAction extends TransportTasksAction<CancellableTask, CancelTasksRequest, CancelTasksResponse, TaskInfo> {

    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";

    @Inject
    public TransportCancelTasksAction(Settings settings, ThreadPool threadPool, ClusterService clusterService,
                                      TransportService transportService, ActionFilters actionFilters, IndexNameExpressionResolver
                                          indexNameExpressionResolver) {
        super(settings, CancelTasksAction.NAME, threadPool, clusterService, transportService, actionFilters,
            indexNameExpressionResolver, CancelTasksRequest::new, CancelTasksResponse::new, ThreadPool.Names.MANAGEMENT);
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, BanParentTaskRequest::new, ThreadPool.Names.SAME, new
            BanParentRequestHandler());
    }

    @Override
    protected CancelTasksResponse newResponse(CancelTasksRequest request, List<TaskInfo> tasks, List<TaskOperationFailure>
        taskOperationFailures, List<FailedNodeException> failedNodeExceptions) {
        return new CancelTasksResponse(tasks, taskOperationFailures, failedNodeExceptions);
    }

    @Override
    protected TaskInfo readTaskResponse(StreamInput in) throws IOException {
        return new TaskInfo(in);
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
    protected synchronized TaskInfo taskOperation(CancelTasksRequest request, CancellableTask cancellableTask) {
        final BanLock banLock = new BanLock(nodes -> removeBanOnNodes(cancellableTask, nodes));
        Set<String> childNodes = taskManager.cancel(cancellableTask, request.getReason(), banLock::onTaskFinished);
        if (childNodes != null) {
            if (childNodes.isEmpty()) {
                logger.trace("cancelling task {} with no children", cancellableTask.getId());
                return cancellableTask.taskInfo(clusterService.localNode(), false);
            } else {
                logger.trace("cancelling task {} with children on nodes [{}]", cancellableTask.getId(), childNodes);
                setBanOnNodes(request.getReason(), cancellableTask, childNodes, banLock);
                return cancellableTask.taskInfo(clusterService.localNode(), false);
            }
        } else {
            logger.trace("task {} is already cancelled", cancellableTask.getId());
            throw new IllegalStateException("task with id " + cancellableTask.getId() + " is already cancelled");
        }
    }

    @Override
    protected boolean accumulateExceptions() {
        return true;
    }

    private void setBanOnNodes(String reason, CancellableTask task, Set<String> nodes, BanLock banLock) {
        sendSetBanRequest(nodes,
            BanParentTaskRequest.createSetBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId()), reason),
            banLock);
    }

    private void removeBanOnNodes(CancellableTask task, Set<String> nodes) {
        sendRemoveBanRequest(nodes,
            BanParentTaskRequest.createRemoveBanParentTaskRequest(new TaskId(clusterService.localNode().getId(), task.getId())));
    }

    private void sendSetBanRequest(Set<String> nodes, BanParentTaskRequest request, BanLock banLock) {
        ClusterState clusterState = clusterService.state();
        for (String node : nodes) {
            DiscoveryNode discoveryNode = clusterState.getNodes().get(node);
            if (discoveryNode != null) {
                // Check if node still in the cluster
                logger.debug("Sending ban for tasks with the parent [{}] to the node [{}], ban [{}]", request.parentTaskId, node,
                    request.ban);
                transportService.sendRequest(discoveryNode, BAN_PARENT_ACTION_NAME, request,
                    new EmptyTransportResponseHandler(ThreadPool.Names.SAME) {
                        @Override
                        public void handleResponse(TransportResponse.Empty response) {
                            banLock.onBanSet();
                        }

                        @Override
                        public void handleException(TransportException exp) {
                            banLock.onBanSet();
                        }
                    });
            } else {
                banLock.onBanSet();
                logger.debug("Cannot send ban for tasks with the parent [{}] to the node [{}] - the node no longer in the cluster",
                    request.parentTaskId, node);
            }
        }
    }

    private void sendRemoveBanRequest(Set<String> nodes, BanParentTaskRequest request) {
        ClusterState clusterState = clusterService.state();
        for (String node : nodes) {
            DiscoveryNode discoveryNode = clusterState.getNodes().get(node);
            if (discoveryNode != null) {
                // Check if node still in the cluster
                logger.debug("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node);
                transportService.sendRequest(discoveryNode, BAN_PARENT_ACTION_NAME, request, EmptyTransportResponseHandler
                    .INSTANCE_SAME);
            } else {
                logger.debug("Cannot send remove ban request for tasks with the parent [{}] to the node [{}] - the node no longer in " +
                    "the cluster", request.parentTaskId, node);
            }
        }
    }

    private static class BanLock {
        private final Consumer<Set<String>> finish;
        private final AtomicInteger counter;
        private final AtomicReference<Set<String>> nodes = new AtomicReference<>();

        public BanLock(Consumer<Set<String>> finish) {
            counter = new AtomicInteger(0);
            this.finish = finish;
        }

        public void onBanSet() {
            if (counter.decrementAndGet() == 0) {
                finish();
            }
        }

        public void onTaskFinished(Set<String> nodes) {
            this.nodes.set(nodes);
            if (counter.addAndGet(nodes.size()) == 0) {
                finish();
            }
        }

        public void finish() {
            finish.accept(nodes.get());
        }

    }

    private static class BanParentTaskRequest extends TransportRequest {

        private TaskId parentTaskId;

        private boolean ban;

        private String reason;

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
        }

        public BanParentTaskRequest() {
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            super.readFrom(in);
            parentTaskId = TaskId.readFromStream(in);
            ban = in.readBoolean();
            if (ban) {
                reason = in.readString();
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
        }
    }

    class BanParentRequestHandler implements TransportRequestHandler<BanParentTaskRequest> {
        @Override
        public void messageReceived(final BanParentTaskRequest request, final TransportChannel channel) throws Exception {
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
