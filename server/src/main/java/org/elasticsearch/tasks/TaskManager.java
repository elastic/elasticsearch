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

package org.elasticsearch.tasks;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager implements ClusterStateApplier {

    public static final String BAN_PARENT_ACTION_NAME = "internal:admin/tasks/ban";

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    /** Rest headers that are copied to the task */
    private final List<String> taskHeaders;
    private final ThreadPool threadPool;

    private final ConcurrentMapLong<Task> tasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final ConcurrentMapLong<CancellableTaskHolder> cancellableTasks = ConcurrentCollections
        .newConcurrentMapLongWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, String> banedParents = new ConcurrentHashMap<>();

    private TaskResultsService taskResultsService;

    private DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

    private final ByteSizeValue maxHeaderSize;

    private final TransportService transportService;

    public TaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders, TransportService transportService) {
        this.threadPool = threadPool;
        this.taskHeaders = new ArrayList<>(taskHeaders);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.transportService = transportService;
    }

    public void registerBanRequestHandler() {
        transportService.registerRequestHandler(BAN_PARENT_ACTION_NAME, ThreadPool.Names.SAME, BanParentTaskRequest::new,
            new BanParentRequestHandler());
    }

    public void setTaskResultsService(TaskResultsService taskResultsService) {
        assert this.taskResultsService == null;
        this.taskResultsService = taskResultsService;
    }

    /**
     * Registers a task without parent task
     */
    public Task register(String type, String action, TaskAwareRequest request) {
        Map<String, String> headers = new HashMap<>();
        long headerSize = 0;
        long maxSize = maxHeaderSize.getBytes();
        ThreadContext threadContext = threadPool.getThreadContext();
        for (String key : taskHeaders) {
            String httpHeader = threadContext.getHeader(key);
            if (httpHeader != null) {
                headerSize += key.length() * 2 + httpHeader.length() * 2;
                if (headerSize > maxSize) {
                    throw new IllegalArgumentException("Request exceeded the maximum size of task headers " + maxHeaderSize);
                }
                headers.put(key, httpHeader);
            }
        }
        Task task = request.createTask(taskIdGenerator.incrementAndGet(), type, action, request.getParentTask(), headers);
        Objects.requireNonNull(task);
        assert task.getParentTaskId().equals(request.getParentTask()) : "Request [ " + request + "] didn't preserve it parentTaskId";
        if (logger.isTraceEnabled()) {
            logger.trace("register {} [{}] [{}] [{}]", task.getId(), type, action, task.getDescription());
        }

        if (task instanceof CancellableTask) {
            registerCancellableTask(task);
        } else {
            Task previousTask = tasks.put(task.getId(), task);
            assert previousTask == null;
        }
        return task;
    }

    private void registerCancellableTask(Task task) {
        CancellableTask cancellableTask = (CancellableTask) task;
        CancellableTaskHolder holder = new CancellableTaskHolder(cancellableTask);
        CancellableTaskHolder oldHolder = cancellableTasks.put(task.getId(), holder);
        assert oldHolder == null;
        // Check if this task was banned before we start it
        if (task.getParentTaskId().isSet() && banedParents.isEmpty() == false) {
            String reason = banedParents.get(task.getParentTaskId());
            if (reason != null) {
                try {
                    holder.cancel(reason);
                    throw new IllegalStateException("Task cancelled before it started: " + reason);
                } finally {
                    // let's clean up the registration
                    unregister(task);
                }
            }
        }
    }

    /**
     * Cancels a task. The task manager also tries to cancel all children tasks
     * of the current task, when requested. Once cancellation of the children tasks is done, the listener is triggered.
     */
    public void cancel(CancellableTask cancellableTask, String reason, ActionListener<TaskInfo> listener) {
        final boolean cancelled;
        if (cancellableTask.shouldCancelChildrenOnCancellation()) {
            final BanLock banLock = new BanLock(lastDiscoveryNodes.getSize(), () -> removeBanOnNodes(cancellableTask, lastDiscoveryNodes));
            cancelled = cancel(cancellableTask, reason, banLock::onTaskFinished);
            if (cancelled) {
                // /In case the task has some child tasks, we need to wait for until ban is set on all nodes
                logger.trace("cancelling task {} on child nodes", cancellableTask.getId());
                AtomicInteger responses = new AtomicInteger(lastDiscoveryNodes.getSize());
                List<Exception> failures = new ArrayList<>();
                setBanOnNodes(reason, cancellableTask, lastDiscoveryNodes, new ActionListener<Void>() {
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
                                listener.onResponse(cancellableTask.taskInfo(lastDiscoveryNodes.getLocalNodeId(), false));
                            }
                        }
                    }
                });
            }
        } else {
            cancelled = cancel(cancellableTask, reason,
                () -> listener.onResponse(cancellableTask.taskInfo(lastDiscoveryNodes.getLocalNodeId(), false)));
            if (cancelled) {
                logger.trace("task {} doesn't have any children that should be cancelled", cancellableTask.getId());
            }
        }
        if (cancelled == false) {
            logger.trace("task {} is already cancelled", cancellableTask.getId());
            throw new IllegalStateException("task with id " + cancellableTask.getId() + " is already cancelled");
        }
    }

    /**
     * Cancels a task
     * <p>
     * Returns true if cancellation was started successful, null otherwise. Once the cancellation has been completed,
     * the provided runnable listener gerts invoked.
     */
    public boolean cancel(CancellableTask task, String reason, Runnable listener) {
        CancellableTaskHolder holder = cancellableTasks.get(task.getId());
        if (holder != null) {
            logger.trace("cancelling task with id {}", task.getId());
            return holder.cancel(reason, listener);
        }
        return false;
    }

    /**
     * Unregister the task
     */
    public Task unregister(Task task) {
        logger.trace("unregister task for id: {}", task.getId());
        if (task instanceof CancellableTask) {
            CancellableTaskHolder holder = cancellableTasks.remove(task.getId());
            if (holder != null) {
                holder.finish();
                return holder.getTask();
            } else {
                return null;
            }
        } else {
            return tasks.remove(task.getId());
        }
    }

    /**
     * Stores the task failure
     */
    public <Response extends ActionResponse> void storeResult(Task task, Exception error, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to store anything, shouldn't really be here - just pass the error along
            listener.onFailure(error);
            return;
        }
        final TaskResult taskResult;
        try {
            taskResult = task.result(localNode, error);
        } catch (IOException ex) {
            logger.warn(() -> new ParameterizedMessage("couldn't store error {}", ExceptionsHelper.detailedMessage(error)), ex);
            listener.onFailure(ex);
            return;
        }
        taskResultsService.storeResult(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onFailure(error);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("couldn't store error {}", ExceptionsHelper.detailedMessage(error)), e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Stores the task result
     */
    public <Response extends ActionResponse> void storeResult(Task task, Response response, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to store anything, shouldn't really be here - just pass the response along
            logger.warn("couldn't store response {}, the node didn't join the cluster yet", response);
            listener.onResponse(response);
            return;
        }
        final TaskResult taskResult;
        try {
            taskResult = task.result(localNode, response);
        } catch (IOException ex) {
            logger.warn(() -> new ParameterizedMessage("couldn't store response {}", response), ex);
            listener.onFailure(ex);
            return;
        }

        taskResultsService.storeResult(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn(() -> new ParameterizedMessage("couldn't store response {}", response), e);
                listener.onFailure(e);
            }
        });
    }

    /**
     * Returns the list of currently running tasks on the node
     */
    public Map<Long, Task> getTasks() {
        HashMap<Long, Task> taskHashMap = new HashMap<>(this.tasks);
        for (CancellableTaskHolder holder : cancellableTasks.values()) {
            taskHashMap.put(holder.getTask().getId(), holder.getTask());
        }
        return Collections.unmodifiableMap(taskHashMap);
    }


    /**
     * Returns the list of currently running tasks on the node that can be cancelled
     */
    public Map<Long, CancellableTask> getCancellableTasks() {
        HashMap<Long, CancellableTask> taskHashMap = new HashMap<>();
        for (CancellableTaskHolder holder : cancellableTasks.values()) {
            taskHashMap.put(holder.getTask().getId(), holder.getTask());
        }
        return Collections.unmodifiableMap(taskHashMap);
    }

    /**
     * Returns a task with given id, or null if the task is not found.
     */
    public Task getTask(long id) {
        Task task = tasks.get(id);
        if (task != null) {
            return task;
        } else {
            return getCancellableTask(id);
        }
    }

    /**
     * Returns a cancellable task with given id, or null if the task is not found.
     */
    public CancellableTask getCancellableTask(long id) {
        CancellableTaskHolder holder = cancellableTasks.get(id);
        if (holder != null) {
            return holder.getTask();
        } else {
            return null;
        }
    }

    /**
     * Returns the number of currently banned tasks.
     * <p>
     * Will be used in task manager stats and for debugging.
     */
    public int getBanCount() {
        return banedParents.size();
    }

    /**
     * Bans all tasks with the specified parent task from execution, cancels all tasks that are currently executing.
     * <p>
     * This method is called when a parent task that has children is cancelled.
     */
    private void setBan(TaskId parentTaskId, String reason) {
        logger.trace("setting ban for the parent task {} {}", parentTaskId, reason);

        // Set the ban first, so the newly created tasks cannot be registered
        synchronized (banedParents) {
            if (lastDiscoveryNodes.nodeExists(parentTaskId.getNodeId())) {
                // Only set the ban if the node is the part of the cluster
                banedParents.put(parentTaskId, reason);
            }
        }

        // Now go through already running tasks and cancel them
        for (Map.Entry<Long, CancellableTaskHolder> taskEntry : cancellableTasks.entrySet()) {
            CancellableTaskHolder holder = taskEntry.getValue();
            if (holder.hasParent(parentTaskId)) {
                holder.cancel(reason);
            }
        }
    }

    /**
     * Removes the ban for the specified parent task.
     * <p>
     * This method is called when a previously banned task finally cancelled
     */
    private void removeBan(TaskId parentTaskId) {
        logger.trace("removing ban for the parent task {}", parentTaskId);
        banedParents.remove(parentTaskId);
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        lastDiscoveryNodes = event.state().getNodes();
        if (event.nodesRemoved()) {
            synchronized (banedParents) {
                lastDiscoveryNodes = event.state().getNodes();
                // Remove all bans that were registered by nodes that are no longer in the cluster state
                Iterator<TaskId> banIterator = banedParents.keySet().iterator();
                while (banIterator.hasNext()) {
                    TaskId taskId = banIterator.next();
                    if (lastDiscoveryNodes.nodeExists(taskId.getNodeId()) == false) {
                        logger.debug("Removing ban for the parent [{}] on the node [{}], reason: the parent node is gone", taskId,
                            event.state().getNodes().getLocalNode());
                        banIterator.remove();
                    }
                }
            }
            // Cancel cancellable tasks for the nodes that are gone
            for (Map.Entry<Long, CancellableTaskHolder> taskEntry : cancellableTasks.entrySet()) {
                CancellableTaskHolder holder = taskEntry.getValue();
                CancellableTask task = holder.getTask();
                TaskId parentTaskId = task.getParentTaskId();
                if (parentTaskId.isSet() && lastDiscoveryNodes.nodeExists(parentTaskId.getNodeId()) == false) {
                    if (task.cancelOnParentLeaving()) {
                        holder.cancel("Coordinating node [" + parentTaskId.getNodeId() + "] left the cluster");
                    }
                }
            }
        }
    }

    /**
     * Blocks the calling thread, waiting for the task to vanish from the TaskManager.
     */
    public void waitForTaskCompletion(Task task, long untilInNanos) {
        while (System.nanoTime() - untilInNanos < 0) {
            if (getTask(task.getId()) == null) {
                return;
            }
            try {
                Thread.sleep(WAIT_FOR_COMPLETION_POLL.millis());
            } catch (InterruptedException e) {
                throw new ElasticsearchException("Interrupted waiting for completion of [{}]", e, task);
            }
        }
        throw new ElasticsearchTimeoutException("Timed out waiting for completion of [{}]", task);
    }

    private static class CancellableTaskHolder {

        private static final String TASK_FINISHED_MARKER = "task finished";

        private final CancellableTask task;

        private volatile String cancellationReason = null;

        private volatile Runnable cancellationListener = null;

        CancellableTaskHolder(CancellableTask task) {
            this.task = task;
        }

        /**
         * Marks task as cancelled.
         * <p>
         * Returns true if cancellation was successful, false otherwise.
         */
        public boolean cancel(String reason, Runnable listener) {
            final boolean cancelled;
            synchronized (this) {
                assert reason != null;
                if (cancellationReason == null) {
                    cancellationReason = reason;
                    cancellationListener = listener;
                    cancelled = true;
                } else {
                    // Already cancelled by somebody else
                    cancelled = false;
                }
            }
            if (cancelled) {
                task.cancel(reason);
            }
            return cancelled;
        }

        /**
         * Marks task as cancelled.
         * <p>
         * Returns true if cancellation was successful, false otherwise.
         */
        public boolean cancel(String reason) {
            return cancel(reason, null);
        }

        /**
         * Marks task as finished.
         */
        public void finish() {
            Runnable listener = null;
            synchronized (this) {
                if (cancellationReason != null) {
                    // The task was cancelled, we need to notify the listener
                    if (cancellationListener != null) {
                        listener = cancellationListener;
                        cancellationListener = null;
                    }
                } else {
                    cancellationReason = TASK_FINISHED_MARKER;
                }
            }
            // We need to call the listener outside of the synchronised section to avoid potential bottle necks
            // in the listener synchronization
            if (listener != null) {
                listener.run();
            }
        }

        boolean hasParent(TaskId parentTaskId) {
            return task.getParentTaskId().equals(parentTaskId);
        }

        public CancellableTask getTask() {
            return task;
        }
    }

    private void removeBanOnNodes(CancellableTask task, DiscoveryNodes nodes) {
        BanParentTaskRequest request = BanParentTaskRequest.createRemoveBanParentTaskRequest(
            new TaskId(lastDiscoveryNodes.getLocalNodeId(), task.getId()));
        for (ObjectObjectCursor<String, DiscoveryNode> node : nodes.getNodes()) {
            logger.debug("Sending remove ban for tasks with the parent [{}] to the node [{}]", request.parentTaskId, node.key);
            transportService.sendRequest(node.value, BAN_PARENT_ACTION_NAME, request, EmptyTransportResponseHandler
                .INSTANCE_SAME);
        }
    }

    private void setBanOnNodes(String reason, CancellableTask task, DiscoveryNodes nodes, ActionListener<Void> listener) {
        BanParentTaskRequest request = BanParentTaskRequest.createSetBanParentTaskRequest(
            new TaskId(lastDiscoveryNodes.getLocalNodeId(), task.getId()), reason);
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

    private static class BanLock {
        private final Runnable finish;
        private final AtomicInteger counter;
        private final int nodesSize;

        BanLock(int nodesSize, Runnable finish) {
            counter = new AtomicInteger(0);
            this.finish = finish;
            this.nodesSize = nodesSize;
        }

        void onBanSet() {
            if (counter.decrementAndGet() == 0) {
                finish();
            }
        }

        void onTaskFinished() {
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
                    lastDiscoveryNodes.getLocalNodeId(), request.reason);
                setBan(request.parentTaskId, request.reason);
            } else {
                logger.debug("Removing ban for the parent [{}] on the node [{}]", request.parentTaskId,
                    lastDiscoveryNodes.getLocalNodeId());
                removeBan(request.parentTaskId);
            }
            channel.sendResponse(TransportResponse.Empty.INSTANCE);
        }
    }
}
