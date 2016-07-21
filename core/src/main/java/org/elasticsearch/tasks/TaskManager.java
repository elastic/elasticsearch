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

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.common.unit.TimeValue.timeValueMillis;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager extends AbstractComponent implements ClusterStateListener {
    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    private final ConcurrentMapLong<Task> tasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final ConcurrentMapLong<CancellableTaskHolder> cancellableTasks = ConcurrentCollections
        .newConcurrentMapLongWithAggressiveConcurrency();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, String> banedParents = new ConcurrentHashMap<>();

    private TaskPersistenceService taskResultsService;

    private DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

    public TaskManager(Settings settings) {
        super(settings);
    }

    public void setTaskResultsService(TaskPersistenceService taskResultsService) {
        assert this.taskResultsService == null;
        this.taskResultsService = taskResultsService;
    }

    /**
     * Registers a task without parent task
     * <p>
     * Returns the task manager tracked task or null if the task doesn't support the task manager
     */
    public Task register(String type, String action, TransportRequest request) {
        Task task = request.createTask(taskIdGenerator.incrementAndGet(), type, action, request.getParentTask());
        if (task == null) {
            return null;
        }
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
     * Cancels a task
     * <p>
     * Returns a set of nodes with child tasks where this task should be cancelled if cancellation was successful, null otherwise.
     */
    public Set<String> cancel(CancellableTask task, String reason, Consumer<Set<String>> listener) {
        CancellableTaskHolder holder = cancellableTasks.get(task.getId());
        if (holder != null) {
            logger.trace("cancelling task with id {}", task.getId());
            return holder.cancel(reason, listener);
        }
        return null;
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
    public <Response extends ActionResponse> void persistResult(Task task, Exception error, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to persist anything, shouldn't really be here - just pass the error along
            listener.onFailure(error);
            return;
        }
        final PersistedTaskInfo taskResult;
        try {
            taskResult = task.result(localNode, error);
        } catch (IOException ex) {
            logger.warn("couldn't persist error {}", ex, ExceptionsHelper.detailedMessage(error));
            listener.onFailure(ex);
            return;
        }
        taskResultsService.persist(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onFailure(error);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("couldn't persist error {}", e, ExceptionsHelper.detailedMessage(error));
                listener.onFailure(e);
            }
        });
    }

    /**
     * Stores the task result
     */
    public <Response extends ActionResponse> void persistResult(Task task, Response response, ActionListener<Response> listener) {
        DiscoveryNode localNode = lastDiscoveryNodes.getLocalNode();
        if (localNode == null) {
            // too early to persist anything, shouldn't really be here - just pass the response along
            logger.warn("couldn't persist response {}, the node didn't join the cluster yet", response);
            listener.onResponse(response);
            return;
        }
        final PersistedTaskInfo taskResult;
        try {
            taskResult = task.result(localNode, response);
        } catch (IOException ex) {
            logger.warn("couldn't persist response {}", ex, response);
            listener.onFailure(ex);
            return;
        }

        taskResultsService.persist(taskResult, new ActionListener<Void>() {
            @Override
            public void onResponse(Void aVoid) {
                listener.onResponse(response);
            }

            @Override
            public void onFailure(Exception e) {
                logger.warn("couldn't persist response {}", e, response);
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
    public void setBan(TaskId parentTaskId, String reason) {
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
    public void removeBan(TaskId parentTaskId) {
        logger.trace("removing ban for the parent task {}", parentTaskId);
        banedParents.remove(parentTaskId);
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
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

    public void registerChildTask(Task task, String node) {
        if (task == null || task instanceof CancellableTask == false) {
            // We don't have a cancellable task - not much we can do here
            return;
        }
        CancellableTaskHolder holder = cancellableTasks.get(task.getId());
        if (holder != null) {
            holder.registerChildTaskNode(node);
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

        private final Set<String> nodesWithChildTasks = new HashSet<>();

        private volatile String cancellationReason = null;

        private volatile Consumer<Set<String>> cancellationListener = null;

        public CancellableTaskHolder(CancellableTask task) {
            this.task = task;
        }

        /**
         * Marks task as cancelled.
         * <p>
         * Returns a set of nodes with child tasks where this task should be cancelled if cancellation was successful, null otherwise.
         */
        public Set<String> cancel(String reason, Consumer<Set<String>> listener) {
            Set<String> nodes;
            synchronized (this) {
                assert reason != null;
                if (cancellationReason == null) {
                    cancellationReason = reason;
                    cancellationListener = listener;
                    nodes = Collections.unmodifiableSet(nodesWithChildTasks);
                } else {
                    // Already cancelled by somebody else
                    nodes = null;
                }
            }
            if (nodes != null) {
                task.cancel(reason);
            }
            return nodes;
        }

        /**
         * Marks task as cancelled.
         * <p>
         * Returns a set of nodes with child tasks where this task should be cancelled if cancellation was successful, null otherwise.
         */
        public Set<String> cancel(String reason) {
            return cancel(reason, null);
        }

        /**
         * Marks task as finished.
         */
        public void finish() {
            Consumer<Set<String>> listener = null;
            Set<String> nodes = null;
            synchronized (this) {
                if (cancellationReason != null) {
                    // The task was cancelled, we need to notify the listener
                    if (cancellationListener != null) {
                        listener = cancellationListener;
                        nodes = Collections.unmodifiableSet(nodesWithChildTasks);
                        cancellationListener = null;
                    }
                } else {
                    cancellationReason = TASK_FINISHED_MARKER;
                }
            }
            // We need to call the listener outside of the synchronised section to avoid potential bottle necks
            // in the listener synchronization
            if (listener != null) {
                listener.accept(nodes);
            }

        }

        public boolean hasParent(TaskId parentTaskId) {
            return task.getParentTaskId().equals(parentTaskId);
        }

        public CancellableTask getTask() {
            return task;
        }

        public synchronized void registerChildTaskNode(String nodeId) {
            if (cancellationReason == null) {
                nodesWithChildTasks.add(nodeId);
            } else {
                throw new IllegalStateException("cannot register child task request, the task is already cancelled");
            }
        }
    }

}
