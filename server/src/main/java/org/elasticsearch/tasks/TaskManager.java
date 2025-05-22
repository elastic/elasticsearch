/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.support.TransportAction;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Assertions;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.telemetry.tracing.Tracer;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TaskTransportChannel;
import org.elasticsearch.transport.TcpChannel;
import org.elasticsearch.transport.TcpTransportChannel;
import org.elasticsearch.transport.Transport;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    /** Rest headers that are copied to the task */
    private final Set<String> taskHeaders;
    private final ThreadPool threadPool;

    private final Map<Long, Task> tasks = ConcurrentCollections.newConcurrentMapWithAggressiveConcurrency();

    private final CancellableTasksTracker<CancellableTaskHolder> cancellableTasks = new CancellableTasksTracker<>();

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, Ban> bannedParents = new ConcurrentHashMap<>();

    private TaskResultsService taskResultsService;

    private DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

    private final Tracer tracer;

    private final ByteSizeValue maxHeaderSize;
    private final Map<TcpChannel, ChannelPendingTaskTracker> channelPendingTaskTrackers = ConcurrentCollections.newConcurrentMap();
    private final SetOnce<TaskCancellationService> cancellationService = new SetOnce<>();

    private final List<RemovedTaskListener> removedTaskListeners = new CopyOnWriteArrayList<>();

    // For testing
    public TaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        this(settings, threadPool, taskHeaders, Tracer.NOOP);
    }

    public TaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders, Tracer tracer) {
        this.threadPool = threadPool;
        this.taskHeaders = Set.copyOf(taskHeaders);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
        this.tracer = tracer;
    }

    public void setTaskResultsService(TaskResultsService taskResultsService) {
        assert this.taskResultsService == null;
        this.taskResultsService = taskResultsService;
    }

    public void setTaskCancellationService(TaskCancellationService taskCancellationService) {
        this.cancellationService.set(taskCancellationService);
    }

    /**
     * Registers a task without parent task
     */
    public Task register(String type, String action, TaskAwareRequest request) {
        return register(type, action, request, true);
    }

    /**
     * Registers a task without a parent task, and specifies whether to trace the request. You should prefer
     * to call {@link #register(String, String, TaskAwareRequest)}, since it is rare to want to avoid
     * tracing a task.
     */
    public Task register(String type, String action, TaskAwareRequest request, boolean traceRequest) {
        Map<String, String> headers = new HashMap<>();
        long headerSize = 0;
        long maxSize = maxHeaderSize.getBytes();
        ThreadContext threadContext = threadPool.getThreadContext();

        assert threadContext.hasTraceContext() == false : "Expected threadContext to have no traceContext fields";

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
            registerCancellableTask(task, request.getRequestId(), traceRequest);
        } else {
            Task previousTask = tasks.put(task.getId(), task);
            assert previousTask == null;
            if (traceRequest) {
                startTrace(threadContext, task);
            }
        }
        return task;
    }

    // package private for testing
    void startTrace(ThreadContext threadContext, Task task) {
        TaskId parentTask = task.getParentTaskId();
        Map<String, Object> attributes = Map.of(
            Tracer.AttributeKeys.TASK_ID,
            task.getId(),
            Tracer.AttributeKeys.PARENT_TASK_ID,
            parentTask.toString()
        );
        tracer.startTrace(threadContext, task, task.getAction(), attributes);
    }

    public <Request extends ActionRequest, Response extends ActionResponse> Task registerAndExecute(
        String type,
        TransportAction<Request, Response> action,
        Request request,
        Transport.Connection localConnection,
        ActionListener<Response> taskListener
    ) {
        final Releasable unregisterChildNode;
        if (request.getParentTask().isSet()) {
            unregisterChildNode = registerChildConnection(request.getParentTask().getId(), localConnection);
        } else {
            unregisterChildNode = null;
        }

        try (var ignored = threadPool.getThreadContext().newTraceContext()) {
            final Task task;
            try {
                task = register(type, action.actionName, request);
            } catch (TaskCancelledException e) {
                Releasables.close(unregisterChildNode);
                throw e;
            }
            action.execute(task, request, new ActionListener<>() {
                @Override
                public void onResponse(Response response) {
                    try {
                        release();
                    } finally {
                        taskListener.onResponse(response);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        if (request.getParentTask().isSet()) {
                            cancelChildLocal(request.getParentTask(), request.getRequestId(), e.toString());
                        }
                        release();
                    } finally {
                        taskListener.onFailure(e);
                    }
                }

                @Override
                public String toString() {
                    return this.getClass().getName() + "{" + taskListener + "}{" + task + "}";
                }

                private void release() {
                    Releasables.close(unregisterChildNode, () -> unregister(task));
                }
            });
            return task;
        }
    }

    private void registerCancellableTask(Task task, long requestId, boolean traceRequest) {
        CancellableTask cancellableTask = (CancellableTask) task;
        CancellableTaskHolder holder = new CancellableTaskHolder(cancellableTask);
        cancellableTasks.put(task, requestId, holder);
        if (traceRequest) {
            startTrace(threadPool.getThreadContext(), task);
        }
        // Check if this task was banned before we start it.
        if (task.getParentTaskId().isSet()) {
            final Ban ban = bannedParents.get(task.getParentTaskId());
            if (ban != null) {
                try {
                    holder.cancel(ban.reason);
                    throw new TaskCancelledException("task cancelled before starting [" + ban.reason + ']');
                } finally {
                    // let's clean up the registration
                    unregister(task);
                }
            }
        }
    }

    private TaskCancellationService getCancellationService() {
        final TaskCancellationService service = cancellationService.get();
        if (service != null) {
            return service;
        } else {
            assert false : "TaskCancellationService is not initialized";
            throw new IllegalStateException("TaskCancellationService is not initialized");
        }
    }

    /**
     * Cancels a task
     * <p>
     * After starting cancellation on the parent task, the task manager tries to cancel all children tasks
     * of the current task. Once cancellation of the children tasks is done, the listener is triggered.
     * If the task is completed or unregistered from TaskManager, then the listener is called immediately.
     */
    public void cancel(CancellableTask task, String reason, Runnable listener) {
        CancellableTaskHolder holder = cancellableTasks.get(task.getId());
        if (holder != null) {
            logger.trace("cancelling task with id {}", task.getId());
            holder.cancel(reason, listener);
        } else {
            listener.run();
        }
    }

    /**
     * Cancels children tasks of the specified parent, with the request ID specified, as long as the request ID is positive.
     *
     * Note: There may be multiple children for the same request ID. In this edge case all these multiple children are cancelled.
     */
    public void cancelChildLocal(TaskId parentTaskId, long childRequestId, String reason) {
        if (childRequestId > 0) {
            List<CancellableTaskHolder> children = cancellableTasks.getChildrenByRequestId(parentTaskId, childRequestId).toList();
            if (children.isEmpty() == false) {
                for (CancellableTaskHolder child : children) {
                    if (logger.isTraceEnabled()) {
                        logger.trace(
                            "cancelling child task [{}] of parent task [{}] and request ID [{}] with reason [{}]",
                            child.getTask(),
                            parentTaskId,
                            childRequestId,
                            reason
                        );
                    }
                    child.cancel(reason);
                }
            }
        }
    }

    /**
     * Send an Action to cancel children tasks of the specified parent, with the request ID specified.
     *
     * Note: There may be multiple children for the same request ID. In this edge case all these multiple children are cancelled.
     */
    public void cancelChildRemote(TaskId parentTask, long childRequestId, Transport.Connection childConnection, String reason) {
        getCancellationService().cancelChildRemote(parentTask, childRequestId, childConnection, reason);
    }

    /**
     * Unregister the task
     */
    public Task unregister(Task task) {
        logger.trace("unregister task for id: {}", task.getId());
        try {
            if (task instanceof CancellableTask) {
                CancellableTaskHolder holder = cancellableTasks.remove(task);
                if (holder != null) {
                    holder.finish();
                    assert holder.task == task;
                    return holder.getTask();
                } else {
                    return null;
                }
            } else {
                final Task removedTask = tasks.remove(task.getId());
                assert removedTask == null || removedTask == task;
                return removedTask;
            }
        } finally {
            tracer.stopTrace(task);
            for (RemovedTaskListener listener : removedTaskListeners) {
                listener.onRemoved(task);
            }
        }
    }

    public void registerRemovedTaskListener(RemovedTaskListener removedTaskListener) {
        removedTaskListeners.add(removedTaskListener);
    }

    public void unregisterRemovedTaskListener(RemovedTaskListener removedTaskListener) {
        removedTaskListeners.remove(removedTaskListener);
    }

    /**
     * Register a connection on which a child task will execute on the target connection. The returned {@link Releasable} must be called
     * to unregister the child connection once the child task is completed or failed.
     *
     * @return Releasable that must be closed once the child task completes or {@code null} if no cancellable task for the given id exists
     */
    @Nullable
    public Releasable registerChildConnection(long taskId, Transport.Connection childConnection) {
        assert TransportService.unwrapConnection(childConnection) == childConnection : "Child connection must be unwrapped";
        final CancellableTaskHolder holder = cancellableTasks.get(taskId);
        if (holder != null) {
            logger.trace("register child connection [{}] task [{}]", childConnection, taskId);
            holder.registerChildConnection(childConnection);
            return Releasables.releaseOnce(() -> {
                logger.trace("unregister child connection [{}] task [{}]", childConnection, taskId);
                holder.unregisterChildConnection(childConnection);
            });
        }
        return null;
    }

    // package private for testing
    Integer childTasksPerConnection(long taskId, Transport.Connection childConnection) {
        final CancellableTaskHolder holder = cancellableTasks.get(taskId);
        if (holder != null) {
            return holder.childTasksPerConnection.get(childConnection);
        }
        return null;
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
            logger.warn(() -> format("couldn't store error %s", ExceptionsHelper.stackTrace(error)), ex);
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
                logger.warn(() -> format("couldn't store error %s", ExceptionsHelper.stackTrace(error)), e);
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
            logger.warn(() -> format("couldn't store response %s", response), ex);
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
                logger.warn(() -> format("couldn't store response %s", response), e);
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
     * Bans all tasks with the specified parent task from execution, cancels all tasks that are currently executing.
     * <p>
     * This method is called when a parent task that has children is cancelled.
     * @return a list of pending cancellable child tasks
     */
    public List<CancellableTask> setBan(TaskId parentTaskId, String reason, TransportChannel channel) {
        logger.trace("setting ban for the parent task {} {}", parentTaskId, reason);
        synchronized (bannedParents) {
            final Ban ban = bannedParents.computeIfAbsent(parentTaskId, k -> new Ban(reason));
            while (channel instanceof TaskTransportChannel) {
                channel = ((TaskTransportChannel) channel).getChannel();
            }
            if (channel instanceof TcpTransportChannel) {
                startTrackingChannel(((TcpTransportChannel) channel).getChannel(), ban::registerChannel);
            } else {
                assert TransportService.isDirectResponseChannel(channel) : "expect direct channel; got [" + channel + "]";
                ban.registerChannel(DIRECT_CHANNEL_TRACKER);
            }
        }
        return cancellableTasks.getByParent(parentTaskId).map(t -> t.task).toList();
    }

    /**
     * Removes the ban for the specified parent task.
     * <p>
     * This method is called when a previously banned task finally cancelled
     */
    public void removeBan(TaskId parentTaskId) {
        logger.trace("removing ban for the parent task {}", parentTaskId);
        bannedParents.remove(parentTaskId);
    }

    // for testing
    public Set<TaskId> getBannedTaskIds() {
        return Collections.unmodifiableSet(bannedParents.keySet());
    }

    // for testing
    public boolean assertCancellableTaskConsistency() {
        return cancellableTasks.assertConsistent();
    }

    private class Ban {
        final String reason;
        final Set<ChannelPendingTaskTracker> channels;

        Ban(String reason) {
            assert Thread.holdsLock(bannedParents);
            this.reason = reason;
            this.channels = new HashSet<>();
        }

        void registerChannel(ChannelPendingTaskTracker channel) {
            assert Thread.holdsLock(bannedParents);
            channels.add(channel);
        }

        boolean unregisterChannel(ChannelPendingTaskTracker channel) {
            assert Thread.holdsLock(bannedParents);
            return channels.remove(channel);
        }

        int registeredChannels() {
            assert Thread.holdsLock(bannedParents);
            return channels.size();
        }

        @Override
        public String toString() {
            return "Ban{" + "reason=" + reason + ", channels=" + channels + '}';
        }
    }

    /**
     * Start rejecting new child requests as the parent task was cancelled.
     *
     * @param taskId                the parent task id
     * @param onChildTasksCompleted called when all child tasks are completed or failed
     * @return a set of current connections that have outstanding child tasks
     */
    public Collection<Transport.Connection> startBanOnChildTasks(long taskId, String reason, Runnable onChildTasksCompleted) {
        final CancellableTaskHolder holder = cancellableTasks.get(taskId);
        if (holder != null) {
            return holder.startBan(reason, onChildTasksCompleted);
        } else {
            onChildTasksCompleted.run();
            return Collections.emptySet();
        }
    }

    @Override
    public void applyClusterState(ClusterChangedEvent event) {
        lastDiscoveryNodes = event.state().getNodes();
    }

    private static class CancellableTaskHolder {
        private final CancellableTask task;
        private boolean finished = false;
        private List<Runnable> cancellationListeners = null;
        private Map<Transport.Connection, Integer> childTasksPerConnection = null;
        private String banChildrenReason;
        private List<Runnable> childTaskCompletedListeners = null;

        CancellableTaskHolder(CancellableTask task) {
            this.task = task;
        }

        void cancel(String reason, Runnable listener) {
            final Runnable toRun;
            synchronized (this) {
                if (finished) {
                    assert cancellationListeners == null;
                    toRun = listener;
                } else {
                    toRun = () -> {};
                    if (listener != null) {
                        if (cancellationListeners == null) {
                            cancellationListeners = new ArrayList<>();
                        }
                        cancellationListeners.add(listener);
                    }
                }
            }
            try {
                task.cancel(reason);
            } finally {
                if (toRun != null) {
                    toRun.run();
                }
            }
        }

        void cancel(String reason) {
            task.cancel(reason);
        }

        /**
         * Marks task as finished.
         */
        public void finish() {
            final List<Runnable> listeners;
            synchronized (this) {
                this.finished = true;
                if (cancellationListeners == null) {
                    return;
                }
                listeners = cancellationListeners;
                cancellationListeners = null;
            }
            // We need to call the listener outside of the synchronised section to avoid potential bottle necks
            // in the listener synchronization
            notifyListeners(listeners);
        }

        private void notifyListeners(List<Runnable> listeners) {
            assert Thread.holdsLock(this) == false;
            Exception rootException = null;
            for (Runnable listener : listeners) {
                try {
                    listener.run();
                } catch (RuntimeException inner) {
                    rootException = ExceptionsHelper.useOrSuppress(rootException, inner);
                }
            }
            ExceptionsHelper.reThrowIfNotNull(rootException);
        }

        public CancellableTask getTask() {
            return task;
        }

        synchronized void registerChildConnection(Transport.Connection connection) {
            if (banChildrenReason != null) {
                throw new TaskCancelledException("parent task was cancelled [" + banChildrenReason + ']');
            }
            if (childTasksPerConnection == null) {
                childTasksPerConnection = new HashMap<>();
            }
            childTasksPerConnection.merge(connection, 1, Integer::sum);
        }

        void unregisterChildConnection(Transport.Connection node) {
            final List<Runnable> listeners;
            synchronized (this) {
                if (childTasksPerConnection.merge(node, -1, Integer::sum) == 0) {
                    childTasksPerConnection.remove(node);
                }
                if (childTasksPerConnection.isEmpty() == false || this.childTaskCompletedListeners == null) {
                    return;
                }
                listeners = childTaskCompletedListeners;
                childTaskCompletedListeners = null;
            }
            notifyListeners(listeners);
        }

        Set<Transport.Connection> startBan(String reason, Runnable onChildTasksCompleted) {
            final Set<Transport.Connection> pendingChildConnections;
            final Runnable toRun;
            synchronized (this) {
                assert reason != null;
                // noinspection ConstantConditions just in case we get a null value with assertions disabled
                banChildrenReason = reason == null ? "none" : reason;
                if (childTasksPerConnection == null) {
                    pendingChildConnections = Collections.emptySet();
                } else {
                    pendingChildConnections = Set.copyOf(childTasksPerConnection.keySet());
                }
                if (pendingChildConnections.isEmpty()) {
                    assert childTaskCompletedListeners == null;
                    toRun = onChildTasksCompleted;
                } else {
                    toRun = () -> {};
                    if (childTaskCompletedListeners == null) {
                        childTaskCompletedListeners = new ArrayList<>();
                    }
                    childTaskCompletedListeners.add(onChildTasksCompleted);
                }
            }
            toRun.run();
            return pendingChildConnections;
        }
    }

    /**
     * Start tracking a cancellable task with its tcp channel, so if the channel gets closed we can get a set of
     * pending tasks associated that channel and cancel them as these results won't be retrieved by the parent task.
     *
     * @return a releasable that should be called when this pending task is completed
     */
    public Releasable startTrackingCancellableChannelTask(TcpChannel channel, CancellableTask task) {
        assert cancellableTasks.get(task.getId()) != null : "task [" + task.getId() + "] is not registered yet";
        final ChannelPendingTaskTracker tracker = startTrackingChannel(channel, trackerChannel -> trackerChannel.addTask(task));
        return () -> tracker.removeTask(task);
    }

    private ChannelPendingTaskTracker startTrackingChannel(TcpChannel channel, Consumer<ChannelPendingTaskTracker> onRegister) {
        final ChannelPendingTaskTracker tracker = channelPendingTaskTrackers.compute(channel, (k, curr) -> {
            if (curr == null) {
                curr = new ChannelPendingTaskTracker();
            }
            onRegister.accept(curr);
            return curr;
        });
        if (tracker.registered.compareAndSet(false, true)) {
            channel.addCloseListener(ActionListener.running(() -> {
                final ChannelPendingTaskTracker removedTracker = channelPendingTaskTrackers.remove(channel);
                assert removedTracker == tracker;
                onChannelClosed(tracker);
            }));
        }
        return tracker;
    }

    // for testing
    final int numberOfChannelPendingTaskTrackers() {
        return channelPendingTaskTrackers.size();
    }

    private static final ChannelPendingTaskTracker DIRECT_CHANNEL_TRACKER = new ChannelPendingTaskTracker();

    private static class ChannelPendingTaskTracker {
        final AtomicBoolean registered = new AtomicBoolean();
        final Semaphore permits = Assertions.ENABLED ? new Semaphore(Integer.MAX_VALUE) : null;
        final Set<CancellableTask> pendingTasks = ConcurrentCollections.newConcurrentSet();

        void addTask(CancellableTask task) {
            assert permits.tryAcquire() : "tracker was drained";
            final boolean added = pendingTasks.add(task);
            assert added : "task " + task.getId() + " is in the pending list already";
            assert releasePermit();
        }

        boolean acquireAllPermits() {
            permits.acquireUninterruptibly(Integer.MAX_VALUE);
            return true;
        }

        boolean releasePermit() {
            permits.release();
            return true;
        }

        Set<CancellableTask> drainTasks() {
            assert acquireAllPermits(); // do not release permits so we can't add tasks to this tracker after draining
            return Collections.unmodifiableSet(pendingTasks);
        }

        void removeTask(CancellableTask task) {
            final boolean removed = pendingTasks.remove(task);
            assert removed : "task is not in the pending list: " + task;
        }
    }

    private void onChannelClosed(ChannelPendingTaskTracker channel) {
        final Set<CancellableTask> tasks = channel.drainTasks();
        if (tasks.isEmpty() == false) {
            threadPool.generic().execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    logger.warn("failed to cancel tasks on channel closed", e);
                }

                @Override
                protected void doRun() {
                    for (CancellableTask task : tasks) {
                        cancelTaskAndDescendants(task, "channel was closed", false, ActionListener.noop());
                    }
                }
            });
        }

        // Unregister the closing channel and remove bans whose has no registered channels
        synchronized (bannedParents) {
            bannedParents.values().removeIf(ban -> ban.unregisterChannel(channel) && ban.registeredChannels() == 0);
        }
    }

    public void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        getCancellationService().cancelTaskAndDescendants(task, reason, waitForCompletion, listener);
    }

    public Set<String> getTaskHeaders() {
        return taskHeaders;
    }
}
