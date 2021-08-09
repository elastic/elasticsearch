/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.tasks;

import com.carrotsearch.hppc.ObjectIntHashMap;
import com.carrotsearch.hppc.ObjectIntMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.ConcurrentMapLong;
import org.elasticsearch.common.util.concurrent.ThreadContext;
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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.elasticsearch.http.HttpTransportSettings.SETTING_HTTP_MAX_HEADER_SIZE;

/**
 * Task Manager service for keeping track of currently running tasks on the nodes
 */
public class TaskManager implements ClusterStateApplier {

    private static final Logger logger = LogManager.getLogger(TaskManager.class);

    private static final TimeValue WAIT_FOR_COMPLETION_POLL = timeValueMillis(100);

    /** Rest headers that are copied to the task */
    private final List<String> taskHeaders;
    private final ThreadPool threadPool;

    private final ConcurrentMapLong<Task> tasks = ConcurrentCollections.newConcurrentMapLongWithAggressiveConcurrency();

    private final CancellableTasksTracker<CancellableTaskHolder> cancellableTasks
        = new CancellableTasksTracker<>(new CancellableTaskHolder[0]);

    private final AtomicLong taskIdGenerator = new AtomicLong();

    private final Map<TaskId, Ban> bannedParents = new ConcurrentHashMap<>();

    private TaskResultsService taskResultsService;

    private volatile DiscoveryNodes lastDiscoveryNodes = DiscoveryNodes.EMPTY_NODES;

    private final ByteSizeValue maxHeaderSize;
    private final Map<TcpChannel, ChannelPendingTaskTracker> channelPendingTaskTrackers = ConcurrentCollections.newConcurrentMap();
    private final SetOnce<TaskCancellationService> cancellationService = new SetOnce<>();

    public TaskManager(Settings settings, ThreadPool threadPool, Set<String> taskHeaders) {
        this.threadPool = threadPool;
        this.taskHeaders = new ArrayList<>(taskHeaders);
        this.maxHeaderSize = SETTING_HTTP_MAX_HEADER_SIZE.get(settings);
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
        cancellableTasks.put(task, holder);
        // Check if this task was banned before we start it. The empty check is used to avoid
        // computing the hash code of the parent taskId as most of the time bannedParents is empty.
        if (task.getParentTaskId().isSet() && bannedParents.isEmpty() == false) {
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
     * Unregister the task
     */
    public Task unregister(Task task) {
        logger.trace("unregister task for id: {}", task.getId());
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
    }

    /**
     * Register a connection on which a child task will execute on the target connection. The returned {@link Releasable} must be called
     * to unregister the child connection once the child task is completed or failed.
     */
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
        return () -> {};
    }

    public DiscoveryNode localNode() {
        return lastDiscoveryNodes.getLocalNode();
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
        return bannedParents.size();
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
            if (channel.getVersion().onOrAfter(Version.V_7_12_0)) {
                final Ban ban = bannedParents.computeIfAbsent(parentTaskId, k -> new Ban(reason, true));
                assert ban.perChannel : "not a ban per channel";
                while (channel instanceof TaskTransportChannel) {
                    channel = ((TaskTransportChannel) channel).getChannel();
                }
                if (channel instanceof TcpTransportChannel) {
                    startTrackingChannel(((TcpTransportChannel) channel).getChannel(), ban::registerChannel);
                } else {
                    assert channel.getChannelType().equals("direct") : "expect direct channel; got [" + channel + "]";
                    ban.registerChannel(DIRECT_CHANNEL_TRACKER);
                }
            } else {
                if (lastDiscoveryNodes.nodeExists(parentTaskId.getNodeId())) {
                    // Only set the ban if the node is the part of the cluster
                    final Ban existing = bannedParents.put(parentTaskId, new Ban(reason, false));
                    assert existing == null || existing.perChannel == false : "not a ban per node";
                }
            }
        }
        return cancellableTasks.getByParent(parentTaskId).map(t -> t.task).collect(Collectors.toList());
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
        final boolean perChannel; // TODO: Remove this in 8.0
        final Set<ChannelPendingTaskTracker> channels;

        Ban(String reason, boolean perChannel) {
            assert Thread.holdsLock(bannedParents);
            this.reason = reason;
            this.perChannel = perChannel;
            if (perChannel) {
                this.channels = new HashSet<>();
            } else {
                this.channels = Collections.emptySet();
            }
        }

        void registerChannel(ChannelPendingTaskTracker channel) {
            assert Thread.holdsLock(bannedParents);
            assert perChannel : "not a ban per channel";
            channels.add(channel);
        }

        boolean unregisterChannel(ChannelPendingTaskTracker channel) {
            assert Thread.holdsLock(bannedParents);
            assert perChannel : "not a ban per channel";
            return channels.remove(channel);
        }

        int registeredChannels() {
            assert Thread.holdsLock(bannedParents);
            assert perChannel : "not a ban per channel";
            return channels.size();
        }

        @Override
        public String toString() {
            return "Ban{" + "reason='" + reason + '\'' + ", perChannel=" + perChannel + ", channels=" + channels + '}';
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
        if (event.nodesRemoved()) {
            synchronized (bannedParents) {
                lastDiscoveryNodes = event.state().getNodes();
                // Remove all bans that were registered by nodes that are no longer in the cluster state
                final Iterator<Map.Entry<TaskId, Ban>> banIterator = bannedParents.entrySet().iterator();
                while (banIterator.hasNext()) {
                    final Map.Entry<TaskId, Ban> ban = banIterator.next();
                    if (ban.getValue().perChannel == false && lastDiscoveryNodes.nodeExists(ban.getKey().getNodeId()) == false) {
                        logger.debug("Removing ban for the parent [{}] on the node [{}], reason: the parent node is gone",
                            ban.getKey(), event.state().getNodes().getLocalNode());
                        banIterator.remove();
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
        private final CancellableTask task;
        private boolean finished = false;
        private List<Runnable> cancellationListeners = null;
        private ObjectIntMap<Transport.Connection> childTasksPerConnection = null;
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
                if (cancellationListeners != null) {
                    listeners = cancellationListeners;
                    cancellationListeners = null;
                } else {
                    listeners = Collections.emptyList();
                }
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

        public boolean hasParent(TaskId parentTaskId) {
            return task.getParentTaskId().equals(parentTaskId);
        }

        public CancellableTask getTask() {
            return task;
        }

        synchronized void registerChildConnection(Transport.Connection connection) {
            if (banChildrenReason != null) {
                throw new TaskCancelledException("parent task was cancelled [" + banChildrenReason + ']');
            }
            if (childTasksPerConnection == null) {
                childTasksPerConnection = new ObjectIntHashMap<>();
            }
            childTasksPerConnection.addTo(connection, 1);
        }

        void unregisterChildConnection(Transport.Connection node) {
            final List<Runnable> listeners;
            synchronized (this) {
                if (childTasksPerConnection.addTo(node, -1) == 0) {
                    childTasksPerConnection.remove(node);
                }
                if (childTasksPerConnection.isEmpty() && this.childTaskCompletedListeners != null) {
                    listeners = childTaskCompletedListeners;
                    childTaskCompletedListeners = null;
                } else {
                    listeners = Collections.emptyList();
                }
            }
            notifyListeners(listeners);
        }

        Set<Transport.Connection> startBan(String reason, Runnable onChildTasksCompleted) {
            final Set<Transport.Connection> pendingChildConnections;
            final Runnable toRun;
            synchronized (this) {
                assert reason != null;
                //noinspection ConstantConditions just in case we get a null value with assertions disabled
                banChildrenReason = reason == null ? "none" : reason;
                if (childTasksPerConnection == null) {
                    pendingChildConnections = Collections.emptySet();
                } else {
                    pendingChildConnections = StreamSupport.stream(childTasksPerConnection.spliterator(), false)
                        .map(e -> e.key).collect(Collectors.toSet());
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
            channel.addCloseListener(ActionListener.wrap(
                r -> {
                    final ChannelPendingTaskTracker removedTracker = channelPendingTaskTrackers.remove(channel);
                    assert removedTracker == tracker;
                    onChannelClosed(tracker);
                },
                e -> {
                    assert false : new AssertionError("must not be here", e);
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
            assert removed : "task " + task.getId() + " is not in the pending list";
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
                        cancelTaskAndDescendants(task, "channel was closed", false, ActionListener.wrap(() -> {}));
                    }
                }
            });
        }

        // Unregister the closing channel and remove bans whose has no registered channels
        synchronized (bannedParents) {
            final Iterator<Map.Entry<TaskId, Ban>> iterator = bannedParents.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<TaskId, Ban> entry = iterator.next();
                final Ban ban = entry.getValue();
                if (ban.perChannel) {
                    if (ban.unregisterChannel(channel) && entry.getValue().registeredChannels() == 0) {
                        iterator.remove();
                    }
                }
            }
        }
    }

    public void cancelTaskAndDescendants(CancellableTask task, String reason, boolean waitForCompletion, ActionListener<Void> listener) {
        final TaskCancellationService service = cancellationService.get();
        if (service != null) {
            service.cancelTaskAndDescendants(task, reason, waitForCompletion, listener);
        } else {
            assert false : "TaskCancellationService is not initialized";
            throw new IllegalStateException("TaskCancellationService is not initialized");
        }
    }
}
