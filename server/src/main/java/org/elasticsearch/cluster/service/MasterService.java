/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.TransportMasterNodeAction;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.ClusterStateUpdateTask;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.PrioritizedRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskAwareRequest;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.tasks.TaskManager;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;
import static org.elasticsearch.core.Strings.format;

public class MasterService extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(MasterService.class);

    public static final Setting<TimeValue> MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "cluster.service.slow_master_task_logging_threshold",
        TimeValue.timeValueSeconds(10),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING = Setting.positiveTimeSetting(
        "cluster.service.master_service_starvation_logging_threshold",
        TimeValue.timeValueMinutes(5),
        Setting.Property.NodeScope
    );

    static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    public static final String STATE_UPDATE_ACTION_NAME = "publish_cluster_state_update";

    private final ClusterStateTaskExecutor<ClusterStateUpdateTask> unbatchedExecutor;

    ClusterStatePublisher clusterStatePublisher;

    private final String nodeName;

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private volatile TimeValue slowTaskLoggingThreshold;
    private final TimeValue starvationLoggingThreshold;

    protected final ThreadPool threadPool;
    private final TaskManager taskManager;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;
    private final CountedQueue[] queues;
    private final AtomicInteger totalQueueSize = new AtomicInteger();
    private volatile Batch currentlyExecutingBatch;
    private final Map<Priority, CountedQueue> queuesByPriority;
    private final LongSupplier insertionIndexSupplier = new AtomicLong()::incrementAndGet;

    private final ClusterStateUpdateStatsTracker clusterStateUpdateStatsTracker = new ClusterStateUpdateStatsTracker();

    public MasterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, TaskManager taskManager) {
        this.nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));

        this.slowTaskLoggingThreshold = MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);

        this.starvationLoggingThreshold = MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING.get(settings);

        this.threadPool = threadPool;
        this.taskManager = taskManager;

        final var queuesByPriorityBuilder = new EnumMap<Priority, CountedQueue>(Priority.class);
        final var priorities = Priority.values();
        queues = new CountedQueue[priorities.length];
        var queueIndex = 0;
        for (final var priority : priorities) {
            final var queue = new CountedQueue(priority);
            queues[queueIndex++] = queue;
            queuesByPriorityBuilder.put(priority, queue);
        }
        this.queuesByPriority = Collections.unmodifiableMap(queuesByPriorityBuilder);
        this.unbatchedExecutor = getUnbatchedExecutor();
    }

    private static ClusterStateTaskExecutor<ClusterStateUpdateTask> getUnbatchedExecutor() {
        return new ClusterStateTaskExecutor<>() {
            @Override
            @SuppressForbidden(reason = "consuming published cluster state for legacy reasons")
            public ClusterState execute(BatchExecutionContext<ClusterStateUpdateTask> batchExecutionContext) throws Exception {
                final var currentState = batchExecutionContext.initialState();
                final var taskContexts = batchExecutionContext.taskContexts();
                assert taskContexts.size() == 1 : "this only supports a single task but received " + taskContexts;
                final var taskContext = taskContexts.get(0);
                final var task = taskContext.getTask();
                final var newState = task.execute(currentState);
                if (task instanceof ClusterStateAckListener ackListener) {
                    taskContext.success(publishedState -> task.clusterStateProcessed(currentState, publishedState), ackListener);
                } else {
                    taskContext.success(publishedState -> task.clusterStateProcessed(currentState, publishedState));
                }
                return newState;
            }

            @Override
            public String describeTasks(List<ClusterStateUpdateTask> tasks) {
                return ""; // only one task, so its source is enough
            }
        };
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(ClusterStatePublisher publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(java.util.function.Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = createThreadPoolExecutor();
    }

    protected PrioritizedEsThreadPoolExecutor createThreadPoolExecutor() {
        return EsExecutors.newSinglePrioritizing(
            nodeName + "/" + MASTER_UPDATE_THREAD_NAME,
            daemonThreadFactory(nodeName, MASTER_UPDATE_THREAD_NAME),
            threadPool.getThreadContext(),
            threadPool.scheduler(),
            new MasterServiceStarvationWatcher(
                starvationLoggingThreshold.getMillis(),
                threadPool::relativeTimeInMillis,
                () -> threadPoolExecutor
            )
        );
    }

    public ClusterStateUpdateStats getClusterStateUpdateStats() {
        return clusterStateUpdateStatsTracker.getStatistics();
    }

    @Override
    protected synchronized void doStop() {
        // TODO drain queues before terminating the executor?
        ThreadPool.terminate(threadPoolExecutor, 10, TimeUnit.SECONDS);
    }

    @Override
    protected synchronized void doClose() {}

    /**
     * The current cluster state exposed by the discovery layer. Package-visible for tests.
     */
    ClusterState state() {
        return clusterStateSupplier.get();
    }

    public static boolean isMasterUpdateThread() {
        return Thread.currentThread().getName().contains('[' + MASTER_UPDATE_THREAD_NAME + ']');
    }

    public static boolean assertMasterUpdateOrTestThread() {
        return ThreadPool.assertCurrentThreadPool(MASTER_UPDATE_THREAD_NAME);
    }

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert isMasterUpdateThread() == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    private <T extends ClusterStateTaskListener> void executeAndPublishBatch(
        final ClusterStateTaskExecutor<T> executor,
        final List<ExecutionResult<T>> executionResults,
        final BatchSummary summary
    ) {
        if (lifecycle.started() == false) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            return;
        }

        logger.debug("executing cluster state update for [{}]", summary);
        final ClusterState previousClusterState = state();

        if (previousClusterState.nodes().isLocalNodeElectedMaster() == false && executor.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            for (ExecutionResult<T> executionResult : executionResults) {
                executionResult.onBatchFailure(new NotMasterException("no longer master"));
            }
            return;
        }

        final long computationStartTime = threadPool.rawRelativeTimeInMillis();
        final var newClusterState = patchVersions(
            previousClusterState,
            executeTasks(previousClusterState, executionResults, executor, summary, threadPool.getThreadContext())
        );
        // fail all tasks that have failed
        for (final var executionResult : executionResults) {
            executionResult.notifyOnFailure();
        }
        final TimeValue computationTime = getTimeSince(computationStartTime);
        logExecutionTime(computationTime, "compute cluster state update", summary);

        if (previousClusterState == newClusterState) {
            final long notificationStartTime = threadPool.rawRelativeTimeInMillis();
            for (final var executionResult : executionResults) {
                final var contextPreservingAckListener = executionResult.getContextPreservingAckListener();
                if (contextPreservingAckListener != null) {
                    // no need to wait for ack if nothing changed, the update can be counted as acknowledged
                    contextPreservingAckListener.onAckSuccess();
                }
                executionResult.onClusterStateUnchanged(newClusterState);
            }
            final TimeValue executionTime = getTimeSince(notificationStartTime);
            logExecutionTime(executionTime, "notify listeners on unchanged cluster state", summary);
            clusterStateUpdateStatsTracker.onUnchangedClusterState(computationTime.millis(), executionTime.millis());
        } else {
            try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                publishClusterStateUpdate(executor, summary, previousClusterState, executionResults, newClusterState, computationTime);
            }
        }
    }

    private <T extends ClusterStateTaskListener> void publishClusterStateUpdate(
        ClusterStateTaskExecutor<T> executor,
        BatchSummary summary,
        ClusterState previousClusterState,
        List<ExecutionResult<T>> executionResults,
        ClusterState newClusterState,
        TimeValue computationTime
    ) {
        final Task task = taskManager.register("master", STATE_UPDATE_ACTION_NAME, new TaskAwareRequest() {
            @Override
            public void setParentTask(TaskId taskId) {}

            @Override
            public TaskId getParentTask() {
                return TaskId.EMPTY_TASK_ID;
            }

            @Override
            public String getDescription() {
                return "publication of cluster state [" + newClusterState.getVersion() + "]";
            }
        });
        try {
            if (logger.isTraceEnabled()) {
                logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
            } else {
                logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
            }
            final long publicationStartTime = threadPool.rawRelativeTimeInMillis();
            try {
                final ClusterStatePublicationEvent clusterStatePublicationEvent = new ClusterStatePublicationEvent(
                    summary,
                    previousClusterState,
                    newClusterState,
                    task,
                    computationTime.millis(),
                    publicationStartTime
                );

                // new cluster state, notify all listeners
                final DiscoveryNodes.Delta nodesDelta = newClusterState.nodes().delta(previousClusterState.nodes());
                if (nodesDelta.hasChanges() && logger.isInfoEnabled()) {
                    String nodesDeltaSummary = nodesDelta.shortSummary();
                    if (nodesDeltaSummary.length() > 0) {
                        logger.info(
                            "{}, term: {}, version: {}, delta: {}",
                            summary,
                            newClusterState.term(),
                            newClusterState.version(),
                            nodesDeltaSummary
                        );
                    }
                }

                logger.debug("publishing cluster state version [{}]", newClusterState.version());
                // initialize routing nodes and the indices lookup concurrently, we will need both of them for the cluster state
                // application and can compute them while we wait for the other nodes during publication
                newClusterState.initializeAsync(threadPool.generic());
                publish(
                    clusterStatePublicationEvent,
                    new CompositeTaskAckListener(
                        executionResults.stream()
                            .map(ExecutionResult::getContextPreservingAckListener)
                            .filter(Objects::nonNull)
                            .map(
                                contextPreservingAckListener -> new TaskAckListener(
                                    contextPreservingAckListener,
                                    newClusterState.version(),
                                    newClusterState.nodes(),
                                    threadPool
                                )
                            )
                            .toList()
                    ),
                    new ActionListener<>() {
                        @Override
                        public void onResponse(Void unused) {
                            final long notificationStartTime = threadPool.rawRelativeTimeInMillis();
                            for (final var executionResult : executionResults) {
                                executionResult.onPublishSuccess(newClusterState);
                            }

                            try {
                                executor.clusterStatePublished(newClusterState);
                            } catch (Exception e) {
                                logger.error(
                                    () -> format(
                                        "exception thrown while notifying executor of new cluster state publication [%s]",
                                        summary
                                    ),
                                    e
                                );
                            }
                            final TimeValue executionTime = getTimeSince(notificationStartTime);
                            logExecutionTime(
                                executionTime,
                                "notify listeners on successful publication of cluster state (version: "
                                    + newClusterState.version()
                                    + ", uuid: "
                                    + newClusterState.stateUUID()
                                    + ')',
                                summary
                            );
                            clusterStateUpdateStatsTracker.onPublicationSuccess(
                                threadPool.rawRelativeTimeInMillis(),
                                clusterStatePublicationEvent,
                                executionTime.millis()
                            );
                        }

                        @Override
                        public void onFailure(Exception exception) {
                            if (exception instanceof FailedToCommitClusterStateException failedToCommitClusterStateException) {
                                final long notificationStartTime = threadPool.rawRelativeTimeInMillis();
                                final long version = newClusterState.version();
                                logger.warn(
                                    () -> format("failing [%s]: failed to commit cluster state version [%s]", summary, version),
                                    exception
                                );
                                for (final var executionResult : executionResults) {
                                    executionResult.onPublishFailure(failedToCommitClusterStateException);
                                }
                                final long notificationMillis = threadPool.rawRelativeTimeInMillis() - notificationStartTime;
                                clusterStateUpdateStatsTracker.onPublicationFailure(
                                    threadPool.rawRelativeTimeInMillis(),
                                    clusterStatePublicationEvent,
                                    notificationMillis
                                );
                            } else {
                                assert publicationMayFail() : exception;
                                clusterStateUpdateStatsTracker.onPublicationFailure(
                                    threadPool.rawRelativeTimeInMillis(),
                                    clusterStatePublicationEvent,
                                    0L
                                );
                                handleException(summary, publicationStartTime, newClusterState, exception);
                            }
                        }
                    }
                );
            } catch (Exception e) {
                handleException(summary, publicationStartTime, newClusterState, e);
            }
        } finally {
            taskManager.unregister(task);
        }
    }

    protected boolean publicationMayFail() {
        return false;
    }

    private TimeValue getTimeSince(long startTimeMillis) {
        return TimeValue.timeValueMillis(Math.max(0, threadPool.rawRelativeTimeInMillis() - startTimeMillis));
    }

    protected void publish(
        ClusterStatePublicationEvent clusterStatePublicationEvent,
        ClusterStatePublisher.AckListener ackListener,
        ActionListener<Void> publicationListener
    ) {
        final var fut = new PlainActionFuture<Void>() {
            @Override
            protected boolean blockingAllowed() {
                return isMasterUpdateThread() || super.blockingAllowed();
            }
        };
        clusterStatePublisher.publish(clusterStatePublicationEvent, fut, ackListener);

        ActionListener.completeWith(
            publicationListener,
            () -> FutureUtils.get(fut) // indefinitely wait for publication to complete
        );
    }

    private void handleException(BatchSummary summary, long startTimeMillis, ClusterState newClusterState, Exception e) {
        final TimeValue executionTime = getTimeSince(startTimeMillis);
        final long version = newClusterState.version();
        final String stateUUID = newClusterState.stateUUID();
        final String fullState = newClusterState.toString();
        logger.warn(
            () -> format(
                "took [%s] and then failed to publish updated cluster state (version: %s, uuid: %s) for [%s]:\n%s",
                executionTime,
                version,
                stateUUID,
                summary,
                fullState
            ),
            e
        );
        // TODO: do we want to call updateTask.onFailure here?
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterState newClusterState) {
        if (previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = incrementVersion(newClusterState);
            if (previousClusterState.routingTable() != newClusterState.routingTable()) {
                builder.routingTable(newClusterState.routingTable().withIncrementedVersion());
            }
            if (previousClusterState.metadata() != newClusterState.metadata()) {
                builder.metadata(newClusterState.metadata().withIncrementedVersion());
            }

            final var previousMetadata = newClusterState.metadata();
            newClusterState = builder.build();
            assert previousMetadata.sameIndicesLookup(newClusterState.metadata());
        }

        return newClusterState;
    }

    public Builder incrementVersion(ClusterState clusterState) {
        return ClusterState.builder(clusterState).incrementVersion();
    }

    private static class TaskTimeoutHandler extends AbstractRunnable {

        private final TimeValue timeout;
        private final String source;
        private final AtomicBoolean executed;
        private final ClusterStateTaskListener listener;

        private TaskTimeoutHandler(TimeValue timeout, String source, AtomicBoolean executed, ClusterStateTaskListener listener) {
            this.timeout = timeout;
            this.source = source;
            this.executed = executed;
            this.listener = listener;
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            completeTask(e);
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected failure executing task timeout handler", e);
            assert false : e;
            completeTask(e);
        }

        @Override
        public boolean isForceExecution() {
            return true;
        }

        @Override
        protected void doRun() {
            completeTask(new ProcessClusterEventTimeoutException(timeout, source));
        }

        private void completeTask(Exception e) {
            if (executed.compareAndSet(false, true)) {
                listener.onFailure(e);
            }
        }
    }

    /**
     * Submits an unbatched cluster state update task. This method exists for legacy reasons but is deprecated and forbidden in new
     * production code because unbatched tasks are a source of performance and stability bugs. You should instead implement your update
     * logic in a dedicated {@link ClusterStateTaskExecutor} which is reused across multiple task instances. The task itself is typically
     * just a collection of parameters consumed by the executor, together with any listeners to be notified when execution completes.
     *
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update
     */
    @Deprecated
    public void submitUnbatchedStateUpdateTask(String source, ClusterStateUpdateTask updateTask) {
        // TODO reject if not STARTED
        final var summary = new BatchSummary(source);
        final var restorableContext = threadPool.getThreadContext().newRestorableContext(true);
        final var executed = new AtomicBoolean(false);
        final Scheduler.Cancellable timeoutCancellable;
        final var timeout = updateTask.timeout();
        if (timeout != null && timeout.millis() > 0) {
            // TODO needs tests for timeout behaviour
            timeoutCancellable = threadPool.schedule(
                new TaskTimeoutHandler(timeout, source, executed, updateTask),
                timeout,
                ThreadPool.Names.GENERIC
            );
        } else {
            timeoutCancellable = null;
        }
        final long insertionIndex = insertionIndexSupplier.getAsLong();
        final long insertionTime = threadPool.relativeTimeInMillis();

        queuesByPriority.get(updateTask.priority()).execute(new Batch() {
            private volatile boolean isRunning;

            @Override
            public Stream<PendingClusterTask> getPending(long currentTimeMillis) {
                if (isTimedOut()) {
                    // TODO test that task is not shown pending after timeout
                    return Stream.of();
                }
                return Stream.of(
                    new PendingClusterTask(
                        insertionIndex, // TODO tests for insertion index of unbatched tasks
                        updateTask.priority(),
                        new Text(source),
                        currentTimeMillis - insertionTime, // TODO tests for insertion time of unbatched tasks
                        executed.get()
                    )
                );
            }

            @Override
            public int getPendingCount() {
                // TODO test that task is not counted after timeout
                return isTimedOut() ? 0 : 1;
            }

            private boolean isTimedOut() {
                return executed.get() && isRunning == false;
            }

            @Override
            public void onRejection(FailedToCommitClusterStateException e) {
                try {
                    if (acquireForExecution()) {
                        try (var ignored = restorableContext.get()) {
                            updateTask.onFailure(e);
                        }
                    }
                } catch (Exception e2) {
                    e2.addSuppressed(e);
                    logger.error(() -> format("unexpected exception failing task [%s]", source), e2);
                    assert false : e2;
                }
            }

            @Override
            public void run() {
                isRunning = true;
                try {
                    if (acquireForExecution()) {
                        executeAndPublishBatch(unbatchedExecutor, List.of(new ExecutionResult<>(updateTask, restorableContext)), summary);
                    }
                } finally {
                    isRunning = false;
                }
            }

            private boolean acquireForExecution() {
                if (executed.compareAndSet(false, true) == false) {
                    return false;
                }
                if (timeoutCancellable != null) {
                    timeoutCancellable.cancel();
                }
                return true;
            }
        });
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        final var currentTimeMillis = threadPool.relativeTimeInMillis();
        return Stream.concat(Stream.ofNullable(currentlyExecutingBatch), Arrays.stream(queues).flatMap(q -> q.queue.stream()))
            .flatMap(e -> e.getPending(currentTimeMillis))
            .toList();
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        var result = getPendingCountOrZero(currentlyExecutingBatch); // single volatile read
        for (final var queue : queues) {
            for (final var entry : queue.queue) {
                result += entry.getPendingCount();
            }
        }
        return result;
    }

    private static int getPendingCountOrZero(@Nullable Batch batch) {
        return batch == null ? 0 : batch.getPendingCount();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        // TODO AwaitsFix this doesn't give accurate answers any more
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    private void logExecutionTime(TimeValue executionTime, String activity, BatchSummary summary) {
        if (executionTime.getMillis() > slowTaskLoggingThreshold.getMillis()) {
            logger.warn(
                "took [{}/{}ms] to {} for [{}], which exceeds the warn threshold of [{}]",
                executionTime,
                executionTime.getMillis(),
                activity,
                summary,
                slowTaskLoggingThreshold
            );
        } else {
            logger.debug("took [{}] to {} for [{}]", executionTime, activity, summary);
        }
    }

    /**
     * A wrapper around a {@link ClusterStateAckListener} which restores the given thread context before delegating to the inner listener's
     * callbacks, and also logs and swallows any exceptions thrown. One of these is created for each task in the batch that passes a
     * {@link ClusterStateAckListener} to {@link ClusterStateTaskExecutor.TaskContext#success}.
     */
    private record ContextPreservingAckListener(
        ClusterStateAckListener listener,
        Supplier<ThreadContext.StoredContext> context,
        Runnable restoreResponseHeaders
    ) {

        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        public void onAckSuccess() {
            try (ThreadContext.StoredContext ignore = context.get()) {
                restoreResponseHeaders.run();
                listener.onAllNodesAcked();
            } catch (Exception inner) {
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        public void onAckFailure(@Nullable Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                restoreResponseHeaders.run();
                listener.onAckFailure(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked or failed", inner);
            }
        }

        public void onAckTimeout() {
            try (ThreadContext.StoredContext ignore = context.get()) {
                restoreResponseHeaders.run();
                listener.onAckTimeout();
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying on ack timeout", e);
            }
        }

        public TimeValue ackTimeout() {
            return listener.ackTimeout();
        }
    }

    /**
     * A wrapper around a {@link ContextPreservingAckListener} which keeps track of acks received during publication and notifies the inner
     * listener when sufficiently many have been received. One of these is created for each {@link ContextPreservingAckListener} once the
     * state for publication has been computed.
     */
    private static class TaskAckListener {

        private final ContextPreservingAckListener contextPreservingAckListener;
        private final CountDown countDown;
        private final DiscoveryNode masterNode;
        private final ThreadPool threadPool;
        private final long clusterStateVersion;
        private volatile Scheduler.Cancellable ackTimeoutCallback;
        private Exception lastFailure;

        TaskAckListener(
            ContextPreservingAckListener contextPreservingAckListener,
            long clusterStateVersion,
            DiscoveryNodes nodes,
            ThreadPool threadPool
        ) {
            this.contextPreservingAckListener = contextPreservingAckListener;
            this.clusterStateVersion = clusterStateVersion;
            this.threadPool = threadPool;
            this.masterNode = nodes.getMasterNode();
            int countDown = 0;
            for (DiscoveryNode node : nodes) {
                // we always wait for at least the master node
                if (node.equals(masterNode) || contextPreservingAckListener.mustAck(node)) {
                    countDown++;
                }
            }
            logger.trace("expecting {} acknowledgements for cluster_state update (version: {})", countDown, clusterStateVersion);
            this.countDown = new CountDown(countDown + 1); // we also wait for onCommit to be called
        }

        public void onCommit(TimeValue commitTime) {
            TimeValue ackTimeout = contextPreservingAckListener.ackTimeout();
            if (ackTimeout == null) {
                ackTimeout = TimeValue.ZERO;
            }
            final TimeValue timeLeft = TimeValue.timeValueNanos(Math.max(0, ackTimeout.nanos() - commitTime.nanos()));
            if (timeLeft.nanos() == 0L) {
                onTimeout();
            } else if (countDown.countDown()) {
                finish();
            } else {
                this.ackTimeoutCallback = threadPool.schedule(this::onTimeout, timeLeft, ThreadPool.Names.GENERIC);
                // re-check if onNodeAck has not completed while we were scheduling the timeout
                if (countDown.isCountedDown()) {
                    ackTimeoutCallback.cancel();
                }
            }
        }

        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            if (node.equals(masterNode) == false && contextPreservingAckListener.mustAck(node) == false) {
                return;
            }
            if (e == null) {
                logger.trace("ack received from node [{}], cluster_state update (version: {})", node, clusterStateVersion);
            } else {
                this.lastFailure = e;
                logger.debug(() -> format("ack received from node [%s], cluster_state update (version: %s)", node, clusterStateVersion), e);
            }

            if (countDown.countDown()) {
                finish();
            }
        }

        private void finish() {
            logger.trace("all expected nodes acknowledged cluster_state update (version: {})", clusterStateVersion);
            if (ackTimeoutCallback != null) {
                ackTimeoutCallback.cancel();
            }
            final var failure = lastFailure;
            if (failure == null) {
                contextPreservingAckListener.onAckSuccess();
            } else {
                contextPreservingAckListener.onAckFailure(failure);
            }
        }

        public void onTimeout() {
            if (countDown.fastForward()) {
                logger.trace("timeout waiting for acknowledgement for cluster_state update (version: {})", clusterStateVersion);
                contextPreservingAckListener.onAckTimeout();
            }
        }
    }

    /**
     * A wrapper around the collection of {@link TaskAckListener}s for a publication.
     */
    private record CompositeTaskAckListener(List<TaskAckListener> listeners) implements ClusterStatePublisher.AckListener {

        @Override
        public void onCommit(TimeValue commitTime) {
            for (TaskAckListener listener : listeners) {
                listener.onCommit(commitTime);
            }
        }

        @Override
        public void onNodeAck(DiscoveryNode node, @Nullable Exception e) {
            for (TaskAckListener listener : listeners) {
                listener.onNodeAck(node, e);
            }
        }
    }

    private static class ExecutionResult<T extends ClusterStateTaskListener> implements ClusterStateTaskExecutor.TaskContext<T> {
        private final T task;
        private final Supplier<ThreadContext.StoredContext> threadContextSupplier;

        @Nullable // if the task is incomplete or failed or onPublicationSuccess supplied
        Consumer<ClusterState> publishedStateConsumer;

        @Nullable // if the task is incomplete or failed or publishedStateConsumer supplied
        Runnable onPublicationSuccess;

        @Nullable // if the task is incomplete or failed or doesn't listen for acks
        ClusterStateAckListener clusterStateAckListener;

        @Nullable // if the task is incomplete or succeeded
        Exception failure;

        @Nullable
        Map<String, List<String>> responseHeaders;

        ExecutionResult(T task, Supplier<ThreadContext.StoredContext> threadContextSupplier) {
            this.task = task;
            this.threadContextSupplier = threadContextSupplier;
        }

        @Override
        public T getTask() {
            return task;
        }

        private boolean incomplete() {
            assert assertMasterUpdateOrTestThread();
            return publishedStateConsumer == null && onPublicationSuccess == null && failure == null;
        }

        // [HISTORICAL NOTE] In the past, tasks executed by the master service would automatically be notified of acks if they implemented
        // the ClusterStateAckListener interface (the interface formerly known as AckedClusterStateTaskListener). This implicit behaviour
        // was a little troublesome and was removed in favour of having the executor explicitly register an ack listener (where necessary)
        // for each task it successfully executes. Making this change carried the risk that someone might implement a new task in the future
        // which relied on the old implicit behaviour based on the interfaces that the task implements instead of the explicit behaviour in
        // the executor. We protect against this with some weird-looking assertions in the success() methods below which insist that
        // ack-listening tasks register themselves as their own ack listener. If you want to supply a different ack listener then you must
        // remove the ClusterStateAckListener interface from the task to make it clear that the task itself is not expecting to be notified
        // of acks.
        //
        // Note that the old implicit behaviour lives on in the unbatched() executor so that it can correctly execute either a
        // ClusterStateUpdateTask or an AckedClusterStateUpdateTask.

        @Override
        public void success(Runnable onPublicationSuccess) {
            assert getTask() instanceof ClusterStateAckListener == false // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must explicitly supply themselves as the ack listener";
            assert incomplete();
            this.onPublicationSuccess = Objects.requireNonNull(onPublicationSuccess);
        }

        @Override
        public void success(Consumer<ClusterState> publishListener) {
            assert getTask() instanceof ClusterStateAckListener == false // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must explicitly supply themselves as the ack listener";
            assert incomplete();
            this.publishedStateConsumer = Objects.requireNonNull(publishListener);
        }

        @Override
        public void success(Runnable onPublicationSuccess, ClusterStateAckListener clusterStateAckListener) {
            assert getTask() == clusterStateAckListener || getTask() instanceof ClusterStateAckListener == false
                // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must not supply a separate clusterStateAckListener";
            assert incomplete();
            this.onPublicationSuccess = Objects.requireNonNull(onPublicationSuccess);
            this.clusterStateAckListener = Objects.requireNonNull(clusterStateAckListener);
        }

        @Override
        public void success(Consumer<ClusterState> publishListener, ClusterStateAckListener clusterStateAckListener) {
            assert getTask() == clusterStateAckListener || getTask() instanceof ClusterStateAckListener == false
                // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must not supply a separate clusterStateAckListener";
            assert incomplete();
            this.publishedStateConsumer = Objects.requireNonNull(publishListener);
            this.clusterStateAckListener = Objects.requireNonNull(clusterStateAckListener);
        }

        @Override
        public void onFailure(Exception failure) {
            assert incomplete();
            this.failure = Objects.requireNonNull(failure);
        }

        @Override
        public Releasable captureResponseHeaders() {
            final ThreadContext threadContext = null; // TODO updateTask.getThreadContext();
            final var storedContext = threadContext.newStoredContext();
            return Releasables.wrap(() -> {
                final var newResponseHeaders = threadContext.getResponseHeaders();
                if (newResponseHeaders.isEmpty()) {
                    return;
                }
                if (responseHeaders == null) {
                    responseHeaders = new HashMap<>(newResponseHeaders);
                } else {
                    for (final var newResponseHeader : newResponseHeaders.entrySet()) {
                        responseHeaders.compute(newResponseHeader.getKey(), (ignored, oldValue) -> {
                            if (oldValue == null) {
                                return newResponseHeader.getValue();
                            }
                            return CollectionUtils.concatLists(oldValue, newResponseHeader.getValue());
                        });
                    }
                }
            }, storedContext);
        }

        private void restoreResponseHeaders() {
            if (responseHeaders != null) {
                for (final var responseHeader : responseHeaders.entrySet()) {
                    for (final var value : responseHeader.getValue()) {
                        // TODO
                        // updateTask.getThreadContext().addResponseHeader(responseHeader.getKey(), value);
                    }
                }
            }
        }

        void onBatchFailure(Exception failure) {
            // if the whole batch resulted in an exception then this overrides any task-level results whether successful or not
            this.failure = Objects.requireNonNull(failure);
            this.publishedStateConsumer = null;
            this.clusterStateAckListener = null;
        }

        void onPublishSuccess(ClusterState newClusterState) {
            if (publishedStateConsumer == null && onPublicationSuccess == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = threadContextSupplier.get()) {
                restoreResponseHeaders();
                if (onPublicationSuccess == null) {
                    publishedStateConsumer.accept(newClusterState);
                } else {
                    onPublicationSuccess.run();
                }
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying of new cluster state", e);
            }
        }

        void onClusterStateUnchanged(ClusterState clusterState) {
            if (publishedStateConsumer == null && onPublicationSuccess == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = threadContextSupplier.get()) {
                restoreResponseHeaders();
                if (onPublicationSuccess == null) {
                    publishedStateConsumer.accept(clusterState);
                } else {
                    onPublicationSuccess.run();
                }
            } catch (Exception e) {
                logger.error("exception thrown by listener while notifying of unchanged cluster state", e);
            }
        }

        void onPublishFailure(FailedToCommitClusterStateException e) {
            if (publishedStateConsumer == null && onPublicationSuccess == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = threadContextSupplier.get()) {
                restoreResponseHeaders();
                getTask().onFailure(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener notifying of failure", inner);
            }
        }

        ContextPreservingAckListener getContextPreservingAckListener() {
            assert incomplete() == false;
            if (clusterStateAckListener == null) {
                return null;
            } else {
                return new ContextPreservingAckListener(clusterStateAckListener, threadContextSupplier, this::restoreResponseHeaders);
            }
        }

        @Override
        public String toString() {
            return "ExecutionResult[" + task + "]";
        }

        void notifyOnFailure() {
            if (failure != null) {
                try (ThreadContext.StoredContext ignore = threadContextSupplier.get()) {
                    task.onFailure(failure);
                } catch (Exception inner) {
                    inner.addSuppressed(failure);
                    logger.error("exception thrown by listener notifying of failure", inner);
                }
            }
        }
    }

    private static <T extends ClusterStateTaskListener> ClusterState executeTasks(
        ClusterState previousClusterState,
        List<ExecutionResult<T>> executionResults,
        ClusterStateTaskExecutor<T> executor,
        BatchSummary summary,
        ThreadContext threadContext
    ) {
        final var resultingState = innerExecuteTasks(previousClusterState, executionResults, executor, summary, threadContext);
        if (previousClusterState != resultingState
            && previousClusterState.nodes().isLocalNodeElectedMaster()
            && (resultingState.nodes().isLocalNodeElectedMaster() == false)) {
            throw new AssertionError("update task submitted to MasterService cannot remove master");
        }
        assert assertAllTasksComplete(executionResults);
        return resultingState;
    }

    private static <T extends ClusterStateTaskListener> boolean assertAllTasksComplete(List<ExecutionResult<T>> executionResults) {
        for (final var executionResult : executionResults) {
            assert executionResult.incomplete() == false : "missing result for " + executionResult;
        }
        return true;
    }

    @SuppressWarnings("unchecked")
    private static <T extends ClusterStateTaskListener> List<ClusterStateTaskExecutor.TaskContext<T>> castTaskContexts(
        List<?> executionResults
    ) {
        // the input is unmodifiable so it is ok to cast to a more general element type
        return (List<ClusterStateTaskExecutor.TaskContext<T>>) executionResults;
    }

    private static <T extends ClusterStateTaskListener> ClusterState innerExecuteTasks(
        ClusterState previousClusterState,
        List<ExecutionResult<T>> executionResults,
        ClusterStateTaskExecutor<T> executor,
        BatchSummary summary,
        ThreadContext threadContext
    ) {
        final List<ClusterStateTaskExecutor.TaskContext<T>> taskContexts = castTaskContexts(executionResults);
        try (var ignored = threadContext.newStoredContext()) {
            // if the executor leaks a response header then this will cause a test failure, but we also store the context here to be sure
            // to avoid leaking headers in production that were missed by tests

            try {
                return executor.execute(
                    new ClusterStateTaskExecutor.BatchExecutionContext<>(
                        previousClusterState,
                        taskContexts,
                        threadContext::newStoredContext
                    )
                );
            } catch (Exception e) {
                logger.trace(
                    () -> format(
                        "failed to execute cluster state update (on version: [%s], uuid: [%s]) for [%s]\n%s%s%s",
                        previousClusterState.version(),
                        previousClusterState.stateUUID(),
                        summary,
                        previousClusterState.nodes(),
                        previousClusterState.routingTable(),
                        previousClusterState.getRoutingNodes()
                    ),
                    e
                );
                for (final var executionResult : executionResults) {
                    executionResult.onBatchFailure(e);
                }
                return previousClusterState;
            } finally {
                assert threadContext.getResponseHeaders().isEmpty()
                    : """
                        Batched task executors must marshal response headers to the appropriate task context (e.g. using \
                        TaskContext#captureResponseHeaders) or suppress them (e.g. using BatchExecutionContext#dropHeadersContext) and \
                        must not leak them to the master service, but executor ["""
                        + executor
                        + "] leaked the following headers: "
                        + threadContext.getResponseHeaders();
            }
        }
    }

    private static class MasterServiceStarvationWatcher implements PrioritizedEsThreadPoolExecutor.StarvationWatcher {

        private final long warnThreshold;
        private final LongSupplier nowMillisSupplier;
        private final Supplier<PrioritizedEsThreadPoolExecutor> threadPoolExecutorSupplier;

        // accesses of these mutable fields are synchronized (on this)
        private long lastLogMillis;
        private long nonemptySinceMillis;
        private boolean isEmpty = true;

        MasterServiceStarvationWatcher(
            long warnThreshold,
            LongSupplier nowMillisSupplier,
            Supplier<PrioritizedEsThreadPoolExecutor> threadPoolExecutorSupplier
        ) {
            this.nowMillisSupplier = nowMillisSupplier;
            this.threadPoolExecutorSupplier = threadPoolExecutorSupplier;
            this.warnThreshold = warnThreshold;
        }

        @Override
        public synchronized void onEmptyQueue() {
            isEmpty = true;
        }

        @Override
        public void onNonemptyQueue() {
            final long nowMillis = nowMillisSupplier.getAsLong();
            final long nonemptyDurationMillis;
            synchronized (this) {
                if (isEmpty) {
                    isEmpty = false;
                    nonemptySinceMillis = nowMillis;
                    lastLogMillis = nowMillis;
                    return;
                }

                if (nowMillis - lastLogMillis < warnThreshold) {
                    return;
                }

                lastLogMillis = nowMillis;
                nonemptyDurationMillis = nowMillis - nonemptySinceMillis;
            }

            final PrioritizedEsThreadPoolExecutor threadPoolExecutor = threadPoolExecutorSupplier.get();
            final TimeValue maxTaskWaitTime = threadPoolExecutor.getMaxTaskWaitTime();
            logger.warn(
                "pending task queue has been nonempty for [{}/{}ms] which is longer than the warn threshold of [{}ms];"
                    + " there are currently [{}] pending tasks, the oldest of which has age [{}/{}ms]",
                TimeValue.timeValueMillis(nonemptyDurationMillis),
                nonemptyDurationMillis,
                warnThreshold,
                threadPoolExecutor.getNumberOfPendingTasks(),
                maxTaskWaitTime,
                maxTaskWaitTime.millis()
            );
        }
    }

    private static class ClusterStateUpdateStatsTracker {

        private long unchangedTaskCount;
        private long publicationSuccessCount;
        private long publicationFailureCount;

        private long unchangedComputationElapsedMillis;
        private long unchangedNotificationElapsedMillis;

        private long successfulComputationElapsedMillis;
        private long successfulPublicationElapsedMillis;
        private long successfulContextConstructionElapsedMillis;
        private long successfulCommitElapsedMillis;
        private long successfulCompletionElapsedMillis;
        private long successfulMasterApplyElapsedMillis;
        private long successfulNotificationElapsedMillis;

        private long failedComputationElapsedMillis;
        private long failedPublicationElapsedMillis;
        private long failedContextConstructionElapsedMillis;
        private long failedCommitElapsedMillis;
        private long failedCompletionElapsedMillis;
        private long failedMasterApplyElapsedMillis;
        private long failedNotificationElapsedMillis;

        synchronized void onUnchangedClusterState(long computationElapsedMillis, long notificationElapsedMillis) {
            unchangedTaskCount += 1;
            unchangedComputationElapsedMillis += computationElapsedMillis;
            unchangedNotificationElapsedMillis += notificationElapsedMillis;
        }

        synchronized void onPublicationSuccess(
            long currentTimeMillis,
            ClusterStatePublicationEvent clusterStatePublicationEvent,
            long notificationElapsedMillis
        ) {
            publicationSuccessCount += 1;
            successfulComputationElapsedMillis += clusterStatePublicationEvent.getComputationTimeMillis();
            successfulPublicationElapsedMillis += currentTimeMillis - clusterStatePublicationEvent.getPublicationStartTimeMillis();
            successfulContextConstructionElapsedMillis += clusterStatePublicationEvent.getPublicationContextConstructionElapsedMillis();
            successfulCommitElapsedMillis += clusterStatePublicationEvent.getPublicationCommitElapsedMillis();
            successfulCompletionElapsedMillis += clusterStatePublicationEvent.getPublicationCompletionElapsedMillis();
            successfulMasterApplyElapsedMillis += clusterStatePublicationEvent.getMasterApplyElapsedMillis();
            successfulNotificationElapsedMillis += notificationElapsedMillis;
        }

        synchronized void onPublicationFailure(
            long currentTimeMillis,
            ClusterStatePublicationEvent clusterStatePublicationEvent,
            long notificationMillis
        ) {
            publicationFailureCount += 1;
            failedComputationElapsedMillis += clusterStatePublicationEvent.getComputationTimeMillis();
            failedPublicationElapsedMillis += currentTimeMillis - clusterStatePublicationEvent.getPublicationStartTimeMillis();
            failedContextConstructionElapsedMillis += clusterStatePublicationEvent.maybeGetPublicationContextConstructionElapsedMillis();
            failedCommitElapsedMillis += clusterStatePublicationEvent.maybeGetPublicationCommitElapsedMillis();
            failedCompletionElapsedMillis += clusterStatePublicationEvent.maybeGetPublicationCompletionElapsedMillis();
            failedMasterApplyElapsedMillis += clusterStatePublicationEvent.maybeGetMasterApplyElapsedMillis();
            failedNotificationElapsedMillis += notificationMillis;
        }

        synchronized ClusterStateUpdateStats getStatistics() {
            return new ClusterStateUpdateStats(
                unchangedTaskCount,
                publicationSuccessCount,
                publicationFailureCount,
                unchangedComputationElapsedMillis,
                unchangedNotificationElapsedMillis,
                successfulComputationElapsedMillis,
                successfulPublicationElapsedMillis,
                successfulContextConstructionElapsedMillis,
                successfulCommitElapsedMillis,
                successfulCompletionElapsedMillis,
                successfulMasterApplyElapsedMillis,
                successfulNotificationElapsedMillis,
                failedComputationElapsedMillis,
                failedPublicationElapsedMillis,
                failedContextConstructionElapsedMillis,
                failedCommitElapsedMillis,
                failedCompletionElapsedMillis,
                failedMasterApplyElapsedMillis,
                failedNotificationElapsedMillis
            );
        }
    }

    public static boolean isPublishFailureException(Exception e) {
        return e instanceof NotMasterException || e instanceof FailedToCommitClusterStateException;
    }

    private final Runnable queuesProcessor = new Runnable() {
        @Override
        public void run() {
            assert threadPool.getThreadContext().isSystemContext();
            assert totalQueueSize.get() > 0;
            assert currentlyExecutingBatch == null;
            try {
                takeNextItem().run();
            } catch (Exception e) {
                logger.error("unexpected exception executing queue entry", e);
                assert false : e;
            } finally {
                currentlyExecutingBatch = null;
                if (totalQueueSize.decrementAndGet() > 0) {
                    forkQueueProcessor();
                }
            }
        }

        @Override
        public String toString() {
            return "master service queue processor";
        }
    };

    private Batch takeNextItem() {
        assert totalQueueSize.get() > 0;
        assert currentlyExecutingBatch == null;
        for (final var queue : queues) {
            var item = queue.queue.poll();
            if (item != null) {
                currentlyExecutingBatch = item;
                return item;
            }
        }
        logger.error("queue processor found no items");
        assert false : "queue processor found no items";
        throw new IllegalStateException("queue processor found no items");
    }

    private void forkQueueProcessor() {
        try {
            // TODO explicitly reject if not STARTED here?
            assert totalQueueSize.get() > 0;
            final var threadContext = threadPool.getThreadContext();
            try (var ignored = threadContext.stashContext()) {
                threadContext.markAsSystemContext();
                threadPoolExecutor.execute(queuesProcessor);
            }
        } catch (Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            drainQueueOnRejection(new FailedToCommitClusterStateException("node closed", e));
        }
    }

    private void drainQueueOnRejection(FailedToCommitClusterStateException e) {
        assert totalQueueSize.get() > 0;
        do {
            final var nextItem = takeNextItem();
            try {
                nextItem.onRejection(e);
            } catch (Exception e2) {
                e2.addSuppressed(e);
                logger.error(() -> format("exception failing item on rejection [%s]", nextItem), e2);
                assert false : e2;
            } finally {
                currentlyExecutingBatch = null;
            }
        } while (totalQueueSize.decrementAndGet() > 0);
    }

    /**
     * Queue which tracks the count of items, allowing it to determine (in a threadsafe fashion) the transitions between empty and nonempty,
     * so that it can spawn an action to process its elements if and only if it's needed. This allows it to ensure that there is only ever
     * at most one active {@link #queuesProcessor}, and that there's always a pending processor if there is work to be done.
     *
     * There is one of these queues for each priority level.
     */
    private class CountedQueue {
        private final ConcurrentLinkedQueue<Batch> queue = new ConcurrentLinkedQueue<>();
        private final Priority priority;

        CountedQueue(Priority priority) {
            this.priority = priority;
        }

        void execute(Batch runner) {
            queue.add(runner);
            if (totalQueueSize.getAndIncrement() == 0) {
                forkQueueProcessor();
                // temporary fix to make sure queue remains nonempty until all tasks processed, so that getMaxTaskWaitTime and starvation
                // logging still work TODO AwaitsFix shouldn't be necessary, get rid of this
                try {
                    threadPoolExecutor.execute(new PrioritizedRunnable(Priority.LANGUID) {
                        @Override
                        public void run() {}

                        @Override
                        public String toString() {
                            return "awaitsfix thread keepalive";
                        }
                    });
                } catch (Exception e) {
                    // rejected, nbd
                }
            }
        }

        Priority priority() {
            return priority;
        }
    }

    private interface Batch {
        Stream<PendingClusterTask> getPending(long currentTimeMillis);

        int getPendingCount();

        void run();

        /**
         * @param e is a {@link FailedToCommitClusterStateException} to cause things like {@link TransportMasterNodeAction} to retry after
         *         submitting a task to a master which shut down.
         */
        // TODO maybe should be a NodeClosedException instead, but this doesn't trigger retries today.
        void onRejection(FailedToCommitClusterStateException e);
    }

    /**
     * Create a new task queue which can be used to submit tasks for execution by the master service. Tasks submitted to the same queue
     * (while the master service is otherwise busy) will be batched together into a single cluster state update. You should therefore re-use
     * each queue as much as possible.
     *
     * @param name The name of the queue, which is mostly useful for debugging.
     *
     * @param priority The priority at which tasks submitted to the queue are executed. Avoid priorites other than {@link Priority#NORMAL}
     *                 where possible. A stream of higher-priority tasks can starve lower-priority ones from running. Higher-priority tasks
     *                 should definitely re-use the same {@link MasterServiceTaskQueue} so that they are executed in batches.
     *
     * @param executor The executor which processes each batch of tasks.
     *
     * @param <T> The type of the tasks
     *
     * @return A new batching task queue.
     */
    public <T extends ClusterStateTaskListener> MasterServiceTaskQueue<T> getTaskQueue(
        String name,
        Priority priority,
        ClusterStateTaskExecutor<T> executor
    ) {
        return new BatchingTaskQueue<>(
            name,
            this::executeAndPublishBatch,
            insertionIndexSupplier,
            queuesByPriority.get(priority),
            executor,
            threadPool
        );
    }

    @FunctionalInterface
    private interface BatchConsumer<T extends ClusterStateTaskListener> {
        void runBatch(ClusterStateTaskExecutor<T> executor, List<ExecutionResult<T>> tasks, BatchSummary summary);
    }

    /**
     * Actual implementation of {@link MasterServiceTaskQueue} exposed to clients. Conceptually, each entry in each {@link CountedQueue} is
     * a {@link BatchingTaskQueue} representing a batch of tasks to be executed. Clients may add more tasks to each of these queues prior to
     * their execution.
     *
     * Works similarly to {@link CountedQueue} in that the queue size is tracked in a threadsafe fashion so that we can detect transitions
     * between empty and nonempty queues and arrange to process the queue if and only if it's nonempty. There is only ever one active
     * processor for each such queue.
     *
     * Works differently from {@link CountedQueue} in that each time the queue is processed it will drain all the pending items at once and
     * process them in a single batch.
     *
     * Also handles that tasks may time out before being processed.
     */
    private static class BatchingTaskQueue<T extends ClusterStateTaskListener> implements MasterServiceTaskQueue<T> {

        private final ConcurrentLinkedQueue<Entry<T>> queue = new ConcurrentLinkedQueue<>();
        private final ConcurrentLinkedQueue<Entry<T>> executing = new ConcurrentLinkedQueue<>(); // executing tasks are also shown in APIs
        private final AtomicInteger queueSize = new AtomicInteger();
        private final String name;
        private final BatchConsumer<T> batchConsumer;
        private final LongSupplier insertionIndexSupplier;
        private final CountedQueue countedQueue;
        private final ClusterStateTaskExecutor<T> executor;
        private final ThreadPool threadPool;
        private final Batch processor = new Processor();

        BatchingTaskQueue(
            String name,
            BatchConsumer<T> batchConsumer,
            LongSupplier insertionIndexSupplier,
            CountedQueue countedQueue,
            ClusterStateTaskExecutor<T> executor,
            ThreadPool threadPool
        ) {
            this.name = name;
            this.batchConsumer = batchConsumer;
            this.insertionIndexSupplier = insertionIndexSupplier;
            this.countedQueue = countedQueue;
            this.executor = executor;
            this.threadPool = threadPool;
        }

        @Override
        public void submitTask(String source, T task, @Nullable TimeValue timeout) {
            // TODO reject if not STARTED
            final var executed = new AtomicBoolean(false);
            final Scheduler.Cancellable timeoutCancellable;
            if (timeout != null && timeout.millis() > 0) {
                // TODO needs tests for timeout behaviour
                timeoutCancellable = threadPool.schedule(
                    new TaskTimeoutHandler(timeout, source, executed, task),
                    timeout,
                    ThreadPool.Names.GENERIC
                );
            } else {
                timeoutCancellable = null;
            }

            queue.add(
                new Entry<>(
                    source,
                    task,
                    insertionIndexSupplier.getAsLong(),
                    threadPool.relativeTimeInMillis(),
                    executed,
                    threadPool.getThreadContext().newRestorableContext(true),
                    timeoutCancellable
                )
            );

            if (queueSize.getAndIncrement() == 0) {
                countedQueue.execute(processor);
            }
        }

        @Override
        public String toString() {
            return "BatchingTaskQueue[" + name + "]";
        }

        private record Entry<T extends ClusterStateTaskListener> (
            String source,
            T task,
            long insertionIndex,
            long insertionTimeMillis,
            AtomicBoolean executed,
            Supplier<ThreadContext.StoredContext> storedContextSupplier,
            @Nullable Scheduler.Cancellable timeoutCancellable
        ) {
            boolean acquireForExecution() {
                if (executed.compareAndSet(false, true) == false) {
                    return false;
                }

                if (timeoutCancellable != null) {
                    timeoutCancellable.cancel();
                }
                return true;
            }

            void onRejection(FailedToCommitClusterStateException e) {
                if (acquireForExecution()) {
                    try (var ignored = storedContextSupplier.get()) { // TODO test for correct context here
                        task.onFailure(e);
                    } catch (Exception e2) {
                        e2.addSuppressed(e);
                        logger.error(() -> format("exception failing task [%s] on rejection", task), e2);
                        assert false : e2;
                    }
                }
            }
        }

        private class Processor implements Batch {
            @Override
            public void onRejection(FailedToCommitClusterStateException e) {
                final var items = queueSize.getAndSet(0);
                for (int i = 0; i < items; i++) {
                    final var entry = queue.poll();
                    assert entry != null;
                    entry.onRejection(e); // TODO test to verify FTCCSE here
                }
            }

            @Override
            public void run() {
                assert executing.isEmpty() : executing;
                final var entryCount = queueSize.getAndSet(0);
                var taskCount = 0;
                for (int i = 0; i < entryCount; i++) {
                    final var entry = queue.poll();
                    assert entry != null;
                    if (entry.acquireForExecution()) {
                        taskCount += 1;
                        executing.add(entry);
                    }
                }
                if (taskCount == 0) {
                    return;
                }
                final var tasks = new ArrayList<ExecutionResult<T>>(taskCount);
                final var tasksBySource = new HashMap<String, List<T>>();
                for (final var entry : executing) {
                    tasks.add(new ExecutionResult<>(entry.task(), entry.storedContextSupplier()));
                    tasksBySource.computeIfAbsent(entry.source(), ignored -> new ArrayList<>()).add(entry.task());
                }
                try {
                    batchConsumer.runBatch(executor, tasks, new BatchSummary(buildTasksDescription(taskCount, tasksBySource)));
                } finally {
                    assert executing.size() == taskCount;
                    executing.clear();
                }
            }

            private static final int MAX_TASK_DESCRIPTION_CHARS = 8 * 1024;

            private String buildTasksDescription(int taskCount, Map<String, List<T>> processTasksBySource) {
                // TODO test for how the description is grouped by source, and the behaviour when it gets too long
                // TODO make this lazy
                final var output = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit(
                    (Iterable<String>) () -> processTasksBySource.entrySet().stream().map(entry -> {
                        var tasks = executor.describeTasks(entry.getValue());
                        return tasks.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasks + "]";
                    }).filter(s -> s.isEmpty() == false).iterator(),
                    ", ",
                    "",
                    "",
                    MAX_TASK_DESCRIPTION_CHARS,
                    output
                );
                if (output.length() > MAX_TASK_DESCRIPTION_CHARS) {
                    output.append(" (").append(taskCount).append(" tasks in total)");
                }
                return output.toString();
            }

            @Override
            public Stream<PendingClusterTask> getPending(long currentTimeMillis) {
                return Stream.concat(
                    executing.stream()
                        .map(
                            entry -> new PendingClusterTask(
                                entry.insertionIndex(),
                                countedQueue.priority(),
                                new Text(entry.source()),
                                currentTimeMillis - entry.insertionTimeMillis(),
                                true
                            )
                        ),
                    queue.stream()
                        // TODO test that timed-out entries are not returned
                        .filter(entry -> entry.executed().get() == false)
                        .map(
                            entry -> new PendingClusterTask(
                                entry.insertionIndex(), // TODO tests for insertion indices
                                countedQueue.priority(),
                                new Text(entry.source()),
                                currentTimeMillis - entry.insertionTimeMillis(), // TODO tests for insertion times
                                false
                            )
                        )
                );
            }

            @Override
            public int getPendingCount() {
                int count = executing.size();
                for (final var entry : queue) {
                    if (entry.executed().get() == false) {
                        // TODO test that timed-out entries are not counted
                        count += 1;
                    }
                }
                return count;
            }

            @Override
            public String toString() {
                return "process queue for [" + name + "]";
            }
        }
    }
}
