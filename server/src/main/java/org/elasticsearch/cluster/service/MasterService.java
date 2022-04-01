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
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterState.Builder;
import org.elasticsearch.cluster.ClusterStateAckListener;
import org.elasticsearch.cluster.ClusterStatePublicationEvent;
import org.elasticsearch.cluster.ClusterStateTaskConfig;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.ClusterStateTaskListener;
import org.elasticsearch.cluster.coordination.ClusterStatePublisher;
import org.elasticsearch.cluster.coordination.FailedToCommitClusterStateException;
import org.elasticsearch.cluster.metadata.ProcessClusterEventTimeoutException;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.common.util.concurrent.PrioritizedEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.Node;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

import static org.elasticsearch.common.util.concurrent.EsExecutors.daemonThreadFactory;

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

    ClusterStatePublisher clusterStatePublisher;

    private final String nodeName;

    private java.util.function.Supplier<ClusterState> clusterStateSupplier;

    private volatile TimeValue slowTaskLoggingThreshold;
    private final TimeValue starvationLoggingThreshold;

    protected final ThreadPool threadPool;

    private volatile PrioritizedEsThreadPoolExecutor threadPoolExecutor;
    private volatile Batcher taskBatcher;

    private final ClusterStateUpdateStatsTracker clusterStateUpdateStatsTracker = new ClusterStateUpdateStatsTracker();

    public MasterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool) {
        this.nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));

        this.slowTaskLoggingThreshold = MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);

        this.starvationLoggingThreshold = MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING.get(settings);

        this.threadPool = threadPool;
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
        taskBatcher = new Batcher(logger, threadPoolExecutor);
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

    @SuppressWarnings("unchecked")
    class Batcher extends TaskBatcher {

        Batcher(Logger logger, PrioritizedEsThreadPoolExecutor threadExecutor) {
            super(logger, threadExecutor);
        }

        @Override
        protected void onTimeout(BatchedTask task, TimeValue timeout) {
            threadPool.generic()
                .execute(() -> ((UpdateTask) task).onFailure(new ProcessClusterEventTimeoutException(timeout, task.source)));
        }

        @Override
        protected void run(Object batchingKey, List<? extends BatchedTask> tasks, String tasksSummary) {
            runTasks((ClusterStateTaskExecutor<ClusterStateTaskListener>) batchingKey, (List<UpdateTask>) tasks, tasksSummary);
        }

        class UpdateTask extends BatchedTask {
            private final ClusterStateTaskListener listener;
            private final Supplier<ThreadContext.StoredContext> threadContextSupplier;

            UpdateTask(
                Priority priority,
                String source,
                ClusterStateTaskListener task,
                Supplier<ThreadContext.StoredContext> threadContextSupplier,
                ClusterStateTaskExecutor<?> executor
            ) {
                super(priority, source, executor, task);
                this.threadContextSupplier = threadContextSupplier;
                this.listener = task;
            }

            @Override
            public String describeTasks(List<? extends BatchedTask> tasks) {
                return ((ClusterStateTaskExecutor<ClusterStateTaskListener>) batchingKey).describeTasks(
                    tasks.stream().map(task -> (ClusterStateTaskListener) task.task).toList()
                );
            }

            public void onFailure(Exception e) {
                try (ThreadContext.StoredContext ignore = threadContextSupplier.get()) {
                    listener.onFailure(e);
                } catch (Exception inner) {
                    inner.addSuppressed(e);
                    logger.error("exception thrown by listener notifying of failure", inner);
                }
            }

            public void onNoLongerMaster() {
                try (ThreadContext.StoredContext ignore = threadContextSupplier.get()) {
                    listener.onNoLongerMaster();
                } catch (Exception e) {
                    logger.error("exception thrown by listener while notifying no longer master", e);
                }
            }

            @Nullable
            public ContextPreservingAckListener wrapInTaskContext(@Nullable ClusterStateAckListener clusterStateAckListener) {
                return clusterStateAckListener == null
                    ? null
                    : new ContextPreservingAckListener(Objects.requireNonNull(clusterStateAckListener), threadContextSupplier);
            }
        }
    }

    @Override
    protected synchronized void doStop() {
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

    public static boolean assertNotMasterUpdateThread(String reason) {
        assert isMasterUpdateThread() == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be the master service thread. Reason: [" + reason + "]";
        return true;
    }

    private void runTasks(
        ClusterStateTaskExecutor<ClusterStateTaskListener> executor,
        List<Batcher.UpdateTask> updateTasks,
        String summary
    ) {
        if (lifecycle.started() == false) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            return;
        }

        logger.debug("executing cluster state update for [{}]", summary);
        final ClusterState previousClusterState = state();

        if (previousClusterState.nodes().isLocalNodeElectedMaster() == false && executor.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            updateTasks.forEach(Batcher.UpdateTask::onNoLongerMaster);
            return;
        }

        final long computationStartTime = threadPool.rawRelativeTimeInMillis();
        final var executionResults = updateTasks.stream().map(ExecutionResult::new).toList();
        final var newClusterState = patchVersions(
            previousClusterState,
            executeTasks(previousClusterState, executionResults, executor, summary)
        );
        // fail all tasks that have failed
        for (final var executionResult : executionResults) {
            if (executionResult.failure != null) {
                executionResult.updateTask.onFailure(executionResult.failure);
            }
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
                                    () -> new ParameterizedMessage(
                                        "exception thrown while notifying executor of new cluster state publication [{}]",
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
                                    () -> new ParameterizedMessage(
                                        "failing [{}]: failed to commit cluster state version [{}]",
                                        summary,
                                        version
                                    ),
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
                                assert false : exception;
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
        }
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

    private void handleException(String summary, long startTimeMillis, ClusterState newClusterState, Exception e) {
        final TimeValue executionTime = getTimeSince(startTimeMillis);
        final long version = newClusterState.version();
        final String stateUUID = newClusterState.stateUUID();
        final String fullState = newClusterState.toString();
        logger.warn(
            new ParameterizedMessage(
                "took [{}] and then failed to publish updated cluster state (version: {}, uuid: {}) for [{}]:\n{}",
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

    /**
     * Submits a cluster state update task
     * @param source     the source of the cluster state update task
     * @param updateTask the full context for the cluster state update, which implements {@link ClusterStateTaskListener} so that it is
     *                   notified when it is executed.
     * @param executor   the executor for the task; tasks that share the same executor instance may be batched together
     *
     */
    public <T extends ClusterStateTaskConfig & ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        T updateTask,
        ClusterStateTaskExecutor<T> executor
    ) {
        submitStateUpdateTask(source, updateTask, updateTask, executor);
    }

    /**
     * Submits a cluster state update task; submitted updates will be
     * batched across the same instance of executor. The exact batching
     * semantics depend on the underlying implementation but a rough
     * guideline is that if the update task is submitted while there
     * are pending update tasks for the same executor, these update
     * tasks will all be executed on the executor in a single batch
     *
     * @param source   the source of the cluster state update task
     * @param task     the state needed for the cluster state update task, which implements {@link ClusterStateTaskListener} so that it is
     *                 notified when it is executed.
     * @param config   the cluster state update task configuration
     * @param executor the cluster state update task executor; tasks
     *                 that share the same executor will be executed
     *                 batches on this executor
     * @param <T>      the type of the cluster state update task state
     *
     */
    public <T extends ClusterStateTaskListener> void submitStateUpdateTask(
        String source,
        T task,
        ClusterStateTaskConfig config,
        ClusterStateTaskExecutor<T> executor
    ) {
        if (lifecycle.started() == false) {
            return;
        }
        final ThreadContext threadContext = threadPool.getThreadContext();
        final Supplier<ThreadContext.StoredContext> supplier = threadContext.newRestorableContext(true);
        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            threadContext.markAsSystemContext();
            taskBatcher.submitTask(taskBatcher.new UpdateTask(config.priority(), source, task, supplier, executor), config.timeout());
        } catch (EsRejectedExecutionException e) {
            // ignore cases where we are shutting down..., there is really nothing interesting
            // to be done here...
            if (lifecycle.stoppedOrClosed() == false) {
                throw e;
            }
        }
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        return Arrays.stream(threadPoolExecutor.getPending()).map(pending -> {
            assert pending.task instanceof SourcePrioritizedRunnable
                : "thread pool executor should only use SourcePrioritizedRunnable instances but found: "
                    + pending.task.getClass().getName();
            SourcePrioritizedRunnable task = (SourcePrioritizedRunnable) pending.task;
            return new PendingClusterTask(
                pending.insertionOrder,
                pending.priority,
                new Text(task.source()),
                task.getAgeInMillis(),
                pending.executing
            );
        }).toList();
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return threadPoolExecutor.getNumberOfPendingTasks();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        return threadPoolExecutor.getMaxTaskWaitTime();
    }

    private void logExecutionTime(TimeValue executionTime, String activity, String summary) {
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
    private record ContextPreservingAckListener(ClusterStateAckListener listener, Supplier<ThreadContext.StoredContext> context) {

        public boolean mustAck(DiscoveryNode discoveryNode) {
            return listener.mustAck(discoveryNode);
        }

        public void onAckSuccess() {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAllNodesAcked();
            } catch (Exception inner) {
                logger.error("exception thrown by listener while notifying on all nodes acked", inner);
            }
        }

        public void onAckFailure(@Nullable Exception e) {
            try (ThreadContext.StoredContext ignore = context.get()) {
                listener.onAckFailure(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener while notifying on all nodes acked or failed", inner);
            }
        }

        public void onAckTimeout() {
            try (ThreadContext.StoredContext ignore = context.get()) {
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
                logger.debug(
                    () -> new ParameterizedMessage(
                        "ack received from node [{}], cluster_state update (version: {})",
                        node,
                        clusterStateVersion
                    ),
                    e
                );
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
        final Batcher.UpdateTask updateTask;

        @Nullable // if the task is incomplete or failed
        ActionListener<ClusterState> publishListener;

        @Nullable // if the task is incomplete or failed or doesn't listen for acks
        ClusterStateAckListener clusterStateAckListener;

        @Nullable // if the task is incomplete or succeeded
        Exception failure;

        ExecutionResult(Batcher.UpdateTask updateTask) {
            this.updateTask = updateTask;
        }

        @SuppressWarnings("unchecked") // trust us this is ok
        @Override
        public T getTask() {
            return (T) updateTask.getTask();
        }

        private boolean incomplete() {
            assert MasterService.isMasterUpdateThread() || Thread.currentThread().getName().startsWith("TEST-")
                : Thread.currentThread().getName();
            return publishListener == null && failure == null;
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
        public void success(ActionListener<ClusterState> publishListener) {
            assert getTask() instanceof ClusterStateAckListener == false // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must explicitly supply themselves as the ack listener";
            assert incomplete();
            this.publishListener = Objects.requireNonNull(publishListener);
        }

        @Override
        public void success(ActionListener<ClusterState> publishListener, ClusterStateAckListener clusterStateAckListener) {
            assert getTask() == clusterStateAckListener || getTask() instanceof ClusterStateAckListener == false
                // see [HISTORICAL NOTE] above
                : "tasks that implement ClusterStateAckListener must not supply a separate clusterStateAckListener";
            assert incomplete();
            this.publishListener = Objects.requireNonNull(publishListener);
            this.clusterStateAckListener = Objects.requireNonNull(clusterStateAckListener);
        }

        @Override
        public void onFailure(Exception failure) {
            assert incomplete();
            this.failure = Objects.requireNonNull(failure);
        }

        void onBatchFailure(Exception failure) {
            // if the whole batch resulted in an exception then this overrides any task-level results whether successful or not
            this.failure = Objects.requireNonNull(failure);
            this.publishListener = null;
            this.clusterStateAckListener = null;
        }

        void onPublishSuccess(ClusterState newClusterState) {
            if (publishListener == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = updateTask.threadContextSupplier.get()) {
                publishListener.onResponse(newClusterState);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of new cluster state:\n{}",
                        newClusterState
                    ),
                    e
                );
            }
        }

        void onClusterStateUnchanged(ClusterState clusterState) {
            if (publishListener == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = updateTask.threadContextSupplier.get()) {
                publishListener.onResponse(clusterState);
            } catch (Exception e) {
                logger.error(
                    () -> new ParameterizedMessage(
                        "exception thrown by listener while notifying of unchanged cluster state:\n{}",
                        clusterState
                    ),
                    e
                );
            }
        }

        void onPublishFailure(FailedToCommitClusterStateException e) {
            if (publishListener == null) {
                assert failure != null;
                return;
            }
            try (ThreadContext.StoredContext ignored = updateTask.threadContextSupplier.get()) {
                publishListener.onFailure(e);
            } catch (Exception inner) {
                inner.addSuppressed(e);
                logger.error("exception thrown by listener notifying of failure", inner);
            }
        }

        ContextPreservingAckListener getContextPreservingAckListener() {
            assert incomplete() == false;
            return updateTask.wrapInTaskContext(clusterStateAckListener);
        }

        @Override
        public String toString() {
            return "TaskContextImpl[" + updateTask.getTask() + "]";
        }
    }

    private static ClusterState executeTasks(
        ClusterState previousClusterState,
        List<ExecutionResult<ClusterStateTaskListener>> executionResults,
        ClusterStateTaskExecutor<ClusterStateTaskListener> executor,
        String summary
    ) {
        final var resultingState = innerExecuteTasks(previousClusterState, executionResults, executor, summary);
        if (previousClusterState != resultingState
            && previousClusterState.nodes().isLocalNodeElectedMaster()
            && (resultingState.nodes().isLocalNodeElectedMaster() == false)) {
            throw new AssertionError("update task submitted to MasterService cannot remove master");
        }
        assert assertAllTasksComplete(executionResults);
        return resultingState;
    }

    private static boolean assertAllTasksComplete(List<ExecutionResult<ClusterStateTaskListener>> executionResults) {
        for (final var executionResult : executionResults) {
            assert executionResult.incomplete() == false : "missing result for " + executionResult;
        }
        return true;
    }

    @SuppressWarnings("unchecked") // the input is unmodifiable so it is ok to cast to a more general element type
    private static List<ClusterStateTaskExecutor.TaskContext<ClusterStateTaskListener>> castTaskContexts(List<?> executionResults) {
        return (List<ClusterStateTaskExecutor.TaskContext<ClusterStateTaskListener>>) executionResults;
    }

    private static ClusterState innerExecuteTasks(
        ClusterState previousClusterState,
        List<ExecutionResult<ClusterStateTaskListener>> executionResults,
        ClusterStateTaskExecutor<ClusterStateTaskListener> executor,
        String summary
    ) {
        final var taskContexts = castTaskContexts(executionResults);
        try {
            return executor.execute(previousClusterState, taskContexts);
        } catch (Exception e) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "failed to execute cluster state update (on version: [{}], uuid: [{}]) for [{}]\n{}{}{}",
                    previousClusterState.version(),
                    previousClusterState.stateUUID(),
                    summary,
                    previousClusterState.nodes(),
                    previousClusterState.routingTable(),
                    previousClusterState.getRoutingNodes()
                ), // may be expensive => construct message lazily
                e
            );
            for (final var executionResult : executionResults) {
                executionResult.onBatchFailure(e);
            }
            return previousClusterState;
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

}
