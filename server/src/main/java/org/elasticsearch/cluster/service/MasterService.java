/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.service;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DelegatingActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
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
import org.elasticsearch.cluster.metadata.ProjectMetadata;
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
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Assertions;
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
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
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

    public static final String MASTER_UPDATE_THREAD_NAME = "masterService#updateTask";

    public static final String STATE_UPDATE_ACTION_NAME = "publish_cluster_state_update";

    private final ClusterStateTaskExecutor<ClusterStateUpdateTask> unbatchedExecutor;

    private ClusterStatePublisher clusterStatePublisher;
    private Supplier<ClusterState> clusterStateSupplier;

    private final String nodeName;

    private volatile TimeValue slowTaskLoggingThreshold;
    private final TimeValue starvationLoggingThreshold;

    protected final ThreadPool threadPool;
    private final TaskManager taskManager;
    private final ThreadContext.StoredContext clusterStateUpdateContext;

    private volatile ExecutorService threadPoolExecutor;
    private final AtomicInteger totalQueueSize = new AtomicInteger();
    private volatile Batch currentlyExecutingBatch;
    private final Map<Priority, PerPriorityQueue> queuesByPriority;
    private final LongSupplier insertionIndexSupplier = new AtomicLong()::incrementAndGet;

    private final ClusterStateUpdateStatsTracker clusterStateUpdateStatsTracker = new ClusterStateUpdateStatsTracker();
    private final StarvationWatcher starvationWatcher = new StarvationWatcher();

    public MasterService(Settings settings, ClusterSettings clusterSettings, ThreadPool threadPool, TaskManager taskManager) {
        this.nodeName = Objects.requireNonNull(Node.NODE_NAME_SETTING.get(settings));

        this.slowTaskLoggingThreshold = MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING.get(settings);
        clusterSettings.addSettingsUpdateConsumer(MASTER_SERVICE_SLOW_TASK_LOGGING_THRESHOLD_SETTING, this::setSlowTaskLoggingThreshold);

        this.starvationLoggingThreshold = MASTER_SERVICE_STARVATION_LOGGING_THRESHOLD_SETTING.get(settings);

        this.threadPool = threadPool;
        this.taskManager = taskManager;
        this.clusterStateUpdateContext = getClusterStateUpdateContext(threadPool.getThreadContext());

        final var queuesByPriorityBuilder = new EnumMap<Priority, PerPriorityQueue>(Priority.class);
        for (final var priority : Priority.values()) {
            queuesByPriorityBuilder.put(priority, new PerPriorityQueue(priority));
        }
        this.queuesByPriority = Collections.unmodifiableMap(queuesByPriorityBuilder);
        this.unbatchedExecutor = new UnbatchedExecutor();
    }

    private static ThreadContext.StoredContext getClusterStateUpdateContext(ThreadContext threadContext) {
        try (var ignored = threadContext.newStoredContext()) {
            // capture the context in which to run all cluster state updates here where we know it to be very clean
            assert threadContext.isDefaultContext() : "must only create MasterService in a clean ThreadContext";
            threadContext.markAsSystemContext();
            return threadContext.newStoredContext();
        }
    }

    private void setSlowTaskLoggingThreshold(TimeValue slowTaskLoggingThreshold) {
        this.slowTaskLoggingThreshold = slowTaskLoggingThreshold;
    }

    public synchronized void setClusterStatePublisher(ClusterStatePublisher publisher) {
        clusterStatePublisher = publisher;
    }

    public synchronized void setClusterStateSupplier(Supplier<ClusterState> clusterStateSupplier) {
        this.clusterStateSupplier = clusterStateSupplier;
    }

    @Override
    protected synchronized void doStart() {
        Objects.requireNonNull(clusterStatePublisher, "please set a cluster state publisher before starting");
        Objects.requireNonNull(clusterStateSupplier, "please set a cluster state supplier before starting");
        threadPoolExecutor = createThreadPoolExecutor();
    }

    protected ExecutorService createThreadPoolExecutor() {
        return EsExecutors.newScaling(
            nodeName + "/" + MASTER_UPDATE_THREAD_NAME,
            0,
            1,
            60,
            TimeUnit.SECONDS,
            true,
            daemonThreadFactory(nodeName, MASTER_UPDATE_THREAD_NAME),
            threadPool.getThreadContext()
        );
    }

    public ClusterStateUpdateStats getClusterStateUpdateStats() {
        return clusterStateUpdateStatsTracker.getStatistics();
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
        final BatchSummary summary,
        final ActionListener<Void> listener
    ) {
        if (lifecycle.started() == false) {
            logger.debug("processing [{}]: ignoring, master service not started", summary);
            listener.onResponse(null);
            return;
        }

        logger.debug("executing cluster state update for [{}]", summary);
        final ClusterState previousClusterState = state();

        if (previousClusterState.nodes().isLocalNodeElectedMaster() == false && executor.runOnlyOnMaster()) {
            logger.debug("failing [{}]: local node is no longer master", summary);
            for (ExecutionResult<T> executionResult : executionResults) {
                executionResult.onBatchFailure(new NotMasterException("no longer master"));
                executionResult.notifyFailure();
            }
            listener.onResponse(null);
            return;
        }

        final long computationStartTime = threadPool.rawRelativeTimeInMillis();
        final var newClusterState = patchVersions(
            previousClusterState,
            executeTasks(previousClusterState, executionResults, executor, summary, threadPool.getThreadContext())
        );
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
            listener.onResponse(null);
        } else {
            final long publicationStartTime = threadPool.rawRelativeTimeInMillis();
            try (var ignored = threadPool.getThreadContext().newTraceContext()) {
                final var newClusterStateVersion = newClusterState.getVersion();

                final Task task = taskManager.register("master", STATE_UPDATE_ACTION_NAME, new TaskAwareRequest() {
                    @Override
                    public void setParentTask(TaskId taskId) {}

                    @Override
                    public void setRequestId(long requestId) {}

                    @Override
                    public TaskId getParentTask() {
                        return TaskId.EMPTY_TASK_ID;
                    }

                    @Override
                    public String getDescription() {
                        return "publication of cluster state [" + newClusterStateVersion + "]";
                    }
                });

                ActionListener.run(
                    new DelegatingActionListener<Void, Void>(
                        ActionListener.runAfter(listener, () -> taskManager.unregister(task)).delegateResponse((l, e) -> {
                            assert publicationMayFail() : e;
                            handleException(summary, publicationStartTime, newClusterState, e);
                            l.onResponse(null);
                        })
                    ) {
                        @Override
                        public void onResponse(Void response) {
                            delegate.onResponse(response);
                        }

                        @Override
                        public String toString() {
                            return "listener for publication of cluster state [" + newClusterStateVersion + "]";
                        }
                    },
                    l -> publishClusterStateUpdate(
                        executor,
                        summary,
                        previousClusterState,
                        executionResults,
                        newClusterState,
                        computationTime,
                        publicationStartTime,
                        task,
                        l
                    )
                );
            }
        }
    }

    private <T extends ClusterStateTaskListener> void publishClusterStateUpdate(
        ClusterStateTaskExecutor<T> executor,
        BatchSummary summary,
        ClusterState previousClusterState,
        List<ExecutionResult<T>> executionResults,
        ClusterState newClusterState,
        TimeValue computationTime,
        long publicationStartTime,
        Task task,
        ActionListener<Void> listener
    ) {
        if (logger.isTraceEnabled()) {
            logger.trace("cluster state updated, source [{}]\n{}", summary, newClusterState);
        } else {
            logger.debug("cluster state updated, version [{}], source [{}]", newClusterState.version(), summary);
        }

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
            ActionListener.runAfter(new ActionListener<>() {
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
                            () -> format("exception thrown while notifying executor of new cluster state publication [%s]", summary),
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
                        logger.warn(() -> format("failing [%s]: failed to commit cluster state version [%s]", summary, version), exception);
                        for (final var executionResult : executionResults) {
                            executionResult.onPublishFailure(failedToCommitClusterStateException);
                        }
                        final long notificationMillis = threadPool.rawRelativeTimeInMillis() - notificationStartTime;
                        clusterStateUpdateStatsTracker.onPublicationFailure(
                            threadPool.rawRelativeTimeInMillis(),
                            clusterStatePublicationEvent,
                            notificationMillis
                        );
                    } else if (exception instanceof EsRejectedExecutionException esRejectedExecutionException) {
                        assert esRejectedExecutionException.isExecutorShutdown();
                        clusterStateUpdateStatsTracker.onPublicationFailure(
                            threadPool.rawRelativeTimeInMillis(),
                            clusterStatePublicationEvent,
                            0L
                        );
                        final long version = newClusterState.version();
                        logger.debug(
                            () -> format("shut down during publication of cluster state version [%s]: [%s]", version, summary),
                            exception
                        );
                        // TODO also bubble the failure up to the tasks too, see https://github.com/elastic/elasticsearch/issues/94930
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

                @Override
                public String toString() {
                    return Strings.format(
                        "publication completion listener for version [%d]",
                        clusterStatePublicationEvent.getNewState().version()
                    );
                }
            }, new Runnable() {
                @Override
                public void run() {
                    listener.onResponse(null);
                }

                @Override
                public String toString() {
                    return listener + "/onResponse";
                }
            })
        );
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
        clusterStatePublisher.publish(
            clusterStatePublicationEvent,
            // Fork the completion of publicationListener back onto the master service thread, mainly for legacy reasons; note that this
            // might be rejected if the MasterService shut down mid-publication. The master service thread remains idle until this listener
            // is completed at the end of the publication, at which point the publicationListener performs various bits of cleanup and then
            // picks up the next waiting task.
            new ThreadedActionListener<>(
                threadPoolExecutor,
                new ContextPreservingActionListener<>(threadPool.getThreadContext().newRestorableContext(false), publicationListener)
            ),
            ackListener
        );
    }

    private void handleException(BatchSummary summary, long startTimeMillis, ClusterState newClusterState, Exception e) {
        logger.warn(
            () -> format(
                "took [%s] and then failed to publish updated cluster state (version: %s, uuid: %s) for [%s]:\n%s",
                getTimeSince(startTimeMillis),
                newClusterState.version(),
                newClusterState.stateUUID(),
                summary,
                newClusterState
            ),
            e
        );
    }

    private ClusterState patchVersions(ClusterState previousClusterState, ClusterState newClusterState) {
        if (previousClusterState != newClusterState) {
            // only the master controls the version numbers
            Builder builder = incrementVersion(newClusterState);
            if (previousClusterState.metadata() != newClusterState.metadata()) {
                builder.metadata(newClusterState.metadata().withIncrementedVersion());
            }

            final var previousMetadata = newClusterState.metadata();
            newClusterState = builder.build();
            if (Assertions.ENABLED) {
                for (ProjectMetadata previousProject : previousMetadata.projects().values()) {
                    final var newProject = newClusterState.metadata().projects().get(previousProject.id());
                    assert newProject == null || previousProject.sameIndicesLookup(newProject);
                }
            }
        }

        return newClusterState;
    }

    public Builder incrementVersion(ClusterState clusterState) {
        return ClusterState.builder(clusterState).incrementVersion();
    }

    private static boolean versionNumbersPreserved(ClusterState oldState, ClusterState newState) {
        if (oldState.nodes().getMasterNodeId() == null && newState.nodes().getMasterNodeId() != null) {
            return true; // NodeJoinExecutor is special, we trust it to do the right thing with versions
        }

        if (oldState.version() != newState.version()) {
            return false;
        }
        if (oldState.metadata().version() != newState.metadata().version()) {
            return false;
        }
        return true;
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
        createTaskQueue("unbatched", updateTask.priority(), unbatchedExecutor).submitTask(source, updateTask, updateTask.timeout());
    }

    private static class UnbatchedExecutor implements ClusterStateTaskExecutor<ClusterStateUpdateTask> {
        @Override
        @SuppressForbidden(reason = "consuming published cluster state for legacy reasons")
        public ClusterState execute(BatchExecutionContext<ClusterStateUpdateTask> batchExecutionContext) throws Exception {
            assert batchExecutionContext.taskContexts().size() == 1
                : "this only supports a single task but received " + batchExecutionContext.taskContexts();
            final var taskContext = batchExecutionContext.taskContexts().get(0);
            final var task = taskContext.getTask();
            final ClusterState newState;
            try (var ignored = taskContext.captureResponseHeaders()) {
                newState = task.execute(batchExecutionContext.initialState());
            }
            final Consumer<ClusterState> publishListener = publishedState -> task.clusterStateProcessed(
                batchExecutionContext.initialState(),
                publishedState
            );
            if (task instanceof ClusterStateAckListener ackListener) {
                taskContext.success(publishListener, ackListener);
            } else {
                taskContext.success(publishListener);
            }
            return newState;
        }

        @Override
        public String describeTasks(List<ClusterStateUpdateTask> tasks) {
            return ""; // one task, so the source is enough
        }
    }

    /**
     * Returns the tasks that are pending.
     */
    public List<PendingClusterTask> pendingTasks() {
        final var currentTimeMillis = threadPool.relativeTimeInMillis();
        return allBatchesStream().flatMap(e -> e.getPending(currentTimeMillis)).toList();
    }

    /**
     * Returns the number of currently pending tasks.
     */
    public int numberOfPendingTasks() {
        return allBatchesStream().mapToInt(Batch::getPendingCount).sum();
    }

    /**
     * Returns the maximum wait time for tasks in the queue
     *
     * @return A zero time value if the queue is empty, otherwise the time value oldest task waiting in the queue
     */
    public TimeValue getMaxTaskWaitTime() {
        final var oldestTaskTimeMillis = allBatchesStream().mapToLong(Batch::getCreationTimeMillis).min().orElse(Long.MAX_VALUE);

        if (oldestTaskTimeMillis == Long.MAX_VALUE) {
            return TimeValue.ZERO;
        }

        return TimeValue.timeValueMillis(threadPool.relativeTimeInMillis() - oldestTaskTimeMillis);
    }

    private Stream<Batch> allBatchesStream() {
        return Stream.concat(
            Stream.ofNullable(currentlyExecutingBatch),
            queuesByPriority.values().stream().filter(Objects::nonNull).flatMap(q -> q.queue.stream())
        );
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

        public void onAckFailure(Exception e) {
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
        private final TimeValue ackTimeout;
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
            this.ackTimeout = Objects.requireNonNull(contextPreservingAckListener.ackTimeout());
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
            if (ackTimeout.millis() < 0) {
                if (countDown.countDown()) {
                    finish();
                }
                return;
            }

            final TimeValue timeLeft = TimeValue.timeValueNanos(Math.max(0, ackTimeout.nanos() - commitTime.nanos()));
            if (timeLeft.nanos() == 0L) {
                onTimeout();
            } else if (countDown.countDown()) {
                finish();
            } else {
                this.ackTimeoutCallback = threadPool.schedule(this::onTimeout, timeLeft, threadPool.generic());
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
        private final String source;
        private final T task;
        private final ThreadContext threadContext;
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

        ExecutionResult(String source, T task, ThreadContext threadContext, Supplier<ThreadContext.StoredContext> threadContextSupplier) {
            this.source = source;
            this.task = task;
            this.threadContext = threadContext;
            this.threadContextSupplier = threadContextSupplier;
        }

        public String getSource() {
            return source;
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
            final var storedContext = threadContextSupplier.get();
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
                        threadContext.addResponseHeader(responseHeader.getKey(), value);
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
                notifyFailure();
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
                notifyFailure();
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
                var taskFailure = failure;
                failure = new FailedToCommitClusterStateException(e.getMessage(), e);
                failure.addSuppressed(taskFailure);
                notifyFailure();
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

        void notifyFailure() {
            assert failure != null;
            try (ThreadContext.StoredContext ignore = threadContextSupplier.get()) {
                restoreResponseHeaders();
                getTask().onFailure(failure);
            } catch (Exception inner) {
                inner.addSuppressed(failure);
                logger.error("exception thrown by listener notifying of failure", inner);
            }
        }

        ContextPreservingAckListener getContextPreservingAckListener() {
            assert incomplete() == false;
            if (clusterStateAckListener == null || failure != null) {
                return null;
            } else {
                return new ContextPreservingAckListener(clusterStateAckListener, threadContextSupplier, this::restoreResponseHeaders);
            }
        }

        @Override
        public String toString() {
            return "TaskContext[" + task + "]";
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
        assert assertAllTasksComplete(executor, executionResults);
        return resultingState;
    }

    private static <T extends ClusterStateTaskListener> boolean assertAllTasksComplete(
        ClusterStateTaskExecutor<T> executor,
        List<ExecutionResult<T>> executionResults
    ) {
        final var incompleteTaskContexts = executionResults.stream().filter(ExecutionResult::incomplete).toList();
        assert incompleteTaskContexts.isEmpty()
            : "cluster state task executors must mark all tasks as successful or failed, but ["
                + executor
                + "] left the following tasks incomplete: "
                + incompleteTaskContexts;
        return true;
    }

    static final String TEST_ONLY_EXECUTOR_MAY_CHANGE_VERSION_NUMBER_TRANSIENT_NAME = "test_only_executor_may_change_version_number";

    private static <T extends ClusterStateTaskListener> ClusterState innerExecuteTasks(
        ClusterState previousClusterState,
        List<ExecutionResult<T>> executionResults,
        ClusterStateTaskExecutor<T> executor,
        BatchSummary summary,
        ThreadContext threadContext
    ) {
        try (var ignored = threadContext.newStoredContext()) {
            // if the executor leaks a response header then this will cause a test failure, but we also store the context here to be sure
            // to avoid leaking headers in production that were missed by tests

            try {
                final var updatedState = executor.execute(
                    new ClusterStateTaskExecutor.BatchExecutionContext<>(
                        previousClusterState,
                        executionResults,
                        threadContext::newStoredContext
                    )
                );
                if (versionNumbersPreserved(previousClusterState, updatedState) == false) {
                    // Shenanigans! Executors mustn't meddle with version numbers. Perhaps the executor based its update on the wrong
                    // initial state, potentially losing an intervening cluster state update. That'd be very bad!
                    final var exception = new IllegalStateException(
                        "cluster state update executor did not preserve version numbers: [" + summary.toString() + "]"
                    );
                    assert threadContext.getTransient(TEST_ONLY_EXECUTOR_MAY_CHANGE_VERSION_NUMBER_TRANSIENT_NAME) != null : exception;
                    throw exception;
                }
                return updatedState;
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

    private class StarvationWatcher {
        // accesses of these mutable fields are synchronized (on this)
        private long lastLogMillis;
        private long nonemptySinceMillis;
        private boolean isEmpty = true;

        synchronized void onEmptyQueue() {
            isEmpty = true;
        }

        void onNonemptyQueue() {
            final long nowMillis = threadPool.relativeTimeInMillis();
            final long nonemptyDurationMillis;
            synchronized (this) {
                if (isEmpty) {
                    isEmpty = false;
                    nonemptySinceMillis = nowMillis;
                    lastLogMillis = nowMillis;
                    return;
                }

                if (nowMillis - lastLogMillis < starvationLoggingThreshold.millis()) {
                    return;
                }

                lastLogMillis = nowMillis;
                nonemptyDurationMillis = nowMillis - nonemptySinceMillis;
            }

            final TimeValue maxTaskWaitTime = getMaxTaskWaitTime();
            logger.warn(
                "pending task queue has been nonempty for [{}/{}ms] which is longer than the warn threshold of [{}ms];"
                    + " there are currently [{}] pending tasks, the oldest of which has age [{}/{}ms]",
                TimeValue.timeValueMillis(nonemptyDurationMillis),
                nonemptyDurationMillis,
                starvationLoggingThreshold.millis(),
                numberOfPendingTasks(),
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

    private final Runnable queuesProcessor = new AbstractRunnable() {
        @Override
        public void doRun() {
            assert threadPool.getThreadContext().isSystemContext();
            assert totalQueueSize.get() > 0;
            assert currentlyExecutingBatch == null;

            ActionListener.run(new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    onCompletion();
                }

                @Override
                public void onFailure(Exception e) {
                    logger.error("unexpected exception executing queue entry", e);
                    assert false : e;
                    onCompletion();
                }

                @Override
                public String toString() {
                    return "master service batch completion listener";
                }
            }, batchCompletionListener -> {
                final var nextBatch = takeNextBatch();
                assert currentlyExecutingBatch == nextBatch;
                if (lifecycle.started()) {
                    nextBatch.run(batchCompletionListener);
                } else {
                    nextBatch.onRejection(new FailedToCommitClusterStateException("node closed", getRejectionException()));
                    batchCompletionListener.onResponse(null);
                }
            });
        }

        @Override
        public void onFailure(Exception e) {
            logger.error("unexpected exception executing queue entry", e);
            assert false : e;
            onCompletion();
        }

        private void onCompletion() {
            currentlyExecutingBatch = null;
            if (totalQueueSize.decrementAndGet() > 0) {
                starvationWatcher.onNonemptyQueue();
                forkQueueProcessor();
            } else {
                starvationWatcher.onEmptyQueue();
            }
        }

        @Override
        public void onRejection(Exception e) {
            assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
            drainQueueOnRejection(new FailedToCommitClusterStateException("node closed", e));
        }

        @Override
        public String toString() {
            return "master service queue processor";
        }
    };

    private Batch takeNextBatch() {
        assert totalQueueSize.get() > 0;
        assert currentlyExecutingBatch == null;
        for (final var queue : queuesByPriority.values()) {
            var batch = queue.queue.poll();
            if (batch != null) {
                currentlyExecutingBatch = batch;
                return batch;
            }
        }
        logger.error("queue processor found no items");
        assert false : "queue processor found no items";
        throw new IllegalStateException("queue processor found no items");
    }

    private void forkQueueProcessor() {
        // single-threaded: started when totalQueueSize transitions from 0 to 1 and keeps calling itself until the queue is drained.
        if (lifecycle.started() == false) {
            drainQueueOnRejection(new FailedToCommitClusterStateException("node closed", getRejectionException()));
            return;
        }

        assert totalQueueSize.get() > 0;
        final var threadContext = threadPool.getThreadContext();
        try (var ignored = threadContext.newStoredContext()) {
            clusterStateUpdateContext.restore();
            threadPoolExecutor.execute(queuesProcessor);
        }
    }

    private EsRejectedExecutionException getRejectionException() {
        assert lifecycle.started() == false;
        return new EsRejectedExecutionException("master service is in state [" + lifecycleState() + "]", true);
    }

    private void drainQueueOnRejection(FailedToCommitClusterStateException e) {
        assert totalQueueSize.get() > 0;
        do {
            assert currentlyExecutingBatch == null;
            final var nextBatch = takeNextBatch();
            assert currentlyExecutingBatch == nextBatch;
            try {
                nextBatch.onRejection(e);
            } catch (Exception e2) {
                e2.addSuppressed(e);
                logger.error(() -> format("exception failing batch on rejection [%s]", nextBatch), e2);
                assert false : e2;
            } finally {
                currentlyExecutingBatch = null;
            }
        } while (totalQueueSize.decrementAndGet() > 0);
    }

    /**
     * Queue of batches of tasks for a single priority level. Tracks its count of batches in {@link #totalQueueSize}, allowing detection (in
     * a threadsafe fashion) of the transitions between empty and nonempty, so that it can spawn a processor if and only if it's needed.
     * This allows it to ensure that there is only ever at most one active {@link #queuesProcessor}, and that there's always a pending
     * processor if there is work to be done.
     *
     * There is one of these queues for each priority level.
     */
    private class PerPriorityQueue {
        private final ConcurrentLinkedQueue<Batch> queue = new ConcurrentLinkedQueue<>();
        private final Priority priority;

        PerPriorityQueue(Priority priority) {
            this.priority = priority;
        }

        void execute(Batch runner) {
            queue.add(runner);
            if (totalQueueSize.getAndIncrement() == 0) {
                starvationWatcher.onEmptyQueue();
                forkQueueProcessor();
            }
        }

        Priority priority() {
            return priority;
        }
    }

    private interface Batch {

        void run(ActionListener<Void> listener);

        /**
         * Called when the batch is rejected due to the master service shutting down.
         *
         * @param e is a {@link FailedToCommitClusterStateException} to cause things like {@link TransportMasterNodeAction} to retry after
         *          submitting a task to a master which shut down. {@code e.getCause()} is the rejection exception, which should be a
         *          {@link EsRejectedExecutionException} with {@link EsRejectedExecutionException#isExecutorShutdown()} true.
         */
        // Should really be a NodeClosedException instead, but this exception type doesn't trigger retries today.
        void onRejection(FailedToCommitClusterStateException e);

        /**
         * @return number of tasks in this batch if the batch is pending, or {@code 0} if the batch is not pending.
         */
        int getPendingCount();

        /**
         * @return the tasks in this batch if the batch is pending, or an empty stream if the batch is not pending.
         */
        Stream<PendingClusterTask> getPending(long currentTimeMillis);

        /**
         * @return the earliest insertion time of the tasks in this batch if the batch is pending, or {@link Long#MAX_VALUE} otherwise.
         */
        long getCreationTimeMillis();
    }

    /**
     * Create a new task queue which can be used to submit tasks for execution by the master service. Tasks submitted to the same queue
     * (while the master service is otherwise busy) will be batched together into a single cluster state update. You should therefore re-use
     * each queue as much as possible.
     *
     * @param name The name of the queue, which is mostly useful for debugging.
     *
     * @param priority The priority at which tasks submitted to the queue are executed. Avoid priorities other than {@link Priority#NORMAL}
     *                 where possible. A stream of higher-priority tasks can starve lower-priority ones from running. Higher-priority tasks
     *                 should definitely re-use the same {@link MasterServiceTaskQueue} so that they are executed in batches.
     *
     * @param executor The executor which processes each batch of tasks.
     *
     * @param <T> The type of the tasks
     *
     * @return A new batching task queue.
     */
    public <T extends ClusterStateTaskListener> MasterServiceTaskQueue<T> createTaskQueue(
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
        void runBatch(
            ClusterStateTaskExecutor<T> executor,
            List<ExecutionResult<T>> tasks,
            BatchSummary summary,
            ActionListener<Void> listener
        );
    }

    private static class TaskTimeoutHandler<T extends ClusterStateTaskListener> extends AbstractRunnable {

        private final TimeValue timeout;
        private final String source;
        private final AtomicReference<T> taskHolder; // atomically read and set to null by at most one of {execute, timeout}

        private TaskTimeoutHandler(TimeValue timeout, String source, AtomicReference<T> taskHolder) {
            this.timeout = timeout;
            this.source = source;
            this.taskHolder = taskHolder;
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
            final var task = taskHolder.getAndSet(null);
            if (task != null) {
                logger.trace("timing out [{}][{}] after [{}]", source, task, timeout);
                task.onFailure(e);
            }
        }

        @Override
        public String toString() {
            return getTimeoutTaskDescription(source, taskHolder.get(), timeout);
        }

    }

    static String getTimeoutTaskDescription(String source, Object task, TimeValue timeout) {
        return Strings.format("master service timeout handler for [%s][%s] after [%s]", source, task, timeout);
    }

    /**
     * Actual implementation of {@link MasterServiceTaskQueue} exposed to clients. Conceptually, each entry in each {@link PerPriorityQueue}
     * is a {@link BatchingTaskQueue} representing a batch of tasks to be executed. Clients may add more tasks to each of these queues prior
     * to their execution.
     *
     * Works similarly to {@link PerPriorityQueue} in that the queue size is tracked in a threadsafe fashion so that we can detect
     * transitions between empty and nonempty queues and arrange to process the queue if and only if it's nonempty. There is only ever one
     * active processor for each such queue.
     *
     * Works differently from {@link PerPriorityQueue} in that each time the queue is processed it will drain all the pending items at once
     * and process them in a single batch.
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
        private final PerPriorityQueue perPriorityQueue;
        private final ClusterStateTaskExecutor<T> executor;
        private final ThreadPool threadPool;
        private final Batch processor = new Processor();

        BatchingTaskQueue(
            String name,
            BatchConsumer<T> batchConsumer,
            LongSupplier insertionIndexSupplier,
            PerPriorityQueue perPriorityQueue,
            ClusterStateTaskExecutor<T> executor,
            ThreadPool threadPool
        ) {
            this.name = name;
            this.batchConsumer = batchConsumer;
            this.insertionIndexSupplier = insertionIndexSupplier;
            this.perPriorityQueue = perPriorityQueue;
            this.executor = executor;
            this.threadPool = threadPool;
        }

        @Override
        public void submitTask(String source, T task, @Nullable TimeValue timeout) {
            final var taskHolder = new AtomicReference<>(task);
            final Scheduler.Cancellable timeoutCancellable;
            if (timeout != null && timeout.millis() > 0) {
                try {
                    timeoutCancellable = threadPool.schedule(
                        new TaskTimeoutHandler<>(timeout, source, taskHolder),
                        timeout,
                        threadPool.generic()
                    );
                } catch (Exception e) {
                    assert e instanceof EsRejectedExecutionException esre && esre.isExecutorShutdown() : e;
                    task.onFailure(
                        new FailedToCommitClusterStateException(
                            "could not schedule timeout handler for [%s][%s] on queue [%s]",
                            e,
                            source,
                            task,
                            name
                        )
                    );
                    return;
                }
            } else {
                timeoutCancellable = null;
            }

            queue.add(
                new Entry<>(
                    source,
                    taskHolder,
                    insertionIndexSupplier.getAsLong(),
                    threadPool.relativeTimeInMillis(),
                    threadPool.getThreadContext().newRestorableContext(true),
                    timeoutCancellable
                )
            );

            if (queueSize.getAndIncrement() == 0) {
                perPriorityQueue.execute(processor);
            }
        }

        @Override
        public String toString() {
            return "BatchingTaskQueue[" + name + "]";
        }

        private record Entry<T extends ClusterStateTaskListener>(
            String source,
            AtomicReference<T> taskHolder,
            long insertionIndex,
            long insertionTimeMillis,
            Supplier<ThreadContext.StoredContext> storedContextSupplier,
            @Nullable Scheduler.Cancellable timeoutCancellable
        ) {
            T acquireForExecution() {
                final var task = taskHolder.getAndSet(null);
                if (task != null && timeoutCancellable != null) {
                    timeoutCancellable.cancel();
                }
                return task;
            }

            void onRejection(FailedToCommitClusterStateException e) {
                final var task = acquireForExecution();
                if (task != null) {
                    try (var ignored = storedContextSupplier.get()) {
                        task.onFailure(e);
                    } catch (Exception e2) {
                        e2.addSuppressed(e);
                        logger.error(() -> format("exception failing task [%s] on rejection", task), e2);
                        assert false : e2;
                    }
                }
            }

            boolean isPending() {
                return taskHolder().get() != null;
            }
        }

        private class Processor implements Batch {
            @Override
            public void onRejection(FailedToCommitClusterStateException e) {
                final var items = queueSize.getAndSet(0);
                for (int i = 0; i < items; i++) {
                    final var entry = queue.poll();
                    assert entry != null;
                    entry.onRejection(e);
                }
            }

            @Override
            public void run(ActionListener<Void> listener) {
                assert executing.isEmpty() : executing;
                final var entryCount = queueSize.getAndSet(0);
                var taskCount = 0;
                final var tasks = new ArrayList<ExecutionResult<T>>(entryCount);
                for (int i = 0; i < entryCount; i++) {
                    final var entry = queue.poll();
                    assert entry != null;
                    final var task = entry.acquireForExecution();
                    if (task != null) {
                        taskCount += 1;
                        executing.add(entry);
                        tasks.add(
                            new ExecutionResult<>(entry.source(), task, threadPool.getThreadContext(), entry.storedContextSupplier())
                        );
                    }
                }
                if (taskCount == 0) {
                    listener.onResponse(null);
                    return;
                }
                final var finalTaskCount = taskCount;
                ActionListener.run(ActionListener.runBefore(listener, () -> {
                    assert executing.size() == finalTaskCount;
                    executing.clear();
                }), l -> batchConsumer.runBatch(executor, tasks, new BatchSummary(() -> buildTasksDescription(tasks)), l));
            }

            private String buildTasksDescription(List<ExecutionResult<T>> tasks) {
                final var tasksBySource = new HashMap<String, List<T>>();
                for (final var entry : tasks) {
                    tasksBySource.computeIfAbsent(entry.getSource(), ignored -> new ArrayList<>()).add(entry.getTask());
                }

                final var output = new StringBuilder();
                Strings.collectionToDelimitedStringWithLimit((Iterable<String>) () -> tasksBySource.entrySet().stream().map(entry -> {
                    var tasksDescription = executor.describeTasks(entry.getValue());
                    return tasksDescription.isEmpty() ? entry.getKey() : entry.getKey() + "[" + tasksDescription + "]";
                }).filter(s -> s.isEmpty() == false).iterator(), ", ", MAX_TASK_DESCRIPTION_CHARS, output);
                if (output.length() > MAX_TASK_DESCRIPTION_CHARS) {
                    output.append(" (").append(tasks.size()).append(" tasks in total)");
                }
                return output.toString();
            }

            @Override
            public Stream<PendingClusterTask> getPending(long currentTimeMillis) {
                return Stream.concat(
                    executing.stream().map(entry -> makePendingTask(entry, currentTimeMillis, true)),
                    queue.stream().filter(Entry::isPending).map(entry -> makePendingTask(entry, currentTimeMillis, false))
                );
            }

            private PendingClusterTask makePendingTask(Entry<T> entry, long currentTimeMillis, boolean executing) {
                return new PendingClusterTask(
                    entry.insertionIndex(),
                    perPriorityQueue.priority(),
                    new Text(entry.source()),
                    // in case an element was added to the queue after we cached the current time, we count the wait time as 0
                    Math.max(0L, currentTimeMillis - entry.insertionTimeMillis()),
                    executing
                );
            }

            @Override
            public int getPendingCount() {
                int count = executing.size();
                for (final var entry : queue) {
                    if (entry.isPending()) {
                        count += 1;
                    }
                }
                return count;
            }

            @Override
            public long getCreationTimeMillis() {
                return Stream.concat(executing.stream(), queue.stream().filter(Entry::isPending))
                    .mapToLong(Entry::insertionTimeMillis)
                    .min()
                    .orElse(Long.MAX_VALUE);
            }

            @Override
            public String toString() {
                return "process queue for [" + name + "]";
            }
        }
    }

    static final int MAX_TASK_DESCRIPTION_CHARS = 8 * 1024;
}
