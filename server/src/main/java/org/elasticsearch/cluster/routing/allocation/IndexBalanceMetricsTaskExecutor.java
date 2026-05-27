/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Persistent task executor that spawns periodic index balance computation on one node.
 * Delegates the actual computation to {@link IndexBalanceMetricsComputer}.
 */
public final class IndexBalanceMetricsTaskExecutor extends PersistentTasksExecutor<IndexBalanceMetricsTaskExecutor.TaskParams> {

    private static final Logger logger = LogManager.getLogger(IndexBalanceMetricsTaskExecutor.class);

    public static final String TASK_NAME = "index-balance-metrics";

    /**
     * Dynamic setting controlling whether the index balance metrics task is enabled.
     * Intended for serverless deployments; defaults to false.
     */
    public static final Setting<Boolean> INDEX_BALANCE_METRICS_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.index_balance_metrics.enabled",
        false,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    /**
     * Dynamic setting for the interval at which the task runs its computation.
     * Default is 1 minute, minimum 100 milliseconds.
     */
    public static final Setting<TimeValue> INDEX_BALANCE_METRICS_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.index_balance_metrics.refresh_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueMillis(100),
        Setting.Property.Dynamic,
        Setting.Property.NodeScope
    );

    /**
     * Transport version for the index balance metrics feature.
     */
    public static final TransportVersion INDEX_BALANCE_METRICS = TransportVersion.fromName("index_balance_metrics");

    private static final List<NamedXContentRegistry.Entry> NAMED_XCONTENT_PARSERS = List.of(
        new NamedXContentRegistry.Entry(PersistentTaskParams.class, new ParseField(TASK_NAME), TaskParams::fromXContent)
    );
    private static final List<NamedWriteableRegistry.Entry> NAMED_WRITEABLES = List.of(
        new NamedWriteableRegistry.Entry(PersistentTaskParams.class, TASK_NAME, TaskParams::new)
    );

    /**
     * Parameters for the index balance metrics persistent task. No parameters are required.
     */
    public record TaskParams() implements PersistentTaskParams {

        public static final TaskParams INSTANCE = new TaskParams();

        public static final ObjectParser<TaskParams, Void> PARSER = new ObjectParser<>(TASK_NAME, true, () -> INSTANCE);

        public TaskParams(StreamInput ignored) {
            this();
        }

        public static TaskParams fromXContent(XContentParser parser) {
            return PARSER.apply(parser, null);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.endObject();
            return builder;
        }

        @Override
        public String getWriteableName() {
            return TASK_NAME;
        }

        @Override
        public TransportVersion getMinimalSupportedVersion() {
            return INDEX_BALANCE_METRICS;
        }

        @Override
        public void writeTo(StreamOutput out) {}
    }

    private final ClusterService clusterService;
    private final AtomicReference<Task> executorNodeTask = new AtomicReference<>();
    private volatile TimeValue computationInterval;

    /**
     * Creates the executor instance, registers pull-based gauges on the given {@link MeterRegistry}.
     */
    public IndexBalanceMetricsTaskExecutor(ClusterService clusterService, MeterRegistry meterRegistry) {
        super(TASK_NAME, clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        final var primaryMetricNames = IndexBalanceMetricsComputer.metricNames("primary");
        final var replicaMetricNames = IndexBalanceMetricsComputer.metricNames("replica");
        for (int i = 0; i < IndexBalanceMetricsComputer.BUCKET_DEFINITIONS.length; i++) {
            final int bucket = i;
            final var label = IndexBalanceMetricsComputer.BUCKET_DEFINITIONS[i].label();
            meterRegistry.registerLongsGauge(
                primaryMetricNames[i],
                "Number of indices with " + label + " primary shard imbalance",
                "{index}",
                () -> publishIfNotEmpty(executorNodeTask, true, bucket)
            );
            meterRegistry.registerLongsGauge(
                replicaMetricNames[i],
                "Number of indices with " + label + " replica shard imbalance",
                "{index}",
                () -> publishIfNotEmpty(executorNodeTask, false, bucket)
            );
        }
        final var clusterSettings = clusterService.getClusterSettings();
        clusterSettings.initializeAndWatch(INDEX_BALANCE_METRICS_REFRESH_INTERVAL_SETTING, this::updateComputationInterval);
    }

    private static List<LongWithAttributes> publishIfNotEmpty(AtomicReference<Task> executorNodeTask, boolean primary, int bucketIndex) {
        final var task = executorNodeTask.get();
        if (task == null) {
            return List.of();
        }
        final var state = task.getLastState();
        if (state == null) {
            return List.of();
        }
        final var histogram = primary ? state.primaryBalanceHistogram() : state.replicaBalanceHistogram();
        return List.of(new LongWithAttributes(histogram[bucketIndex]));
    }

    public static List<NamedXContentRegistry.Entry> getNamedXContentParsers() {
        return NAMED_XCONTENT_PARSERS;
    }

    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        return NAMED_WRITEABLES;
    }

    @Override
    public Scope scope() {
        return Scope.CLUSTER;
    }

    @Override
    protected void nodeOperation(AllocatedPersistentTask task, TaskParams params, PersistentTaskState state) {
        final var indexBalanceMetricsTask = (Task) task;
        final var existingTask = executorNodeTask.getAndSet(indexBalanceMetricsTask);
        if (existingTask != null) {
            assert existingTask.stopped : "We should never start a new task when there's still one running";
            existingTask.markAsCompleted();
        }
        indexBalanceMetricsTask.startScheduledComputation();
    }

    @Override
    protected Task createTask(
        long id,
        String type,
        String action,
        TaskId parentTaskId,
        PersistentTasksCustomMetadata.PersistentTask<TaskParams> taskInProgress,
        Map<String, String> headers
    ) {
        assert INDEX_BALANCE_METRICS_ENABLED_SETTING.get(clusterService.getSettings())
            : "index balance metrics task requires [" + INDEX_BALANCE_METRICS_ENABLED_SETTING.getKey() + "] to be enabled";
        return new Task(
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            clusterService.threadPool(),
            clusterService,
            () -> computationInterval,
            executorNodeTask
        );
    }

    private void updateComputationInterval(TimeValue newComputationInterval) {
        this.computationInterval = newComputationInterval;
        final var task = executorNodeTask.get();
        if (task != null) {
            task.requestRecomputation();
        }
    }

    /**
     * Persistent task that runs on a single node. Schedules periodic computation at a configurable interval;
     * when the routing table changes, {@link #needsComputation} is set and the next run delegates to
     * {@link IndexBalanceMetricsComputer#compute(ClusterState)}. Cancellation stops the runnable and listener.
     */
    public static class Task extends AllocatedPersistentTask {

        private final ThreadPool threadPool;
        private final Executor managementExecutor;
        private final ClusterService clusterService;
        private final AtomicReference<IndexBalanceMetricsComputer.IndexBalanceHistograms> lastState = new AtomicReference<>();
        private final ClusterStateListener routingTableChangedListener;
        private final Supplier<TimeValue> pollIntervalSupplier;
        private final Object lifecycleLock = new Object();
        /** Set when routing table changes; consumed by the computation runnable. */
        private final AtomicBoolean needsComputation;
        private final AtomicReference<Task> executorNodeTask;
        private Scheduler.Cancellable scheduledComputation;
        private volatile boolean stopped;

        Task(
            long id,
            String type,
            String action,
            String description,
            TaskId parentTask,
            Map<String, String> headers,
            ThreadPool threadPool,
            ClusterService clusterService,
            Supplier<TimeValue> pollIntervalSupplier,
            AtomicReference<Task> executorNodeTask
        ) {
            super(id, type, action, description, parentTask, headers);
            this.threadPool = threadPool;
            this.managementExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
            this.clusterService = clusterService;
            this.routingTableChangedListener = this::onRoutingTableChanged;
            this.pollIntervalSupplier = pollIntervalSupplier;
            this.needsComputation = new AtomicBoolean(false);
            this.executorNodeTask = executorNodeTask;
        }

        private void onRoutingTableChanged(ClusterChangedEvent event) {
            if (event.routingTableChanged()) {
                needsComputation.set(true);
            }
        }

        // package private for testing
        void startScheduledComputation() {
            synchronized (lifecycleLock) {
                if (stopped) {
                    return;
                }
                logger.info("Starting index balance metrics task");
                needsComputation.set(true);
                clusterService.addListener(routingTableChangedListener);
                scheduleComputation(pollIntervalSupplier.get());
            }
        }

        // package private for testing
        void requestRecomputation() {
            synchronized (lifecycleLock) {
                if (stopped) {
                    return;
                }
                cancelScheduledComputation();
                scheduleComputation(pollIntervalSupplier.get());
            }
        }

        @Override
        public void markAsCompleted() {
            super.markAsCompleted();
            stopListeningAndCancelComputation();
        }

        @Override
        protected void onCancelled() {
            stopListeningAndCancelComputation();
        }

        private void scheduleComputation(TimeValue interval) {
            assert Thread.holdsLock(lifecycleLock) : "Must hold lifecycle lock";
            if (threadPool.scheduler().isShutdown()) {
                return;
            }
            assert scheduledComputation == null : "Must not already have a scheduled computation";
            scheduledComputation = threadPool.scheduleWithFixedDelay(this::runComputation, interval, managementExecutor);
        }

        private void runComputation() {
            if (stopped) {
                return;
            }
            if (needsComputation.getAndSet(false)) {
                var result = IndexBalanceMetricsComputer.compute(clusterService.state());
                lastState.set(result);
            }
        }

        private void cancelScheduledComputation() {
            assert Thread.holdsLock(lifecycleLock) : "Must hold lifecycle lock";
            if (scheduledComputation != null) {
                scheduledComputation.cancel();
                scheduledComputation = null;
            }
        }

        private void stopListeningAndCancelComputation() {
            synchronized (lifecycleLock) {
                stopped = true;
                clusterService.removeListener(routingTableChangedListener);
                cancelScheduledComputation();
                executorNodeTask.compareAndSet(this, null);
            }
        }

        /** Package-visible for testing: returns the current scheduled computation cancellable, or null if none. */
        @Nullable
        Scheduler.Cancellable getScheduledComputation() {
            return scheduledComputation;
        }

        /** Returns the index balance metrics persistent task from the cluster state, or {@code null} if not present. */
        @Nullable
        public static PersistentTasksCustomMetadata.PersistentTask<?> findTask(ClusterState clusterState) {
            return ClusterPersistentTasksCustomMetadata.getTaskWithId(clusterState, TASK_NAME);
        }

        /**
         * Get the last computed state if there is one, and the computation task is not cancelled/completed
         *
         * @return The last computed state, or null if there is none to report
         */
        @Nullable
        public IndexBalanceMetricsComputer.IndexBalanceHistograms getLastState() {
            if (stopped) {
                return null;
            }
            return lastState.get();
        }
    }
}
