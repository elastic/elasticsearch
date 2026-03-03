/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.persistent.AllocatedPersistentTask;
import org.elasticsearch.persistent.ClusterPersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTaskParams;
import org.elasticsearch.persistent.PersistentTaskState;
import org.elasticsearch.persistent.PersistentTasksCustomMetadata;
import org.elasticsearch.persistent.PersistentTasksExecutor;
import org.elasticsearch.persistent.PersistentTasksService;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteTransportException;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Persistent task executor that spawns periodic index balance computation on one node.
 * Delegates the actual computation to {@link IndexBalanceMetrics}. Only registered when
 * metrics are enabled (see NodeConstruction).
 */
public final class IndexBalanceMetricsTask extends PersistentTasksExecutor<IndexBalanceMetricsTask.TaskParams> {

    public static final String TASK_NAME = "index-balance-metrics";

    /**
     * Node-scope setting controlling whether the index balance metrics task is enabled.
     * Intended for serverless deployments; defaults to false.
     */
    public static final Setting<Boolean> INDEX_BALANCE_METRICS_ENABLED_SETTING = Setting.boolSetting(
        "cluster.routing.allocation.index_balance_metrics.enabled",
        false,
        Setting.Property.NodeScope
    );

    /**
     * Dynamic setting for the interval at which the task runs its refresh.
     * Default is 1 minute, minimum 100 milliseconds.
     */
    public static final Setting<TimeValue> INDEX_BALANCE_METRIC_REFRESH_INTERVAL_SETTING = Setting.timeSetting(
        "cluster.routing.allocation.index_balance_metric_refresh_interval",
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

    public static final String PRIMARY_IMBALANCE_METRIC_NAME = "es.index_balance.primary.imbalance.current";
    public static final String REPLICA_IMBALANCE_METRIC_NAME = "es.index_balance.replica.imbalance.current";

    private final ClusterService clusterService;
    private final PersistentTasksService persistentTasksService;
    private final ClusterSettings clusterSettings;
    private final IndexBalanceMetrics indexBalanceMetrics;
    private final ClusterStateListener taskStarter;

    /**
     * Creates the executor instance, registers pull-based gauges on the given {@link MeterRegistry},
     * and starts listening for cluster state changes to auto-create the persistent task.
     */
    public IndexBalanceMetricsTask(
        ClusterService clusterService,
        PersistentTasksService persistentTasksService,
        ClusterSettings clusterSettings,
        MeterRegistry meterRegistry
    ) {
        super(TASK_NAME, clusterService.threadPool().executor(ThreadPool.Names.MANAGEMENT));
        this.clusterService = clusterService;
        this.persistentTasksService = persistentTasksService;
        this.clusterSettings = clusterSettings;
        this.indexBalanceMetrics = new IndexBalanceMetrics();
        this.taskStarter = this::startTask;
        clusterService.addListener(taskStarter);
        meterRegistry.registerLongsGauge(
            PRIMARY_IMBALANCE_METRIC_NAME,
            "Histogram of primary shard imbalance ratios across indices",
            "{index}",
            () -> buildBucketMeasurements(indexBalanceMetrics.getLastState().primaryBalanceHistogram())
        );
        meterRegistry.registerLongsGauge(
            REPLICA_IMBALANCE_METRIC_NAME,
            "Histogram of replica shard imbalance ratios across indices",
            "{index}",
            () -> buildBucketMeasurements(indexBalanceMetrics.getLastState().replicaBalanceHistogram())
        );
    }

    private static Collection<LongWithAttributes> buildBucketMeasurements(int[] histogram) {
        final var measurements = new ArrayList<LongWithAttributes>(IndexBalanceMetrics.BUCKET_COUNT);
        for (int i = 0; i < IndexBalanceMetrics.BUCKET_COUNT; i++) {
            measurements.add(new LongWithAttributes(histogram[i], Map.of("indexImbalanceBucket", IndexBalanceMetrics.BUCKET_LABELS[i])));
        }
        return measurements;
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
        ((Task) task).startScheduledRefresh();
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
        return new Task(
            id,
            type,
            action,
            getDescription(taskInProgress),
            parentTaskId,
            headers,
            clusterService.threadPool(),
            clusterSettings,
            clusterService,
            indexBalanceMetrics
        );
    }

    private static boolean shouldStartTask(ClusterChangedEvent event) {
        return event.localNodeMaster() && event.state().clusterRecovered() && Task.findTask(event.state()) == null;
    }

    void startTask(ClusterChangedEvent event) {
        if (shouldStartTask(event) == false) {
            return;
        }
        persistentTasksService.sendStartRequest(
            TASK_NAME,
            TASK_NAME,
            TaskParams.INSTANCE,
            TimeValue.timeValueSeconds(30),
            ActionListener.wrap(r -> {}, e -> {
                if (e instanceof NodeClosedException) {
                    return;
                }
                Throwable t = e instanceof RemoteTransportException ? e.getCause() : e;
                if (t instanceof ResourceAlreadyExistsException == false) {
                    clusterService.addListener(taskStarter);
                }
            })
        );
    }

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

    /**
     * Persistent task that runs on a single node. Schedules periodic refresh at a configurable interval;
     * when the routing table changes, {@link #needRefresh} is set and the next run delegates to
     * {@link IndexBalanceMetrics#compute(ClusterState)}. Cancellation stops the runnable and listener.
     */
    public static class Task extends AllocatedPersistentTask {

        private final ThreadPool threadPool;
        private final Executor managementExecutor;
        private final ClusterSettings clusterSettings;
        private final ClusterService clusterService;
        private final IndexBalanceMetrics indexBalanceMetrics;
        private final ClusterStateListener routingTableChangedListener;
        private final AtomicReference<Scheduler.Cancellable> scheduledRefresh = new AtomicReference<>();
        private volatile boolean cancelled;
        /** Set when routing table changes; consumed by the refresh runnable. */
        private volatile boolean needRefresh;

        Task(
            long id,
            String type,
            String action,
            String description,
            TaskId parentTask,
            Map<String, String> headers,
            ThreadPool threadPool,
            ClusterSettings clusterSettings,
            ClusterService clusterService,
            IndexBalanceMetrics indexBalanceMetrics
        ) {
            super(id, type, action, description, parentTask, headers);
            this.threadPool = threadPool;
            this.managementExecutor = threadPool.executor(ThreadPool.Names.MANAGEMENT);
            this.clusterSettings = clusterSettings;
            this.clusterService = clusterService;
            this.indexBalanceMetrics = indexBalanceMetrics;
            this.routingTableChangedListener = this::onRoutingTableChanged;
        }

        private void onRoutingTableChanged(ClusterChangedEvent event) {
            if (event.routingTableChanged()) {
                needRefresh = true;
            }
        }

        void startScheduledRefresh() {
            needRefresh = true;
            clusterService.addListener(routingTableChangedListener);
            scheduleRefresh(clusterSettings.get(INDEX_BALANCE_METRIC_REFRESH_INTERVAL_SETTING));
            clusterSettings.addSettingsUpdateConsumer(INDEX_BALANCE_METRIC_REFRESH_INTERVAL_SETTING, this::onRefreshIntervalChanged);
        }

        private void onRefreshIntervalChanged(TimeValue newInterval) {
            if (cancelled) {
                return;
            }
            cancelScheduledRefresh();
            scheduleRefresh(newInterval);
        }

        private void scheduleRefresh(TimeValue interval) {
            if (cancelled) {
                return;
            }
            final var cancellable = threadPool.scheduleWithFixedDelay(this::runRefresh, interval, managementExecutor);
            scheduledRefresh.set(cancellable);
        }

        private void runRefresh() {
            if (needRefresh) {
                needRefresh = false;
                indexBalanceMetrics.compute(clusterService.state());
            }
        }

        private void cancelScheduledRefresh() {
            final var previous = scheduledRefresh.getAndSet(null);
            if (previous != null) {
                previous.cancel();
            }
        }

        @Override
        protected void onCancelled() {
            cancelled = true;
            clusterService.removeListener(routingTableChangedListener);
            cancelScheduledRefresh();
            indexBalanceMetrics.resetState();
        }

        /** Package-visible for testing: returns the current scheduled refresh cancellable, or null if none. */
        @Nullable
        Scheduler.Cancellable getScheduledRefresh() {
            return scheduledRefresh.get();
        }

        /** Returns the index balance metrics persistent task from the cluster state, or {@code null} if not present. */
        @Nullable
        public static PersistentTasksCustomMetadata.PersistentTask<?> findTask(ClusterState clusterState) {
            return ClusterPersistentTasksCustomMetadata.getTaskWithId(clusterState, TASK_NAME);
        }
    }
}
