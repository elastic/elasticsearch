/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.telemetry;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.transform.TransformNode;
import org.elasticsearch.xpack.transform.transforms.TransformTask;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Per-node APM gauges for the cross-project status of transforms running on this node.
 * Only constructed (in {@code Transform.createComponents}) when cross-project search is enabled
 * and the node carries the transform role; on all other nodes the component does not exist.
 *
 * <p>Follows the {@code MlConfigMetrics} pattern: a background poll iterates the
 * {@link TransformNode} task registry on a fixed interval, reads each task's per-search facts
 * off its {@code TransformContext}, and caches the counts; the gauge observers read the cached
 * snapshot, so the APM poll callback is constant-time and both gauges emit one coherent view.
 * The registry is bounded by the tasks running on this node: the persistent-task executor
 * registers a task alongside its scheduler registration, and the task deregisters itself on
 * teardown. A deregistered task is still counted by the next poll, so a run that ends between
 * polls (a batch transform) shows up in exactly one snapshot.
 *
 * <p>Two gauges, each emitting one {@link LongWithAttributes} per attribute value:
 * <ul>
 * <li>{@value TRANSFORM_CPS_UIAM_AUTH_CURRENT} — {@code auth_type}: {@code "uiam"} for
 * transforms with a UIAM cloud credential, {@code "legacy"} for those without.</li>
 * <li>{@value TRANSFORM_CPS_ACTIVE_CURRENT} — {@code scope}: {@code "cross_project"} for
 * transforms whose last search touched a linked project, {@code "origin"} otherwise.</li>
 * </ul>
 * Summing across nodes in ES|QL:
 * {@code sum(last_over_time(metric)) BY <attribute>, BUCKET(@timestamp, 5 minute)}.
 *
 * <p>Nodes that run no transforms (or whose transforms have not yet completed a search) emit
 * nothing and are invisible in APM aggregations.
 */
public final class TransformCrossProjectMetrics extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(TransformCrossProjectMetrics.class);

    /** APM metric name for running transforms broken down by UIAM migration status. */
    public static final String TRANSFORM_CPS_UIAM_AUTH_CURRENT = "es.transform.cps.uiam.auth.current";

    /** APM metric name for running transforms broken down by whether their last search went cross-project. */
    public static final String TRANSFORM_CPS_ACTIVE_CURRENT = "es.transform.cps.active.current";

    private static final Map<String, Object> UIAM_ATTRIBUTES = Map.of("auth_type", "uiam");
    private static final Map<String, Object> LEGACY_ATTRIBUTES = Map.of("auth_type", "legacy");
    private static final Map<String, Object> CROSS_PROJECT_ATTRIBUTES = Map.of("scope", "cross_project");
    private static final Map<String, Object> ORIGIN_ATTRIBUTES = Map.of("scope", "origin");

    /** How often the background poll refreshes the cached counts. The 10s floor exists so integration tests can poll fast. */
    public static final Setting<TimeValue> POLL_INTERVAL_SETTING = Setting.timeSetting(
        "xpack.transform.cps.metrics.poll_interval",
        TimeValue.timeValueMinutes(1),
        TimeValue.timeValueSeconds(10),
        Setting.Property.NodeScope
    );

    /** One coherent snapshot for both gauges, refreshed by {@link #poll()}. */
    record CpsCounts(long uiam, long legacy, long crossProject, long origin) {
        static final CpsCounts EMPTY = new CpsCounts(0, 0, 0, 0);

        boolean isEmpty() {
            return uiam == 0 && legacy == 0 && crossProject == 0 && origin == 0;
        }
    }

    private final ThreadPool threadPool;
    private final TimeValue pollInterval;
    private final TransformNode transformNode;
    private final List<AutoCloseable> registeredMetrics = new ArrayList<>();

    /** Per-search facts of a task, captured so they outlive its registration in {@link TransformNode}. */
    record TaskFacts(Boolean uiamAuth, Boolean lastSearchCrossProject) {
        static TaskFacts of(TransformTask task) {
            return new TaskFacts(task.getContext().getUiamAuth(), task.getContext().getLastSearchCrossProject());
        }
    }

    // tasks that deregistered since the last poll, by transform id, so a run that ends between polls (e.g. batch) is counted once
    private final Map<String, TaskFacts> departedSinceLastPoll = ConcurrentCollections.newConcurrentMap();
    private volatile CpsCounts cachedCounts = CpsCounts.EMPTY;
    private Scheduler.Cancellable scheduledPoll;

    /**
     * @param meterRegistry the APM registry to register gauges against
     * @param threadPool    used to schedule the background poll
     * @param pollInterval  resolved {@link #POLL_INTERVAL_SETTING}
     * @param transformNode holds the registry of transform tasks running on this node
     */
    public TransformCrossProjectMetrics(
        MeterRegistry meterRegistry,
        ThreadPool threadPool,
        TimeValue pollInterval,
        TransformNode transformNode
    ) {
        this.threadPool = threadPool;
        this.pollInterval = pollInterval;
        this.transformNode = transformNode;
        transformNode.addDeregistrationListener(task -> departedSinceLastPoll.put(task.getTransformId(), TaskFacts.of(task)));
        registeredMetrics.add(
            meterRegistry.registerLongAsyncGauge(
                TRANSFORM_CPS_UIAM_AUTH_CURRENT,
                "Number of running transforms on this node broken down by UIAM migration status",
                "transforms",
                this::recordUiamAuth
            )
        );
        registeredMetrics.add(
            meterRegistry.registerLongAsyncGauge(
                TRANSFORM_CPS_ACTIVE_CURRENT,
                "Number of running transforms on this node broken down by whether their last search went cross-project",
                "transforms",
                this::recordCrossProjectActive
            )
        );
    }

    private void recordUiamAuth(LongAsyncMeasurement measurement) {
        CpsCounts counts = cachedCounts;
        if (counts.isEmpty() == false) {
            measurement.record(counts.uiam(), UIAM_ATTRIBUTES);
            measurement.record(counts.legacy(), LEGACY_ATTRIBUTES);
        }
    }

    private void recordCrossProjectActive(LongAsyncMeasurement measurement) {
        CpsCounts counts = cachedCounts;
        if (counts.isEmpty() == false) {
            measurement.record(counts.crossProject(), CROSS_PROJECT_ATTRIBUTES);
            measurement.record(counts.origin(), ORIGIN_ATTRIBUTES);
        }
    }

    @Override
    protected void doStart() {
        try {
            scheduledPoll = threadPool.scheduleWithFixedDelay(this::poll, pollInterval, threadPool.generic());
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown() == false) {
                throw e;
            }
        }
    }

    /** Counts the running tasks plus those that deregistered since the last poll, and refreshes the cached counts. */
    void poll() {
        try {
            Map<String, TaskFacts> factsById = new HashMap<>();
            for (String transformId : departedSinceLastPoll.keySet()) {
                TaskFacts departed = departedSinceLastPoll.remove(transformId);
                if (departed != null) {
                    factsById.put(transformId, departed);
                }
            }
            // a task that departed and registered again since the last poll is counted once, with its current facts
            for (TransformTask task : transformNode.getTransformTasks()) {
                factsById.put(task.getTransformId(), TaskFacts.of(task));
            }

            long uiam = 0;
            long legacy = 0;
            long crossProject = 0;
            long origin = 0;
            for (TaskFacts facts : factsById.values()) {
                var uiamAuth = facts.uiamAuth();
                if (uiamAuth != null) {
                    if (uiamAuth) {
                        uiam++;
                    } else {
                        legacy++;
                    }
                }
                var lastSearchCrossProject = facts.lastSearchCrossProject();
                if (lastSearchCrossProject != null) {
                    if (lastSearchCrossProject) {
                        crossProject++;
                    } else {
                        origin++;
                    }
                }
            }
            cachedCounts = new CpsCounts(uiam, legacy, crossProject, origin);
        } catch (Exception e) {
            logger.atDebug()
                .withThrowable(e)
                .log("Failed to emit metrics for Transform Cross-Project. These are best-effort and can be skipped.");
            cachedCounts = CpsCounts.EMPTY;
        }
    }

    @Override
    protected void doStop() {
        if (scheduledPoll != null) {
            scheduledPoll.cancel();
            scheduledPoll = null;
        }
    }

    @Override
    protected void doClose() {
        for (AutoCloseable metric : registeredMetrics) {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        }
    }
}
