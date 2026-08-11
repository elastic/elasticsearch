/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * APM counters for Painless allocation-threshold breaches: how often scripts crossed the warning threshold, and how often
 * they were failed by the enforcement limit. Both are per node, so the usual per-cluster (and so, in serverless, per-project)
 * aggregation comes from the reporting pipeline rather than from an attribute here.
 * <p>
 * Breaches are counted with a {@code context} attribute — the script context, e.g. {@code score} or {@code ingest} — which is
 * the first thing an operator needs in order to know where to look. The script <i>name</i> is deliberately not an attribute:
 * inline scripts are named per source, so it would be unbounded cardinality. The name and source go to the log message
 * instead, where volume is bounded by the once-per-execution warning latch.
 * <p>
 * The instance is held statically because the only callers are {@link AllocationGuard} methods invoked directly from
 * generated script bytecode, which has no object to carry a reference on. It starts as {@link #NOOP} and is installed once at
 * node startup from {@code PainlessPlugin#createComponents}, which always runs before any script can execute; a node without
 * telemetry configured simply keeps the no-op.
 */
public final class AllocationMetrics {

    public static final String METRIC_WARN_EXCEEDED = "es.script.painless.allocation.warn_exceeded.total";
    public static final String METRIC_LIMIT_EXCEEDED = "es.script.painless.allocation.limit_exceeded.total";

    /** Attribute naming the script context a breach happened in; bounded cardinality, unlike a script name. */
    static final String CONTEXT_ATTRIBUTE = "context";

    /** Used until (and if) real telemetry is installed, and by tests that do not care about metrics. */
    public static final AllocationMetrics NOOP = new AllocationMetrics(MeterRegistry.NOOP);

    private static volatile AllocationMetrics instance = NOOP;

    private final LongCounter warnExceededCounter;
    private final LongCounter limitExceededCounter;

    public AllocationMetrics(MeterRegistry meterRegistry) {
        warnExceededCounter = meterRegistry.registerLongCounter(
            METRIC_WARN_EXCEEDED,
            "number of Painless script executions whose heuristic allocation total crossed the per-context warning threshold",
            "unit"
        );
        limitExceededCounter = meterRegistry.registerLongCounter(
            METRIC_LIMIT_EXCEEDED,
            "number of Painless script executions failed for exceeding the per-context heuristic allocation limit",
            "unit"
        );
    }

    /** Installs the node's metrics at startup. Also used by tests to install a capturing registry and to restore {@link #NOOP}. */
    public static void setInstance(AllocationMetrics allocationMetrics) {
        instance = allocationMetrics;
    }

    static AllocationMetrics getInstance() {
        return instance;
    }

    /** Counts one execution that crossed the warning threshold; called at most once per execution, per the warning latch. */
    void recordWarnExceeded(String scriptContextName) {
        warnExceededCounter.incrementBy(1L, Map.of(CONTEXT_ATTRIBUTE, scriptContextName));
    }

    /** Counts one execution failed by the allocation limit. */
    void recordLimitExceeded(String scriptContextName) {
        limitExceededCounter.incrementBy(1L, Map.of(CONTEXT_ATTRIBUTE, scriptContextName));
    }
}
