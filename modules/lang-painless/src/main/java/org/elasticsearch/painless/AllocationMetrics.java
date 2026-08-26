/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.painless;

import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;

/**
 * Records how much each Painless script execution allocated, so an operator can see the real distribution of script
 * allocation before deciding whether to enforce a limit. Per node; per-cluster (and in serverless, per-project) aggregation
 * comes from the reporting pipeline.
 * <p>
 * A histogram rather than a counter, because the useful question is the shape of the distribution — the tail is what
 * matters, and a mean over all executions hides it. Attributed by script context only: the script <i>name</i> is
 * deliberately not an attribute, since inline scripts are named per source and cardinality would be unbounded.
 * <p>
 * Instances are held as a {@code final} field on each compiled script class, injected by the factory at instantiation time.
 * {@code PainlessPlugin} owns the instance and passes the engine a {@code Supplier} view of it, since the engine is built
 * before {@code createComponents} provides a {@code MeterRegistry}; a node without telemetry keeps {@link #NOOP}.
 */
public final class AllocationMetrics {

    public static final String METRIC_EXECUTION_ALLOCATION = "es.script.painless.allocation.execution.histogram";

    /** Attribute naming the script context an execution ran in. */
    static final String CONTEXT_ATTRIBUTE = "context";

    /**
     * Explicit bucket boundaries: powers of two from 1kb (2^10) through 16gb (2^34), with the implicit under- and overflow
     * buckets covering the ends. Doubling steps keep the ladder readable across the five orders of magnitude that separate a
     * trivial script from one large enough to threaten a heap, without the resolution a default ladder would spend on sizes
     * nobody acts on.
     */
    static final List<Long> BUCKET_BOUNDARIES = LongStream.rangeClosed(10, 34).mapToObj(power -> 1L << power).toList();

    /** Used until real telemetry is installed, and by tests that do not care about metrics. */
    public static final AllocationMetrics NOOP = new AllocationMetrics(MeterRegistry.NOOP);

    private final LongHistogram executionAllocationHistogram;

    public AllocationMetrics(MeterRegistry meterRegistry) {
        executionAllocationHistogram = meterRegistry.registerLongHistogram(
            METRIC_EXECUTION_ALLOCATION,
            "heuristic bytes allocated by a single Painless script execution",
            "by",
            BUCKET_BOUNDARIES
        );
    }

    /** Records one execution's total. Called once per execution, from the generated {@code execute} method's return path. */
    public void recordExecutionAllocation(String scriptContextName, long totalBytes) {
        executionAllocationHistogram.record(totalBytes, Map.of(CONTEXT_ATTRIBUTE, scriptContextName));
    }
}
