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
 * Records how much each script execution allocated, so an operator can see the real distribution before committing to a
 * limit. A histogram because the tail is what matters and a mean hides it. Attributed by script context only: inline
 * scripts are named per source, so the script name would be unbounded cardinality.
 */
public final class AllocationMetrics {

    static final String METRIC_EXECUTION_ALLOCATION = "es.script.painless.allocation.execution.histogram";

    /** The script context an execution ran in. */
    static final String CONTEXT_ATTRIBUTE = "context";

    /** Powers of two from 1kb to 16gb, with implicit under- and overflow buckets at the ends. */
    static final List<Long> BUCKET_BOUNDARIES = LongStream.rangeClosed(10, 34).mapToObj(power -> 1L << power).toList();

    /** Stands in until real telemetry is installed. */
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

    /** A recorder for scripts of one context, built once per compile. */
    public ContextRecorder forContext(String scriptContextName) {
        return new ContextRecorder(executionAllocationHistogram, Map.of(CONTEXT_ATTRIBUTE, scriptContextName));
    }

    /**
     * Records executions of one script context. A script's context is fixed when it compiles, so the recorder is built
     * then and injected into the generated class as a static constant: recording an execution needs no attribute work.
     */
    public static final class ContextRecorder {

        private final LongHistogram executionAllocationHistogram;
        private final Map<String, Object> attributes;

        private ContextRecorder(LongHistogram executionAllocationHistogram, Map<String, Object> attributes) {
            this.executionAllocationHistogram = executionAllocationHistogram;
            this.attributes = attributes;
        }

        /** Records one execution's total, from the generated {@code execute} method's return path. */
        public void recordExecutionAllocation(long totalBytes) {
            executionAllocationHistogram.record(totalBytes, attributes);
        }
    }
}
