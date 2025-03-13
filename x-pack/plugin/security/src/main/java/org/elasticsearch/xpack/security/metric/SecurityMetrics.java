/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.metric;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Objects;
import java.util.function.LongSupplier;

/**
 * This class provides a common way for registering and collecting different types of security metrics.
 * It allows for recoding the number of successful and failed executions as well as to record the execution time.
 *
 * @param <C> The type of context object which is used to attach additional attributes to collected metrics.
 */
public final class SecurityMetrics<C> {

    private final LongCounter successCounter;
    private final LongCounter failuresCounter;
    private final LongHistogram timeHistogram;

    private final SecurityMetricAttributesBuilder<C> attributesBuilder;
    private final LongSupplier nanoTimeSupplier;
    private final SecurityMetricType metricType;

    public SecurityMetrics(
        final SecurityMetricType metricType,
        final MeterRegistry meterRegistry,
        final SecurityMetricAttributesBuilder<C> attributesBuilder,
        final LongSupplier nanoTimeSupplier
    ) {
        this.metricType = Objects.requireNonNull(metricType);
        this.successCounter = metricType.successMetricInfo().registerAsLongCounter(meterRegistry);
        this.failuresCounter = metricType.failuresMetricInfo().registerAsLongCounter(meterRegistry);
        this.timeHistogram = metricType.timeMetricInfo().registerAsLongHistogram(meterRegistry);
        this.attributesBuilder = Objects.requireNonNull(attributesBuilder);
        this.nanoTimeSupplier = Objects.requireNonNull(nanoTimeSupplier);
    }

    public SecurityMetricType type() {
        return this.metricType;
    }

    /**
     * Returns a value of nanoseconds that may be used for relative time calculations.
     * This method should only be used for calculating time deltas.
     */
    public long relativeTimeInNanos() {
        return nanoTimeSupplier.getAsLong();
    }

    /**
     * Records a single success execution.
     *
     * @param context The context object which is used to attach additional attributes to success metric.
     */
    public void recordSuccess(final C context) {
        this.successCounter.incrementBy(1L, attributesBuilder.build(context));
    }

    /**
     * Records a single failed execution.
     *
     * @param context       The context object which is used to attach additional attributes to failed metric.
     * @param failureReason The optional failure reason which is stored as an attributed with recorded failure metric.
     */
    public void recordFailure(final C context, final String failureReason) {
        this.failuresCounter.incrementBy(1L, attributesBuilder.build(context, failureReason));
    }

    /**
     * Records a time in nanoseconds. This method should be called after the execution with provided start time.
     * The {@link #relativeTimeInNanos()} should be used to record the start time.
     *
     * @param context       The context object which is used to attach additional attributes to collected metric.
     * @param startTimeNano The start time (in nanoseconds) before the execution.
     */
    public void recordTime(final C context, final long startTimeNano) {
        final long timeInNanos = relativeTimeInNanos() - startTimeNano;
        this.timeHistogram.record(timeInNanos, this.attributesBuilder.build(context));
    }

}
