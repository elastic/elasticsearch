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

/**
 * Holds all metric information needed to register a metric in {@link MeterRegistry}.
 *
 * @param name          The unique metric name.
 * @param description   The brief metric description.
 * @param unit          The metric unit (e.g. count).
 */
public record SecurityMetricInfo(String name, String description, String unit) {

    public SecurityMetricInfo(String name, String description, String unit) {
        this.name = Objects.requireNonNull(name);
        this.description = Objects.requireNonNull(description);
        this.unit = Objects.requireNonNull(unit);
    }

    public LongCounter registerAsLongCounter(MeterRegistry meterRegistry) {
        return meterRegistry.registerLongCounter(this.name(), this.description(), this.unit());
    }

    public LongHistogram registerAsLongHistogram(MeterRegistry meterRegistry) {
        return meterRegistry.registerLongHistogram(this.name(), this.description(), this.unit());
    }

}
