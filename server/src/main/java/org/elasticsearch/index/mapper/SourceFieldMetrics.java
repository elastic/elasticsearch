/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.function.LongSupplier;

/**
 * Contains metrics for operations involving source field.
 */
public class SourceFieldMetrics {
    public static final SourceFieldMetrics NOOP = new SourceFieldMetrics(MeterRegistry.NOOP, () -> 0);

    public static final String SYNTHETIC_SOURCE_LOAD_LATENCY = "es.mapper.synthetic_source.load.latency.histogram";
    public static final String SYNTHETIC_SOURCE_INCOMPATIBLE_MAPPING = "es.mapper.synthetic_source.incompatible_mapping.total";

    private final LongSupplier relativeTimeSupplier;

    private final LongHistogram syntheticSourceLoadLatency;
    private final LongCounter syntheticSourceIncompatibleMapping;

    public SourceFieldMetrics(MeterRegistry meterRegistry, LongSupplier relativeTimeSupplier) {
        this.syntheticSourceLoadLatency = meterRegistry.registerLongHistogram(
            SYNTHETIC_SOURCE_LOAD_LATENCY,
            "Time it takes to load fields and construct synthetic source",
            "ms"
        );
        this.syntheticSourceIncompatibleMapping = meterRegistry.registerLongCounter(
            SYNTHETIC_SOURCE_INCOMPATIBLE_MAPPING,
            "Number of create/update index operations using mapping not compatible with synthetic source",
            "count"
        );
        this.relativeTimeSupplier = relativeTimeSupplier;
    }

    public LongSupplier getRelativeTimeSupplier() {
        return relativeTimeSupplier;
    }

    public void recordSyntheticSourceLoadLatency(TimeValue value) {
        this.syntheticSourceLoadLatency.record(value.millis());
    }

    public void recordSyntheticSourceIncompatibleMapping() {
        this.syntheticSourceIncompatibleMapping.increment();
    }
}
