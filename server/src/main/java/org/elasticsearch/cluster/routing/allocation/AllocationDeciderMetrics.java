/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Collection;
import java.util.function.Supplier;

/**
 * A place where metrics related to allocation deciders can live
 */
public class AllocationDeciderMetrics {

    public static final String WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE = "es.allocator.deciders.write_load.max_latency_value.current";

    private final MeterRegistry meterRegistry;

    public AllocationDeciderMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public void registerWriteLoadDeciderMaxLatencyGauge(Supplier<Collection<LongWithAttributes>> maxLatencySupplier) {
        meterRegistry.registerLongsGauge(
            WRITE_LOAD_DECIDER_MAX_LATENCY_VALUE,
            "max latency for write load decider",
            "ms",
            maxLatencySupplier
        );
    }
}
