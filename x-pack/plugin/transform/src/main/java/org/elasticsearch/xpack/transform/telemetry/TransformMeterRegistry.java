/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.telemetry;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Objects;

public record TransformMeterRegistry(LongCounter autoMigrationCount) {
    public TransformMeterRegistry {
        Objects.requireNonNull(autoMigrationCount);
    }

    public static TransformMeterRegistry create(MeterRegistry meterRegistry) {
        return new TransformMeterRegistry(
            meterRegistry.registerLongCounter(
                "es.transform.automigration.count.total",
                "Count of when a Transform is automatically migrated from a deprecated setting or feature",
                "count"
            )
        );
    }

    public static TransformMeterRegistry noOp() {
        return new TransformMeterRegistry(LongCounter.NOOP);
    }
}
