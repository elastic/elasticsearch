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

public record TransformMeterRegistry(LongCounter autoMigrationCount, LongCounter runningWithoutCredentialsCount) {
    public TransformMeterRegistry {
        Objects.requireNonNull(autoMigrationCount);
        Objects.requireNonNull(runningWithoutCredentialsCount);
    }

    public static TransformMeterRegistry create(MeterRegistry meterRegistry) {
        return new TransformMeterRegistry(
            meterRegistry.registerLongCounter(
                "es.transform.automigration.count.total",
                "Count of when a Transform is automatically migrated from a deprecated setting or feature",
                "count"
            ),
            meterRegistry.registerLongCounter(
                "es.transform.missing_credentials.count.total",
                "Count of transform runs started without stored security credentials while security is enabled",
                "count"
            )
        );
    }

    public static TransformMeterRegistry noOp() {
        return new TransformMeterRegistry(LongCounter.NOOP, LongCounter.NOOP);
    }
}
