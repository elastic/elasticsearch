/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.inference;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Objects;

/**
 * Fleet-wide telemetry for trained model deployment ids that are not safe as a single filesystem path
 * component. Used to measure adoption impact before a future release rejects such ids on the start
 * deployment API.
 */
public final class DeploymentPathUnsafeIdTelemetry {

    public static final DeploymentPathUnsafeIdTelemetry NOOP = new DeploymentPathUnsafeIdTelemetry(MeterRegistry.NOOP);

    public static final String PATH_UNSAFE_ID_START_METRIC = "es.ml.inference.deployment.path_unsafe_id.total";

    private final LongCounter pathUnsafeIdStartCounter;

    public DeploymentPathUnsafeIdTelemetry(MeterRegistry meterRegistry) {
        Objects.requireNonNull(meterRegistry);
        this.pathUnsafeIdStartCounter = meterRegistry.registerLongCounter(
            PATH_UNSAFE_ID_START_METRIC,
            "Count of trained model deployment start operations whose effective deployment id is not path-safe.",
            "starts"
        );
    }

    public void recordPathUnsafeDeploymentIdStart() {
        pathUnsafeIdStartCounter.incrementBy(1);
    }
}
