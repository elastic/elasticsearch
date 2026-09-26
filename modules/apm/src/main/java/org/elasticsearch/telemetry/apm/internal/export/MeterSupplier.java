/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.sdk.common.CompletableResultCode;

import java.util.function.Supplier;

public interface MeterSupplier extends Supplier<Meter>, AutoCloseable {

    /**
     * Initiates a best-effort export of buffered metrics. Callers must join the result with an appropriate timeout.
     */
    default CompletableResultCode attemptFlushMetrics() {
        return CompletableResultCode.ofSuccess();
    }

    /**
     * Returns the underlying {@link MeterProvider} for wiring SDK self-monitoring into other exporters.
     * Defaults to {@link MeterProvider#noop()} when the supplier does not expose a concrete provider.
     */
    default MeterProvider getMeterProvider() {
        return MeterProvider.noop();
    }

    @Override
    default void close() {}
}
