/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.export;

import io.opentelemetry.api.OpenTelemetry;

import java.util.function.Supplier;

/**
 * Analogous to {@link MeterSupplier} but for traces. Supplies an {@link OpenTelemetry} instance
 * (rather than just a {@code Tracer}) because callers also need {@code getPropagators()} for
 * context propagation.
 */
public interface TraceSupplier extends Supplier<OpenTelemetry>, AutoCloseable {

    /**
     * Export any buffered traces on a best-effort basis.
     */
    default void attemptFlushTraces() {}

    @Override
    default void close() {}
}
