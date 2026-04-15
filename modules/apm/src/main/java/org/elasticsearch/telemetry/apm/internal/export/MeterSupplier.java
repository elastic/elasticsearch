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

import java.nio.file.Path;
import java.util.function.Supplier;

public interface MeterSupplier extends Supplier<Meter>, AutoCloseable {

    /**
     * Export any buffered metrics on a best-effort basis.
     * <p>
     * This defaults to a no-op just to support the fairly widespread practice of using a lambda for this in tests.
     */
    default void attemptFlushMetrics() {}

    /**
     * Late-initializes the disk buffer directory for metric export buffering.
     * Only meaningful for suppliers that support disk-backed buffering.
     */
    default void setDiskBufferPath(Path path) {}

    @Override
    default void close() {}
}
