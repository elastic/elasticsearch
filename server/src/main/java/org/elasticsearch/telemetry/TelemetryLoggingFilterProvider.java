/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.core.Nullable;

/**
 * Interface for plugins to supply a per-appender {@link TelemetryLogEventFilter}.
 * Implement this interface (directly on your {@link org.elasticsearch.plugins.Plugin} class
 * or on a delegate) to attach a filter to a named telemetry log appender.
 * Appender name constants for APM are defined in {@code OtelSdkExportLogsSupplier}.
 */
public interface TelemetryLoggingFilterProvider {
    /**
     * Return a filter for the named appender, or {@code null} if this provider
     * has no filter for it.
     */
    @Nullable
    TelemetryLogEventFilter getLogFilter(String appenderName);
}
