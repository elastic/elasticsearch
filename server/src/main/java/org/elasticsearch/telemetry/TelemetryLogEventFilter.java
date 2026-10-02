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

import java.util.Map;

/**
 * Filter for telemetry log appenders. Implementations can inspect, rewrite, or drop a log event
 * before it is exported.
 */
@FunctionalInterface
public interface TelemetryLogEventFilter {
    /**
     * @param event the log event to inspect
     * @return the event to forward (possibly rewritten), or {@code null} to drop it
     */
    @Nullable
    Map<String, Object> filter(Map<String, Object> event);
}
