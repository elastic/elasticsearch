/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.test.otelfilter;

import org.elasticsearch.common.logging.activity.QueryLogging;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.telemetry.TelemetryLogEventFilter;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Test plugin that installs a {@link TelemetryLoggingFilterProvider} on the querylog OTel appender.
 *
 * <p>The filter:
 * <ul>
 *   <li>Drops events whose {@code indices} field contains DROP_INDEX_NAME.</li>
 *   <li>Adds {@code MARKER_FIELD: MARKER_VALUE} to all other events, so tests can assert the
 *       filter ran.</li>
 * </ul>
 */
public class TestOtelFilterPlugin extends Plugin implements TelemetryLoggingFilterProvider {

    public static final String DROP_INDEX_NAME = "filter_test_drop_index";

    /** Attribute key added by the filter to events that are not dropped. */
    public static final String MARKER_FIELD = "test.filter.applied";

    /** Value of {@link #MARKER_FIELD}. */
    public static final String MARKER_VALUE = "yes";

    @Override
    public TelemetryLogEventFilter getLogFilter(String appenderName) {
        if ("querylog_otel".equals(appenderName) == false) {
            return null;
        }
        return event -> {
            Object indices = event.get(QueryLogging.QUERY_FIELD_INDICES);
            if (indices instanceof String[] arrayIndices && arrayIndices.length > 0 && arrayIndices[0].equals(DROP_INDEX_NAME)) {
                return null;
            }
            Map<String, Object> modified = new HashMap<>(event);
            modified.put(MARKER_FIELD, MARKER_VALUE);
            return modified;
        };
    }
}
