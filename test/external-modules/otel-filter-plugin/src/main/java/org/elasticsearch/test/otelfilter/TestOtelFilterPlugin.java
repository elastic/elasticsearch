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
import org.elasticsearch.telemetry.TelemetryLogResourceProvider;
import org.elasticsearch.telemetry.TelemetryLoggingFilterProvider;

import java.util.HashMap;
import java.util.Map;

/**
 * Test plugin that exercises the two hooks the OTel logs export offers plugins.
 *
 * <p>It installs a {@link TelemetryLoggingFilterProvider} on the querylog OTel appender, whose filter:
 * <ul>
 *   <li>Drops events whose {@code indices} field contains DROP_INDEX_NAME.</li>
 *   <li>Adds {@code MARKER_FIELD: MARKER_VALUE} to all other events, so tests can assert the
 *       filter ran.</li>
 * </ul>
 *
 * <p>It also implements {@link TelemetryLogResourceProvider} to override the {@code service.name} on
 * the log-delivery resource, standing in for the plugin Serverless supplies in production.
 */
public class TestOtelFilterPlugin extends Plugin implements TelemetryLoggingFilterProvider, TelemetryLogResourceProvider {

    public static final String DROP_INDEX_NAME = "filter_test_drop_index";

    /** Attribute key added by the filter to events that are not dropped. */
    public static final String MARKER_FIELD = "test.filter.applied";

    /** Value of {@link #MARKER_FIELD}. */
    public static final String MARKER_VALUE = "yes";

    /**
     * The {@code service.name} this plugin puts on exported log records.
     */
    public static final String LOG_SERVICE_NAME = "elasticsearch-build-hamster";

    @Override
    public String serviceName() {
        return LOG_SERVICE_NAME;
    }

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
