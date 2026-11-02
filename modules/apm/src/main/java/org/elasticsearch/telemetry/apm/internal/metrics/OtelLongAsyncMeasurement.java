/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.ObservableLongMeasurement;

import org.elasticsearch.telemetry.metric.LongAsyncMeasurement;

import java.util.Map;

class OtelLongAsyncMeasurement implements LongAsyncMeasurement {

    private final String metricName;
    private final ObservableLongMeasurement otelMeasurement;
    private final boolean suppressZeroValues;

    OtelLongAsyncMeasurement(String metricName, ObservableLongMeasurement otelMeasurement, boolean suppressZeroValues) {
        this.metricName = metricName;
        this.otelMeasurement = otelMeasurement;
        this.suppressZeroValues = suppressZeroValues;
    }

    @Override
    public void record(long value) {
        if (suppressZeroValues && value == 0) {
            return;
        }
        otelMeasurement.record(value);
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        if (suppressZeroValues && value == 0) {
            return;
        }
        otelMeasurement.record(value, OtelHelper.fromMap(metricName, attributes));
    }
}
