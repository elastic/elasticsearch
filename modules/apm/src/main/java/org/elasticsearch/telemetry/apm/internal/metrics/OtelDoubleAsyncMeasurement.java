/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;

import org.elasticsearch.telemetry.metric.DoubleAsyncMeasurement;

import java.util.Map;

class OtelDoubleAsyncMeasurement implements DoubleAsyncMeasurement {

    private final String metricName;
    private final ObservableDoubleMeasurement otelMeasurement;
    private final boolean suppressZeroValues;

    OtelDoubleAsyncMeasurement(String metricName, ObservableDoubleMeasurement otelMeasurement, boolean suppressZeroValues) {
        this.metricName = metricName;
        this.otelMeasurement = otelMeasurement;
        this.suppressZeroValues = suppressZeroValues;
    }

    @Override
    public void record(double value) {
        if (suppressZeroValues && value == 0.0) {
            return;
        }
        otelMeasurement.record(value);
    }

    @Override
    public void record(double value, Map<String, Object> attributes) {
        if (suppressZeroValues && value == 0.0) {
            return;
        }
        otelMeasurement.record(value, OtelHelper.fromMap(metricName, attributes));
    }
}
