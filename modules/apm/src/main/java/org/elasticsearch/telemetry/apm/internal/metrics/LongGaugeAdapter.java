/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry.apm.internal.metrics;

import io.opentelemetry.api.metrics.Meter;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * LongGaugeAdapter wraps an otel ObservableLongMeasurement
 */
public class LongGaugeAdapter extends AbstractGaugeAdapter<io.opentelemetry.api.metrics.ObservableLongGauge, Long>
    implements
        org.elasticsearch.telemetry.metric.LongGauge {

    public LongGaugeAdapter(Meter meter, String name, String description, String unit) {
        super(meter, name, description, unit);
    }

    @Override
    io.opentelemetry.api.metrics.ObservableLongGauge buildInstrument(Meter meter) {

        return Objects.requireNonNull(meter)
            .gaugeBuilder(getName())
            .setDescription(getDescription())
            .setUnit(getUnit())
            .ofLongs()
            .buildWithCallback(measurement -> popRecords().forEach((attributes, value) -> measurement.record(value, attributes)));
    }

    @Override
    public void record(long value) {
        record(value, Collections.emptyMap());
    }

    @Override
    public void record(long value, Map<String, Object> attributes) {
        record(value, OtelHelper.fromMap(attributes));
    }
}
