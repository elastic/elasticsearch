/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm;

import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterBuilder;
import io.opentelemetry.api.metrics.MeterProvider;

public class RecordingOtelMeterProvider implements MeterProvider {
    private final RecordingOtelMeter meter = new RecordingOtelMeter();

    public RecordingOtelMeter meter() {
        return meter;
    }

    @Override
    public MeterBuilder meterBuilder(String instrumentationScopeName) {
        return new RecordingMeterBuilder();
    }

    private class RecordingMeterBuilder implements MeterBuilder {

        @Override
        public MeterBuilder setSchemaUrl(String schemaUrl) {
            return this;
        }

        @Override
        public MeterBuilder setInstrumentationVersion(String instrumentationScopeVersion) {
            return this;
        }

        @Override
        public Meter build() {
            return meter;
        }
    }
}
