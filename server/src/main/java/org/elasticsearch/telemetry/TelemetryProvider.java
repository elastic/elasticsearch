/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.telemetry;

import org.elasticsearch.telemetry.metric.Meter;
import org.elasticsearch.telemetry.tracing.Tracer;

public interface TelemetryProvider {

    Tracer getTracer();

    Meter getMeter();

    TelemetryProvider NOOP = new TelemetryProvider() {

        @Override
        public Tracer getTracer() {
            return Tracer.NOOP;
        }

        @Override
        public Meter getMeter() {
            return Meter.NOOP;
        }
    };
}
