/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal.tracing;

import io.opentelemetry.api.OpenTelemetry;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class APMTracerFlushTests extends ESTestCase {

    /** doStop() must call attemptFlushTraces() before close() on the trace supplier. */
    public void testDoStopFlushesBeforeClose() {
        List<String> calls = new ArrayList<>();
        TraceSupplier trackingSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public void attemptFlushTraces() {
                calls.add("attemptFlushTraces");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(settings, trackingSupplier);
        tracer.start();
        tracer.stop();

        assertThat(calls, contains("attemptFlushTraces", "close"));
    }

    /**
     * The public attemptFlushTraces() is gated on enabled — callers should not pay the flush cost when tracing is off.
     */
    public void testAttemptFlushTracesIsNoopWhenDisabled() {
        List<String> calls = new ArrayList<>();
        TraceSupplier trackingSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public void attemptFlushTraces() {
                calls.add("attemptFlushTraces");
            }
        };

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer tracer = new APMTracer(settings, trackingSupplier);
        tracer.start();
        tracer.attemptFlushTraces();

        assertThat(calls, empty());
    }

    /**
     * A flush failure must not prevent close() or service teardown: losing the flush is acceptable, but leaking
     * resources or leaving the tracer in a broken state is not.
     */
    public void testDoStopClosesAndDestroysServicesEvenIfFlushThrows() {
        List<String> calls = new ArrayList<>();
        TraceSupplier trackingSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public void attemptFlushTraces() {
                throw new RuntimeException("simulated flush failure");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(settings, trackingSupplier);
        tracer.start();
        tracer.stop(); // must not throw

        assertThat(calls, contains("close"));
        assertThat(tracer.getSpans(), anEmptyMap());
    }

    /**
     * When tracing is disabled at shutdown, the flush must be skipped entirely — a user who disabled tracing
     * may have done so specifically to prevent data from being exported (e.g. bad pipeline, sensitive data).
     * close() must still be called to release resources.
     */
    public void testDoStopSkipsFlushWhenTracingDisabled() {
        List<String> calls = new ArrayList<>();
        TraceSupplier trackingSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public void attemptFlushTraces() {
                calls.add("attemptFlushTraces");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer tracer = new APMTracer(settings, trackingSupplier);
        tracer.start();
        tracer.stop();

        assertThat(calls, contains("close"));
    }

}
