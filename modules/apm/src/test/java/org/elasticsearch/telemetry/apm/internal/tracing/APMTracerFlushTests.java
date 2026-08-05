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
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class APMTracerFlushTests extends ESTestCase {

    /**
     * A {@link TraceSupplier} that records, in order, when {@code attemptFlushTraces()} and {@code close()} are called
     * so tests can assert the shutdown sequence. The flush result is configurable so the same double can model a
     * successful flush, a failed one, or one that throws.
     */
    private static class RecordingTraceSupplier implements TraceSupplier {
        final List<String> calls = new ArrayList<>();
        private final Supplier<CompletableResultCode> onFlush;

        RecordingTraceSupplier() {
            this(CompletableResultCode::ofSuccess);
        }

        RecordingTraceSupplier(Supplier<CompletableResultCode> onFlush) {
            this.onFlush = onFlush;
        }

        @Override
        public OpenTelemetry get() {
            return OpenTelemetry.noop();
        }

        @Override
        public CompletableResultCode attemptFlushTraces() {
            calls.add("attemptFlushTraces");
            return onFlush.get();
        }

        @Override
        public void close() {
            calls.add("close");
        }
    }

    public void testDoStopFlushesAndDoCloseCloses() {
        RecordingTraceSupplier supplier = new RecordingTraceSupplier();

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(settings, supplier, false, 0, false);
        tracer.start();
        tracer.stop();
        assertThat(supplier.calls, contains("attemptFlushTraces"));

        tracer.close();
        assertThat(supplier.calls, contains("attemptFlushTraces", "close"));
    }

    public void testAttemptFlushTracesIsNoopWhenDisabled() {
        RecordingTraceSupplier supplier = new RecordingTraceSupplier();

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer tracer = new APMTracer(settings, supplier, false, 0, false);
        tracer.start();
        tracer.attemptFlushTraces();

        assertThat(supplier.calls, empty());
    }

    public void testDoStopClosesAndDestroysServicesEvenIfFlushThrows() {
        RecordingTraceSupplier supplier = new RecordingTraceSupplier(() -> { throw new RuntimeException("simulated flush failure"); });

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), true).build();
        APMTracer tracer = new APMTracer(settings, supplier, false, 0, false);
        tracer.start();
        tracer.close(); // must not throw

        assertThat(supplier.calls, contains("attemptFlushTraces", "close"));
        assertThat(tracer.getSpans(), anEmptyMap());
    }

    /**
     * When tracing is disabled at shutdown, the flush must be skipped entirely — a user who disabled tracing
     * may have done so specifically to prevent data from being exported (e.g. bad pipeline, sensitive data).
     * close() must still be called to release resources.
     */
    public void testDoStopSkipsFlushWhenTracingDisabled() {
        RecordingTraceSupplier supplier = new RecordingTraceSupplier();

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), false).build();
        APMTracer tracer = new APMTracer(settings, supplier, false, 0, false);
        tracer.start();
        tracer.close();

        assertThat(supplier.calls, contains("close"));
    }

}
