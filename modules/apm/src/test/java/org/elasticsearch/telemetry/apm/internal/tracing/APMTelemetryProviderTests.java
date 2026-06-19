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
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.sdk.common.CompletableResultCode;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.APMLoggingService;
import org.elasticsearch.telemetry.apm.internal.APMMeterService;
import org.elasticsearch.telemetry.apm.internal.APMTelemetryProvider;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;
import org.elasticsearch.telemetry.apm.internal.export.TraceSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.containsInAnyOrder;

public class APMTelemetryProviderTests extends ESTestCase {

    public void testAttemptFlushAllFlushesMetricsAndTraces() {
        List<String> calls = new ArrayList<>();

        MeterSupplier meterSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public CompletableResultCode attemptFlushMetrics() {
                calls.add("metrics");
                return CompletableResultCode.ofSuccess();
            }
        };

        TraceSupplier traceSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public CompletableResultCode attemptFlushTraces() {
                calls.add("traces");
                return CompletableResultCode.ofSuccess();
            }
        };

        APMTelemetryProvider provider = providerWithSuppliers(meterSupplier, traceSupplier, true, true, 5_000);
        provider.attemptFlush();

        assertThat(calls, containsInAnyOrder("metrics", "traces"));
    }

    public void testAttemptFlushAllReturnsAfterBothComplete() {
        CompletableResultCode metricsResult = new CompletableResultCode();
        CompletableResultCode tracesResult = new CompletableResultCode();

        MeterSupplier meterSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public CompletableResultCode attemptFlushMetrics() {
                return metricsResult;
            }
        };

        TraceSupplier traceSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public CompletableResultCode attemptFlushTraces() {
                return tracesResult;
            }
        };

        // complete both from another thread while attemptFlush() is blocked
        Thread completer = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            metricsResult.succeed();
            tracesResult.succeed();
        });
        completer.setDaemon(true);
        completer.start();

        long start = System.nanoTime();
        APMTelemetryProvider provider = providerWithSuppliers(meterSupplier, traceSupplier, true, true, 5_000);
        provider.attemptFlush();
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertTrue("should wait for results but return before full timeout", elapsedMs >= 50 && elapsedMs < 5_000);
    }

    public void testAttemptFlushAllIsNoopWhenBothDisabled() {
        List<String> calls = new ArrayList<>();

        MeterSupplier meterSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public CompletableResultCode attemptFlushMetrics() {
                calls.add("metrics");
                return CompletableResultCode.ofSuccess();
            }
        };

        TraceSupplier traceSupplier = new TraceSupplier() {
            @Override
            public OpenTelemetry get() {
                return OpenTelemetry.noop();
            }

            @Override
            public CompletableResultCode attemptFlushTraces() {
                calls.add("traces");
                return CompletableResultCode.ofSuccess();
            }
        };

        APMTelemetryProvider provider = providerWithSuppliers(meterSupplier, traceSupplier, false, false, 5_000);
        provider.attemptFlush();

        assertTrue("flush must not be called when disabled", calls.isEmpty());
    }

    private static APMTelemetryProvider providerWithSuppliers(
        MeterSupplier meterSupplier,
        TraceSupplier traceSupplier,
        boolean metricsEnabled,
        boolean tracingEnabled,
        long flushTimeoutMillis
    ) {
        Settings settings = Settings.builder()
            .put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), metricsEnabled)
            .put(APMAgentSettings.TELEMETRY_TRACING_ENABLED_SETTING.getKey(), tracingEnabled)
            .build();
        APMMeterService meterService = new APMMeterService(settings, meterSupplier, () -> OpenTelemetry.noop().getMeter("noop"));
        APMTracer tracer = new APMTracer(settings, traceSupplier, false, 0, false);
        return new APMTelemetryProvider(meterService, tracer, new APMLoggingService(Settings.EMPTY), flushTimeoutMillis);
    }
}
