/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.metrics.Meter;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;

public class APMMeterServiceTests extends ESTestCase {

    /** doStop() must call attemptFlushMetrics() before close() on the OTel supplier. */
    public void testDoStopFlushesBeforeClose() {
        List<String> calls = new ArrayList<>();
        MeterSupplier trackingSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public void attemptFlushMetrics() {
                calls.add("attemptFlushMetrics");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };
        MeterSupplier noopSupplier = () -> OpenTelemetry.noop().getMeter("noop");

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        APMMeterService service = new APMMeterService(settings, trackingSupplier, noopSupplier);
        service.start();
        service.stop();

        assertThat(calls, contains("attemptFlushMetrics", "close"));
    }

    /**
     * The public attemptFlushMetrics() is gated on enabled — callers should not pay the flush cost when metrics are off.
     */
    public void testAttemptFlushMetricsIsNoopWhenDisabled() {
        List<String> calls = new ArrayList<>();
        MeterSupplier trackingSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public void attemptFlushMetrics() {
                calls.add("attemptFlushMetrics");
            }
        };
        MeterSupplier noopSupplier = () -> OpenTelemetry.noop().getMeter("noop");

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
        APMMeterService service = new APMMeterService(settings, trackingSupplier, noopSupplier);
        service.start();
        service.attemptFlushMetrics();

        assertThat(calls, empty());
    }

    /**
     * A flush failure must not prevent close() or the registry switch to noop: losing the flush is acceptable,
     * but leaking resources or leaving the service in a broken state is not.
     */
    public void testDoStopClosesAndSwitchesToNoopEvenIfFlushThrows() {
        List<String> calls = new ArrayList<>();
        MeterSupplier trackingSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public void attemptFlushMetrics() {
                throw new RuntimeException("simulated flush failure");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };
        MeterSupplier trackingNoopSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                calls.add("noop");
                return OpenTelemetry.noop().getMeter("noop");
            }
        };

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), true).build();
        APMMeterService service = new APMMeterService(settings, trackingSupplier, trackingNoopSupplier);
        service.start();
        service.stop(); // must not throw

        assertThat(calls, contains("close", "noop"));
    }

    /**
     * When metrics are disabled at shutdown, the flush must be skipped entirely — a user who disabled metrics
     * may have done so specifically to prevent data from being exported (e.g. bad pipeline, sensitive data).
     * close() must still be called to release resources.
     */
    public void testDoStopSkipsFlushWhenMetricsDisabled() {
        List<String> calls = new ArrayList<>();
        MeterSupplier trackingSupplier = new MeterSupplier() {
            @Override
            public Meter get() {
                return OpenTelemetry.noop().getMeter("test");
            }

            @Override
            public void attemptFlushMetrics() {
                calls.add("attemptFlushMetrics");
            }

            @Override
            public void close() {
                calls.add("close");
            }
        };
        MeterSupplier noopSupplier = () -> OpenTelemetry.noop().getMeter("noop");

        Settings settings = Settings.builder().put(APMAgentSettings.TELEMETRY_METRICS_ENABLED_SETTING.getKey(), false).build();
        APMMeterService service = new APMMeterService(settings, trackingSupplier, noopSupplier);
        service.start();
        service.stop();

        assertThat(calls, contains("close"));
    }

}
