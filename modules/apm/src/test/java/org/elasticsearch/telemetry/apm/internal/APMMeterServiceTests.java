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
import org.elasticsearch.telemetry.apm.internal.APMAgentSettings;
import org.elasticsearch.telemetry.apm.internal.export.MeterSupplier;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.contains;

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
     * doStop() delegates to the supplier unconditionally, regardless of the enabled flag.
     * The supplier is responsible for being a no-op when uninitialised (e.g. resources == null
     * in OtelSdkExportMeterSupplier when metrics were never enabled).
     */
    public void testDoStopDelegatesUnconditionallyWhenMetricsDisabled() {
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

        assertThat(calls, contains("attemptFlushMetrics", "close"));
    }

}
