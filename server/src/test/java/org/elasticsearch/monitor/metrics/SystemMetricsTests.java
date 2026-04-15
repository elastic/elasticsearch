/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.After;
import org.junit.Before;

import java.util.List;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

public class SystemMetricsTests extends ESTestCase {

    @Before
    @SuppressForbidden(reason = "sets system property for test setup")
    public void setup() {
        System.setProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "true");
    }

    @After
    @SuppressForbidden(reason = "clears system property for test teardown")
    public void teardown() {
        System.clearProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY);
    }

    public void testOTelMetricsRegisteredWhenEnabled() {
        testSystemMetrics(true);
    }

    public void testOTelMetricsNotRegisteredWhenNotEnabled() {
        testSystemMetrics(false);
    }

    private static void testSystemMetrics(boolean emitOTelMetrics) {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        try (SystemMetrics systemMetrics = new SystemMetrics(registry, emitOTelMetrics)) {
            systemMetrics.start();

            List<String> registeredGauges = registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_GAUGE);
            assertTrue("jvm.fd.used should always be registered", registeredGauges.contains("jvm.fd.used"));
            assertTrue("jvm.fd.max should always be registered", registeredGauges.contains("jvm.fd.max"));
            assertEquals(
                "jvm.file_descriptor.count should be registered if emitting OTel metrics",
                registeredGauges.contains("jvm.file_descriptor.count"),
                emitOTelMetrics
            );
            assertEquals(
                "jvm.file_descriptor.limit should be registered if emitting OTel metrics",
                registeredGauges.contains("jvm.file_descriptor.limit"),
                emitOTelMetrics
            );
        }
    }
}
