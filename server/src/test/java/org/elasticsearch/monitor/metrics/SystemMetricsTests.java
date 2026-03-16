/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

public class SystemMetricsTests extends ESTestCase {

    public void testLegacyMetricsAlwaysRegistered() {
        System.setProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "true");
        try {
            RecordingMeterRegistry registry = new RecordingMeterRegistry();
            SystemMetrics systemMetrics = new SystemMetrics(registry, false);
            systemMetrics.start();

            List<String> registeredGauges = registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_GAUGE);
            assertTrue("jvm.fd.used should be registered", registeredGauges.contains("jvm.fd.used"));
            assertTrue("jvm.fd.max should be registered", registeredGauges.contains("jvm.fd.max"));
            assertFalse("jvm.file_descriptor.count should NOT be registered", registeredGauges.contains("jvm.file_descriptor.count"));
            assertFalse("jvm.file_descriptor.limit should NOT be registered", registeredGauges.contains("jvm.file_descriptor.limit"));

            systemMetrics.close();
        } finally {
            System.clearProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY);
        }
    }

    public void testOtelMetricsRegisteredWhenEnabled() {
        System.setProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "true");
        try {
            RecordingMeterRegistry registry = new RecordingMeterRegistry();
            SystemMetrics systemMetrics = new SystemMetrics(registry, true);
            systemMetrics.start();

            List<String> registeredGauges = registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_GAUGE);
            assertTrue("jvm.fd.used should be registered", registeredGauges.contains("jvm.fd.used"));
            assertTrue("jvm.fd.max should be registered", registeredGauges.contains("jvm.fd.max"));
            assertTrue("jvm.file_descriptor.count should be registered", registeredGauges.contains("jvm.file_descriptor.count"));
            assertTrue("jvm.file_descriptor.limit should be registered", registeredGauges.contains("jvm.file_descriptor.limit"));

            systemMetrics.close();
        } finally {
            System.clearProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY);
        }
    }
}
