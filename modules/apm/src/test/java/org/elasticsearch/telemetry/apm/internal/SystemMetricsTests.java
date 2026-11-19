/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

public class SystemMetricsTests extends ESTestCase {

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

            List<String> registeredGauges = registry.getRecorder().getRegisteredMetrics(InstrumentType.LONG_ASYNC_GAUGE);
            boolean openFdSupported = ProcessProbe.getOpenFileDescriptorCount() >= 0;
            boolean maxFdSupported = ProcessProbe.getMaxFileDescriptorCount() >= 0;
            assertEquals("jvm.fd.used should be registered if supported", openFdSupported, registeredGauges.contains("jvm.fd.used"));
            assertEquals("jvm.fd.max should be registered if supported", maxFdSupported, registeredGauges.contains("jvm.fd.max"));
            assertEquals(
                "jvm.file_descriptor.count should be registered if emitting OTel metrics",
                openFdSupported && emitOTelMetrics,
                registeredGauges.contains("jvm.file_descriptor.count")
            );
            assertEquals(
                "jvm.file_descriptor.limit should be registered if emitting OTel metrics",
                maxFdSupported && emitOTelMetrics,
                registeredGauges.contains("jvm.file_descriptor.limit")
            );
        }
    }
}
