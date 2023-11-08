/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor;

import org.elasticsearch.monitor.jvm.JvmService;
import org.elasticsearch.monitor.process.ProcessService;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.MeterRegistry;

public class MonitorMetrics {
    private final LongGauge threadsGauge;
    private final LongGauge openFileDescriptorGauge;

    public MonitorMetrics(MeterRegistry meterRegistry, JvmService jvmService, ProcessService processService) {
        this(
            meterRegistry.registerLongGauge(
                "elasticsearch.jvm.threads",
                "The number of threads in the JVM",
                "threads",
                jvmService.threadsCountSupplier()
            ),
            meterRegistry.registerLongGauge(
                "elasticsearch.process.open_file_descriptors",
                "The number of open file descriptors for the Elasticsearch process",
                "files",
                processService.openFileDescriptorSupplier()
            )
        );
    }

    MonitorMetrics(LongGauge threadsGauge, LongGauge openFileDescriptorGauge) {
        this.threadsGauge = threadsGauge;
        this.openFileDescriptorGauge = openFileDescriptorGauge;
    }

    public LongGauge getThreadsGauge() {
        return threadsGauge;
    }

    public LongGauge getOpenFileDescriptorGauge() {
        return openFileDescriptorGauge;
    }
}
