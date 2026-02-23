/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.monitor.jvm.SunThreadInfo;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.process.ProcessProbe;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class SystemMetrics extends AbstractLifecycleComponent {
    private final Logger logger = LogManager.getLogger(SystemMetrics.class);
    public static final String OTEL_METRICS_ENABLED_SYSTEM_PROPERTY = "telemetry.otel.metrics.enabled";

    private static final AllocatedBytesMetrics ALLOCATED_BYTES_METRICS = new AllocatedBytesMetrics();

    private final MeterRegistry registry;
    private final List<AutoCloseable> metrics = new ArrayList<>();

    public SystemMetrics(MeterRegistry registry) {
        this.registry = registry;
    }

    @Override
    protected void doStart() {
        if (Booleans.parseBoolean(System.getProperty(OTEL_METRICS_ENABLED_SYSTEM_PROPERTY, "false")) == false) {
            return;
        }
        registerFileDescriptorsMetrics();
        registerSystemMemoryMetrics();
        registerCgroupMemoryMetrics();
        registerJvmGcMetrics();
        registerSystemCpuMetrics();
    }

    @Override
    protected void doStop() {}

    @Override
    protected void doClose() {
        for (AutoCloseable metric : metrics) {
            try {
                metric.close();
            } catch (Exception e) {
                logger.warn("metrics close() method should not throw Exception", e);
            }
        }
    }

    private void registerFileDescriptorsMetrics() {
        registerLongGaugeUnlessNegative(
            "jvm.file_descriptor.count",
            "The number of opened file descriptors.",
            "{file_descriptor}",
            ProcessProbe::getOpenFileDescriptorCount
        );
        registerLongGaugeUnlessNegative(
            "jvm.file_descriptor.limit",
            "The maximum number of opened file descriptors.",
            "{file_descriptor}",
            ProcessProbe::getMaxFileDescriptorCount
        );
    }

    private void registerSystemMemoryMetrics() {
        if (OsProbe.getInstance().getTotalPhysicalMemorySize() <= 0) {
            return;
        }
        metrics.add(
            registry.registerLongGauge(
                "system.memory.actual.free",
                "Actual free memory in bytes. It is calculated based on the OS. "
                    + "On Linux it consists of the free memory plus caches and buffers."
                    + " On OSX it is a sum of free memory and the inactive memory. On Windows, this value does not include memory "
                    + "consumed by system caches and buffers.",
                "By",
                () -> new LongWithAttributes(OsProbe.getInstance().getActualFreePhysicalMemorySize())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                "system.memory.total",
                "Total memory.",
                "By",
                () -> new LongWithAttributes(OsProbe.getInstance().getTotalPhysicalMemorySize())
            )
        );

        registerLongGaugeUnlessNegative(
            "system.process.memory.size",
            "The total virtual memory the process has.",
            "By",
            ProcessProbe::getTotalVirtualMemorySize
        );

        metrics.add(
            registry.registerLongGauge(
                "jvm.gc.alloc",
                "An approximation of the total amount of memory, in bytes, allocated in heap memory.",
                "By",
                ALLOCATED_BYTES_METRICS::readAllocatedBytes
            )
        );
    }

    private void registerCgroupMemoryMetrics() {
        metrics.add(
            registry.registerLongGauge(
                "system.process.cgroup.memory.mem.usage.bytes",
                "Memory usage in current cgroup slice.",
                "By",
                () -> new LongWithAttributes(OsProbe.getInstance().getCgroupMemoryUsageInBytes().orElse(0L))
            )
        );

        metrics.add(
            registry.registerLongGauge(
                "system.process.cgroup.memory.mem.limit.bytes",
                "Memory limit for current cgroup slice.",
                "By",
                () -> new LongWithAttributes(OsProbe.getInstance().getCgroupMemoryLimitInBytes().orElse(0L))
            )
        );
    }

    private void registerJvmGcMetrics() {
        List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        if (beans.isEmpty()) {
            return;
        }

        metrics.add(registry.registerLongsAsyncCounter("jvm.gc.count", "The total number of collections that have occurred.", "1", () -> {
            var measurements = new ArrayList<LongWithAttributes>(beans.size());
            for (GarbageCollectorMXBean bean : beans) {
                long count = bean.getCollectionCount();
                if (count >= 0) {
                    measurements.add(new LongWithAttributes(count, Map.of("name", bean.getName())));
                }
            }
            return measurements;
        }));

        metrics.add(
            registry.registerLongsAsyncCounter(
                "jvm.gc.time",
                "The approximate accumulated collection elapsed time in " + "milliseconds.",
                "ms",
                () -> {
                    var measurements = new ArrayList<LongWithAttributes>(beans.size());
                    for (GarbageCollectorMXBean bean : beans) {
                        long timeMs = bean.getCollectionTime();
                        if (timeMs >= 0) {
                            measurements.add(new LongWithAttributes(timeMs, Map.of("name", bean.getName())));
                        }
                    }
                    return measurements;
                }
            )
        );
    }

    private void registerSystemCpuMetrics() {
        short initial = OsProbe.getSystemCpuPercent();
        if (initial < 0) {
            return;
        }
        metrics.add(
            registry.registerDoubleGauge(
                "system.cpu.total.norm.pct",
                "System-wide CPU usage as a ratio.",
                "1",
                () -> new DoubleWithAttributes(Math.max(0, OsProbe.getSystemCpuPercent() / 100.0))
            )
        );
    }

    private void registerLongGaugeUnlessNegative(String name, String description, String unit, LongSupplier supplier) {
        long initial = supplier.getAsLong();
        if (initial < 0) {
            return;
        }
        metrics.add(registry.registerLongGauge(name, description, unit, () -> new LongWithAttributes(supplier.getAsLong())));
    }

    private static final class AllocatedBytesMetrics {
        private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
        private volatile SunThreadInfo sunThreadInfo;

        private AllocatedBytesMetrics() {}

        LongWithAttributes readAllocatedBytes() {
            if (sunThreadInfo == null) {
                if (SunThreadInfo.INSTANCE.isThreadAllocatedMemorySupported() == false
                    || SunThreadInfo.INSTANCE.isThreadAllocatedMemoryEnabled() == false) {
                    return new LongWithAttributes(0);
                }
                this.sunThreadInfo = SunThreadInfo.INSTANCE;
            }

            long total = 0;
            for (long id : THREAD_MX_BEAN.getAllThreadIds()) {
                long allocated = sunThreadInfo.getThreadAllocatedBytes(id);
                if (allocated > 0) {
                    total += allocated;
                }
            }
            return new LongWithAttributes(total);
        }
    }
}
