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
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

import static org.elasticsearch.telemetry.TelemetryProvider.OTEL_METRICS_ENABLED_SYSTEM_PROPERTY;

/**
 * Emits system and JVM metrics compatible with the Elastic APM Java agent, complementing
 * the OTel SDK's JMX auto-instrumentation.
 */
public class SystemMetrics extends AbstractLifecycleComponent {
    private static final Logger logger = LogManager.getLogger(SystemMetrics.class);

    private static final MemoryMXBean MEMORY_BEAN = ManagementFactory.getMemoryMXBean();
    private static final ThreadMXBean THREAD_BEAN = ManagementFactory.getThreadMXBean();
    private static final List<MemoryPoolMXBean> MEMORY_POOL_MX_BEANS = ManagementFactory.getMemoryPoolMXBeans();
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
        registerJvmMemoryMetrics();
        registerJvmGcMetrics();
        registerJvmThreadMetrics();
        registerFileDescriptorMetrics();
        registerSystemMemoryMetrics();
        registerCgroupMemoryMetrics();
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

    // TODO: remove when dashboards are migrated to OTel SDK auto-emitted jvm.memory.used, jvm.memory.committed,
    // and jvm.memory.limit (per-pool, with jvm.memory.pool.name and jvm.memory.type attributes)
    private void registerJvmMemoryMetrics() {
        metrics.add(
            registry.registerLongGauge(
                "jvm.memory.heap.used",
                "The amount of used heap memory in bytes.",
                "By",
                () -> new LongWithAttributes(MEMORY_BEAN.getHeapMemoryUsage().getUsed())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                "jvm.memory.heap.committed",
                "The amount of heap memory in bytes that is committed for the JVM to use.",
                "By",
                () -> new LongWithAttributes(MEMORY_BEAN.getHeapMemoryUsage().getCommitted())
            )
        );
        registerLongGaugeUnlessNegative(
            "jvm.memory.heap.max",
            "The maximum amount of heap memory in bytes that can be used for memory management.",
            "By",
            () -> MEMORY_BEAN.getHeapMemoryUsage().getMax()
        );
        metrics.add(
            registry.registerLongGauge(
                "jvm.memory.non_heap.used",
                "The amount of used non-heap memory in bytes.",
                "By",
                () -> new LongWithAttributes(MEMORY_BEAN.getNonHeapMemoryUsage().getUsed())
            )
        );
        metrics.add(
            registry.registerLongGauge(
                "jvm.memory.non_heap.committed",
                "The amount of non-heap memory in bytes that is committed for the JVM to use.",
                "By",
                () -> new LongWithAttributes(MEMORY_BEAN.getNonHeapMemoryUsage().getCommitted())
            )
        );
        registerLongGaugeUnlessNegative(
            "jvm.memory.non_heap.max",
            "The maximum amount of non-heap memory in bytes that can be used for memory management.",
            "By",
            () -> MEMORY_BEAN.getNonHeapMemoryUsage().getMax()
        );
        registerPoolGauges("jvm.memory.heap.pool", MemoryType.HEAP);
        registerPoolGauges("jvm.memory.non_heap.pool", MemoryType.NON_HEAP);
    }

    private void registerPoolGauges(String prefix, MemoryType type) {
        List<MemoryPoolMXBean> pools = MEMORY_POOL_MX_BEANS.stream().filter(p -> p.getType() == type).toList();
        if (pools.isEmpty()) {
            return;
        }
        metrics.add(registry.registerLongsGauge(prefix + ".used", "The amount of memory in bytes used by this pool.", "By", () -> {
            var result = new ArrayList<LongWithAttributes>(pools.size());
            for (MemoryPoolMXBean pool : pools) {
                result.add(new LongWithAttributes(pool.getUsage().getUsed(), Map.of("name", pool.getName())));
            }
            return result;
        }));
        metrics.add(
            registry.registerLongsGauge(
                prefix + ".committed",
                "The amount of memory in bytes committed for the JVM to use in this pool.",
                "By",
                () -> {
                    var result = new ArrayList<LongWithAttributes>(pools.size());
                    for (MemoryPoolMXBean pool : pools) {
                        result.add(new LongWithAttributes(pool.getUsage().getCommitted(), Map.of("name", pool.getName())));
                    }
                    return result;
                }
            )
        );
        metrics.add(
            registry.registerLongsGauge(
                prefix + ".max",
                "The maximum amount of memory in bytes that can be used by this pool.",
                "By",
                () -> {
                    var result = new ArrayList<LongWithAttributes>(pools.size());
                    for (MemoryPoolMXBean pool : pools) {
                        long max = pool.getUsage().getMax();
                        if (max >= 0) {
                            result.add(new LongWithAttributes(max, Map.of("name", pool.getName())));
                        }
                    }
                    return result;
                }
            )
        );
    }

    // TODO: jvm.gc.count and jvm.gc.time can be removed when dashboards are migrated to OTel SDK auto-emitted
    // jvm.gc.duration histogram (per-collector, with jvm.gc.name and jvm.gc.action attributes).
    // jvm.gc.alloc has no OTel SDK equivalent and must be kept.
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
                "The approximate accumulated collection elapsed time in milliseconds.",
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

        metrics.add(
            registry.registerLongGauge(
                "jvm.gc.alloc",
                "An approximation of the total amount of memory, in bytes, allocated in heap memory.",
                "By",
                ALLOCATED_BYTES_METRICS::readAllocatedBytes
            )
        );
    }

    // TODO: remove when dashboards are migrated to OTel SDK auto-emitted jvm.thread.count (ES-14386)
    // (with jvm.thread.daemon and jvm.thread.state attributes)
    private void registerJvmThreadMetrics() {
        metrics.add(
            registry.registerLongGauge(
                "jvm.thread.count",
                "The current number of live threads including both daemon and non-daemon threads.",
                "{thread}",
                () -> new LongWithAttributes(THREAD_BEAN.getThreadCount())
            )
        );
    }

    // TODO: remove jvm.fd.used / jvm.fd.max once dashboards are migrated to the OTel semantic convention names (ES-14386)
    // (jvm.file_descriptor.count / jvm.file_descriptor.limit)
    private void registerFileDescriptorMetrics() {
        registerLongGaugeUnlessNegative(
            "jvm.fd.used",
            "The number of opened file descriptors. As previously emitted by APM Agent.",
            "{file_descriptor}",
            ProcessProbe::getOpenFileDescriptorCount
        );
        registerLongGaugeUnlessNegative(
            "jvm.fd.max",
            "The maximum number of opened file descriptors. As previously emitted by APM Agent.",
            "{file_descriptor}",
            ProcessProbe::getMaxFileDescriptorCount
        );
        registerLongGaugeUnlessNegative(
            "jvm.file_descriptor.count",
            "The number of opened file descriptors. As per the OTel Semantic Convention.",
            "{file_descriptor}",
            ProcessProbe::getOpenFileDescriptorCount
        );
        registerLongGaugeUnlessNegative(
            "jvm.file_descriptor.limit",
            "The maximum number of opened file descriptors. As per the OTel Semantic Convention.",
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
                "Actual free memory in bytes.",
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

    // TODO: system.process.cpu.total.norm.pct can be removed when dashboards are migrated to OTel SDK
    // auto-emitted jvm.cpu.recent_utilization. system.cpu.total.norm.pct has no OTel SDK equivalent and must be kept.
    private void registerSystemCpuMetrics() {
        short initialSystemCpu = OsProbe.getSystemCpuPercent();
        if (initialSystemCpu >= 0) {
            metrics.add(
                registry.registerDoubleGauge(
                    "system.cpu.total.norm.pct",
                    "System-wide CPU usage as a ratio.",
                    "1",
                    () -> new DoubleWithAttributes(Math.max(0, OsProbe.getSystemCpuPercent() / 100.0))
                )
            );
        }
        short initialProcessCpu = ProcessProbe.getProcessCpuPercent();
        if (initialProcessCpu >= 0) {
            metrics.add(
                registry.registerDoubleGauge(
                    "system.process.cpu.total.norm.pct",
                    "Process CPU usage as a ratio.",
                    "1",
                    () -> new DoubleWithAttributes(Math.max(0, ProcessProbe.getProcessCpuPercent() / 100.0))
                )
            );
        }
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
            for (long allocated : sunThreadInfo.getAllThreadAllocatedBytes(THREAD_MX_BEAN.getAllThreadIds())) {
                if (allocated > 0) {
                    total += allocated;
                }
            }
            return new LongWithAttributes(total);
        }
    }
}
