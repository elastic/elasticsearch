/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public final class SystemMetrics {

    private static final String SUN_OS_MX_BEAN = "com.sun.management.OperatingSystemMXBean";
    private static final String UNIX_OS_MX_BEAN = "com.sun.management.UnixOperatingSystemMXBean";
    private static final MethodHandle NOOP_HANDLE = MethodHandles.dropArguments(MethodHandles.constant(long.class, -1L), 0, Object.class);

    private static final OperatingSystemMXBean OS_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();
    private static final MemoryMXBean MEMORY_MX_BEAN = ManagementFactory.getMemoryMXBean();

    private static final MethodHandle GET_MAX_FILE_DESCRIPTOR_COUNT = osLongHandle("getMaxFileDescriptorCount");
    private static final MethodHandle GET_TOTAL_MEMORY_SIZE = osLongHandle("getTotalMemorySize");
    private static final MethodHandle GET_FREE_MEMORY_SIZE = osLongHandle("getFreeMemorySize");
    private static final MethodHandle GET_COMMITTED_VIRTUAL_MEMORY_SIZE = osLongHandle("getCommittedVirtualMemorySize");

    private static final AllocatedBytesMetrics ALLOCATED_BYTES_METRICS = AllocatedBytesMetrics.create();

    private SystemMetrics() {}

    public static void register(MeterRegistry registry) {
        registerFileDescriptorsMetrics(registry);
        registerSystemMemoryMetrics(registry);
        registerCgroupMemoryMetrics(registry);
        registerJvmMemoryMetrics(registry);
        registerJvmGcMetrics(registry);
    }

    private static void registerFileDescriptorsMetrics(MeterRegistry registry) {
        registerLongGauge(registry, "jvm.fd.max", "Maximum number of file descriptors", "{file_descriptor}", GET_MAX_FILE_DESCRIPTOR_COUNT);
    }

    private static void registerSystemMemoryMetrics(MeterRegistry registry) {
        registerLongGauge(registry, "system.memory.total", "Total physical memory", "By", GET_TOTAL_MEMORY_SIZE);
        registerLongGauge(registry, "system.memory.actual.free", "Free physical memory", "By", GET_FREE_MEMORY_SIZE);
        registerLongGauge(registry, "system.process.memory.size", "Process virtual memory size", "By", GET_COMMITTED_VIRTUAL_MEMORY_SIZE);

        if (ALLOCATED_BYTES_METRICS != null) {
            registry.registerLongGauge(
                "jvm.gc.alloc",
                "Bytes allocated since last observation",
                "By",
                ALLOCATED_BYTES_METRICS::observeAllocatedBytesDelta
            );
        }
    }

    private static void registerCgroupMemoryMetrics(MeterRegistry registry) {
        if (isLinux() == false) {
            return;
        }

        registry.registerLongGauge(
            "system.process.cgroup.memory.mem.limit.bytes",
            "Memory limit for current cgroup slice",
            "By",
            () -> new LongWithAttributes(cgroupBytes(OsStats.Cgroup::getMemoryLimitInBytes))
        );
        registry.registerLongGauge(
            "system.process.cgroup.memory.mem.usage.bytes",
            "Memory usage in current cgroup slice",
            "By",
            () -> new LongWithAttributes(cgroupBytes(OsStats.Cgroup::getMemoryUsageInBytes))
        );
    }

    private static long cgroupBytes(java.util.function.Function<OsStats.Cgroup, String> extractor) {
        OsStats.Cgroup cgroup = OsProbe.getInstance().osStats().getCgroup();
        if (cgroup == null) {
            return -1L;
        }
        return parseCgroupBytes(extractor.apply(cgroup));
    }

    private static long parseCgroupBytes(String value) {
        if (value == null || value.equals("max")) {
            return -1L;
        }
        try {
            BigInteger bigInt = new BigInteger(value);
            if (bigInt.signum() < 0 || bigInt.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
                return -1L;
            }
            return bigInt.longValueExact();
        } catch (RuntimeException e) {
            return -1L;
        }
    }

    private static void registerJvmMemoryMetrics(MeterRegistry registry) {
        registry.registerLongGauge(
            "jvm.memory.heap.used",
            "Used heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getHeapMemoryUsage().getUsed())
        );
        registry.registerLongGauge(
            "jvm.memory.heap.committed",
            "Committed heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getHeapMemoryUsage().getCommitted())
        );
        registry.registerLongGauge(
            "jvm.memory.heap.max",
            "Max heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getHeapMemoryUsage().getMax())
        );

        registry.registerLongGauge(
            "jvm.memory.non_heap.used",
            "Used non-heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getNonHeapMemoryUsage().getUsed())
        );
        registry.registerLongGauge(
            "jvm.memory.non_heap.committed",
            "Committed non-heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getNonHeapMemoryUsage().getCommitted())
        );
        registry.registerLongGauge(
            "jvm.memory.non_heap.max",
            "Max non-heap memory",
            "By",
            () -> new LongWithAttributes(MEMORY_MX_BEAN.getNonHeapMemoryUsage().getMax())
        );
    }

    private static void registerJvmGcMetrics(MeterRegistry registry) {
        List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        if (beans.isEmpty()) {
            return;
        }

        registry.registerLongsAsyncCounter("jvm.gc.count", "GC collection count", "1", () -> {
            var measurements = new ArrayList<LongWithAttributes>(beans.size());
            for (GarbageCollectorMXBean bean : beans) {
                long count = bean.getCollectionCount();
                if (count >= 0) {
                    measurements.add(new LongWithAttributes(count, Map.of("name", bean.getName())));
                }
            }
            return measurements;
        });

        registry.registerLongsAsyncCounter("jvm.gc.time", "GC collection time", "ms", () -> {
            var measurements = new ArrayList<LongWithAttributes>(beans.size());
            for (GarbageCollectorMXBean bean : beans) {
                long timeMs = bean.getCollectionTime();
                if (timeMs >= 0) {
                    measurements.add(new LongWithAttributes(timeMs, Map.of("name", bean.getName())));
                }
            }
            return measurements;
        });
    }

    private static void registerLongGauge(MeterRegistry registry, String name, String description, String unit, MethodHandle method) {
        if (method == NOOP_HANDLE) {
            return;
        }
        registry.registerLongGauge(name, description, unit, () -> new LongWithAttributes(invokeLong(method)));
    }

    private static long invokeLong(MethodHandle method) {
        try {
            return (long) method.invokeExact((Object) SystemMetrics.OS_MX_BEAN);
        } catch (Throwable e) {
            return -1L;
        }
    }

    private static MethodHandle osLongHandle(String methodName) {
        Class<?> targetClass = osTargetClass();
        if (targetClass == null) {
            return NOOP_HANDLE;
        }
        try {
            return MethodHandles.publicLookup()
                .findVirtual(targetClass, methodName, MethodType.methodType(long.class))
                .asType(MethodType.methodType(long.class, Object.class));
        } catch (Exception e) {
            return NOOP_HANDLE;
        }
    }

    private static Class<?> osTargetClass() {
        try {
            Class<?> unix = Class.forName(UNIX_OS_MX_BEAN);
            if (unix.isInstance(OS_MX_BEAN)) {
                return unix;
            }
            Class<?> sun = Class.forName(SUN_OS_MX_BEAN);
            if (sun.isInstance(OS_MX_BEAN)) {
                return sun;
            }
        } catch (Exception ignored) {}
        return null;
    }

    private static boolean isLinux() {
        String osName = System.getProperty("os.name");
        return osName.startsWith("Linux");
    }

    private static final class AllocatedBytesMetrics {
        private static final String SUN_THREAD_MXBEAN = "com.sun.management.ThreadMXBean";
        private static final MethodHandle NOOP_ALLOCATED_BYTES = MethodHandles.dropArguments(
            MethodHandles.constant(long[].class, null),
            0,
            Object.class,
            long[].class
        );

        private final Object sunThreadMxBean;
        private final MethodHandle getThreadAllocatedBytes;
        private final AtomicLong lastTotalAllocatedBytes = new AtomicLong(-1);

        private AllocatedBytesMetrics(Object sunThreadMxBean, MethodHandle getThreadAllocatedBytes) {
            this.sunThreadMxBean = sunThreadMxBean;
            this.getThreadAllocatedBytes = getThreadAllocatedBytes;
        }

        static AllocatedBytesMetrics create() {
            try {
                Class<?> clazz = Class.forName(SUN_THREAD_MXBEAN);
                if (clazz.isInstance(THREAD_MX_BEAN) == false) {
                    return null;
                }
                Object sun = THREAD_MX_BEAN;
                MethodHandle isSupported = booleanHandle(clazz, "isThreadAllocatedMemorySupported");
                if (isSupported == null || invokeBoolean(sun, isSupported) == false) {
                    return null;
                }

                MethodHandle isEnabled = booleanHandle(clazz, "isThreadAllocatedMemoryEnabled");
                if (isEnabled == null) {
                    return null;
                }

                if (invokeBoolean(sun, isEnabled) == false) {
                    MethodHandle setEnabled = setThreadAllocatedMemoryEnabledHandle(clazz);
                    if (setEnabled == null) {
                        return null;
                    }
                    try {
                        setEnabled.invokeExact(sun, true);
                    } catch (Throwable e) {
                        return null;
                    }
                    if (invokeBoolean(sun, isEnabled) == false) {
                        return null;
                    }
                }

                MethodHandle getAllocatedBytes = getThreadAllocatedBytesHandle(clazz);
                if (getAllocatedBytes == NOOP_ALLOCATED_BYTES) {
                    return null;
                }

                return new AllocatedBytesMetrics(sun, getAllocatedBytes);
            } catch (Exception e) {
                return null;
            }
        }

        LongWithAttributes observeAllocatedBytesDelta() {
            try {
                long[] threadIds = THREAD_MX_BEAN.getAllThreadIds();
                if (threadIds.length == 0) {
                    return new LongWithAttributes(0);
                }

                long[] allocated = (long[]) getThreadAllocatedBytes.invokeExact(sunThreadMxBean, threadIds);
                long total = 0;
                for (long l : allocated) {
                    total += Math.max(l, 0);
                }

                long previous = lastTotalAllocatedBytes.getAndSet(total);
                long delta = previous >= 0 && total >= previous ? total - previous : 0;
                return new LongWithAttributes(delta);
            } catch (Throwable e) {
                return new LongWithAttributes(0);
            }
        }

        private static MethodHandle booleanHandle(Class<?> clazz, String methodName) {
            try {
                return MethodHandles.publicLookup()
                    .findVirtual(clazz, methodName, MethodType.methodType(boolean.class))
                    .asType(MethodType.methodType(boolean.class, Object.class));
            } catch (Exception e) {
                return null;
            }
        }

        private static MethodHandle setThreadAllocatedMemoryEnabledHandle(Class<?> clazz) {
            try {
                return MethodHandles.publicLookup()
                    .findVirtual(clazz, "setThreadAllocatedMemoryEnabled", MethodType.methodType(void.class, boolean.class))
                    .asType(MethodType.methodType(void.class, Object.class, boolean.class));
            } catch (Exception e) {
                return null;
            }
        }

        private static MethodHandle getThreadAllocatedBytesHandle(Class<?> clazz) {
            try {
                return MethodHandles.publicLookup()
                    .findVirtual(clazz, "getThreadAllocatedBytes", MethodType.methodType(long[].class, long[].class))
                    .asType(MethodType.methodType(long[].class, Object.class, long[].class));
            } catch (Exception e) {
                return NOOP_ALLOCATED_BYTES;
            }
        }

        private static boolean invokeBoolean(Object target, MethodHandle method) {
            try {
                return (boolean) method.invokeExact(target);
            } catch (Throwable e) {
                return false;
            }
        }
    }
}
