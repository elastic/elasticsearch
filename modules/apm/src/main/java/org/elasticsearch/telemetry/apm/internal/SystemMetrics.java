/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.telemetry.apm.internal;

import org.elasticsearch.core.PathUtils;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.monitor.os.OsProbe;
import org.elasticsearch.monitor.os.OsStats;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public final class SystemMetrics {

    private static final String SUN_OS_MX_BEAN = "com.sun.management.OperatingSystemMXBean";
    private static final String UNIX_OS_MX_BEAN = "com.sun.management.UnixOperatingSystemMXBean";

    private static final OperatingSystemMXBean OS_MX_BEAN = ManagementFactory.getOperatingSystemMXBean();
    private static final ThreadMXBean THREAD_MX_BEAN = ManagementFactory.getThreadMXBean();

    private static final MethodHandle GET_MAX_FILE_DESCRIPTOR_COUNT = osLongHandle("getMaxFileDescriptorCount");

    private static final MethodHandle GET_TOTAL_PHYSICAL_MEMORY_SIZE = osLongHandle("getTotalPhysicalMemorySize");
    private static final MethodHandle GET_FREE_PHYSICAL_MEMORY_SIZE = osLongHandle("getFreePhysicalMemorySize");
    private static final MethodHandle GET_COMMITTED_VIRTUAL_MEMORY_SIZE = osLongHandle("getCommittedVirtualMemorySize");

    private static final AllocatedBytesMetrics ALLOCATED_BYTES_METRICS = AllocatedBytesMetrics.create();

    private SystemMetrics() {}

    public static void register(MeterRegistry registry) {
        registerFileDescriptorsMetrics(registry);
        registerSystemMemoryMetrics(registry);
        registerCgroupMemoryMetrics(registry);
        registerJvmGcMetrics(registry);
    }

    private static void registerFileDescriptorsMetrics(MeterRegistry registry) {
        registerLongGaugeUnlessNegative(
            registry,
            "jvm.fd.max",
            "The maximum number of opened file descriptors.",
            "{file_descriptor}",
            () -> invokeLong(GET_MAX_FILE_DESCRIPTOR_COUNT)
        );
    }

    private static void registerSystemMemoryMetrics(MeterRegistry registry) {
        final boolean useProcMeminfo = isLinux() && Files.isReadable(memInfoPath());
        final LongSupplier actualFreeBytes = useProcMeminfo
            ? SystemMetrics::linuxActualFreeMemoryBytes
            : () -> invokeLong(GET_FREE_PHYSICAL_MEMORY_SIZE);
        final LongSupplier totalBytes = useProcMeminfo
            ? SystemMetrics::linuxTotalMemoryBytes
            : () -> invokeLong(GET_TOTAL_PHYSICAL_MEMORY_SIZE);

        registerLongGaugeUnlessNegative(
            registry,
            "system.memory.actual.free",
            "Actual free memory in bytes. It is calculated based on the OS. On Linux it consists of the free memory plus caches "
                + "and buffers. On OSX it is a sum of free memory and the inactive memory. On Windows, this value does not include memory "
                + "consumed by system caches and buffers.",
            "By",
            actualFreeBytes
        );
        registerLongGaugeUnlessNegative(registry, "system.memory.total", "Total memory.", "By", totalBytes);

        registerLongGaugeUnlessNegative(
            registry,
            "system.process.memory.size",
            "The total virtual memory the process has.",
            "By",
            () -> invokeLong(GET_COMMITTED_VIRTUAL_MEMORY_SIZE)
        );

        if (ALLOCATED_BYTES_METRICS != null) {
            registry.registerLongGauge(
                "jvm.gc.alloc",
                "An approximation of the total amount of memory, in bytes, allocated in heap memory.",
                "By",
                ALLOCATED_BYTES_METRICS::readAllocatedBytes
            );
        }
    }

    private static void registerCgroupMemoryMetrics(MeterRegistry registry) {
        if (isLinux() == false) {
            return;
        }

        long usage = cgroupBytes(OsStats.Cgroup::getMemoryUsageInBytes);
        if (usage >= 0) {
            registry.registerLongGauge(
                "system.process.cgroup.memory.mem.usage.bytes",
                "Memory usage in current cgroup slice.",
                "By",
                () -> new LongWithAttributes(cgroupBytes(OsStats.Cgroup::getMemoryUsageInBytes))
            );
        }

        long limit = cgroupBytes(OsStats.Cgroup::getMemoryLimitInBytes);
        if (limit >= 0) {
            registry.registerLongGauge(
                "system.process.cgroup.memory.mem.limit.bytes",
                "Memory limit for current cgroup slice.",
                "By",
                () -> new LongWithAttributes(cgroupBytes(OsStats.Cgroup::getMemoryLimitInBytes))
            );
        }
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

    private static void registerJvmGcMetrics(MeterRegistry registry) {
        List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
        if (beans.isEmpty()) {
            return;
        }

        registry.registerLongsAsyncCounter("jvm.gc.count", "The total number of collections that have occurred.", "1", () -> {
            var measurements = new ArrayList<LongWithAttributes>(beans.size());
            for (GarbageCollectorMXBean bean : beans) {
                long count = bean.getCollectionCount();
                if (count >= 0) {
                    measurements.add(new LongWithAttributes(count, Map.of("name", bean.getName())));
                }
            }
            return measurements;
        });

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
        );
    }

    private static long invokeLong(MethodHandle method) {
        if (method == null) {
            return -1L;
        }
        try {
            return (long) method.invokeExact((Object) SystemMetrics.OS_MX_BEAN);
        } catch (Throwable e) {
            return -1L;
        }
    }

    private static MethodHandle osLongHandle(String methodName) {
        Class<?> targetClass = osTargetClass();
        if (targetClass == null) {
            return null;
        }
        try {
            return MethodHandles.publicLookup()
                .findVirtual(targetClass, methodName, MethodType.methodType(long.class))
                .asType(MethodType.methodType(long.class, Object.class));
        } catch (Exception e) {
            return null;
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

    @SuppressForbidden(reason = "access /proc/meminfo")
    private static Path memInfoPath() {
        return PathUtils.get("/proc/meminfo");
    }

    private static long linuxActualFreeMemoryBytes() {
        long memAvailable = -1;
        long memFree = -1;
        long buffers = -1;
        long cached = -1;

        try {
            List<String> lines = Files.readAllLines(memInfoPath(), StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("MemAvailable:")) {
                    memAvailable = parseMeminfoKbToBytes(line);
                    break;
                } else if (line.startsWith("MemFree:")) {
                    memFree = parseMeminfoKbToBytes(line);
                } else if (line.startsWith("Buffers:")) {
                    buffers = parseMeminfoKbToBytes(line);
                } else if (line.startsWith("Cached:")) {
                    cached = parseMeminfoKbToBytes(line);
                }
            }
        } catch (Exception e) {
            return -1L;
        }

        if (memAvailable >= 0) {
            return memAvailable;
        }
        if (memFree >= 0 && buffers >= 0 && cached >= 0) {
            return memFree + buffers + cached;
        }
        return -1L;
    }

    private static long linuxTotalMemoryBytes() {
        try {
            List<String> lines = Files.readAllLines(memInfoPath(), StandardCharsets.UTF_8);
            for (String line : lines) {
                if (line.isEmpty()) {
                    continue;
                }
                if (line.startsWith("MemTotal:")) {
                    return parseMeminfoKbToBytes(line);
                }
            }
            return -1L;
        } catch (Exception e) {
            return -1L;
        }
    }

    private static long parseMeminfoKbToBytes(String line) {
        // Example: "MemTotal: 51539607552 kB"
        int colon = line.indexOf(':');
        if (colon < 0) {
            return -1L;
        }
        String rest = line.substring(colon + 1).trim();
        if (rest.isEmpty()) {
            return -1L;
        }
        int space = rest.indexOf(' ');
        String number = space >= 0 ? rest.substring(0, space) : rest;
        long kb = Long.parseLong(number);
        return kb * 1024L;
    }

    private static void registerLongGaugeUnlessNegative(
        MeterRegistry registry,
        String name,
        String description,
        String unit,
        LongSupplier supplier
    ) {
        long initial = supplier.getAsLong();
        if (initial < 0) {
            return;
        }
        registry.registerLongGauge(name, description, unit, () -> new LongWithAttributes(supplier.getAsLong()));
    }

    private static final class AllocatedBytesMetrics {
        private static final String SUN_THREAD_MXBEAN = "com.sun.management.ThreadMXBean";

        private final Object sunThreadMxBean;
        private final MethodHandle getThreadAllocatedBytes;

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

                MethodHandle getAllocatedBytes = getThreadAllocatedBytesHandle(clazz);
                if (getAllocatedBytes == null) {
                    return null;
                }

                // try reading once before registering.
                long[] threadIds = THREAD_MX_BEAN.getAllThreadIds();
                getAllocatedBytes.invokeExact((Object) THREAD_MX_BEAN, threadIds);

                return new AllocatedBytesMetrics(THREAD_MX_BEAN, getAllocatedBytes);
            } catch (Throwable t) {
                return null;
            }
        }

        LongWithAttributes readAllocatedBytes() {
            try {
                long[] allocated = (long[]) getThreadAllocatedBytes.invokeExact(sunThreadMxBean, THREAD_MX_BEAN.getAllThreadIds());
                long total = 0;
                for (long l : allocated) {
                    if (l > 0) {
                        total += l;
                    }
                }

                return new LongWithAttributes(total);
            } catch (Throwable e) {
                return new LongWithAttributes(0);
            }
        }

        private static MethodHandle getThreadAllocatedBytesHandle(Class<?> clazz) {
            try {
                return MethodHandles.publicLookup()
                    .findVirtual(clazz, "getThreadAllocatedBytes", MethodType.methodType(long[].class, long[].class))
                    .asType(MethodType.methodType(long[].class, Object.class, long[].class));
            } catch (Exception e) {
                return null;
            }
        }
    }
}
