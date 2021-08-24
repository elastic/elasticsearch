/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.os;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Constants;
import org.elasticsearch.core.SuppressForbidden;
import org.elasticsearch.core.PathUtils;
import org.elasticsearch.monitor.Probes;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * The {@link OsProbe} class retrieves information about the physical and swap size of the machine
 * memory, as well as the system load average and cpu load.
 *
 * <p>In some exceptional cases, it's possible the underlying native methods used by
 * {@link #getFreePhysicalMemorySize()}, {@link #getTotalPhysicalMemorySize()},
 * {@link #getFreeSwapSpaceSize()}, and {@link #getTotalSwapSpaceSize()} can return a
 * negative value. Because of this, we prevent those methods from returning negative values,
 * returning 0 instead.
 *
 * <p>The OS can report a negative number in a number of cases:
 *
 * <ul>
 *   <li>Non-supported OSes (HP-UX, or AIX)
 *   <li>A failure of macOS to initialize host statistics
 *   <li>An OS that does not support the {@code _SC_PHYS_PAGES} or {@code _SC_PAGE_SIZE} flags for the {@code sysconf()} linux kernel call
 *   <li>An overflow of the product of {@code _SC_PHYS_PAGES} and {@code _SC_PAGE_SIZE}
 *   <li>An error case retrieving these values from a linux kernel
 *   <li>A non-standard libc implementation not implementing the required values
 * </ul>
 *
 * <p>For a more exhaustive explanation, see <a href="https://github.com/elastic/elasticsearch/pull/42725"
 *   >https://github.com/elastic/elasticsearch/pull/42725</a>
 */
public class OsProbe {

    private static final OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();

    private static final Method getFreePhysicalMemorySize;
    private static final Method getTotalPhysicalMemorySize;
    private static final Method getFreeSwapSpaceSize;
    private static final Method getTotalSwapSpaceSize;
    private static final Method getSystemLoadAverage;
    private static final Method getSystemCpuLoad;

    static {
        getFreePhysicalMemorySize = getMethod("getFreePhysicalMemorySize");
        getTotalPhysicalMemorySize = getMethod("getTotalPhysicalMemorySize");
        getFreeSwapSpaceSize = getMethod("getFreeSwapSpaceSize");
        getTotalSwapSpaceSize = getMethod("getTotalSwapSpaceSize");
        getSystemLoadAverage = getMethod("getSystemLoadAverage");
        getSystemCpuLoad = getMethod("getSystemCpuLoad");
    }

    /**
     * Returns the amount of free physical memory in bytes.
     */
    public long getFreePhysicalMemorySize() {
        if (getFreePhysicalMemorySize == null) {
            logger.warn("getFreePhysicalMemorySize is not available");
            return 0;
        }
        try {
            final long freeMem = (long) getFreePhysicalMemorySize.invoke(osMxBean);
            if (freeMem < 0) {
                logger.debug("OS reported a negative free memory value [{}]", freeMem);
                return 0;
            }
            return freeMem;
        } catch (Exception e) {
            logger.warn("exception retrieving free physical memory", e);
            return 0;
        }
    }

    /**
     * Returns the total amount of physical memory in bytes.
     */
    public long getTotalPhysicalMemorySize() {
        if (getTotalPhysicalMemorySize == null) {
            logger.warn("getTotalPhysicalMemorySize is not available");
            return 0;
        }
        try {
            long totalMem = (long) getTotalPhysicalMemorySize.invoke(osMxBean);
            if (totalMem < 0) {
                logger.debug("OS reported a negative total memory value [{}]", totalMem);
                return 0;
            }
            if (totalMem == 0 && isDebian8()) {
                // workaround for JDK bug on debian8: https://github.com/elastic/elasticsearch/issues/67089#issuecomment-756114654
                totalMem = getTotalMemFromProcMeminfo();
            }

            return totalMem;
        } catch (Exception e) {
            logger.warn("exception retrieving total physical memory", e);
            return 0;
        }
    }

    /**
     * Returns the amount of free swap space in bytes.
     */
    public long getFreeSwapSpaceSize() {
        if (getFreeSwapSpaceSize == null) {
            logger.warn("getFreeSwapSpaceSize is not available");
            return 0;
        }
        try {
            final long mem = (long) getFreeSwapSpaceSize.invoke(osMxBean);
            if (mem < 0) {
                logger.debug("OS reported a negative free swap space size [{}]", mem);
                return 0;
            }
            return mem;
        } catch (Exception e) {
            logger.warn("exception retrieving free swap space size", e);
            return 0;
        }
    }

    /**
     * Returns the total amount of swap space in bytes.
     */
    public long getTotalSwapSpaceSize() {
        if (getTotalSwapSpaceSize == null) {
            logger.warn("getTotalSwapSpaceSize is not available");
            return 0;
        }
        try {
            final long mem = (long) getTotalSwapSpaceSize.invoke(osMxBean);
            if (mem < 0) {
                logger.debug("OS reported a negative total swap space size [{}]", mem);
                return 0;
            }
            return mem;
        } catch (Exception e) {
            logger.warn("exception retrieving total swap space size", e);
            return 0;
        }
    }

    /**
     * The system load averages as an array.
     *
     * On Windows, this method returns {@code null}.
     *
     * On Linux, this method returns the 1, 5, and 15-minute load averages.
     *
     * On macOS, this method should return the 1-minute load average.
     *
     * @return the available system load averages or {@code null}
     */
    final double[] getSystemLoadAverage() {
        if (Constants.WINDOWS) {
            return null;
        } else if (Constants.LINUX) {
            try {
                final String procLoadAvg = readProcLoadavg();
                assert procLoadAvg.matches("(\\d+\\.\\d+\\s+){3}\\d+/\\d+\\s+\\d+");
                final String[] fields = procLoadAvg.split("\\s+");
                return new double[] { Double.parseDouble(fields[0]), Double.parseDouble(fields[1]), Double.parseDouble(fields[2]) };
            } catch (final IOException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("error reading /proc/loadavg", e);
                }
                return null;
            }
        } else {
            assert Constants.MAC_OS_X;
            if (getSystemLoadAverage == null) {
                return null;
            }
            try {
                final double oneMinuteLoadAverage = (double) getSystemLoadAverage.invoke(osMxBean);
                return new double[] { oneMinuteLoadAverage >= 0 ? oneMinuteLoadAverage : -1, -1, -1 };
            } catch (IllegalAccessException | InvocationTargetException e) {
                if (logger.isDebugEnabled()) {
                    logger.debug("error reading one minute load average from operating system", e);
                }
                return null;
            }
        }
    }

    /**
     * The line from {@code /proc/loadavg}. The first three fields are the load averages averaged over 1, 5, and 15 minutes. The fourth
     * field is two numbers separated by a slash, the first is the number of currently runnable scheduling entities, the second is the
     * number of scheduling entities on the system. The fifth field is the PID of the most recently created process.
     *
     * @return the line from {@code /proc/loadavg} or {@code null}
     */
    @SuppressForbidden(reason = "access /proc/loadavg")
    String readProcLoadavg() throws IOException {
        return readSingleLine(PathUtils.get("/proc/loadavg"));
    }

    public short getSystemCpuPercent() {
        return Probes.getLoadAndScaleToPercent(getSystemCpuLoad, osMxBean);
    }

    /**
     * Reads a file containing a single line.
     *
     * @param path path to the file to read
     * @return the single line
     * @throws IOException if an I/O exception occurs reading the file
     */
    private String readSingleLine(final Path path) throws IOException {
        final List<String> lines = Files.readAllLines(path);
        assert lines.size() == 1 : String.join("\n", lines);
        return lines.get(0);
    }

    // this property is to support a hack to workaround an issue with Docker containers mounting the cgroups hierarchy inconsistently with
    // respect to /proc/self/cgroup; for Docker containers this should be set to "/"
    private static final String CONTROL_GROUPS_HIERARCHY_OVERRIDE = System.getProperty("es.cgroups.hierarchy.override");

    /**
     * A map of the control groups to which the Elasticsearch process belongs. Note that this is a map because the control groups can vary
     * from subsystem to subsystem. Additionally, this map can not be cached because a running process can be reclassified.
     *
     * @return a map from subsystems to the control group for the Elasticsearch process.
     * @throws IOException if an I/O exception occurs reading {@code /proc/self/cgroup}
     */
    private Map<String, String> getControlGroups() throws IOException {
        final List<String> lines = readProcSelfCgroup();
        final Map<String, String> controllerMap = new HashMap<>();
        for (final String line : lines) {
            /*
             * The virtual file /proc/self/cgroup lists the control groups that the Elasticsearch process is a member of. Each line contains
             * three colon-separated fields of the form hierarchy-ID:subsystem-list:cgroup-path. For cgroups version 1 hierarchies, the
             * subsystem-list is a comma-separated list of subsystems. The subsystem-list can be empty if the hierarchy represents a cgroups
             * version 2 hierarchy. For cgroups version 1
             */
            final String[] fields = line.split(":");
            assert fields.length == 3;
            final String[] controllers = fields[1].split(",");
            for (final String controller : controllers) {
                final String controlGroupPath;
                if (CONTROL_GROUPS_HIERARCHY_OVERRIDE != null) {
                    /*
                     * Docker violates the relationship between /proc/self/cgroup and the /sys/fs/cgroup hierarchy. It's possible that this
                     * will be fixed in future versions of Docker with cgroup namespaces, but this requires modern kernels. Thus, we provide
                     * an undocumented hack for overriding the control group path. Do not rely on this hack, it will be removed.
                     */
                    controlGroupPath = CONTROL_GROUPS_HIERARCHY_OVERRIDE;
                } else {
                    controlGroupPath = fields[2];
                }
                final String previous = controllerMap.put(controller, controlGroupPath);
                assert previous == null;
            }
        }
        return controllerMap;
    }

    /**
     * The lines from {@code /proc/self/cgroup}. This file represents the control groups to which the Elasticsearch process belongs. Each
     * line in this file represents a control group hierarchy of the form
     * <p>
     * {@code \d+:([^:,]+(?:,[^:,]+)?):(/.*)}
     * <p>
     * with the first field representing the hierarchy ID, the second field representing a comma-separated list of the subsystems bound to
     * the hierarchy, and the last field representing the control group.
     *
     * @return the lines from {@code /proc/self/cgroup}
     * @throws IOException if an I/O exception occurs reading {@code /proc/self/cgroup}
     */
    @SuppressForbidden(reason = "access /proc/self/cgroup")
    List<String> readProcSelfCgroup() throws IOException {
        final List<String> lines = Files.readAllLines(PathUtils.get("/proc/self/cgroup"));
        assert lines != null && lines.isEmpty() == false;
        return lines;
    }

    /**
     * The total CPU time in nanoseconds consumed by all tasks in the cgroup to which the Elasticsearch process belongs for the {@code
     * cpuacct} subsystem.
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code cpuacct} subsystem
     * @return the total CPU time in nanoseconds
     * @throws IOException if an I/O exception occurs reading {@code cpuacct.usage} for the control group
     */
    private long getCgroupCpuAcctUsageNanos(final String controlGroup) throws IOException {
        return Long.parseLong(readSysFsCgroupCpuAcctCpuAcctUsage(controlGroup));
    }

    /**
     * Returns the line from {@code cpuacct.usage} for the control group to which the Elasticsearch process belongs for the {@code cpuacct}
     * subsystem. This line represents the total CPU time in nanoseconds consumed by all tasks in the same control group.
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code cpuacct} subsystem
     * @return the line from {@code cpuacct.usage}
     * @throws IOException if an I/O exception occurs reading {@code cpuacct.usage} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpuacct")
    String readSysFsCgroupCpuAcctCpuAcctUsage(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/cpuacct", controlGroup, "cpuacct.usage"));
    }

    private long[] getCgroupV2CpuLimit(String controlGroup) throws IOException {
        String entry = readCgroupV2CpuLimit(controlGroup);
        String[] parts = entry.split("\\s+");
        assert parts.length == 2 : "Expected 2 fields in [cpu.max]";

        long[] values = new long[2];

        values[0] = "max".equals(parts[0]) ? -1L : Long.parseLong(parts[0]);
        values[1] = Long.parseLong(parts[1]);
        return values;
    }

    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpu.max")
    private String readCgroupV2CpuLimit(String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/", controlGroup, "cpu.max"));
    }

    /**
     * The total period of time in microseconds for how frequently the Elasticsearch control group's access to CPU resources will be
     * reallocated.
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code cpuacct} subsystem
     * @return the CFS quota period in microseconds
     * @throws IOException if an I/O exception occurs reading {@code cpu.cfs_period_us} for the control group
     */
    private long getCgroupCpuAcctCpuCfsPeriodMicros(final String controlGroup) throws IOException {
        return Long.parseLong(readSysFsCgroupCpuAcctCpuCfsPeriod(controlGroup));
    }

    /**
     * Returns the line from {@code cpu.cfs_period_us} for the control group to which the Elasticsearch process belongs for the {@code cpu}
     * subsystem. This line represents the period of time in microseconds for how frequently the control group's access to CPU resources
     * will be reallocated.
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code cpu} subsystem
     * @return the line from {@code cpu.cfs_period_us}
     * @throws IOException if an I/O exception occurs reading {@code cpu.cfs_period_us} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpu")
    String readSysFsCgroupCpuAcctCpuCfsPeriod(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/cpu", controlGroup, "cpu.cfs_period_us"));
    }

    /**
     * The total time in microseconds that all tasks in the Elasticsearch control group can run during one period as specified by {@code
     * cpu.cfs_period_us}.
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code cpuacct} subsystem
     * @return the CFS quota in microseconds
     * @throws IOException if an I/O exception occurs reading {@code cpu.cfs_quota_us} for the control group
     */
    private long getCgroupCpuAcctCpuCfsQuotaMicros(final String controlGroup) throws IOException {
        return Long.parseLong(readSysFsCgroupCpuAcctCpuAcctCfsQuota(controlGroup));
    }

    /**
     * Returns the line from {@code cpu.cfs_quota_us} for the control group to which the Elasticsearch process belongs for the {@code cpu}
     * subsystem. This line represents the total time in microseconds that all tasks in the control group can run during one period as
     * specified by {@code cpu.cfs_period_us}.
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code cpu} subsystem
     * @return the line from {@code cpu.cfs_quota_us}
     * @throws IOException if an I/O exception occurs reading {@code cpu.cfs_quota_us} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpu")
    String readSysFsCgroupCpuAcctCpuAcctCfsQuota(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/cpu", controlGroup, "cpu.cfs_quota_us"));
    }

    /**
     * The CPU time statistics for all tasks in the Elasticsearch control group.
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code cpuacct} subsystem
     * @return the CPU time statistics
     * @throws IOException if an I/O exception occurs reading {@code cpu.stat} for the control group
     */
    private OsStats.Cgroup.CpuStat getCgroupCpuAcctCpuStat(final String controlGroup) throws IOException {
        final List<String> lines = readSysFsCgroupCpuAcctCpuStat(controlGroup);
        long numberOfPeriods = -1;
        long numberOfTimesThrottled = -1;
        long timeThrottledNanos = -1;
        for (final String line : lines) {
            final String[] fields = line.split("\\s+");
            switch (fields[0]) {
                case "nr_periods":
                    numberOfPeriods = Long.parseLong(fields[1]);
                    break;
                case "nr_throttled":
                    numberOfTimesThrottled = Long.parseLong(fields[1]);
                    break;
                case "throttled_time":
                    timeThrottledNanos = Long.parseLong(fields[1]);
                    break;
            }
        }
        assert numberOfPeriods != -1;
        assert numberOfTimesThrottled != -1;
        assert timeThrottledNanos != -1;
        return new OsStats.Cgroup.CpuStat(numberOfPeriods, numberOfTimesThrottled, timeThrottledNanos);
    }

    /**
     * Returns the lines from {@code cpu.stat} for the control group to which the Elasticsearch process belongs for the {@code cpu}
     * subsystem. These lines represent the CPU time statistics and have the form
     * <blockquote><pre>
     * nr_periods \d+
     * nr_throttled \d+
     * throttled_time \d+
     * </pre></blockquote>
     * where {@code nr_periods} is the number of period intervals as specified by {@code cpu.cfs_period_us} that have elapsed, {@code
     * nr_throttled} is the number of times tasks in the given control group have been throttled, and {@code throttled_time} is the total
     * time in nanoseconds for which tasks in the given control group have been throttled.
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code cpu} subsystem
     * @return the lines from {@code cpu.stat}
     * @throws IOException if an I/O exception occurs reading {@code cpu.stat} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpu")
    List<String> readSysFsCgroupCpuAcctCpuStat(final String controlGroup) throws IOException {
        final List<String> lines = Files.readAllLines(PathUtils.get("/sys/fs/cgroup/cpu", controlGroup, "cpu.stat"));
        assert lines != null && lines.size() == 3;
        return lines;
    }

    /**
     * The maximum amount of user memory (including file cache).
     * If there is no limit then some Linux versions return the maximum value that can be stored in an
     * unsigned 64 bit number, and this will overflow a long, hence the result type is <code>String</code>.
     * (The alternative would have been <code>BigInteger</code> but then it would not be possible to index
     * the OS stats document into Elasticsearch without losing information, as <code>BigInteger</code> is
     * not a supported Elasticsearch type.)
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code memory} subsystem
     * @return the maximum amount of user memory (including file cache)
     * @throws IOException if an I/O exception occurs reading {@code memory.limit_in_bytes} for the control group
     */
    private String getCgroupMemoryLimitInBytes(final String controlGroup) throws IOException {
        return readSysFsCgroupMemoryLimitInBytes(controlGroup);
    }

    /**
     * Returns the line from {@code memory.limit_in_bytes} for the control group to which the Elasticsearch process belongs for the
     * {@code memory} subsystem. This line represents the maximum amount of user memory (including file cache).
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code memory} subsystem
     * @return the line from {@code memory.limit_in_bytes}
     * @throws IOException if an I/O exception occurs reading {@code memory.limit_in_bytes} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/memory")
    String readSysFsCgroupMemoryLimitInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/memory", controlGroup, "memory.limit_in_bytes"));
    }

    /**
     * The maximum amount of user memory (including file cache).
     * If there is no limit then some Linux versions return the maximum value that can be stored in an
     * unsigned 64 bit number, and this will overflow a long, hence the result type is <code>String</code>.
     * (The alternative would have been <code>BigInteger</code> but then it would not be possible to index
     * the OS stats document into Elasticsearch without losing information, as <code>BigInteger</code> is
     * not a supported Elasticsearch type.)
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code memory} subsystem
     * @return the maximum amount of user memory (including file cache)
     * @throws IOException if an I/O exception occurs reading {@code memory.limit_in_bytes} for the control group
     */
    private String getCgroupV2MemoryLimitInBytes(final String controlGroup) throws IOException {
        return readSysFsCgroupV2MemoryLimitInBytes(controlGroup);
    }

    /**
     * Returns the line from {@code memory.max} for the control group to which the Elasticsearch process belongs for the
     * {@code memory} subsystem. This line represents the maximum amount of user memory (including file cache).
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code memory} subsystem
     * @return the line from {@code memory.max}
     * @throws IOException if an I/O exception occurs reading {@code memory.max} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/memory.max")
    String readSysFsCgroupV2MemoryLimitInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/", controlGroup, "memory.max"));
    }

    /**
     * The total current memory usage by processes in the cgroup (in bytes).
     * If there is no limit then some Linux versions return the maximum value that can be stored in an
     * unsigned 64 bit number, and this will overflow a long, hence the result type is <code>String</code>.
     * (The alternative would have been <code>BigInteger</code> but then it would not be possible to index
     * the OS stats document into Elasticsearch without losing information, as <code>BigInteger</code> is
     * not a supported Elasticsearch type.)
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code memory} subsystem
     * @return the total current memory usage by processes in the cgroup (in bytes)
     * @throws IOException if an I/O exception occurs reading {@code memory.limit_in_bytes} for the control group
     */
    private String getCgroupMemoryUsageInBytes(final String controlGroup) throws IOException {
        return readSysFsCgroupMemoryUsageInBytes(controlGroup);
    }

    /**
     * Returns the line from {@code memory.usage_in_bytes} for the control group to which the Elasticsearch process belongs for the
     * {@code memory} subsystem. This line represents the total current memory usage by processes in the cgroup (in bytes).
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code memory} subsystem
     * @return the line from {@code memory.usage_in_bytes}
     * @throws IOException if an I/O exception occurs reading {@code memory.usage_in_bytes} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/memory")
    String readSysFsCgroupMemoryUsageInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/memory", controlGroup, "memory.usage_in_bytes"));
    }

    /**
     * The total current memory usage by processes in the cgroup (in bytes).
     * If there is no limit then some Linux versions return the maximum value that can be stored in an
     * unsigned 64 bit number, and this will overflow a long, hence the result type is <code>String</code>.
     * (The alternative would have been <code>BigInteger</code> but then it would not be possible to index
     * the OS stats document into Elasticsearch without losing information, as <code>BigInteger</code> is
     * not a supported Elasticsearch type.)
     *
     * @param controlGroup the control group for the Elasticsearch process for the {@code memory} subsystem
     * @return the total current memory usage by processes in the cgroup (in bytes)
     * @throws IOException if an I/O exception occurs reading {@code memory.current} for the control group
     */
    private String getCgroupV2MemoryUsageInBytes(final String controlGroup) throws IOException {
        return readSysFsCgroupV2MemoryUsageInBytes(controlGroup);
    }

    /**
     * Returns the line from {@code memory.current} for the control group to which the Elasticsearch process belongs for the
     * {@code memory} subsystem. This line represents the total current memory usage by processes in the cgroup (in bytes).
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code memory} subsystem
     * @return the line from {@code memory.current}
     * @throws IOException if an I/O exception occurs reading {@code memory.current} for the control group
     */
    @SuppressForbidden(reason = "access /sys/fs/cgroup/memory")
    String readSysFsCgroupV2MemoryUsageInBytes(final String controlGroup) throws IOException {
        return readSingleLine(PathUtils.get("/sys/fs/cgroup/", controlGroup, "memory.current"));
    }

    /**
     * Checks if cgroup stats are available by checking for the existence of {@code /proc/self/cgroup}, {@code /sys/fs/cgroup/cpu},
     * {@code /sys/fs/cgroup/cpuacct} and {@code /sys/fs/cgroup/memory}.
     *
     * @return {@code true} if the stats are available, otherwise {@code false}
     */
    @SuppressForbidden(reason = "access /proc/self/cgroup, /sys/fs/cgroup/cpu, /sys/fs/cgroup/cpuacct and /sys/fs/cgroup/memory")
    boolean areCgroupStatsAvailable() throws IOException {
        if (Files.exists(PathUtils.get("/proc/self/cgroup")) == false) {
            return false;
        }

        List<String> lines = readProcSelfCgroup();

        // cgroup v2
        if (lines.size() == 1 && lines.get(0).startsWith("0::")) {
            return Stream.of("/sys/fs/cgroup/cpu.stat", "/sys/fs/cgroup/memory.stat").allMatch(path -> Files.exists(PathUtils.get(path)));
        }

        return Stream.of("/sys/fs/cgroup/cpu", "/sys/fs/cgroup/cpuacct", "/sys/fs/cgroup/memory")
            .allMatch(path -> Files.exists(PathUtils.get(path)));
    }

    /**
     * The CPU statistics for all tasks in the Elasticsearch control group.
     *
     * @param controlGroup the control group to which the Elasticsearch process belongs for the {@code memory} subsystem
     * @return the CPU statistics
     * @throws IOException if an I/O exception occurs reading {@code cpu.stat} for the control group
     */
    private Map<String, Long> getCgroupCpuStats(String controlGroup) throws IOException {
        final List<String> lines = readCgroupV2CpuStats(controlGroup);
        final Map<String, Long> stats = new HashMap<>();

        for (String line : lines) {
            String[] parts = line.split("\n");
            stats.put(parts[0], Long.parseLong(parts[1]));
        }

        final List<String> expectedKeys = List.of("nr_periods", "nr_throttled", "system_usec", "throttled_usec", "usage_usec", "user_usec");
        expectedKeys.forEach(key -> {
            assert stats.containsKey(key) : key;
            assert stats.get(key) != -1 : stats.get(key);
        });

        return stats;
    }

    @SuppressForbidden(reason = "access /sys/fs/cgroup/cpu.stat")
    List<String> readCgroupV2CpuStats(final String controlGroup) throws IOException {
        return Files.readAllLines(PathUtils.get("/sys/fs/cgroup", controlGroup, "cpu.stat"));
    }

    /**
     * Basic cgroup stats.
     *
     * @return basic cgroup stats, or {@code null} if an I/O exception occurred reading the cgroup stats
     */
    private OsStats.Cgroup getCgroup() {
        try {
            if (areCgroupStatsAvailable() == false) {
                return null;
            }

            final Map<String, String> controllerMap = getControlGroups();
            assert controllerMap.isEmpty() == false;

            final String cpuAcctControlGroup;
            final long cgroupCpuAcctUsageNanos;
            final long cgroupCpuAcctCpuCfsPeriodMicros;
            final long cgroupCpuAcctCpuCfsQuotaMicros;
            final String cpuControlGroup;
            final OsStats.Cgroup.CpuStat cpuStat;
            final String memoryControlGroup;
            final String cgroupMemoryLimitInBytes;
            final String cgroupMemoryUsageInBytes;

            if (controllerMap.size() == 1 && controllerMap.containsKey("")) {
                // There's a single hierarchy for all controllers
                cpuControlGroup = cpuAcctControlGroup = memoryControlGroup = controllerMap.get("");

                // `cpuacct` was merged with `cpu` in v2
                final Map<String, Long> cpuStatsMap = getCgroupCpuStats(cpuControlGroup);

                cgroupCpuAcctUsageNanos = cpuStatsMap.get("usage_usec");

                long[] cpuLimits = getCgroupV2CpuLimit(cpuControlGroup);
                cgroupCpuAcctCpuCfsQuotaMicros = cpuLimits[0];
                cgroupCpuAcctCpuCfsPeriodMicros = cpuLimits[1];

                cpuStat = new OsStats.Cgroup.CpuStat(
                    cpuStatsMap.get("nr_periods"),
                    cpuStatsMap.get("nr_throttled"),
                    cpuStatsMap.get("throttled_usec")
                );

                cgroupMemoryLimitInBytes = getCgroupV2MemoryLimitInBytes(memoryControlGroup);
                cgroupMemoryUsageInBytes = getCgroupV2MemoryUsageInBytes(memoryControlGroup);
            } else {
                cpuAcctControlGroup = controllerMap.get("cpuacct");
                if (cpuAcctControlGroup == null) {
                    logger.debug("no [cpuacct] data found in cgroup stats");
                    return null;
                }
                cgroupCpuAcctUsageNanos = getCgroupCpuAcctUsageNanos(cpuAcctControlGroup);

                cpuControlGroup = controllerMap.get("cpu");
                if (cpuControlGroup == null) {
                    logger.debug("no [cpu] data found in cgroup stats");
                    return null;
                }
                cgroupCpuAcctCpuCfsPeriodMicros = getCgroupCpuAcctCpuCfsPeriodMicros(cpuControlGroup);
                cgroupCpuAcctCpuCfsQuotaMicros = getCgroupCpuAcctCpuCfsQuotaMicros(cpuControlGroup);
                cpuStat = getCgroupCpuAcctCpuStat(cpuControlGroup);

                memoryControlGroup = controllerMap.get("memory");
                if (memoryControlGroup == null) {
                    logger.debug("no [memory] data found in cgroup stats");
                    return null;
                }
                cgroupMemoryLimitInBytes = getCgroupMemoryLimitInBytes(memoryControlGroup);
                cgroupMemoryUsageInBytes = getCgroupMemoryUsageInBytes(memoryControlGroup);
            }

            return new OsStats.Cgroup(
                cpuAcctControlGroup,
                cgroupCpuAcctUsageNanos,
                cpuControlGroup,
                cgroupCpuAcctCpuCfsPeriodMicros,
                cgroupCpuAcctCpuCfsQuotaMicros,
                cpuStat,
                memoryControlGroup,
                cgroupMemoryLimitInBytes,
                cgroupMemoryUsageInBytes
            );
        } catch (final IOException e) {
            logger.debug("error reading control group stats", e);
            return null;
        }
    }

    private static class OsProbeHolder {
        private static final OsProbe INSTANCE = new OsProbe();
    }

    public static OsProbe getInstance() {
        return OsProbeHolder.INSTANCE;
    }

    OsProbe() {

    }

    private final Logger logger = LogManager.getLogger(getClass());

    OsInfo osInfo(long refreshInterval, int allocatedProcessors) throws IOException {
        return new OsInfo(
            refreshInterval,
            Runtime.getRuntime().availableProcessors(),
            allocatedProcessors,
            Constants.OS_NAME,
            getPrettyName(),
            Constants.OS_ARCH,
            Constants.OS_VERSION
        );
    }

    private String getPrettyName() throws IOException {
        // TODO: return a prettier name on non-Linux OS
        if (Constants.LINUX) {
            /*
             * We read the lines from /etc/os-release (or /usr/lib/os-release) to extract the PRETTY_NAME. The format of this file is
             * newline-separated key-value pairs. The key and value are separated by an equals symbol (=). The value can unquoted, or
             * wrapped in single- or double-quotes.
             */
            final List<String> etcOsReleaseLines = readOsRelease();
            final List<String> prettyNameLines = etcOsReleaseLines.stream()
                .filter(line -> line.startsWith("PRETTY_NAME"))
                .collect(Collectors.toList());
            assert prettyNameLines.size() <= 1 : prettyNameLines;
            final Optional<String> maybePrettyNameLine = prettyNameLines.size() == 1
                ? Optional.of(prettyNameLines.get(0))
                : Optional.empty();
            if (maybePrettyNameLine.isPresent()) {
                // we trim since some OS contain trailing space, for example, Oracle Linux Server 6.9 has a trailing space after the quote
                final String trimmedPrettyNameLine = maybePrettyNameLine.get().trim();
                final Matcher matcher = Pattern.compile("PRETTY_NAME=(\"?|'?)?([^\"']+)\\1").matcher(trimmedPrettyNameLine);
                final boolean matches = matcher.matches();
                assert matches : trimmedPrettyNameLine;
                assert matcher.groupCount() == 2 : trimmedPrettyNameLine;
                return matcher.group(2);
            } else {
                return Constants.OS_NAME;
            }

        } else {
            return Constants.OS_NAME;
        }
    }

    /**
     * The lines from {@code /etc/os-release} or {@code /usr/lib/os-release} as a fallback, with an additional fallback to
     * {@code /etc/system-release}. These files represent identification of the underlying operating system. The structure of the file is
     * newlines of key-value pairs of shell-compatible variable assignments.
     *
     * @return the lines from {@code /etc/os-release} or {@code /usr/lib/os-release} or {@code /etc/system-release}
     * @throws IOException if an I/O exception occurs reading {@code /etc/os-release} or {@code /usr/lib/os-release} or
     *                     {@code /etc/system-release}
     */
    @SuppressForbidden(reason = "access /etc/os-release or /usr/lib/os-release or /etc/system-release")
    List<String> readOsRelease() throws IOException {
        final List<String> lines;
        if (Files.exists(PathUtils.get("/etc/os-release"))) {
            lines = Files.readAllLines(PathUtils.get("/etc/os-release"));
            assert lines != null && lines.isEmpty() == false;
            return lines;
        } else if (Files.exists(PathUtils.get("/usr/lib/os-release"))) {
            lines = Files.readAllLines(PathUtils.get("/usr/lib/os-release"));
            assert lines != null && lines.isEmpty() == false;
            return lines;
        } else if (Files.exists(PathUtils.get("/etc/system-release"))) {
            // fallback for older Red Hat-like OS
            lines = Files.readAllLines(PathUtils.get("/etc/system-release"));
            assert lines != null && lines.size() == 1;
            return Collections.singletonList("PRETTY_NAME=\"" + lines.get(0) + "\"");
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Returns the lines from /proc/meminfo as a workaround for JDK bugs that prevent retrieval of total system memory
     * on some Linux variants such as Debian8.
     */
    @SuppressForbidden(reason = "access /proc/meminfo")
    List<String> readProcMeminfo() throws IOException {
        final List<String> lines;
        if (Files.exists(PathUtils.get("/proc/meminfo"))) {
            lines = Files.readAllLines(PathUtils.get("/proc/meminfo"));
            assert lines != null && lines.isEmpty() == false;
            return lines;
        } else {
            return Collections.emptyList();
        }
    }

    /**
     * Retrieves system total memory in bytes from /proc/meminfo
     */
    long getTotalMemFromProcMeminfo() throws IOException {
        List<String> meminfoLines = readProcMeminfo();
        final List<String> memTotalLines = meminfoLines.stream().filter(line -> line.startsWith("MemTotal")).collect(Collectors.toList());
        assert memTotalLines.size() <= 1 : memTotalLines;
        if (memTotalLines.size() == 1) {
            final String memTotalLine = memTotalLines.get(0);
            int beginIdx = memTotalLine.indexOf("MemTotal:");
            int endIdx = memTotalLine.lastIndexOf(" kB");
            if (beginIdx + 9 < endIdx) {
                final String memTotalString = memTotalLine.substring(beginIdx + 9, endIdx).trim();
                try {
                    long memTotalInKb = Long.parseLong(memTotalString);
                    return memTotalInKb * 1024;
                } catch (NumberFormatException e) {
                    logger.warn("Unable to retrieve total memory from meminfo line [" + memTotalLine + "]");
                    return 0;
                }
            } else {
                logger.warn("Unable to retrieve total memory from meminfo line [" + memTotalLine + "]");
                return 0;
            }
        } else {
            return 0;
        }
    }

    boolean isDebian8() throws IOException {
        return Constants.LINUX && getPrettyName().equals("Debian GNU/Linux 8 (jessie)");
    }

    public OsStats osStats() {
        final OsStats.Cpu cpu = new OsStats.Cpu(getSystemCpuPercent(), getSystemLoadAverage());
        final OsStats.Mem mem = new OsStats.Mem(getTotalPhysicalMemorySize(), getFreePhysicalMemorySize());
        final OsStats.Swap swap = new OsStats.Swap(getTotalSwapSpaceSize(), getFreeSwapSpaceSize());
        final OsStats.Cgroup cgroup = Constants.LINUX ? getCgroup() : null;
        return new OsStats(System.currentTimeMillis(), cpu, mem, swap, cgroup);
    }

    /**
     * Returns a given method of the OperatingSystemMXBean, or null if the method is not found or unavailable.
     */
    private static Method getMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.OperatingSystemMXBean").getMethod(methodName);
        } catch (Exception e) {
            // not available
            return null;
        }
    }

}
