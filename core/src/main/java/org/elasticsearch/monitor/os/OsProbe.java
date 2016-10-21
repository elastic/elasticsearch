/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.monitor.os;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.Constants;
import org.elasticsearch.common.SuppressForbidden;
import org.elasticsearch.common.io.PathUtils;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.monitor.Probes;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.List;

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
            return -1;
        }
        try {
            return (long) getFreePhysicalMemorySize.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Returns the total amount of physical memory in bytes.
     */
    public long getTotalPhysicalMemorySize() {
        if (getTotalPhysicalMemorySize == null) {
            return -1;
        }
        try {
            return (long) getTotalPhysicalMemorySize.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Returns the amount of free swap space in bytes.
     */
    public long getFreeSwapSpaceSize() {
        if (getFreeSwapSpaceSize == null) {
            return -1;
        }
        try {
            return (long) getFreeSwapSpaceSize.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * Returns the total amount of swap space in bytes.
     */
    public long getTotalSwapSpaceSize() {
        if (getTotalSwapSpaceSize == null) {
            return -1;
        }
        try {
            return (long) getTotalSwapSpaceSize.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    /**
     * The system load averages as an array.
     *
     * On Windows, this method returns {@code null}.
     *
     * On Linux, this method should return the 1, 5, and 15-minute load
     * averages. If obtaining these values from {@code /proc/loadavg}
     * fails, the method will fallback to obtaining the 1-minute load
     * average.
     *
     * On macOS, this method should return the 1-minute load average.
     *
     * @return the available system load averages or {@code null}
     */
    final double[] getSystemLoadAverage() {
        if (Constants.WINDOWS) {
            return null;
        } else if (Constants.LINUX) {
            final String procLoadAvg = readProcLoadavg();
            if (procLoadAvg != null) {
                assert procLoadAvg.matches("(\\d+\\.\\d+\\s+){3}\\d+/\\d+\\s+\\d+");
                final String[] fields = procLoadAvg.split("\\s+");
                try {
                    return new double[]{Double.parseDouble(fields[0]), Double.parseDouble(fields[1]), Double.parseDouble(fields[2])};
                } catch (final NumberFormatException e) {
                    logger.debug((Supplier<?>) () -> new ParameterizedMessage("error parsing /proc/loadavg [{}]", procLoadAvg), e);
                }
            }
            // fallback
        }

        if (getSystemLoadAverage == null) {
            return null;
        }
        try {
            final double oneMinuteLoadAverage = (double) getSystemLoadAverage.invoke(osMxBean);
            return new double[] { oneMinuteLoadAverage >= 0 ? oneMinuteLoadAverage : -1, -1, -1 };
        } catch (final Exception e) {
            logger.debug("error obtaining system load average", e);
            return null;
        }
    }

    /**
     * The line from {@code /proc/loadavg}. The first three fields are
     * the load averages averaged over 1, 5, and 15 minutes. The fourth
     * field is two numbers separated by a slash, the first is the
     * number of currently runnable scheduling entities, the second is
     * the number of scheduling entities on the system. The fifth field
     * is the PID of the most recently created process.
     *
     * @return the line from {@code /proc/loadavg} or {@code null}
     */
    @SuppressForbidden(reason = "access /proc/loadavg")
    String readProcLoadavg() {
        try {
            final List<String> lines = Files.readAllLines(PathUtils.get("/proc/loadavg"));
            assert lines != null && lines.size() == 1;
            return lines.get(0);
        } catch (final IOException e) {
            logger.debug("error reading /proc/loadavg", e);
            return null;
        }
    }

    public short getSystemCpuPercent() {
        return Probes.getLoadAndScaleToPercent(getSystemCpuLoad, osMxBean);
    }

    private static class OsProbeHolder {
        private static final OsProbe INSTANCE = new OsProbe();
    }

    public static OsProbe getInstance() {
        return OsProbeHolder.INSTANCE;
    }

    OsProbe() {
    }

    private final Logger logger = ESLoggerFactory.getLogger(getClass());

    public OsInfo osInfo(long refreshInterval, int allocatedProcessors) {
        return new OsInfo(refreshInterval, Runtime.getRuntime().availableProcessors(),
                allocatedProcessors, Constants.OS_NAME, Constants.OS_ARCH, Constants.OS_VERSION);
    }

    public OsStats osStats() {
        OsStats.Cpu cpu = new OsStats.Cpu(getSystemCpuPercent(), getSystemLoadAverage());
        OsStats.Mem mem = new OsStats.Mem(getTotalPhysicalMemorySize(), getFreePhysicalMemorySize());
        OsStats.Swap swap = new OsStats.Swap(getTotalSwapSpaceSize(), getFreeSwapSpaceSize());
        return new OsStats(System.currentTimeMillis(), cpu, mem , swap);
    }

    /**
     * Returns a given method of the OperatingSystemMXBean,
     * or null if the method is not found or unavailable.
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
