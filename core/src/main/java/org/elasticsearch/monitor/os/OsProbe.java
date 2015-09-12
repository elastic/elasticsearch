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

import org.apache.lucene.util.Constants;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

public class OsProbe {

    private static final OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();

    private static final Method getFreePhysicalMemorySize;
    private static final Method getTotalPhysicalMemorySize;
    private static final Method getFreeSwapSpaceSize;
    private static final Method getTotalSwapSpaceSize;
    private static final Method getSystemLoadAverage;

    static {
        getFreePhysicalMemorySize = getMethod("getFreePhysicalMemorySize");
        getTotalPhysicalMemorySize = getMethod("getTotalPhysicalMemorySize");
        getFreeSwapSpaceSize = getMethod("getFreeSwapSpaceSize");
        getTotalSwapSpaceSize = getMethod("getTotalSwapSpaceSize");
        getSystemLoadAverage = getMethod("getSystemLoadAverage");
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
        } catch (Throwable t) {
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
        } catch (Throwable t) {
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
        } catch (Throwable t) {
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
        } catch (Throwable t) {
            return -1;
        }
    }

    /**
     * Returns the system load average for the last minute.
     */
    public double getSystemLoadAverage() {
        if (getSystemLoadAverage == null) {
            return -1;
        }
        try {
            return (double) getSystemLoadAverage.invoke(osMxBean);
        } catch (Throwable t) {
            return -1;
        }
    }

    private static class OsProbeHolder {
        private final static OsProbe INSTANCE = new OsProbe();
    }

    public static OsProbe getInstance() {
        return OsProbeHolder.INSTANCE;
    }

    private OsProbe() {
    }

    public OsInfo osInfo() {
        OsInfo info = new OsInfo();
        info.availableProcessors = Runtime.getRuntime().availableProcessors();
        info.name = Constants.OS_NAME;
        info.arch = Constants.OS_ARCH;
        info.version = Constants.OS_VERSION;
        return info;
    }

    public OsStats osStats() {
        OsStats stats = new OsStats();
        stats.timestamp = System.currentTimeMillis();
        stats.loadAverage = getSystemLoadAverage();

        OsStats.Mem mem = new OsStats.Mem();
        mem.total = getTotalPhysicalMemorySize();
        mem.free = getFreePhysicalMemorySize();
        stats.mem = mem;

        OsStats.Swap swap = new OsStats.Swap();
        swap.total = getTotalSwapSpaceSize();
        swap.free = getFreeSwapSpaceSize();
        stats.swap = swap;

        return stats;
    }

    /**
     * Returns a given method of the OperatingSystemMXBean,
     * or null if the method is not found or unavailable.
     */
    private static Method getMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.OperatingSystemMXBean").getMethod(methodName);
        } catch (Throwable t) {
            // not available
            return null;
        }
    }
}
