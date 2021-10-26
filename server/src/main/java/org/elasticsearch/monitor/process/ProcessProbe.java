/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.monitor.process;

import org.elasticsearch.bootstrap.BootstrapInfo;
import org.elasticsearch.monitor.Probes;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.reflect.Method;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;

public class ProcessProbe {

    private static final OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();

    private static final Method getMaxFileDescriptorCountField;
    private static final Method getOpenFileDescriptorCountField;
    private static final Method getProcessCpuLoad;
    private static final Method getProcessCpuTime;
    private static final Method getCommittedVirtualMemorySize;

    static {
        getMaxFileDescriptorCountField = getUnixMethod("getMaxFileDescriptorCount");
        getOpenFileDescriptorCountField = getUnixMethod("getOpenFileDescriptorCount");
        getProcessCpuLoad = getMethod("getProcessCpuLoad");
        getProcessCpuTime = getMethod("getProcessCpuTime");
        getCommittedVirtualMemorySize = getMethod("getCommittedVirtualMemorySize");
    }

    private static class ProcessProbeHolder {
        private static final ProcessProbe INSTANCE = new ProcessProbe();
    }

    public static ProcessProbe getInstance() {
        return ProcessProbeHolder.INSTANCE;
    }

    private ProcessProbe() {
    }

    /**
     * Returns the maximum number of file descriptors allowed on the system, or -1 if not supported.
     */
    public long getMaxFileDescriptorCount() {
        if (getMaxFileDescriptorCountField == null) {
            return -1;
        }
        try {
            return (Long) getMaxFileDescriptorCountField.invoke(osMxBean);
        } catch (Exception t) {
            return -1;
        }
    }

    /**
     * Returns the number of opened file descriptors associated with the current process, or -1 if not supported.
     */
    public long getOpenFileDescriptorCount() {
        if (getOpenFileDescriptorCountField == null) {
            return -1;
        }
        try {
            return (Long) getOpenFileDescriptorCountField.invoke(osMxBean);
        } catch (Exception t) {
            return -1;
        }
    }

    /**
     * Returns the process CPU usage in percent
     */
    public short getProcessCpuPercent() {
        return Probes.getLoadAndScaleToPercent(getProcessCpuLoad, osMxBean);
    }

    /**
     * Returns the CPU time (in milliseconds) used by the process on which the Java virtual machine is running, or -1 if not supported.
     */
    public long getProcessCpuTotalTime() {
        if (getProcessCpuTime != null) {
            try {
                long time = (long) getProcessCpuTime.invoke(osMxBean);
                if (time >= 0) {
                    return (time / 1_000_000L);
                }
            } catch (Exception t) {
                return -1;
            }
        }
        return -1;
    }

    /**
     * Returns the size (in bytes) of virtual memory that is guaranteed to be available to the running process
     */
    public long getTotalVirtualMemorySize() {
        if (getCommittedVirtualMemorySize != null) {
            try {
                long virtual = (long) getCommittedVirtualMemorySize.invoke(osMxBean);
                if (virtual >= 0) {
                    return virtual;
                }
            } catch (Exception t) {
                return -1;
            }
        }
        return -1;
    }

    public ProcessInfo processInfo(long refreshInterval) {
        return new ProcessInfo(jvmInfo().pid(), BootstrapInfo.isMemoryLocked(), refreshInterval);
    }

    public ProcessStats processStats() {
        ProcessStats.Cpu cpu = new ProcessStats.Cpu(getProcessCpuPercent(), getProcessCpuTotalTime());
        ProcessStats.Mem mem = new ProcessStats.Mem(getTotalVirtualMemorySize());
        return new ProcessStats(System.currentTimeMillis(), getOpenFileDescriptorCount(), getMaxFileDescriptorCount(), cpu, mem);
    }

    /**
     * Returns a given method of the OperatingSystemMXBean,
     * or null if the method is not found or unavailable.
     */
    private static Method getMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.OperatingSystemMXBean").getMethod(methodName);
        } catch (Exception t) {
            // not available
            return null;
        }
    }

    /**
     * Returns a given method of the UnixOperatingSystemMXBean,
     * or null if the method is not found or unavailable.
     */
    private static Method getUnixMethod(String methodName) {
        try {
            return Class.forName("com.sun.management.UnixOperatingSystemMXBean").getMethod(methodName);
        } catch (Exception t) {
            // not available
            return null;
        }
    }
}
