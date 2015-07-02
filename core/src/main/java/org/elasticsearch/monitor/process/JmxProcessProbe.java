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

package org.elasticsearch.monitor.process;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.reflect.Method;

import static org.elasticsearch.monitor.jvm.JvmInfo.jvmInfo;

public class JmxProcessProbe extends AbstractComponent implements ProcessProbe {

    public static final String TYPE = "jmx";

    private static final OperatingSystemMXBean osMxBean = ManagementFactory.getOperatingSystemMXBean();
    private static final ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

    private static final Method getMaxFileDescriptorCountField;
    private static final Method getOpenFileDescriptorCountField;
    private static final Method getProcessCpuLoad;
    private static final Method getProcessCpuTime;
    private static final Method getCommittedVirtualMemorySize;


    static {
        Method method = null;
        try {
            method = osMxBean.getClass().getDeclaredMethod("getMaxFileDescriptorCount");
            method.setAccessible(true);
        } catch (Exception e) {
            // not available
        }
        getMaxFileDescriptorCountField = method;

        method = null;
        try {
            method = osMxBean.getClass().getDeclaredMethod("getOpenFileDescriptorCount");
            method.setAccessible(true);
        } catch (Exception e) {
            // not available
        }
        getOpenFileDescriptorCountField = method;

        method = null;
        try {
            method = osMxBean.getClass().getDeclaredMethod("getProcessCpuLoad");
            method.setAccessible(true);
        } catch (Exception e) {
            // not available
        }
        getProcessCpuLoad = method;

        method = null;
        try {
            method = osMxBean.getClass().getDeclaredMethod("getProcessCpuTime");
            method.setAccessible(true);
        } catch (Exception e) {
            // not available
        }
        getProcessCpuTime = method;

        method = null;
        try {
            method = osMxBean.getClass().getDeclaredMethod("getCommittedVirtualMemorySize");
            method.setAccessible(true);
        } catch (Exception e) {
            // not available
        }
        getCommittedVirtualMemorySize = method;
    }

    public static long getMaxFileDescriptorCount() {
        if (getMaxFileDescriptorCountField == null) {
            return -1;
        }
        try {
            return (Long) getMaxFileDescriptorCountField.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    public static long getOpenFileDescriptorCount() {
        if (getOpenFileDescriptorCountField == null) {
            return -1;
        }
        try {
            return (Long) getOpenFileDescriptorCountField.invoke(osMxBean);
        } catch (Exception e) {
            return -1;
        }
    }

    @Inject
    public JmxProcessProbe(Settings settings) {
        super(settings);
    }

    /**
     * @return the process identifier (PID), as returned
     * by the RuntimeMXBean bean through JVMInfo class.
     */
    @Override
    public Long pid() {
        return jvmInfo().pid();
    }

    /**
     * @return the maximum number of file descriptors,
     * as returned by the OperatingSystemMXBean bean.
     */
    @Override
    public Long maxFileDescriptor() {
        try {
            if (getMaxFileDescriptorCountField != null) {
                return (Long) getMaxFileDescriptorCountField.invoke(osMxBean);
            }
        } catch (Exception e) {
            logger.trace("exception when accessing OperatingSystemMXBean.getMaxFileDescriptorCountField()");
        }
        return null;
    }

    /**
     * @return the number of open file descriptors,
     * as returned by the OperatingSystemMXBean bean.
     */
    @Override
    public Long openFileDescriptor() {
        try {
            if (getOpenFileDescriptorCountField != null) {
                return (Long) getOpenFileDescriptorCountField.invoke(osMxBean);
            }
        } catch (Exception e) {
            logger.trace("exception when accessing OperatingSystemMXBean.getOpenFileDescriptorCountField()");
        }
        return null;
    }

    /**
     * @return the CPU load the current JVM is taking
     */
    @Override
    public Short processCpuLoad() {
        try {
            if (getProcessCpuLoad != null) {
                double load = (double) getProcessCpuLoad.invoke(osMxBean);
                if (load >= 0) {
                    return (short) (load * 100);
                }
            }
        } catch (Exception e) {
            logger.trace("exception when accessing OperatingSystemMXBean.getProcessCpuLoad()");
        }
        return null;
    }

    /**
     * @return the CPU time used by the process on which the Java virtual machine is running in millis
     */
    @Override
    public Long processCpuTime() {
        try {
            if (getProcessCpuTime != null) {
                long time = (long) getProcessCpuTime.invoke(osMxBean);
                if (time >= 0) {
                    return (time / 1_000_000L);
                }
            }
        } catch (Exception e) {
            logger.trace("exception when accessing OperatingSystemMXBean.getProcessCpuTime()");
        }
        return null;
    }

    @Override
    public Long processSystemTime() {
        if (!threadMxBean.isThreadCpuTimeSupported()) {
            throw new UnsupportedOperationException("JmxProcessProbe does not support process user time");
        }
        try {
            long[] threadIds = threadMxBean.getAllThreadIds();

            long sumCpuTime = 0;
            for (long threadId : threadIds) {
                if (threadId == Thread.currentThread().getId()) {
                    // exclude current polling thread
                    continue;
                }

                long cpuTime = threadMxBean.getThreadCpuTime(threadId);
                if (cpuTime >= 0) {
                    sumCpuTime += cpuTime;
                }
            }

            long sumUserTime = 0;
            for (long threadId : threadIds) {
                if (threadId == Thread.currentThread().getId()) {
                    // exclude current polling thread
                    continue;
                }

                long userTime = threadMxBean.getThreadUserTime(threadId);
                if (userTime >= 0) {
                    sumUserTime += userTime;
                }
            }

            return (sumCpuTime - sumUserTime) / 1_000_000L;
        } catch (Exception e) {
            logger.trace("exception when accessing ThreadMxBean.getAllThreadIds()");
        }
        return null;
    }

    @Override
    public Long processUserTime() {
        if (!threadMxBean.isThreadCpuTimeSupported()) {
            throw new UnsupportedOperationException("JmxProcessProbe does not support process user time");
        }
        try {
            long sum = 0;
            long[] threadIds = threadMxBean.getAllThreadIds();
            for (long threadId : threadIds) {
                if (threadId == Thread.currentThread().getId()) {
                    // exclude current polling thread
                    continue;
                }

                long userTime = threadMxBean.getThreadUserTime(threadId);
                if (userTime >= 0) {
                    sum += userTime;
                }
            }

            return sum / 1_000_000L;
        } catch (Exception e) {
            logger.trace("exception when accessing ThreadMxBean.getAllThreadIds()");
        }
        return null;
    }


    /**
     * @return Returns the amount of virtual memory that is guaranteed to be available to the running process in bytes
     */
    @Override
    public Long totalVirtualMemorySize() {
        try {
            if (getCommittedVirtualMemorySize != null) {
                long virtual = (long) getCommittedVirtualMemorySize.invoke(osMxBean);
                if (virtual >= 0) {
                    return virtual;
                }
            }
        } catch (Exception e) {
            logger.trace("exception when accessing OperatingSystemMXBean.getCommittedVirtualMemorySize()");
        }
        return null;
    }

    @Override
    public Long residentMemorySize() {
        // Use /proc/self/stat (or /proc/[pid]/status VmRSS?)
        throw new UnsupportedOperationException("JmxProcessProbe does not support provide resident memory size");
    }

    @Override
    public Long sharedMemorySize() {
        // Use /proc/self/stat (or /proc/[pid]/status VmLib?)
        throw new UnsupportedOperationException("JmxProcessProbe does not support provide shared memory size");
    }
}
