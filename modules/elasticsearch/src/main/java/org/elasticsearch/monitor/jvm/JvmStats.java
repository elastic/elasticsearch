/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.util.SizeValue;
import org.elasticsearch.util.TimeValue;
import org.elasticsearch.util.io.Streamable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.lang.management.*;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author kimchy (Shay Banon)
 */
public class JvmStats implements Streamable, Serializable {

    private static RuntimeMXBean runtimeMXBean;
    private static MemoryMXBean memoryMXBean;
    private static ThreadMXBean threadMXBean;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
    }

    public static JvmStats jvmStats() {
        long gcCollectionCount = 0;
        long gcCollectionTime = 0;
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        for (GarbageCollectorMXBean gcMxBean : gcMxBeans) {
            long tmp = gcMxBean.getCollectionCount();
            if (tmp != -1) {
                gcCollectionCount += tmp;
            }
            tmp = gcMxBean.getCollectionTime();
            if (tmp != -1) {
                gcCollectionTime += tmp;
            }
        }
        return new JvmStats(System.currentTimeMillis(), runtimeMXBean.getUptime(),
                memoryMXBean.getHeapMemoryUsage().getCommitted(), memoryMXBean.getHeapMemoryUsage().getUsed(),
                memoryMXBean.getNonHeapMemoryUsage().getCommitted(), memoryMXBean.getNonHeapMemoryUsage().getUsed(),
                threadMXBean.getThreadCount(), threadMXBean.getPeakThreadCount(), gcCollectionCount, gcCollectionTime);
    }

    private long timestamp = -1;

    private long uptime;

    private long memoryHeapCommitted;

    private long memoryHeapUsed;

    private long memoryNonHeapCommitted;

    private long memoryNonHeapUsed;

    private int threadCount;

    private int peakThreadCount;

    private long gcCollectionCount;

    private long gcCollectionTime;

    private JvmStats() {
    }

    public JvmStats(long timestamp, long uptime,
                    long memoryHeapCommitted, long memoryHeapUsed, long memoryNonHeapCommitted, long memoryNonHeapUsed,
                    int threadCount, int peakThreadCount, long gcCollectionCount, long gcCollectionTime) {
        this.timestamp = timestamp;
        this.uptime = uptime;
        this.memoryHeapCommitted = memoryHeapCommitted;
        this.memoryHeapUsed = memoryHeapUsed;
        this.memoryNonHeapCommitted = memoryNonHeapCommitted;
        this.memoryNonHeapUsed = memoryNonHeapUsed;
        this.threadCount = threadCount;
        this.peakThreadCount = peakThreadCount;
        this.gcCollectionCount = gcCollectionCount;
        this.gcCollectionTime = gcCollectionTime;
    }

    public long timestamp() {
        return timestamp;
    }

    public long uptime() {
        return uptime;
    }

    public SizeValue memoryHeapCommitted() {
        return new SizeValue(memoryHeapCommitted);
    }

    public SizeValue memoryHeapUsed() {
        return new SizeValue(memoryHeapUsed);
    }

    public SizeValue memoryNonHeapCommitted() {
        return new SizeValue(memoryNonHeapCommitted);
    }

    public SizeValue memoryNonHeapUsed() {
        return new SizeValue(memoryNonHeapUsed);
    }

    public int threadCount() {
        return threadCount;
    }

    public int peakThreadCount() {
        return peakThreadCount;
    }

    public long gcCollectionCount() {
        return gcCollectionCount;
    }

    public TimeValue gcCollectionTime() {
        return new TimeValue(gcCollectionTime, TimeUnit.MILLISECONDS);
    }

    public static JvmStats readJvmStats(DataInput in) throws IOException, ClassNotFoundException {
        JvmStats jvmStats = new JvmStats();
        jvmStats.readFrom(in);
        return jvmStats;
    }

    @Override public void readFrom(DataInput in) throws IOException, ClassNotFoundException {
        timestamp = in.readLong();
        uptime = in.readLong();
        memoryHeapCommitted = in.readLong();
        memoryHeapUsed = in.readLong();
        memoryNonHeapCommitted = in.readLong();
        memoryNonHeapUsed = in.readLong();
        threadCount = in.readInt();
        peakThreadCount = in.readInt();
        gcCollectionCount = in.readLong();
        gcCollectionTime = in.readLong();
    }

    @Override public void writeTo(DataOutput out) throws IOException {
        out.writeLong(timestamp);
        out.writeLong(uptime);
        out.writeLong(memoryHeapCommitted);
        out.writeLong(memoryHeapUsed);
        out.writeLong(memoryNonHeapCommitted);
        out.writeLong(memoryNonHeapUsed);
        out.writeInt(threadCount);
        out.writeInt(peakThreadCount);
        out.writeLong(gcCollectionCount);
        out.writeLong(gcCollectionTime);
    }
}
