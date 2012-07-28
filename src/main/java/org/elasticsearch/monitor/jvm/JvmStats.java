/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
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

import com.google.common.collect.Iterators;
import org.elasticsearch.common.Booleans;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.*;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class JvmStats implements Streamable, Serializable, ToXContent {

    private static boolean enableLastGc;

    public static boolean isLastGcEnabled() {
        return enableLastGc;
    }

    private final static RuntimeMXBean runtimeMXBean;
    private final static MemoryMXBean memoryMXBean;
    private final static ThreadMXBean threadMXBean;

    private static Method managementFactoryPlatformMXBeansMethod;

    private static Method getLastGcInfoMethod;
    private static Method getMemoryUsageBeforeGcMethod;
    private static Method getMemoryUsageAfterGcMethod;
    private static Method getStartTimeMethod;
    private static Method getEndTimeMethod;
    private static Method getDurationMethod;

    private static boolean bufferPoolsEnabled;
    private static Class bufferPoolMXBeanClass;
    private static Method bufferPoolMXBeanNameMethod;
    private static Method bufferPoolMXBeanCountMethod;
    private static Method bufferPoolMXBeanTotalCapacityMethod;
    private static Method bufferPoolMXBeanMemoryUsedMethod;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();

        try {
            managementFactoryPlatformMXBeansMethod = ManagementFactory.class.getMethod("getPlatformMXBeans", Class.class);
        } catch (Throwable e) {
            managementFactoryPlatformMXBeansMethod = null;
        }


        try {
            bufferPoolMXBeanClass = Class.forName("java.lang.management.BufferPoolMXBean");
            bufferPoolMXBeanNameMethod = bufferPoolMXBeanClass.getMethod("getName");
            bufferPoolMXBeanCountMethod = bufferPoolMXBeanClass.getMethod("getCount");
            bufferPoolMXBeanTotalCapacityMethod = bufferPoolMXBeanClass.getMethod("getTotalCapacity");
            bufferPoolMXBeanMemoryUsedMethod = bufferPoolMXBeanClass.getMethod("getMemoryUsed");
            bufferPoolsEnabled = true;
        } catch (Throwable t) {
            bufferPoolsEnabled = false;
        }

        JvmInfo info = JvmInfo.jvmInfo();
        boolean defaultEnableLastGc = false;
        if (info.versionAsInteger() == 170) {
            defaultEnableLastGc = info.versionUpdatePack() >= 4;
        } else if (info.versionAsInteger() > 170) {
            defaultEnableLastGc = true;
        }
        // always disable lastG, some reports it gives are strange...
        defaultEnableLastGc = false;

        boolean enableLastGc = Booleans.parseBoolean(System.getProperty("monitor.jvm.enable_last_gc"), defaultEnableLastGc);
        if (enableLastGc) {
            try {
                Class sunGcClass = Class.forName("com.sun.management.GarbageCollectorMXBean");
                Class gcInfoClass = Class.forName("com.sun.management.GcInfo");

                getLastGcInfoMethod = sunGcClass.getDeclaredMethod("getLastGcInfo");
                getLastGcInfoMethod.setAccessible(true);

                getMemoryUsageBeforeGcMethod = gcInfoClass.getDeclaredMethod("getMemoryUsageBeforeGc");
                getMemoryUsageBeforeGcMethod.setAccessible(true);
                getMemoryUsageAfterGcMethod = gcInfoClass.getDeclaredMethod("getMemoryUsageAfterGc");
                getMemoryUsageAfterGcMethod.setAccessible(true);
                getStartTimeMethod = gcInfoClass.getDeclaredMethod("getStartTime");
                getStartTimeMethod.setAccessible(true);
                getEndTimeMethod = gcInfoClass.getDeclaredMethod("getEndTime");
                getEndTimeMethod.setAccessible(true);
                getDurationMethod = gcInfoClass.getDeclaredMethod("getDuration");
                getDurationMethod.setAccessible(true);

            } catch (Throwable ex) {
                enableLastGc = false;
            }
        }

        JvmStats.enableLastGc = enableLastGc;
    }

    public static JvmStats jvmStats() {
        JvmStats stats = new JvmStats(System.currentTimeMillis(), runtimeMXBean.getUptime());
        stats.mem = new Mem();
        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        stats.mem.heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        memUsage = memoryMXBean.getNonHeapMemoryUsage();
        stats.mem.nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        stats.mem.pools = new MemoryPool[memoryPoolMXBeans.size()];
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
            MemoryUsage usage = memoryPoolMXBean.getUsage();
            MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
            stats.mem.pools[i] = new MemoryPool(memoryPoolMXBean.getName(),
                    usage.getUsed() < 0 ? 0 : usage.getUsed(),
                    usage.getMax() < 0 ? 0 : usage.getMax(),
                    peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                    peakUsage.getMax() < 0 ? 0 : peakUsage.getMax()
            );
        }

        stats.threads = new Threads();
        stats.threads.count = threadMXBean.getThreadCount();
        stats.threads.peakCount = threadMXBean.getPeakThreadCount();

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        stats.gc = new GarbageCollectors();
        stats.gc.collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < stats.gc.collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            stats.gc.collectors[i] = new GarbageCollector();
            stats.gc.collectors[i].name = gcMxBean.getName();
            stats.gc.collectors[i].collectionCount = gcMxBean.getCollectionCount();
            stats.gc.collectors[i].collectionTime = gcMxBean.getCollectionTime();
            if (enableLastGc) {
                try {
                    Object lastGcInfo = getLastGcInfoMethod.invoke(gcMxBean);
                    if (lastGcInfo != null) {
                        Map<String, MemoryUsage> usageBeforeGc = (Map<String, MemoryUsage>) getMemoryUsageBeforeGcMethod.invoke(lastGcInfo);
                        Map<String, MemoryUsage> usageAfterGc = (Map<String, MemoryUsage>) getMemoryUsageAfterGcMethod.invoke(lastGcInfo);
                        long startTime = (Long) getStartTimeMethod.invoke(lastGcInfo);
                        long endTime = (Long) getEndTimeMethod.invoke(lastGcInfo);
                        long duration = (Long) getDurationMethod.invoke(lastGcInfo);

                        long previousMemoryUsed = 0;
                        long memoryUsed = 0;
                        long memoryMax = 0;
                        for (Map.Entry<String, MemoryUsage> entry : usageBeforeGc.entrySet()) {
                            previousMemoryUsed += entry.getValue().getUsed();
                        }
                        for (Map.Entry<String, MemoryUsage> entry : usageAfterGc.entrySet()) {
                            MemoryUsage mu = entry.getValue();
                            memoryUsed += mu.getUsed();
                            memoryMax += mu.getMax();
                        }

                        stats.gc.collectors[i].lastGc = new GarbageCollector.LastGc(startTime, endTime, memoryMax, previousMemoryUsed, memoryUsed, duration);
                    }
                } catch (Exception e) {
//                    e.printStackTrace();
                }
            }
        }


        if (bufferPoolsEnabled) {
            try {
                List bufferPools = (List) managementFactoryPlatformMXBeansMethod.invoke(null, bufferPoolMXBeanClass);
                stats.bufferPools = new ArrayList<BufferPool>(bufferPools.size());
                for (Object bufferPool : bufferPools) {
                    String name = (String) bufferPoolMXBeanNameMethod.invoke(bufferPool);
                    Long count = (Long) bufferPoolMXBeanCountMethod.invoke(bufferPool);
                    Long totalCapacity = (Long) bufferPoolMXBeanTotalCapacityMethod.invoke(bufferPool);
                    Long memoryUsed = (Long) bufferPoolMXBeanMemoryUsedMethod.invoke(bufferPool);
                    stats.bufferPools.add(new BufferPool(name, count, totalCapacity, memoryUsed));
                }
            } catch (Throwable t) {
                //t.printStackTrace();
            }
        }

        return stats;
    }

    long timestamp = -1;

    long uptime;

    Mem mem;

    Threads threads;

    GarbageCollectors gc;

    List<BufferPool> bufferPools;

    private JvmStats() {
    }

    public JvmStats(long timestamp, long uptime) {
        this.timestamp = timestamp;
        this.uptime = uptime;
    }

    public long timestamp() {
        return timestamp;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TimeValue uptime() {
        return new TimeValue(uptime);
    }

    public TimeValue getUptime() {
        return uptime();
    }

    public Mem mem() {
        return this.mem;
    }

    public Mem getMem() {
        return mem();
    }

    public Threads threads() {
        return threads;
    }

    public Threads getThreads() {
        return threads();
    }

    public GarbageCollectors gc() {
        return gc;
    }

    public GarbageCollectors getGc() {
        return gc();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.field(Fields.UPTIME, uptime().format());
        builder.field(Fields.UPTIME_IN_MILLIS, uptime().millis());
        if (mem != null) {
            builder.startObject(Fields.MEM);
            builder.field(Fields.HEAP_USED, mem.heapUsed().toString());
            builder.field(Fields.HEAP_USED_IN_BYTES, mem.heapUsed().bytes());
            builder.field(Fields.HEAP_COMMITTED, mem.heapCommitted().toString());
            builder.field(Fields.HEAP_COMMITTED_IN_BYTES, mem.heapCommitted().bytes());

            builder.field(Fields.NON_HEAP_USED, mem.nonHeapUsed().toString());
            builder.field(Fields.NON_HEAP_USED_IN_BYTES, mem.nonHeapUsed);
            builder.field(Fields.NON_HEAP_COMMITTED, mem.nonHeapCommitted().toString());
            builder.field(Fields.NON_HEAP_COMMITTED_IN_BYTES, mem.nonHeapCommitted);

            builder.startObject(Fields.POOLS);
            for (MemoryPool pool : mem) {
                builder.startObject(pool.name(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.USED, pool.used().toString());
                builder.field(Fields.USED_IN_BYTES, pool.used);
                builder.field(Fields.MAX, pool.max().toString());
                builder.field(Fields.MAX_IN_BYTES, pool.max);

                builder.field(Fields.PEAK_USED, pool.peakUsed().toString());
                builder.field(Fields.PEAK_USED_IN_BYTES, pool.peakUsed);
                builder.field(Fields.PEAK_MAX, pool.peakMax().toString());
                builder.field(Fields.PEAK_MAX_IN_BYTES, pool.peakMax);

                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }
        if (threads != null) {
            builder.startObject(Fields.THREADS);
            builder.field(Fields.COUNT, threads.count());
            builder.field(Fields.PEAK_COUNT, threads.peakCount());
            builder.endObject();
        }
        if (gc != null) {
            builder.startObject(Fields.GC);
            builder.field(Fields.COLLECTION_COUNT, gc.collectionCount());
            builder.field(Fields.COLLECTION_TIME, gc.collectionTime().format());
            builder.field(Fields.COLLECTION_TIME_IN_MILLIS, gc.collectionTime().millis());

            builder.startObject(Fields.COLLECTORS);
            for (GarbageCollector collector : gc) {
                builder.startObject(collector.name(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.COLLECTION_COUNT, collector.collectionCount());
                builder.field(Fields.COLLECTION_TIME, collector.collectionTime().format());
                builder.field(Fields.COLLECTION_TIME_IN_MILLIS, collector.collectionTime().millis());
                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }

        if (bufferPools != null) {
            builder.startObject(Fields.BUFFER_POOLS);
            for (BufferPool bufferPool : bufferPools) {
                builder.startObject(bufferPool.name(), XContentBuilder.FieldCaseConversion.NONE);
                builder.field(Fields.COUNT, bufferPool.count());
                builder.field(Fields.USED, bufferPool.used().toString());
                builder.field(Fields.USED_IN_BYTES, bufferPool.used);
                builder.field(Fields.TOTAL_CAPACITY, bufferPool.totalCapacity().toString());
                builder.field(Fields.TOTAL_CAPACITY_IN_BYTES, bufferPool.totalCapacity);
                builder.endObject();
            }
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final XContentBuilderString JVM = new XContentBuilderString("jvm");
        static final XContentBuilderString TIMESTAMP = new XContentBuilderString("timestamp");
        static final XContentBuilderString UPTIME = new XContentBuilderString("uptime");
        static final XContentBuilderString UPTIME_IN_MILLIS = new XContentBuilderString("uptime_in_millis");

        static final XContentBuilderString MEM = new XContentBuilderString("mem");
        static final XContentBuilderString HEAP_USED = new XContentBuilderString("heap_used");
        static final XContentBuilderString HEAP_USED_IN_BYTES = new XContentBuilderString("heap_used_in_bytes");
        static final XContentBuilderString HEAP_COMMITTED = new XContentBuilderString("heap_committed");
        static final XContentBuilderString HEAP_COMMITTED_IN_BYTES = new XContentBuilderString("heap_committed_in_bytes");

        static final XContentBuilderString NON_HEAP_USED = new XContentBuilderString("non_heap_used");
        static final XContentBuilderString NON_HEAP_USED_IN_BYTES = new XContentBuilderString("non_heap_used_in_bytes");
        static final XContentBuilderString NON_HEAP_COMMITTED = new XContentBuilderString("non_heap_committed");
        static final XContentBuilderString NON_HEAP_COMMITTED_IN_BYTES = new XContentBuilderString("non_heap_committed_in_bytes");

        static final XContentBuilderString POOLS = new XContentBuilderString("pools");
        static final XContentBuilderString USED = new XContentBuilderString("used");
        static final XContentBuilderString USED_IN_BYTES = new XContentBuilderString("used_in_bytes");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString MAX_IN_BYTES = new XContentBuilderString("max_in_bytes");
        static final XContentBuilderString PEAK_USED = new XContentBuilderString("peak_used");
        static final XContentBuilderString PEAK_USED_IN_BYTES = new XContentBuilderString("peak_used_in_bytes");
        static final XContentBuilderString PEAK_MAX = new XContentBuilderString("peak_max");
        static final XContentBuilderString PEAK_MAX_IN_BYTES = new XContentBuilderString("peak_max_in_bytes");

        static final XContentBuilderString THREADS = new XContentBuilderString("threads");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString PEAK_COUNT = new XContentBuilderString("peak_count");

        static final XContentBuilderString GC = new XContentBuilderString("gc");
        static final XContentBuilderString COLLECTORS = new XContentBuilderString("collectors");
        static final XContentBuilderString COLLECTION_COUNT = new XContentBuilderString("collection_count");
        static final XContentBuilderString COLLECTION_TIME = new XContentBuilderString("collection_time");
        static final XContentBuilderString COLLECTION_TIME_IN_MILLIS = new XContentBuilderString("collection_time_in_millis");

        static final XContentBuilderString BUFFER_POOLS = new XContentBuilderString("buffer_pools");
        static final XContentBuilderString NAME = new XContentBuilderString("name");
        static final XContentBuilderString TOTAL_CAPACITY = new XContentBuilderString("total_capacity");
        static final XContentBuilderString TOTAL_CAPACITY_IN_BYTES = new XContentBuilderString("total_capacity_in_bytes");
    }


    public static JvmStats readJvmStats(StreamInput in) throws IOException {
        JvmStats jvmStats = new JvmStats();
        jvmStats.readFrom(in);
        return jvmStats;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        timestamp = in.readVLong();
        uptime = in.readVLong();

        mem = Mem.readMem(in);
        threads = Threads.readThreads(in);
        gc = GarbageCollectors.readGarbageCollectors(in);

        if (in.readBoolean()) {
            int size = in.readVInt();
            bufferPools = new ArrayList<BufferPool>(size);
            for (int i = 0; i < size; i++) {
                BufferPool bufferPool = new BufferPool();
                bufferPool.readFrom(in);
                bufferPools.add(bufferPool);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(timestamp);
        out.writeVLong(uptime);

        mem.writeTo(out);
        threads.writeTo(out);
        gc.writeTo(out);

        if (bufferPools == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            out.writeVInt(bufferPools.size());
            for (BufferPool bufferPool : bufferPools) {
                bufferPool.writeTo(out);
            }
        }
    }

    public static class GarbageCollectors implements Streamable, Serializable, Iterable<GarbageCollector> {

        GarbageCollector[] collectors;

        GarbageCollectors() {
        }

        public static GarbageCollectors readGarbageCollectors(StreamInput in) throws IOException {
            GarbageCollectors collectors = new GarbageCollectors();
            collectors.readFrom(in);
            return collectors;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            collectors = new GarbageCollector[in.readVInt()];
            for (int i = 0; i < collectors.length; i++) {
                collectors[i] = GarbageCollector.readGarbageCollector(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(collectors.length);
            for (GarbageCollector gc : collectors) {
                gc.writeTo(out);
            }
        }

        public GarbageCollector[] collectors() {
            return this.collectors;
        }

        @Override
        public Iterator<GarbageCollector> iterator() {
            return Iterators.forArray(collectors);
        }

        public long collectionCount() {
            long collectionCount = 0;
            for (GarbageCollector gc : collectors) {
                collectionCount += gc.collectionCount();
            }
            return collectionCount;
        }

        public TimeValue collectionTime() {
            long collectionTime = 0;
            for (GarbageCollector gc : collectors) {
                collectionTime += gc.collectionTime;
            }
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }
    }

    public static class GarbageCollector implements Streamable, Serializable {

        public static class LastGc implements Streamable {

            long startTime;
            long endTime;
            long max;
            long beforeUsed;
            long afterUsed;
            long duration;

            LastGc() {
            }

            public LastGc(long startTime, long endTime, long max, long beforeUsed, long afterUsed, long duration) {
                this.startTime = startTime;
                this.endTime = endTime;
                this.max = max;
                this.beforeUsed = beforeUsed;
                this.afterUsed = afterUsed;
                this.duration = duration;
            }

            public long startTime() {
                return this.startTime;
            }

            public long getStartTime() {
                return startTime();
            }

            public long endTime() {
                return this.endTime;
            }

            public long getEndTime() {
                return endTime();
            }

            public ByteSizeValue max() {
                return new ByteSizeValue(max);
            }

            public ByteSizeValue getMax() {
                return max();
            }

            public ByteSizeValue afterUsed() {
                return new ByteSizeValue(afterUsed);
            }

            public ByteSizeValue getAfterUsed() {
                return afterUsed();
            }

            public ByteSizeValue beforeUsed() {
                return new ByteSizeValue(beforeUsed);
            }

            public ByteSizeValue getBeforeUsed() {
                return beforeUsed();
            }

            public ByteSizeValue reclaimed() {
                return new ByteSizeValue(beforeUsed - afterUsed);
            }

            public ByteSizeValue getReclaimed() {
                return reclaimed();
            }

            public TimeValue duration() {
                return new TimeValue(this.duration);
            }

            public TimeValue getDuration() {
                return duration();
            }

            public static LastGc readLastGc(StreamInput in) throws IOException {
                LastGc lastGc = new LastGc();
                lastGc.readFrom(in);
                return lastGc;
            }

            @Override
            public void readFrom(StreamInput in) throws IOException {
                startTime = in.readVLong();
                endTime = in.readVLong();
                max = in.readVLong();
                beforeUsed = in.readVLong();
                afterUsed = in.readVLong();
                duration = in.readVLong();
            }

            @Override
            public void writeTo(StreamOutput out) throws IOException {
                out.writeVLong(startTime);
                out.writeVLong(endTime);
                out.writeVLong(max);
                out.writeVLong(beforeUsed);
                out.writeVLong(afterUsed);
                out.writeVLong(duration);
            }
        }

        String name;
        long collectionCount;
        long collectionTime;
        LastGc lastGc;

        GarbageCollector() {
        }

        public static GarbageCollector readGarbageCollector(StreamInput in) throws IOException {
            GarbageCollector gc = new GarbageCollector();
            gc.readFrom(in);
            return gc;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            collectionCount = in.readVLong();
            collectionTime = in.readVLong();
            if (in.readBoolean()) {
                lastGc = LastGc.readLastGc(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeVLong(collectionCount);
            out.writeVLong(collectionTime);
            if (lastGc == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                lastGc.writeTo(out);
            }
        }

        public String name() {
            return name;
        }

        public String getName() {
            return name();
        }

        public long collectionCount() {
            return collectionCount;
        }

        public long getCollectionCount() {
            return collectionCount();
        }

        public TimeValue collectionTime() {
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }

        public TimeValue getCollectionTime() {
            return collectionTime();
        }

        public LastGc lastGc() {
            return this.lastGc;
        }

        public LastGc getLastGc() {
            return lastGc();
        }
    }

    public static class Threads implements Streamable, Serializable {

        int count;
        int peakCount;

        Threads() {
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }

        public int peakCount() {
            return peakCount;
        }

        public int getPeakCount() {
            return peakCount();
        }

        public static Threads readThreads(StreamInput in) throws IOException {
            Threads threads = new Threads();
            threads.readFrom(in);
            return threads;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            count = in.readVInt();
            peakCount = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(count);
            out.writeVInt(peakCount);
        }
    }

    public static class MemoryPool implements Streamable, Serializable {

        String name;
        long used;
        long max;

        long peakUsed;
        long peakMax;

        MemoryPool() {

        }

        public MemoryPool(String name, long used, long max, long peakUsed, long peakMax) {
            this.name = name;
            this.used = used;
            this.max = max;
            this.peakUsed = peakUsed;
            this.peakMax = peakMax;
        }

        public static MemoryPool readMemoryPool(StreamInput in) throws IOException {
            MemoryPool pool = new MemoryPool();
            pool.readFrom(in);
            return pool;
        }

        public String name() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }

        public ByteSizeValue used() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getUsed() {
            return used();
        }

        public ByteSizeValue max() {
            return new ByteSizeValue(max);
        }

        public ByteSizeValue getMax() {
            return max();
        }

        public ByteSizeValue peakUsed() {
            return new ByteSizeValue(peakUsed);
        }

        public ByteSizeValue getPeakUsed() {
            return peakUsed();
        }

        public ByteSizeValue peakMax() {
            return new ByteSizeValue(peakMax);
        }

        public ByteSizeValue getPeakMax() {
            return peakMax();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            used = in.readVLong();
            max = in.readVLong();
            peakUsed = in.readVLong();
            peakMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeVLong(used);
            out.writeVLong(max);
            out.writeVLong(peakUsed);
            out.writeVLong(peakMax);
        }
    }

    public static class Mem implements Streamable, Serializable, Iterable<MemoryPool> {

        long heapCommitted;
        long heapUsed;
        long nonHeapCommitted;
        long nonHeapUsed;

        MemoryPool[] pools = new MemoryPool[0];

        Mem() {
        }

        public static Mem readMem(StreamInput in) throws IOException {
            Mem mem = new Mem();
            mem.readFrom(in);
            return mem;
        }

        @Override
        public Iterator<MemoryPool> iterator() {
            return Iterators.forArray(pools);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            heapCommitted = in.readVLong();
            heapUsed = in.readVLong();
            nonHeapCommitted = in.readVLong();
            nonHeapUsed = in.readVLong();

            pools = new MemoryPool[in.readVInt()];
            for (int i = 0; i < pools.length; i++) {
                pools[i] = MemoryPool.readMemoryPool(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVLong(heapCommitted);
            out.writeVLong(heapUsed);
            out.writeVLong(nonHeapCommitted);
            out.writeVLong(nonHeapUsed);

            out.writeVInt(pools.length);
            for (MemoryPool pool : pools) {
                pool.writeTo(out);
            }
        }

        public ByteSizeValue heapCommitted() {
            return new ByteSizeValue(heapCommitted);
        }

        public ByteSizeValue getHeapCommitted() {
            return heapCommitted();
        }

        public ByteSizeValue heapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        public ByteSizeValue getHeapUsed() {
            return heapUsed();
        }

        public ByteSizeValue nonHeapCommitted() {
            return new ByteSizeValue(nonHeapCommitted);
        }

        public ByteSizeValue getNonHeapCommitted() {
            return nonHeapCommitted();
        }

        public ByteSizeValue nonHeapUsed() {
            return new ByteSizeValue(nonHeapUsed);
        }

        public ByteSizeValue getNonHeapUsed() {
            return nonHeapUsed();
        }
    }

    public static class BufferPool implements Streamable {

        String name;
        long count;
        long totalCapacity;
        long used;

        BufferPool() {
        }

        public BufferPool(String name, long count, long totalCapacity, long used) {
            this.name = name;
            this.count = count;
            this.totalCapacity = totalCapacity;
            this.used = used;
        }

        public String name() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }

        public long count() {
            return this.count;
        }

        public long getCount() {
            return this.count;
        }

        public ByteSizeValue totalCapacity() {
            return new ByteSizeValue(totalCapacity);
        }

        public ByteSizeValue getTotalCapacity() {
            return totalCapacity();
        }

        public ByteSizeValue used() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getUsed() {
            return used();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            count = in.readLong();
            totalCapacity = in.readLong();
            used = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeLong(count);
            out.writeLong(totalCapacity);
            out.writeLong(used);
        }
    }
}
