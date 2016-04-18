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

package org.elasticsearch.monitor.jvm;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.lang.management.BufferPoolMXBean;
import java.lang.management.ClassLoadingMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class JvmStats implements Streamable, ToXContent {

    private final static RuntimeMXBean runtimeMXBean;
    private final static MemoryMXBean memoryMXBean;
    private final static ThreadMXBean threadMXBean;
    private final static ClassLoadingMXBean classLoadingMXBean;

    static {
        runtimeMXBean = ManagementFactory.getRuntimeMXBean();
        memoryMXBean = ManagementFactory.getMemoryMXBean();
        threadMXBean = ManagementFactory.getThreadMXBean();
        classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
    }

    public static JvmStats jvmStats() {
        JvmStats stats = new JvmStats(System.currentTimeMillis(), runtimeMXBean.getUptime());
        stats.mem = new Mem();
        MemoryUsage memUsage = memoryMXBean.getHeapMemoryUsage();
        stats.mem.heapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.heapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();
        stats.mem.heapMax = memUsage.getMax() < 0 ? 0 : memUsage.getMax();
        memUsage = memoryMXBean.getNonHeapMemoryUsage();
        stats.mem.nonHeapUsed = memUsage.getUsed() < 0 ? 0 : memUsage.getUsed();
        stats.mem.nonHeapCommitted = memUsage.getCommitted() < 0 ? 0 : memUsage.getCommitted();

        List<MemoryPoolMXBean> memoryPoolMXBeans = ManagementFactory.getMemoryPoolMXBeans();
        List<MemoryPool> pools = new ArrayList<>();
        for (int i = 0; i < memoryPoolMXBeans.size(); i++) {
            try {
                MemoryPoolMXBean memoryPoolMXBean = memoryPoolMXBeans.get(i);
                MemoryUsage usage = memoryPoolMXBean.getUsage();
                MemoryUsage peakUsage = memoryPoolMXBean.getPeakUsage();
                String name = GcNames.getByMemoryPoolName(memoryPoolMXBean.getName(), null);
                if (name == null) { // if we can't resolve it, its not interesting.... (Per Gen, Code Cache)
                    continue;
                }
                pools.add(new MemoryPool(name,
                        usage.getUsed() < 0 ? 0 : usage.getUsed(),
                        usage.getMax() < 0 ? 0 : usage.getMax(),
                        peakUsage.getUsed() < 0 ? 0 : peakUsage.getUsed(),
                        peakUsage.getMax() < 0 ? 0 : peakUsage.getMax()
                ));
            } catch (OutOfMemoryError err) {
                throw err; // rethrow
            } catch (Throwable ex) {
                /* ignore some JVMs might barf here with:
                 * java.lang.InternalError: Memory Pool not found
                 * we just omit the pool in that case!*/
            }
        }
        stats.mem.pools = pools.toArray(new MemoryPool[pools.size()]);

        stats.threads = new Threads();
        stats.threads.count = threadMXBean.getThreadCount();
        stats.threads.peakCount = threadMXBean.getPeakThreadCount();

        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        stats.gc = new GarbageCollectors();
        stats.gc.collectors = new GarbageCollector[gcMxBeans.size()];
        for (int i = 0; i < stats.gc.collectors.length; i++) {
            GarbageCollectorMXBean gcMxBean = gcMxBeans.get(i);
            stats.gc.collectors[i] = new GarbageCollector();
            stats.gc.collectors[i].name = GcNames.getByGcName(gcMxBean.getName(), gcMxBean.getName());
            stats.gc.collectors[i].collectionCount = gcMxBean.getCollectionCount();
            stats.gc.collectors[i].collectionTime = gcMxBean.getCollectionTime();
        }

        try {
            List<BufferPoolMXBean> bufferPools = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class);
            stats.bufferPools = new ArrayList<>(bufferPools.size());
            for (BufferPoolMXBean bufferPool : bufferPools) {
                stats.bufferPools.add(new BufferPool(bufferPool.getName(), bufferPool.getCount(), bufferPool.getTotalCapacity(), bufferPool.getMemoryUsed()));
            }
        } catch (Throwable t) {
            // buffer pools are not available
        }

        stats.classes = new Classes();
        stats.classes.loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        stats.classes.totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        stats.classes.unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

        return stats;
    }

    long timestamp = -1;
    long uptime;
    Mem mem;
    Threads threads;
    GarbageCollectors gc;
    List<BufferPool> bufferPools;
    Classes classes;

    private JvmStats() {
    }

    public JvmStats(long timestamp, long uptime) {
        this.timestamp = timestamp;
        this.uptime = uptime;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public TimeValue getUptime() {
        return new TimeValue(uptime);
    }

    public Mem getMem() {
        return this.mem;
    }

    public Threads getThreads() {
        return threads;
    }

    public GarbageCollectors getGc() {
        return gc;
    }

    public List<BufferPool> getBufferPools() {
        return bufferPools;
    }

    public Classes getClasses() {
        return classes;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.JVM);
        builder.field(Fields.TIMESTAMP, timestamp);
        builder.timeValueField(Fields.UPTIME_IN_MILLIS, Fields.UPTIME, uptime);
        if (mem != null) {
            builder.startObject(Fields.MEM);

            builder.byteSizeField(Fields.HEAP_USED_IN_BYTES, Fields.HEAP_USED, mem.heapUsed);
            if (mem.getHeapUsedPercent() >= 0) {
                builder.field(Fields.HEAP_USED_PERCENT, mem.getHeapUsedPercent());
            }
            builder.byteSizeField(Fields.HEAP_COMMITTED_IN_BYTES, Fields.HEAP_COMMITTED, mem.heapCommitted);
            builder.byteSizeField(Fields.HEAP_MAX_IN_BYTES, Fields.HEAP_MAX, mem.heapMax);
            builder.byteSizeField(Fields.NON_HEAP_USED_IN_BYTES, Fields.NON_HEAP_USED, mem.nonHeapUsed);
            builder.byteSizeField(Fields.NON_HEAP_COMMITTED_IN_BYTES, Fields.NON_HEAP_COMMITTED, mem.nonHeapCommitted);

            builder.startObject(Fields.POOLS);
            for (MemoryPool pool : mem) {
                builder.startObject(pool.getName());
                builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, pool.used);
                builder.byteSizeField(Fields.MAX_IN_BYTES, Fields.MAX, pool.max);

                builder.byteSizeField(Fields.PEAK_USED_IN_BYTES, Fields.PEAK_USED, pool.peakUsed);
                builder.byteSizeField(Fields.PEAK_MAX_IN_BYTES, Fields.PEAK_MAX, pool.peakMax);

                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }
        if (threads != null) {
            builder.startObject(Fields.THREADS);
            builder.field(Fields.COUNT, threads.getCount());
            builder.field(Fields.PEAK_COUNT, threads.getPeakCount());
            builder.endObject();
        }
        if (gc != null) {
            builder.startObject(Fields.GC);

            builder.startObject(Fields.COLLECTORS);
            for (GarbageCollector collector : gc) {
                builder.startObject(collector.getName());
                builder.field(Fields.COLLECTION_COUNT, collector.getCollectionCount());
                builder.timeValueField(Fields.COLLECTION_TIME_IN_MILLIS, Fields.COLLECTION_TIME, collector.collectionTime);
                builder.endObject();
            }
            builder.endObject();

            builder.endObject();
        }

        if (bufferPools != null) {
            builder.startObject(Fields.BUFFER_POOLS);
            for (BufferPool bufferPool : bufferPools) {
                builder.startObject(bufferPool.getName());
                builder.field(Fields.COUNT, bufferPool.getCount());
                builder.byteSizeField(Fields.USED_IN_BYTES, Fields.USED, bufferPool.used);
                builder.byteSizeField(Fields.TOTAL_CAPACITY_IN_BYTES, Fields.TOTAL_CAPACITY, bufferPool.totalCapacity);
                builder.endObject();
            }
            builder.endObject();
        }

        if (classes != null) {
            builder.startObject(Fields.CLASSES);
            builder.field(Fields.CURRENT_LOADED_COUNT, classes.getLoadedClassCount());
            builder.field(Fields.TOTAL_LOADED_COUNT, classes.getTotalLoadedClassCount());
            builder.field(Fields.TOTAL_UNLOADED_COUNT, classes.getUnloadedClassCount());
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }

    static final class Fields {
        static final String JVM = "jvm";
        static final String TIMESTAMP = "timestamp";
        static final String UPTIME = "uptime";
        static final String UPTIME_IN_MILLIS = "uptime_in_millis";

        static final String MEM = "mem";
        static final String HEAP_USED = "heap_used";
        static final String HEAP_USED_IN_BYTES = "heap_used_in_bytes";
        static final String HEAP_USED_PERCENT = "heap_used_percent";
        static final String HEAP_MAX = "heap_max";
        static final String HEAP_MAX_IN_BYTES = "heap_max_in_bytes";
        static final String HEAP_COMMITTED = "heap_committed";
        static final String HEAP_COMMITTED_IN_BYTES = "heap_committed_in_bytes";

        static final String NON_HEAP_USED = "non_heap_used";
        static final String NON_HEAP_USED_IN_BYTES = "non_heap_used_in_bytes";
        static final String NON_HEAP_COMMITTED = "non_heap_committed";
        static final String NON_HEAP_COMMITTED_IN_BYTES = "non_heap_committed_in_bytes";

        static final String POOLS = "pools";
        static final String USED = "used";
        static final String USED_IN_BYTES = "used_in_bytes";
        static final String MAX = "max";
        static final String MAX_IN_BYTES = "max_in_bytes";
        static final String PEAK_USED = "peak_used";
        static final String PEAK_USED_IN_BYTES = "peak_used_in_bytes";
        static final String PEAK_MAX = "peak_max";
        static final String PEAK_MAX_IN_BYTES = "peak_max_in_bytes";

        static final String THREADS = "threads";
        static final String COUNT = "count";
        static final String PEAK_COUNT = "peak_count";

        static final String GC = "gc";
        static final String COLLECTORS = "collectors";
        static final String COLLECTION_COUNT = "collection_count";
        static final String COLLECTION_TIME = "collection_time";
        static final String COLLECTION_TIME_IN_MILLIS = "collection_time_in_millis";

        static final String BUFFER_POOLS = "buffer_pools";
        static final String NAME = "name";
        static final String TOTAL_CAPACITY = "total_capacity";
        static final String TOTAL_CAPACITY_IN_BYTES = "total_capacity_in_bytes";

        static final String CLASSES = "classes";
        static final String CURRENT_LOADED_COUNT = "current_loaded_count";
        static final String TOTAL_LOADED_COUNT = "total_loaded_count";
        static final String TOTAL_UNLOADED_COUNT = "total_unloaded_count";
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
            bufferPools = new ArrayList<>(size);
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

    public static class GarbageCollectors implements Streamable, Iterable<GarbageCollector> {

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

        public GarbageCollector[] getCollectors() {
            return this.collectors;
        }

        @Override
        public Iterator<GarbageCollector> iterator() {
            return Arrays.stream(collectors).iterator();
        }
    }

    public static class GarbageCollector implements Streamable {

        String name;
        long collectionCount;
        long collectionTime;

        GarbageCollector() {
        }

        public static GarbageCollector readGarbageCollector(StreamInput in) throws IOException {
            GarbageCollector gc = new GarbageCollector();
            gc.readFrom(in);
            return gc;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            collectionCount = in.readVLong();
            collectionTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(collectionCount);
            out.writeVLong(collectionTime);
        }

        public String getName() {
            return this.name;
        }

        public long getCollectionCount() {
            return this.collectionCount;
        }

        public TimeValue getCollectionTime() {
            return new TimeValue(collectionTime, TimeUnit.MILLISECONDS);
        }
    }

    public static class Threads implements Streamable {

        int count;
        int peakCount;

        Threads() {
        }

        public int getCount() {
            return count;
        }

        public int getPeakCount() {
            return peakCount;
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

    public static class MemoryPool implements Streamable {

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

        public String getName() {
            return this.name;
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
        }

        public ByteSizeValue getMax() {
            return new ByteSizeValue(max);
        }

        public ByteSizeValue getPeakUsed() {
            return new ByteSizeValue(peakUsed);
        }

        public ByteSizeValue getPeakMax() {
            return new ByteSizeValue(peakMax);
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            used = in.readVLong();
            max = in.readVLong();
            peakUsed = in.readVLong();
            peakMax = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeVLong(used);
            out.writeVLong(max);
            out.writeVLong(peakUsed);
            out.writeVLong(peakMax);
        }
    }

    public static class Mem implements Streamable, Iterable<MemoryPool> {

        long heapCommitted;
        long heapUsed;
        long heapMax;
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
            return Arrays.stream(pools).iterator();
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            heapCommitted = in.readVLong();
            heapUsed = in.readVLong();
            nonHeapCommitted = in.readVLong();
            nonHeapUsed = in.readVLong();
            heapMax = in.readVLong();
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
            out.writeVLong(heapMax);
            out.writeVInt(pools.length);
            for (MemoryPool pool : pools) {
                pool.writeTo(out);
            }
        }

        public ByteSizeValue getHeapCommitted() {
            return new ByteSizeValue(heapCommitted);
        }

        public ByteSizeValue getHeapUsed() {
            return new ByteSizeValue(heapUsed);
        }

        /**
         * returns the maximum heap size. 0 bytes signals unknown.
         */
        public ByteSizeValue getHeapMax() {
            return new ByteSizeValue(heapMax);
        }

        /**
         * returns the heap usage in percent. -1 signals unknown.
         */
        public short getHeapUsedPercent() {
            if (heapMax == 0) {
                return -1;
            }
            return (short) (heapUsed * 100 / heapMax);
        }

        public ByteSizeValue getNonHeapCommitted() {
            return new ByteSizeValue(nonHeapCommitted);
        }

        public ByteSizeValue getNonHeapUsed() {
            return new ByteSizeValue(nonHeapUsed);
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

        public String getName() {
            return this.name;
        }

        public long getCount() {
            return this.count;
        }

        public ByteSizeValue getTotalCapacity() {
            return new ByteSizeValue(totalCapacity);
        }

        public ByteSizeValue getUsed() {
            return new ByteSizeValue(used);
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

    public static class Classes implements Streamable {

        long loadedClassCount;
        long totalLoadedClassCount;
        long unloadedClassCount;

        Classes() {
        }

        public Classes(long loadedClassCount, long totalLoadedClassCount, long unloadedClassCount) {
            this.loadedClassCount = loadedClassCount;
            this.totalLoadedClassCount = totalLoadedClassCount;
            this.unloadedClassCount = unloadedClassCount;
        }

        public long getLoadedClassCount() {
            return loadedClassCount;
        }

        public long getTotalLoadedClassCount() {
            return totalLoadedClassCount;
        }

        public long getUnloadedClassCount() {
            return unloadedClassCount;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            loadedClassCount = in.readLong();
            totalLoadedClassCount = in.readLong();
            unloadedClassCount = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeLong(loadedClassCount);
            out.writeLong(totalLoadedClassCount);
            out.writeLong(unloadedClassCount);
        }
    }
}
