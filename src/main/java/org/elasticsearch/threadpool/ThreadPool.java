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

package org.elasticsearch.threadpool;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import jsr166y.LinkedTransferQueue;
import org.elasticsearch.ElasticSearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.FileSystemUtils;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;
import static org.elasticsearch.common.unit.TimeValue.timeValueSeconds;

/**
 *
 */
public class ThreadPool extends AbstractComponent {

    public static class Names {
        public static final String SAME = "same";
        public static final String CACHED = "cached";
        public static final String INDEX = "index";
        public static final String BULK = "bulk";
        public static final String SEARCH = "search";
        public static final String PERCOLATE = "percolate";
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String MERGE = "merge";
        public static final String REFRESH = "refresh";
        public static final String SNAPSHOT = "snapshot";
    }

    private final ImmutableMap<String, ExecutorHolder> executors;

    private final ScheduledThreadPoolExecutor scheduler;

    private final EstimatedTimeThread estimatedTimeThread;

    public ThreadPool() {
        this(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    @Inject
    public ThreadPool(Settings settings) {
        super(settings);

        Map<String, Settings> groupSettings = settings.getGroups("threadpool");

        Map<String, ExecutorHolder> executors = Maps.newHashMap();
        executors.put(Names.CACHED, build(Names.CACHED, "cached", groupSettings.get(Names.CACHED), settingsBuilder().put("keep_alive", "30s").build()));
        executors.put(Names.INDEX, build(Names.INDEX, "cached", groupSettings.get(Names.INDEX), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.BULK, build(Names.BULK, "cached", groupSettings.get(Names.BULK), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.SEARCH, build(Names.SEARCH, "cached", groupSettings.get(Names.SEARCH), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.PERCOLATE, build(Names.PERCOLATE, "cached", groupSettings.get(Names.PERCOLATE), ImmutableSettings.Builder.EMPTY_SETTINGS));
        executors.put(Names.MANAGEMENT, build(Names.MANAGEMENT, "scaling", groupSettings.get(Names.MANAGEMENT), settingsBuilder().put("keep_alive", "5m").put("size", 5).build()));
        executors.put(Names.FLUSH, build(Names.FLUSH, "scaling", groupSettings.get(Names.FLUSH), settingsBuilder().put("keep_alive", "5m").put("size", 10).build()));
        executors.put(Names.MERGE, build(Names.MERGE, "scaling", groupSettings.get(Names.MERGE), settingsBuilder().put("keep_alive", "5m").put("size", 20).build()));
        executors.put(Names.REFRESH, build(Names.REFRESH, "cached", groupSettings.get(Names.REFRESH), settingsBuilder().put("keep_alive", "1m").build()));
        executors.put(Names.SNAPSHOT, build(Names.SNAPSHOT, "scaling", groupSettings.get(Names.SNAPSHOT), settingsBuilder().put("keep_alive", "5m").put("size", 5).build()));
        executors.put(Names.SAME, new ExecutorHolder(MoreExecutors.sameThreadExecutor(), new Info(Names.SAME, "same")));
        this.executors = ImmutableMap.copyOf(executors);
        this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory(settings, "[scheduler]"));
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);

        TimeValue estimatedTimeInterval = componentSettings.getAsTime("estimated_time_interval", TimeValue.timeValueMillis(200));
        this.estimatedTimeThread = new EstimatedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.estimatedTimeThread.start();
    }

    public long estimatedTimeInMillis() {
        return estimatedTimeThread.estimatedTimeInMillis();
    }

    public ThreadPoolInfo info() {
        List<Info> infos = new ArrayList<Info>();
        for (ExecutorHolder holder : executors.values()) {
            String name = holder.info.name();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            infos.add(holder.info);
        }
        return new ThreadPoolInfo(infos);
    }

    public ThreadPoolStats stats() {
        List<ThreadPoolStats.Stats> stats = new ArrayList<ThreadPoolStats.Stats>();
        for (ExecutorHolder holder : executors.values()) {
            String name = holder.info.name();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            int threads = -1;
            int queue = -1;
            int active = -1;
            if (holder.executor instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) holder.executor;
                threads = threadPoolExecutor.getPoolSize();
                queue = threadPoolExecutor.getQueue().size();
                active = threadPoolExecutor.getActiveCount();
            }
            stats.add(new ThreadPoolStats.Stats(name, threads, queue, active));
        }
        return new ThreadPoolStats(stats);
    }

    public Executor cached() {
        return executor(Names.CACHED);
    }

    public Executor executor(String name) {
        Executor executor = executors.get(name).executor;
        if (executor == null) {
            throw new ElasticSearchIllegalArgumentException("No executor found for [" + name + "]");
        }
        return executor;
    }

    public ScheduledExecutorService scheduler() {
        return this.scheduler;
    }

    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, TimeValue interval) {
        return scheduler.scheduleWithFixedDelay(new LoggingRunnable(command), interval.millis(), interval.millis(), TimeUnit.MILLISECONDS);
    }

    public ScheduledFuture<?> schedule(TimeValue delay, String name, Runnable command) {
        if (!Names.SAME.equals(name)) {
            command = new ThreadedRunnable(command, executor(name));
        }
        return scheduler.schedule(command, delay.millis(), TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        estimatedTimeThread.running = false;
        estimatedTimeThread.interrupt();
        scheduler.shutdown();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor.executor).shutdown();
            }
        }
    }

    public void shutdownNow() {
        estimatedTimeThread.running = false;
        estimatedTimeThread.interrupt();
        scheduler.shutdownNow();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor.executor).shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor instanceof ThreadPoolExecutor) {
                result &= ((ThreadPoolExecutor) executor.executor).awaitTermination(timeout, unit);
            }
        }
        return result;
    }

    private ExecutorHolder build(String name, String defaultType, @Nullable Settings settings, Settings defaultSettings) {
        if (settings == null) {
            settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        }
        String type = settings.get("type", defaultType);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(settings, "[" + name + "]");
        if ("same".equals(type)) {
            logger.debug("creating thread_pool [{}], type [{}]", name, type);
            return new ExecutorHolder(MoreExecutors.sameThreadExecutor(), new Info(name, type));
        } else if ("cached".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            logger.debug("creating thread_pool [{}], type [{}], keep_alive [{}]", name, type, keepAlive);
            Executor executor = new EsThreadPoolExecutor(0, Integer.MAX_VALUE,
                    keepAlive.millis(), TimeUnit.MILLISECONDS,
                    new SynchronousQueue<Runnable>(),
                    threadFactory);
            return new ExecutorHolder(executor, new Info(name, type, -1, -1, keepAlive, null));
        } else if ("fixed".equals(type)) {
            int size = settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5));
            SizeValue capacity = settings.getAsSize("capacity", settings.getAsSize("queue_size", defaultSettings.getAsSize("queue_size", null)));
            RejectedExecutionHandler rejectedExecutionHandler;
            String rejectSetting = settings.get("reject_policy", defaultSettings.get("reject_policy", "abort"));
            if ("abort".equals(rejectSetting)) {
                rejectedExecutionHandler = new AbortPolicy();
            } else if ("caller".equals(rejectSetting)) {
                rejectedExecutionHandler = new ThreadPoolExecutor.CallerRunsPolicy();
            } else {
                throw new ElasticSearchIllegalArgumentException("reject_policy [" + rejectSetting + "] not valid for [" + name + "] thread pool");
            }
            logger.debug("creating thread_pool [{}], type [{}], size [{}], queue_size [{}], reject_policy [{}]", name, type, size, capacity, rejectSetting);
            Executor executor = new EsThreadPoolExecutor(size, size,
                    0L, TimeUnit.MILLISECONDS,
                    capacity == null ? new LinkedTransferQueue<Runnable>() : new ArrayBlockingQueue<Runnable>((int) capacity.singles()),
                    threadFactory, rejectedExecutionHandler);
            return new ExecutorHolder(executor, new Info(name, type, size, size, null, capacity));
        } else if ("scaling".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            int min = settings.getAsInt("min", defaultSettings.getAsInt("min", 1));
            int size = settings.getAsInt("max", settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5)));
            logger.debug("creating thread_pool [{}], type [{}], min [{}], size [{}], keep_alive [{}]", name, type, min, size, keepAlive);
            Executor executor = EsExecutors.newScalingExecutorService(min, size, keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory);
            return new ExecutorHolder(executor, new Info(name, type, min, size, keepAlive, null));
        } else if ("blocking".equals(type)) {
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultSettings.getAsTime("keep_alive", timeValueMinutes(5)));
            int min = settings.getAsInt("min", defaultSettings.getAsInt("min", 1));
            int size = settings.getAsInt("max", settings.getAsInt("size", defaultSettings.getAsInt("size", Runtime.getRuntime().availableProcessors() * 5)));
            SizeValue capacity = settings.getAsSize("capacity", settings.getAsSize("queue_size", defaultSettings.getAsSize("queue_size", new SizeValue(1000))));
            TimeValue waitTime = settings.getAsTime("wait_time", defaultSettings.getAsTime("wait_time", timeValueSeconds(60)));
            logger.debug("creating thread_pool [{}], type [{}], min [{}], size [{}], queue_size [{}], keep_alive [{}], wait_time [{}]", name, type, min, size, capacity.singles(), keepAlive, waitTime);
            Executor executor = EsExecutors.newBlockingExecutorService(min, size, keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory, (int) capacity.singles(), waitTime.millis(), TimeUnit.MILLISECONDS);
            return new ExecutorHolder(executor, new Info(name, type, min, size, keepAlive, capacity));
        }
        throw new ElasticSearchIllegalArgumentException("No type found [" + type + "], for [" + name + "]");
    }

    class LoggingRunnable implements Runnable {

        private final Runnable runnable;

        LoggingRunnable(Runnable runnable) {
            this.runnable = runnable;
        }

        @Override
        public void run() {
            try {
                runnable.run();
            } catch (Exception e) {
                logger.warn("failed to run {}", e, runnable.toString());
            }
        }

        @Override
        public int hashCode() {
            return runnable.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override
        public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    class ThreadedRunnable implements Runnable {

        private final Runnable runnable;

        private final Executor executor;

        ThreadedRunnable(Runnable runnable, Executor executor) {
            this.runnable = runnable;
            this.executor = executor;
        }

        @Override
        public void run() {
            executor.execute(runnable);
        }

        @Override
        public int hashCode() {
            return runnable.hashCode();
        }

        @Override
        public boolean equals(Object obj) {
            return runnable.equals(obj);
        }

        @Override
        public String toString() {
            return "[threaded] " + runnable.toString();
        }
    }

    static class EstimatedTimeThread extends Thread {

        final long interval;

        volatile boolean running = true;

        volatile long estimatedTimeInMillis;

        EstimatedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            setDaemon(true);
        }

        public long estimatedTimeInMillis() {
            return this.estimatedTimeInMillis;
        }

        @Override
        public void run() {
            while (running) {
                estimatedTimeInMillis = System.currentTimeMillis();
                try {
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }
                try {
                    FileSystemUtils.checkMkdirsStall(estimatedTimeInMillis);
                } catch (Exception e) {
                    // ignore
                }
            }
        }
    }

    /**
     * A handler for rejected tasks that throws a
     * <tt>RejectedExecutionException</tt>.
     */
    public static class AbortPolicy implements RejectedExecutionHandler {
        /**
         * Creates an <tt>AbortPolicy</tt>.
         */
        public AbortPolicy() {
        }

        /**
         * Always throws RejectedExecutionException.
         *
         * @param r the runnable task requested to be executed
         * @param e the executor attempting to execute this task
         * @throws RejectedExecutionException always.
         */
        public void rejectedExecution(Runnable r, ThreadPoolExecutor e) {
            throw new ThreadPoolRejectedException();
        }
    }

    static class ExecutorHolder {
        public final Executor executor;
        public final Info info;

        ExecutorHolder(Executor executor, Info info) {
            this.executor = executor;
            this.info = info;
        }
    }

    public static class Info implements Streamable, ToXContent {

        private String name;
        private String type;
        private int min;
        private int max;
        private TimeValue keepAlive;
        private SizeValue capacity;

        Info() {

        }

        public Info(String name, String type) {
            this(name, type, -1);
        }

        public Info(String name, String type, int size) {
            this(name, type, size, size, null, null);
        }

        public Info(String name, String type, int min, int max, @Nullable TimeValue keepAlive, @Nullable SizeValue capacity) {
            this.name = name;
            this.type = type;
            this.min = min;
            this.max = max;
            this.keepAlive = keepAlive;
            this.capacity = capacity;
        }

        public String name() {
            return this.name;
        }

        public String getName() {
            return this.name;
        }

        public String type() {
            return this.type;
        }

        public String getType() {
            return this.type;
        }

        public int min() {
            return this.min;
        }

        public int getMin() {
            return this.min;
        }

        public int max() {
            return this.max;
        }

        public int getMax() {
            return this.max;
        }

        @Nullable
        public TimeValue keepAlive() {
            return this.keepAlive;
        }

        @Nullable
        public TimeValue getKeepAlive() {
            return this.keepAlive;
        }

        @Nullable
        public SizeValue capacity() {
            return this.capacity;
        }

        @Nullable
        public SizeValue getCapacity() {
            return this.capacity;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readUTF();
            type = in.readUTF();
            min = in.readInt();
            max = in.readInt();
            if (in.readBoolean()) {
                keepAlive = TimeValue.readTimeValue(in);
            }
            if (in.readBoolean()) {
                capacity = SizeValue.readSizeValue(in);
            }
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeUTF(name);
            out.writeUTF(type);
            out.writeInt(min);
            out.writeInt(max);
            if (keepAlive == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                keepAlive.writeTo(out);
            }
            if (capacity == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                capacity.writeTo(out);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name, XContentBuilder.FieldCaseConversion.NONE);
            builder.field(Fields.TYPE, type);
            if (min != -1) {
                builder.field(Fields.MIN, min);
            }
            if (max != -1) {
                builder.field(Fields.MAX, max);
            }
            if (keepAlive != null) {
                builder.field(Fields.KEEP_ALIVE, keepAlive.toString());
            }
            if (capacity != null) {
                builder.field(Fields.CAPACITY, capacity.toString());
            }
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString TYPE = new XContentBuilderString("type");
            static final XContentBuilderString MIN = new XContentBuilderString("min");
            static final XContentBuilderString MAX = new XContentBuilderString("max");
            static final XContentBuilderString KEEP_ALIVE = new XContentBuilderString("keep_alive");
            static final XContentBuilderString CAPACITY = new XContentBuilderString("capacity");
        }

    }
}
