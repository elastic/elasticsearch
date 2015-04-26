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

package org.elasticsearch.threadpool;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.lucene.util.Counter;
import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsAbortPolicy;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.node.settings.NodeSettingsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.unit.SizeValue.parseSizeValue;
import static org.elasticsearch.common.unit.TimeValue.timeValueMinutes;

/**
 *
 */
public class ThreadPool extends AbstractComponent {

    public static class Names {
        public static final String SAME = "same";
        public static final String GENERIC = "generic";
        public static final String LISTENER = "listener";
        public static final String GET = "get";
        public static final String INDEX = "index";
        public static final String BULK = "bulk";
        public static final String SEARCH = "search";
        public static final String SUGGEST = "suggest";
        public static final String PERCOLATE = "percolate";
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String REFRESH = "refresh";
        public static final String WARMER = "warmer";
        public static final String SNAPSHOT = "snapshot";
        public static final String OPTIMIZE = "optimize";
    }

    public static final String THREADPOOL_GROUP = "threadpool.";

    private volatile ImmutableMap<String, ExecutorHolder> executors;

    private final ImmutableMap<String, Settings> defaultExecutorTypeSettings;

    private final Queue<ExecutorHolder> retiredExecutors = new ConcurrentLinkedQueue<>();

    private final ScheduledThreadPoolExecutor scheduler;

    private final EstimatedTimeThread estimatedTimeThread;


    public ThreadPool(String name) {
        this(ImmutableSettings.builder().put("name", name).build(), null);
    }

    @Inject
    public ThreadPool(Settings settings, @Nullable NodeSettingsService nodeSettingsService) {
        super(settings);

        assert settings.get("name") != null : "ThreadPool's settings should contain a name";

        Map<String, Settings> groupSettings = settings.getGroups(THREADPOOL_GROUP);

        int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
        int halfProcMaxAt5 = Math.min(((availableProcessors + 1) / 2), 5);
        int halfProcMaxAt10 = Math.min(((availableProcessors + 1) / 2), 10);
        defaultExecutorTypeSettings = ImmutableMap.<String, Settings>builder()
                .put(Names.GENERIC, settingsBuilder().put("type", "cached").put("keep_alive", "30s").build())
                .put(Names.INDEX, settingsBuilder().put("type", "fixed").put("size", availableProcessors).put("queue_size", 200).build())
                .put(Names.BULK, settingsBuilder().put("type", "fixed").put("size", availableProcessors).put("queue_size", 50).build())
                .put(Names.GET, settingsBuilder().put("type", "fixed").put("size", availableProcessors).put("queue_size", 1000).build())
                .put(Names.SEARCH, settingsBuilder().put("type", "fixed").put("size", ((availableProcessors * 3) / 2) + 1).put("queue_size", 1000).build())
                .put(Names.SUGGEST, settingsBuilder().put("type", "fixed").put("size", availableProcessors).put("queue_size", 1000).build())
                .put(Names.PERCOLATE, settingsBuilder().put("type", "fixed").put("size", availableProcessors).put("queue_size", 1000).build())
                .put(Names.MANAGEMENT, settingsBuilder().put("type", "scaling").put("keep_alive", "5m").put("size", 5).build())
                // no queue as this means clients will need to handle rejections on listener queue even if the operation succeeded
                // the assumption here is that the listeners should be very lightweight on the listeners side
                .put(Names.LISTENER, settingsBuilder().put("type", "fixed").put("size", halfProcMaxAt10).build())
                .put(Names.FLUSH, settingsBuilder().put("type", "scaling").put("keep_alive", "5m").put("size", halfProcMaxAt5).build())
                .put(Names.REFRESH, settingsBuilder().put("type", "scaling").put("keep_alive", "5m").put("size", halfProcMaxAt10).build())
                .put(Names.WARMER, settingsBuilder().put("type", "scaling").put("keep_alive", "5m").put("size", halfProcMaxAt5).build())
                .put(Names.SNAPSHOT, settingsBuilder().put("type", "scaling").put("keep_alive", "5m").put("size", halfProcMaxAt5).build())
                .put(Names.OPTIMIZE, settingsBuilder().put("type", "fixed").put("size", 1).build())
                .build();

        Map<String, ExecutorHolder> executors = Maps.newHashMap();
        for (Map.Entry<String, Settings> executor : defaultExecutorTypeSettings.entrySet()) {
            executors.put(executor.getKey(), build(executor.getKey(), groupSettings.get(executor.getKey()), executor.getValue()));
        }

        // Building custom thread pools
        for (Map.Entry<String, Settings> entry : groupSettings.entrySet()) {
            if (executors.containsKey(entry.getKey())) {
                continue;
            }
            executors.put(entry.getKey(), build(entry.getKey(), entry.getValue(), ImmutableSettings.EMPTY));
        }

        executors.put(Names.SAME, new ExecutorHolder(MoreExecutors.directExecutor(), new Info(Names.SAME, "same")));
        if (!executors.get(Names.GENERIC).info.getType().equals("cached")) {
            throw new ElasticsearchIllegalArgumentException("generic thread pool must be of type cached");
        }
        this.executors = ImmutableMap.copyOf(executors);
        this.scheduler = new ScheduledThreadPoolExecutor(1, EsExecutors.daemonThreadFactory(settings, "scheduler"), new EsAbortPolicy());
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduler.setRemoveOnCancelPolicy(true);
        if (nodeSettingsService != null) {
            nodeSettingsService.addListener(new ApplySettings());
        }

        TimeValue estimatedTimeInterval = settings.getAsTime("threadpool.estimated_time_interval", TimeValue.timeValueMillis(200));
        this.estimatedTimeThread = new EstimatedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.estimatedTimeThread.start();
    }

    public long estimatedTimeInMillis() {
        return estimatedTimeThread.estimatedTimeInMillis();
    }

    public Counter estimatedTimeInMillisCounter() {
        return estimatedTimeThread.counter;
    }

    public ThreadPoolInfo info() {
        List<Info> infos = new ArrayList<>();
        for (ExecutorHolder holder : executors.values()) {
            String name = holder.info.getName();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            infos.add(holder.info);
        }
        return new ThreadPoolInfo(infos);
    }

    public Info info(String name) {
        ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            return null;
        }
        return holder.info;
    }

    public ThreadPoolStats stats() {
        List<ThreadPoolStats.Stats> stats = new ArrayList<>();
        for (ExecutorHolder holder : executors.values()) {
            String name = holder.info.getName();
            // no need to have info on "same" thread pool
            if ("same".equals(name)) {
                continue;
            }
            int threads = -1;
            int queue = -1;
            int active = -1;
            long rejected = -1;
            int largest = -1;
            long completed = -1;
            if (holder.executor() instanceof ThreadPoolExecutor) {
                ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) holder.executor();
                threads = threadPoolExecutor.getPoolSize();
                queue = threadPoolExecutor.getQueue().size();
                active = threadPoolExecutor.getActiveCount();
                largest = threadPoolExecutor.getLargestPoolSize();
                completed = threadPoolExecutor.getCompletedTaskCount();
                RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
                if (rejectedExecutionHandler instanceof XRejectedExecutionHandler) {
                    rejected = ((XRejectedExecutionHandler) rejectedExecutionHandler).rejected();
                }
            }
            stats.add(new ThreadPoolStats.Stats(name, threads, queue, active, rejected, largest, completed));
        }
        return new ThreadPoolStats(stats);
    }

    public Executor generic() {
        return executor(Names.GENERIC);
    }

    public Executor executor(String name) {
        Executor executor = executors.get(name).executor();
        if (executor == null) {
            throw new ElasticsearchIllegalArgumentException("No executor found for [" + name + "]");
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
            if (executor.executor() instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor.executor()).shutdown();
            }
        }
    }

    public void shutdownNow() {
        estimatedTimeThread.running = false;
        estimatedTimeThread.interrupt();
        scheduler.shutdownNow();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                ((ThreadPoolExecutor) executor.executor()).shutdownNow();
            }
        }
        while (!retiredExecutors.isEmpty()) {
            ((ThreadPoolExecutor) retiredExecutors.remove().executor()).shutdownNow();
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                result &= ((ThreadPoolExecutor) executor.executor()).awaitTermination(timeout, unit);
            }
        }
        while (!retiredExecutors.isEmpty()) {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) retiredExecutors.remove().executor();
            result &= executor.awaitTermination(timeout, unit);
        }
        estimatedTimeThread.join(unit.toMillis(timeout));
        return result;
    }

    private ExecutorHolder build(String name, @Nullable Settings settings, Settings defaultSettings) {
        return rebuild(name, null, settings, defaultSettings);
    }

    private ExecutorHolder rebuild(String name, ExecutorHolder previousExecutorHolder, @Nullable Settings settings, Settings defaultSettings) {
        if (Names.SAME.equals(name)) {
            // Don't allow to change the "same" thread executor
            return previousExecutorHolder;
        }
        if (settings == null) {
            settings = ImmutableSettings.Builder.EMPTY_SETTINGS;
        }
        Info previousInfo = previousExecutorHolder != null ? previousExecutorHolder.info : null;
        String type = settings.get("type", previousInfo != null ? previousInfo.getType() : defaultSettings.get("type"));
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(this.settings, name);
        if ("same".equals(type)) {
            if (previousExecutorHolder != null) {
                logger.debug("updating thread_pool [{}], type [{}]", name, type);
            } else {
                logger.debug("creating thread_pool [{}], type [{}]", name, type);
            }
            return new ExecutorHolder(MoreExecutors.directExecutor(), new Info(name, type));
        } else if ("cached".equals(type)) {
            TimeValue defaultKeepAlive = defaultSettings.getAsTime("keep_alive", timeValueMinutes(5));
            if (previousExecutorHolder != null) {
                if ("cached".equals(previousInfo.getType())) {
                    TimeValue updatedKeepAlive = settings.getAsTime("keep_alive", previousInfo.getKeepAlive());
                    if (!previousInfo.getKeepAlive().equals(updatedKeepAlive)) {
                        logger.debug("updating thread_pool [{}], type [{}], keep_alive [{}]", name, type, updatedKeepAlive);
                        ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setKeepAliveTime(updatedKeepAlive.millis(), TimeUnit.MILLISECONDS);
                        return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, type, -1, -1, updatedKeepAlive, null));
                    }
                    return previousExecutorHolder;
                }
                if (previousInfo.getKeepAlive() != null) {
                    defaultKeepAlive = previousInfo.getKeepAlive();
                }
            }
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultKeepAlive);
            if (previousExecutorHolder != null) {
                logger.debug("updating thread_pool [{}], type [{}], keep_alive [{}]", name, type, keepAlive);
            } else {
                logger.debug("creating thread_pool [{}], type [{}], keep_alive [{}]", name, type, keepAlive);
            }
            Executor executor = EsExecutors.newCached(keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory);
            return new ExecutorHolder(executor, new Info(name, type, -1, -1, keepAlive, null));
        } else if ("fixed".equals(type)) {
            int defaultSize = defaultSettings.getAsInt("size", EsExecutors.boundedNumberOfProcessors(settings));
            SizeValue defaultQueueSize = getAsSizeOrUnbounded(defaultSettings, "queue", getAsSizeOrUnbounded(defaultSettings, "queue_size", null));

            if (previousExecutorHolder != null) {
                if ("fixed".equals(previousInfo.getType())) {
                    SizeValue updatedQueueSize = getAsSizeOrUnbounded(settings, "capacity", getAsSizeOrUnbounded(settings, "queue", getAsSizeOrUnbounded(settings, "queue_size", previousInfo.getQueueSize())));
                    if (Objects.equal(previousInfo.getQueueSize(), updatedQueueSize)) {
                        int updatedSize = settings.getAsInt("size", previousInfo.getMax());
                        if (previousInfo.getMax() != updatedSize) {
                            logger.debug("updating thread_pool [{}], type [{}], size [{}], queue_size [{}]", name, type, updatedSize, updatedQueueSize);
                            // if you think this code is crazy: that's because it is!
                            if (updatedSize > previousInfo.getMax()) {
                                ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setMaximumPoolSize(updatedSize);
                                ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setCorePoolSize(updatedSize);
                            } else {
                                ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setCorePoolSize(updatedSize);
                                ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setMaximumPoolSize(updatedSize);
                            }
                            return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, type, updatedSize, updatedSize, null, updatedQueueSize));
                        }
                        return previousExecutorHolder;
                    }
                }
                if (previousInfo.getMax() >= 0) {
                    defaultSize = previousInfo.getMax();
                }
                defaultQueueSize = previousInfo.getQueueSize();
            }

            int size = settings.getAsInt("size", defaultSize);
            SizeValue queueSize = getAsSizeOrUnbounded(settings, "capacity", getAsSizeOrUnbounded(settings, "queue", getAsSizeOrUnbounded(settings, "queue_size", defaultQueueSize)));
            logger.debug("creating thread_pool [{}], type [{}], size [{}], queue_size [{}]", name, type, size, queueSize);
            Executor executor = EsExecutors.newFixed(size, queueSize == null ? -1 : (int) queueSize.singles(), threadFactory);
            return new ExecutorHolder(executor, new Info(name, type, size, size, null, queueSize));
        } else if ("scaling".equals(type)) {
            TimeValue defaultKeepAlive = defaultSettings.getAsTime("keep_alive", timeValueMinutes(5));
            int defaultMin = defaultSettings.getAsInt("min", 1);
            int defaultSize = defaultSettings.getAsInt("size", EsExecutors.boundedNumberOfProcessors(settings));
            if (previousExecutorHolder != null) {
                if ("scaling".equals(previousInfo.getType())) {
                    TimeValue updatedKeepAlive = settings.getAsTime("keep_alive", previousInfo.getKeepAlive());
                    int updatedMin = settings.getAsInt("min", previousInfo.getMin());
                    int updatedSize = settings.getAsInt("max", settings.getAsInt("size", previousInfo.getMax()));
                    if (!previousInfo.getKeepAlive().equals(updatedKeepAlive) || previousInfo.getMin() != updatedMin || previousInfo.getMax() != updatedSize) {
                        logger.debug("updating thread_pool [{}], type [{}], keep_alive [{}]", name, type, updatedKeepAlive);
                        if (!previousInfo.getKeepAlive().equals(updatedKeepAlive)) {
                            ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setKeepAliveTime(updatedKeepAlive.millis(), TimeUnit.MILLISECONDS);
                        }
                        if (previousInfo.getMin() != updatedMin) {
                            ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setCorePoolSize(updatedMin);
                        }
                        if (previousInfo.getMax() != updatedSize) {
                            ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setMaximumPoolSize(updatedSize);
                        }
                        return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, type, updatedMin, updatedSize, updatedKeepAlive, null));
                    }
                    return previousExecutorHolder;
                }
                if (previousInfo.getKeepAlive() != null) {
                    defaultKeepAlive = previousInfo.getKeepAlive();
                }
                if (previousInfo.getMin() >= 0) {
                    defaultMin = previousInfo.getMin();
                }
                if (previousInfo.getMax() >= 0) {
                    defaultSize = previousInfo.getMax();
                }
            }
            TimeValue keepAlive = settings.getAsTime("keep_alive", defaultKeepAlive);
            int min = settings.getAsInt("min", defaultMin);
            int size = settings.getAsInt("max", settings.getAsInt("size", defaultSize));
            if (previousExecutorHolder != null) {
                logger.debug("updating thread_pool [{}], type [{}], min [{}], size [{}], keep_alive [{}]", name, type, min, size, keepAlive);
            } else {
                logger.debug("creating thread_pool [{}], type [{}], min [{}], size [{}], keep_alive [{}]", name, type, min, size, keepAlive);
            }
            Executor executor = EsExecutors.newScaling(min, size, keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory);
            return new ExecutorHolder(executor, new Info(name, type, min, size, keepAlive, null));
        }
        throw new ElasticsearchIllegalArgumentException("No type found [" + type + "], for [" + name + "]");
    }

    public void updateSettings(Settings settings) {
        Map<String, Settings> groupSettings = settings.getGroups("threadpool");
        if (groupSettings.isEmpty()) {
            return;
        }

        for (Map.Entry<String, Settings> executor : defaultExecutorTypeSettings.entrySet()) {
            Settings updatedSettings = groupSettings.get(executor.getKey());
            if (updatedSettings == null) {
                continue;
            }

            ExecutorHolder oldExecutorHolder = executors.get(executor.getKey());
            ExecutorHolder newExecutorHolder = rebuild(executor.getKey(), oldExecutorHolder, updatedSettings, executor.getValue());
            if (!oldExecutorHolder.equals(newExecutorHolder)) {
                executors = newMapBuilder(executors).put(executor.getKey(), newExecutorHolder).immutableMap();
                if (!oldExecutorHolder.executor().equals(newExecutorHolder.executor()) && oldExecutorHolder.executor() instanceof EsThreadPoolExecutor) {
                    retiredExecutors.add(oldExecutorHolder);
                    ((EsThreadPoolExecutor) oldExecutorHolder.executor()).shutdown(new ExecutorShutdownListener(oldExecutorHolder));
                }
            }
        }

        // Building custom thread pools
        for (Map.Entry<String, Settings> entry : groupSettings.entrySet()) {
            if (defaultExecutorTypeSettings.containsKey(entry.getKey())) {
                continue;
            }

            ExecutorHolder oldExecutorHolder = executors.get(entry.getKey());
            ExecutorHolder newExecutorHolder = rebuild(entry.getKey(), oldExecutorHolder, entry.getValue(), ImmutableSettings.EMPTY);
            // Can't introduce new thread pools at runtime, because The oldExecutorHolder variable will be null in the
            // case the settings contains a thread pool not defined in the initial settings in the constructor. The if
            // statement will then fail and so this prevents the addition of new thread groups at runtime, which is desired.
            if (!newExecutorHolder.equals(oldExecutorHolder)) {
                executors = newMapBuilder(executors).put(entry.getKey(), newExecutorHolder).immutableMap();
                if (!oldExecutorHolder.executor().equals(newExecutorHolder.executor()) && oldExecutorHolder.executor() instanceof EsThreadPoolExecutor) {
                    retiredExecutors.add(oldExecutorHolder);
                    ((EsThreadPoolExecutor) oldExecutorHolder.executor()).shutdown(new ExecutorShutdownListener(oldExecutorHolder));
                }
            }
        }
    }

    /**
     * A thread pool size can also be unbounded and is represented by -1, which is not supported by SizeValue (which only supports positive numbers)
     */
    private SizeValue getAsSizeOrUnbounded(Settings settings, String setting, SizeValue defaultValue) throws SettingsException {
        if ("-1".equals(settings.get(setting))) {
            return null;
        }
        return parseSizeValue(settings.get(setting), defaultValue);
    }

    class ExecutorShutdownListener implements EsThreadPoolExecutor.ShutdownListener {

        private ExecutorHolder holder;

        public ExecutorShutdownListener(ExecutorHolder holder) {
            this.holder = holder;
        }

        @Override
        public void onTerminated() {
            retiredExecutors.remove(holder);
        }
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
        final TimeCounter counter;
        volatile boolean running = true;
        volatile long estimatedTimeInMillis;

        EstimatedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            this.estimatedTimeInMillis = System.currentTimeMillis();
            this.counter = new TimeCounter();
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
            }
        }

        private class TimeCounter extends Counter {

            @Override
            public long addAndGet(long delta) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long get() {
                return estimatedTimeInMillis;
            }
        }
    }

    static class ExecutorHolder {
        private final Executor executor;
        public final Info info;

        ExecutorHolder(Executor executor, Info info) {
            assert executor instanceof EsThreadPoolExecutor || executor == MoreExecutors.directExecutor();
            this.executor = executor;
            this.info = info;
        }

        Executor executor() {
            return executor;
        }
    }

    public static class Info implements Streamable, ToXContent {

        private String name;
        private String type;
        private int min;
        private int max;
        private TimeValue keepAlive;
        private SizeValue queueSize;

        Info() {

        }

        public Info(String name, String type) {
            this(name, type, -1);
        }

        public Info(String name, String type, int size) {
            this(name, type, size, size, null, null);
        }

        public Info(String name, String type, int min, int max, @Nullable TimeValue keepAlive, @Nullable SizeValue queueSize) {
            this.name = name;
            this.type = type;
            this.min = min;
            this.max = max;
            this.keepAlive = keepAlive;
            this.queueSize = queueSize;
        }

        public String getName() {
            return this.name;
        }

        public String getType() {
            return this.type;
        }

        public int getMin() {
            return this.min;
        }

        public int getMax() {
            return this.max;
        }

        @Nullable
        public TimeValue getKeepAlive() {
            return this.keepAlive;
        }

        @Nullable
        public SizeValue getQueueSize() {
            return this.queueSize;
        }

        @Override
        public void readFrom(StreamInput in) throws IOException {
            name = in.readString();
            type = in.readString();
            min = in.readInt();
            max = in.readInt();
            if (in.readBoolean()) {
                keepAlive = TimeValue.readTimeValue(in);
            }
            if (in.readBoolean()) {
                queueSize = SizeValue.readSizeValue(in);
            }
            in.readBoolean(); // here to conform with removed waitTime
            in.readBoolean(); // here to conform with removed rejected setting
            in.readBoolean(); // here to conform with queue type
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type);
            out.writeInt(min);
            out.writeInt(max);
            if (keepAlive == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                keepAlive.writeTo(out);
            }
            if (queueSize == null) {
                out.writeBoolean(false);
            } else {
                out.writeBoolean(true);
                queueSize.writeTo(out);
            }
            out.writeBoolean(false); // here to conform with removed waitTime
            out.writeBoolean(false); // here to conform with removed rejected setting
            out.writeBoolean(false); // here to conform with queue type
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
            if (queueSize == null) {
                builder.field(Fields.QUEUE_SIZE, -1);
            } else {
                builder.field(Fields.QUEUE_SIZE, queueSize.toString());
            }
            builder.endObject();
            return builder;
        }

        static final class Fields {
            static final XContentBuilderString TYPE = new XContentBuilderString("type");
            static final XContentBuilderString MIN = new XContentBuilderString("min");
            static final XContentBuilderString MAX = new XContentBuilderString("max");
            static final XContentBuilderString KEEP_ALIVE = new XContentBuilderString("keep_alive");
            static final XContentBuilderString QUEUE_SIZE = new XContentBuilderString("queue_size");
        }

    }

    class ApplySettings implements NodeSettingsService.Listener {
        @Override
        public void onRefreshSettings(Settings settings) {
            updateSettings(settings);
        }
    }

    /**
     * Returns <code>true</code> if the given service was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (service != null) {
            service.shutdown();
            try {
                if (service.awaitTermination(timeout, timeUnit)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            service.shutdownNow();
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given pool was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit) {
        if (pool != null) {
            pool.shutdown();
            try {
                if (pool.awaitTermination(timeout, timeUnit)) {
                    return true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            // last resort
            pool.shutdownNow();
        }
        return false;
    }

}
