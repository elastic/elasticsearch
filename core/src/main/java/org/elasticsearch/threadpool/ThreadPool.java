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

import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import org.apache.lucene.util.Counter;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.settings.Validator;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.elasticsearch.common.collect.MapBuilder.newMapBuilder;
import static org.elasticsearch.common.settings.Settings.settingsBuilder;
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
        public static final String FORCE_MERGE = "force_merge";
        public static final String FETCH_SHARD_STARTED = "fetch_shard_started";
        public static final String FETCH_SHARD_STORE = "fetch_shard_store";
    }

    public enum ThreadPoolType {
        CACHED("cached"),
        DIRECT("direct"),
        FIXED("fixed"),
        SCALING("scaling");

        private final String type;

        public String getType() {
            return type;
        }

        ThreadPoolType(String type) {
            this.type = type;
        }

        private final static Map<String, ThreadPoolType> TYPE_MAP;

        static {
            Map<String, ThreadPoolType> typeMap = new HashMap<>();
            for (ThreadPoolType threadPoolType : ThreadPoolType.values()) {
                typeMap.put(threadPoolType.getType(), threadPoolType);
            }
            TYPE_MAP = Collections.unmodifiableMap(typeMap);
        }

        public static ThreadPoolType fromType(String type) {
            ThreadPoolType threadPoolType = TYPE_MAP.get(type);
            if (threadPoolType == null) {
                throw new IllegalArgumentException("no ThreadPoolType for " + type);
            }
            return threadPoolType;
        }
    }

    public static Map<String, ThreadPoolType> THREAD_POOL_TYPES;

    static {
        HashMap<String, ThreadPoolType> map = new HashMap<>();
        map.put(Names.SAME, ThreadPoolType.DIRECT);
        map.put(Names.GENERIC, ThreadPoolType.CACHED);
        map.put(Names.LISTENER, ThreadPoolType.FIXED);
        map.put(Names.GET, ThreadPoolType.FIXED);
        map.put(Names.INDEX, ThreadPoolType.FIXED);
        map.put(Names.BULK, ThreadPoolType.FIXED);
        map.put(Names.SEARCH, ThreadPoolType.FIXED);
        map.put(Names.SUGGEST, ThreadPoolType.FIXED);
        map.put(Names.PERCOLATE, ThreadPoolType.FIXED);
        map.put(Names.MANAGEMENT, ThreadPoolType.SCALING);
        map.put(Names.FLUSH, ThreadPoolType.SCALING);
        map.put(Names.REFRESH, ThreadPoolType.SCALING);
        map.put(Names.WARMER, ThreadPoolType.SCALING);
        map.put(Names.SNAPSHOT, ThreadPoolType.SCALING);
        map.put(Names.FORCE_MERGE, ThreadPoolType.FIXED);
        map.put(Names.FETCH_SHARD_STARTED, ThreadPoolType.SCALING);
        map.put(Names.FETCH_SHARD_STORE, ThreadPoolType.SCALING);
        THREAD_POOL_TYPES = Collections.unmodifiableMap(map);
    }

    private static void add(Map<String, Settings> executorSettings, ExecutorSettingsBuilder builder) {
        Settings settings = builder.build();
        String name = settings.get("name");
        executorSettings.put(name, settings);
    }

    private static class ExecutorSettingsBuilder {
        Map<String, String> settings = new HashMap<>();

        public ExecutorSettingsBuilder(String name) {
            settings.put("name", name);
            settings.put("type", THREAD_POOL_TYPES.get(name).getType());
        }

        public ExecutorSettingsBuilder size(int availableProcessors) {
            return add("size", Integer.toString(availableProcessors));
        }

        public ExecutorSettingsBuilder queueSize(int queueSize) {
            return add("queue_size", Integer.toString(queueSize));
        }

        public ExecutorSettingsBuilder keepAlive(String keepAlive) {
            return add("keep_alive", keepAlive);
        }

        private ExecutorSettingsBuilder add(String key, String value) {
            settings.put(key, value);
            return this;
        }

        public Settings build() {
            return settingsBuilder().put(settings).build();
        }
    }

    public static final String THREADPOOL_GROUP = "threadpool.";

    private volatile ImmutableMap<String, ExecutorHolder> executors;

    private final ImmutableMap<String, Settings> defaultExecutorTypeSettings;

    private final Queue<ExecutorHolder> retiredExecutors = new ConcurrentLinkedQueue<>();

    private final ScheduledThreadPoolExecutor scheduler;

    private final EstimatedTimeThread estimatedTimeThread;

    private boolean settingsListenerIsSet = false;

    static final Executor DIRECT_EXECUTOR = MoreExecutors.directExecutor();

    public ThreadPool(String name) {
        this(Settings.builder().put("name", name).build());
    }

    public ThreadPool(Settings settings) {
        super(settings);

        assert settings.get("name") != null : "ThreadPool's settings should contain a name";

        Map<String, Settings> groupSettings = getThreadPoolSettingsGroup(settings);

        int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
        int halfProcMaxAt5 = Math.min(((availableProcessors + 1) / 2), 5);
        int halfProcMaxAt10 = Math.min(((availableProcessors + 1) / 2), 10);

        Map<String, Settings> defaultExecutorTypeSettings = new HashMap<>();
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.GENERIC).keepAlive("30s"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.INDEX).size(availableProcessors).queueSize(200));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.BULK).size(availableProcessors).queueSize(50));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.GET).size(availableProcessors).queueSize(1000));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.SEARCH).size(((availableProcessors * 3) / 2) + 1).queueSize(1000));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.SUGGEST).size(availableProcessors).queueSize(1000));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.PERCOLATE).size(availableProcessors).queueSize(1000));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.MANAGEMENT).size(5).keepAlive("5m"));
        // no queue as this means clients will need to handle rejections on listener queue even if the operation succeeded
        // the assumption here is that the listeners should be very lightweight on the listeners side
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.LISTENER).size(halfProcMaxAt10));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.FLUSH).size(halfProcMaxAt5).keepAlive("5m"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.REFRESH).size(halfProcMaxAt10).keepAlive("5m"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.WARMER).size(halfProcMaxAt5).keepAlive("5m"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.SNAPSHOT).size(halfProcMaxAt5).keepAlive("5m"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.FORCE_MERGE).size(1));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.FETCH_SHARD_STARTED).size(availableProcessors * 2).keepAlive("5m"));
        add(defaultExecutorTypeSettings, new ExecutorSettingsBuilder(Names.FETCH_SHARD_STORE).size(availableProcessors * 2).keepAlive("5m"));

        this.defaultExecutorTypeSettings = ImmutableMap.copyOf(defaultExecutorTypeSettings);

        Map<String, ExecutorHolder> executors = new HashMap<>();

        for (Map.Entry<String, Settings> executor : defaultExecutorTypeSettings.entrySet()) {
            executors.put(executor.getKey(), build(executor.getKey(), groupSettings.get(executor.getKey()), executor.getValue()));
        }

        // Building custom thread pools
        for (Map.Entry<String, Settings> entry : groupSettings.entrySet()) {
            if (executors.containsKey(entry.getKey())) {
                continue;
            }
            executors.put(entry.getKey(), build(entry.getKey(), entry.getValue(), Settings.EMPTY));
        }

        executors.put(Names.SAME, new ExecutorHolder(DIRECT_EXECUTOR, new Info(Names.SAME, ThreadPoolType.DIRECT)));
        if (!executors.get(Names.GENERIC).info.getThreadPoolType().equals(ThreadPoolType.CACHED)) {
            throw new IllegalArgumentException("generic thread pool must be of type cached");
        }
        this.executors = ImmutableMap.copyOf(executors);
        this.scheduler = new ScheduledThreadPoolExecutor(1, EsExecutors.daemonThreadFactory(settings, "scheduler"), new EsAbortPolicy());
        this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
        this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
        this.scheduler.setRemoveOnCancelPolicy(true);

        TimeValue estimatedTimeInterval = settings.getAsTime("threadpool.estimated_time_interval", TimeValue.timeValueMillis(200));
        this.estimatedTimeThread = new EstimatedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.estimatedTimeThread.start();
    }

    private Map<String, Settings> getThreadPoolSettingsGroup(Settings settings) {
        Map<String, Settings> groupSettings = settings.getGroups(THREADPOOL_GROUP);
        validate(groupSettings);
        return groupSettings;
    }

    public void setNodeSettingsService(NodeSettingsService nodeSettingsService) {
        if(settingsListenerIsSet) {
            throw new IllegalStateException("the node settings listener was set more then once");
        }
        nodeSettingsService.addListener(new ApplySettings());
        settingsListenerIsSet = true;
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
            throw new IllegalArgumentException("No executor found for [" + name + "]");
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
        return scheduler.schedule(new LoggingRunnable(command), delay.millis(), TimeUnit.MILLISECONDS);
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
            settings = Settings.Builder.EMPTY_SETTINGS;
        }
        Info previousInfo = previousExecutorHolder != null ? previousExecutorHolder.info : null;
        String type = settings.get("type", previousInfo != null ? previousInfo.getThreadPoolType().getType() : defaultSettings.get("type"));
        ThreadPoolType threadPoolType = ThreadPoolType.fromType(type);
        ThreadFactory threadFactory = EsExecutors.daemonThreadFactory(this.settings, name);
        if (ThreadPoolType.DIRECT == threadPoolType) {
            if (previousExecutorHolder != null) {
                logger.debug("updating thread_pool [{}], type [{}]", name, type);
            } else {
                logger.debug("creating thread_pool [{}], type [{}]", name, type);
            }
            return new ExecutorHolder(DIRECT_EXECUTOR, new Info(name, threadPoolType));
        } else if (ThreadPoolType.CACHED == threadPoolType) {
            if (!Names.GENERIC.equals(name)) {
                throw new IllegalArgumentException("thread pool type cached is reserved only for the generic thread pool and can not be applied to [" + name + "]");
            }
            TimeValue defaultKeepAlive = defaultSettings.getAsTime("keep_alive", timeValueMinutes(5));
            if (previousExecutorHolder != null) {
                if (ThreadPoolType.CACHED == previousInfo.getThreadPoolType()) {
                    TimeValue updatedKeepAlive = settings.getAsTime("keep_alive", previousInfo.getKeepAlive());
                    if (!previousInfo.getKeepAlive().equals(updatedKeepAlive)) {
                        logger.debug("updating thread_pool [{}], type [{}], keep_alive [{}]", name, type, updatedKeepAlive);
                        ((EsThreadPoolExecutor) previousExecutorHolder.executor()).setKeepAliveTime(updatedKeepAlive.millis(), TimeUnit.MILLISECONDS);
                        return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, threadPoolType, -1, -1, updatedKeepAlive, null));
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
            Executor executor = EsExecutors.newCached(name, keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory);
            return new ExecutorHolder(executor, new Info(name, threadPoolType, -1, -1, keepAlive, null));
        } else if (ThreadPoolType.FIXED == threadPoolType) {
            int defaultSize = defaultSettings.getAsInt("size", EsExecutors.boundedNumberOfProcessors(settings));
            SizeValue defaultQueueSize = getAsSizeOrUnbounded(defaultSettings, "queue", getAsSizeOrUnbounded(defaultSettings, "queue_size", null));

            if (previousExecutorHolder != null) {
                if (ThreadPoolType.FIXED == previousInfo.getThreadPoolType()) {
                    SizeValue updatedQueueSize = getAsSizeOrUnbounded(settings, "capacity", getAsSizeOrUnbounded(settings, "queue", getAsSizeOrUnbounded(settings, "queue_size", previousInfo.getQueueSize())));
                    if (Objects.equals(previousInfo.getQueueSize(), updatedQueueSize)) {
                        int updatedSize = applyHardSizeLimit(name, settings.getAsInt("size", previousInfo.getMax()));
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
                            return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, threadPoolType, updatedSize, updatedSize, null, updatedQueueSize));
                        }
                        return previousExecutorHolder;
                    }
                }
                if (previousInfo.getMax() >= 0) {
                    defaultSize = previousInfo.getMax();
                }
                defaultQueueSize = previousInfo.getQueueSize();
            }

            int size = applyHardSizeLimit(name, settings.getAsInt("size", defaultSize));
            SizeValue queueSize = getAsSizeOrUnbounded(settings, "capacity", getAsSizeOrUnbounded(settings, "queue", getAsSizeOrUnbounded(settings, "queue_size", defaultQueueSize)));
            logger.debug("creating thread_pool [{}], type [{}], size [{}], queue_size [{}]", name, type, size, queueSize);
            Executor executor = EsExecutors.newFixed(name, size, queueSize == null ? -1 : (int) queueSize.singles(), threadFactory);
            return new ExecutorHolder(executor, new Info(name, threadPoolType, size, size, null, queueSize));
        } else if (ThreadPoolType.SCALING == threadPoolType) {
            TimeValue defaultKeepAlive = defaultSettings.getAsTime("keep_alive", timeValueMinutes(5));
            int defaultMin = defaultSettings.getAsInt("min", 1);
            int defaultSize = defaultSettings.getAsInt("size", EsExecutors.boundedNumberOfProcessors(settings));
            if (previousExecutorHolder != null) {
                if (ThreadPoolType.SCALING == previousInfo.getThreadPoolType()) {
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
                        return new ExecutorHolder(previousExecutorHolder.executor(), new Info(name, threadPoolType, updatedMin, updatedSize, updatedKeepAlive, null));
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
            Executor executor = EsExecutors.newScaling(name, min, size, keepAlive.millis(), TimeUnit.MILLISECONDS, threadFactory);
            return new ExecutorHolder(executor, new Info(name, threadPoolType, min, size, keepAlive, null));
        }
        throw new IllegalArgumentException("No type found [" + type + "], for [" + name + "]");
    }

    private int applyHardSizeLimit(String name, int size) {
        int availableProcessors = EsExecutors.boundedNumberOfProcessors(settings);
        if ((name.equals(Names.BULK) || name.equals(Names.INDEX)) && size > availableProcessors) {
            // We use a hard max size for the indexing pools, because if too many threads enter Lucene's IndexWriter, it means
            // too many segments written, too frequently, too much merging, etc:
            // TODO: I would love to be loud here (throw an exception if you ask for a too-big size), but I think this is dangerous
            // because on upgrade this setting could be in cluster state and hard for the user to correct?
            logger.warn("requested thread pool size [{}] for [{}] is too large; setting to maximum [{}] instead",
                        size, name, availableProcessors);
            size = availableProcessors;
        }

        return size;
    }

    public void updateSettings(Settings settings) {
        Map<String, Settings> groupSettings = getThreadPoolSettingsGroup(settings);
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
            ExecutorHolder newExecutorHolder = rebuild(entry.getKey(), oldExecutorHolder, entry.getValue(), Settings.EMPTY);
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

    private void validate(Map<String, Settings> groupSettings) {
        for (String key : groupSettings.keySet()) {
            if (!THREAD_POOL_TYPES.containsKey(key)) {
                continue;
            }
            String type = groupSettings.get(key).get("type");
            ThreadPoolType correctThreadPoolType = THREAD_POOL_TYPES.get(key);
            // TODO: the type equality check can be removed after #3760/#6732 are addressed
            if (type != null && !correctThreadPoolType.getType().equals(type)) {
                throw new IllegalArgumentException("setting " + THREADPOOL_GROUP + key + ".type to " + type + " is not permitted; must be " + correctThreadPoolType.getType());
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
            } catch (Throwable t) {
                logger.warn("failed to run {}", t, runnable.toString());
                throw t;
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
            this.estimatedTimeInMillis = TimeValue.nsecToMSec(System.nanoTime());
            this.counter = new TimeCounter();
            setDaemon(true);
        }

        public long estimatedTimeInMillis() {
            return this.estimatedTimeInMillis;
        }

        @Override
        public void run() {
            while (running) {
                estimatedTimeInMillis = TimeValue.nsecToMSec(System.nanoTime());
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
        private ThreadPoolType type;
        private int min;
        private int max;
        private TimeValue keepAlive;
        private SizeValue queueSize;

        Info() {

        }

        public Info(String name, ThreadPoolType type) {
            this(name, type, -1);
        }

        public Info(String name, ThreadPoolType type, int size) {
            this(name, type, size, size, null, null);
        }

        public Info(String name, ThreadPoolType type, int min, int max, @Nullable TimeValue keepAlive, @Nullable SizeValue queueSize) {
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

        public ThreadPoolType getThreadPoolType() {
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
            type = ThreadPoolType.fromType(in.readString());
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
            out.writeString(type.getType());
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
            builder.field(Fields.TYPE, type.getType());
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
                builder.field(Fields.QUEUE_SIZE, queueSize.singles());
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

    public static ThreadPoolTypeSettingsValidator THREAD_POOL_TYPE_SETTINGS_VALIDATOR = new ThreadPoolTypeSettingsValidator();
    private static class ThreadPoolTypeSettingsValidator implements Validator {
        @Override
        public String validate(String setting, String value, ClusterState clusterState) {
            // TODO: the type equality validation can be removed after #3760/#6732 are addressed
            Matcher matcher = Pattern.compile("threadpool\\.(.*)\\.type").matcher(setting);
            if (!matcher.matches()) {
                return null;
            } else {
                String threadPool = matcher.group(1);
                ThreadPool.ThreadPoolType defaultThreadPoolType = ThreadPool.THREAD_POOL_TYPES.get(threadPool);
                ThreadPool.ThreadPoolType threadPoolType;
                try {
                    threadPoolType = ThreadPool.ThreadPoolType.fromType(value);
                } catch (IllegalArgumentException e) {
                    return e.getMessage();
                }
                if (defaultThreadPoolType.equals(threadPoolType)) {
                    return null;
                } else {
                    return String.format(
                            Locale.ROOT,
                            "thread pool type for [%s] can only be updated to [%s] but was [%s]",
                            threadPool,
                            defaultThreadPoolType.getType(),
                            threadPoolType.getType()
                    );
                }
            }

        }
    }

}
