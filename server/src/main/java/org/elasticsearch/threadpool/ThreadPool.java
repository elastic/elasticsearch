/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.threadpool;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionHandler;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.UpdateForV9;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.ReportingService;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.LongAsyncCounter;
import org.elasticsearch.telemetry.metric.LongGauge;
import org.elasticsearch.telemetry.metric.LongWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Map.entry;
import static org.elasticsearch.core.Strings.format;

/**
 * Manages all the Java thread pools we create. {@link Names} contains a list of the thread pools, but plugins can dynamically add more
 * thread pools to instantiate.
 */
public class ThreadPool implements ReportingService<ThreadPoolInfo>, Scheduler {

    private static final Logger logger = LogManager.getLogger(ThreadPool.class);

    /**
     * List of names that identify Java thread pools that are created in {@link ThreadPool#ThreadPool}.
     */
    public static class Names {
        /**
         * All the tasks that do not relate to the purpose of one of the other thread pools should use this thread pool. Try to pick one of
         * the other more specific thread pools where possible.
         */
        public static final String GENERIC = "generic";
        /**
         * Important management tasks that keep the cluster from falling apart.
         * This thread pool ensures cluster coordination tasks do not get blocked by less critical tasks and can continue to make progress.
         * This thread pool also defaults to a single thread, reducing contention on the Coordinator mutex.
         */
        public static final String CLUSTER_COORDINATION = "cluster_coordination";
        public static final String GET = "get";
        public static final String ANALYZE = "analyze";
        public static final String WRITE = "write";
        public static final String SEARCH = "search";
        public static final String SEARCH_WORKER = "search_worker";
        public static final String SEARCH_COORDINATION = "search_coordination";
        public static final String AUTO_COMPLETE = "auto_complete";
        public static final String SEARCH_THROTTLED = "search_throttled";
        /**
         * Cluster management tasks. Tasks that manage data, and tasks that report on cluster health via statistics etc.
         * Not a latency sensitive thread pool: some tasks may time be long-running; and the thread pool size is limited / relatively small.
         */
        public static final String MANAGEMENT = "management";
        public static final String FLUSH = "flush";
        public static final String REFRESH = "refresh";
        public static final String WARMER = "warmer";
        public static final String SNAPSHOT = "snapshot";
        public static final String SNAPSHOT_META = "snapshot_meta";
        public static final String FORCE_MERGE = "force_merge";
        public static final String FETCH_SHARD_STARTED = "fetch_shard_started";
        public static final String FETCH_SHARD_STORE = "fetch_shard_store";
        public static final String SYSTEM_READ = "system_read";
        public static final String SYSTEM_WRITE = "system_write";
        public static final String SYSTEM_CRITICAL_READ = "system_critical_read";
        public static final String SYSTEM_CRITICAL_WRITE = "system_critical_write";
    }

    public static final String THREAD_POOL_METRIC_PREFIX = "es.thread_pool.";
    public static final String THREAD_POOL_METRIC_NAME_COMPLETED = ".threads.completed.total";
    public static final String THREAD_POOL_METRIC_NAME_CURRENT = ".threads.count.current";
    public static final String THREAD_POOL_METRIC_NAME_QUEUE = ".threads.queue.size";
    public static final String THREAD_POOL_METRIC_NAME_ACTIVE = ".threads.active.current";
    public static final String THREAD_POOL_METRIC_NAME_LARGEST = ".threads.largest.current";
    public static final String THREAD_POOL_METRIC_NAME_REJECTED = ".threads.rejected.total";

    public enum ThreadPoolType {
        @Deprecated(forRemoval = true)
        @UpdateForV9 // no longer used, remove in v9
        DIRECT("direct"),
        FIXED("fixed"),
        @Deprecated(forRemoval = true)
        @UpdateForV9 // no longer used, remove in v9
        FIXED_AUTO_QUEUE_SIZE("fixed_auto_queue_size"),
        SCALING("scaling");

        private final String type;

        public String getType() {
            return type;
        }

        ThreadPoolType(String type) {
            this.type = type;
        }

        private static final Map<String, ThreadPoolType> TYPE_MAP = Arrays.stream(ThreadPoolType.values())
            .collect(Collectors.toUnmodifiableMap(ThreadPoolType::getType, Function.identity()));

        public static ThreadPoolType fromType(String type) {
            ThreadPoolType threadPoolType = TYPE_MAP.get(type);
            if (threadPoolType == null) {
                throw new IllegalArgumentException("no ThreadPoolType for " + type);
            }
            return threadPoolType;
        }
    }

    public static final Map<String, ThreadPoolType> THREAD_POOL_TYPES = Map.ofEntries(
        entry(Names.GENERIC, ThreadPoolType.SCALING),
        entry(Names.CLUSTER_COORDINATION, ThreadPoolType.FIXED),
        entry(Names.GET, ThreadPoolType.FIXED),
        entry(Names.ANALYZE, ThreadPoolType.FIXED),
        entry(Names.WRITE, ThreadPoolType.FIXED),
        entry(Names.SEARCH, ThreadPoolType.FIXED),
        entry(Names.SEARCH_WORKER, ThreadPoolType.FIXED),
        entry(Names.SEARCH_COORDINATION, ThreadPoolType.FIXED),
        entry(Names.AUTO_COMPLETE, ThreadPoolType.FIXED),
        entry(Names.MANAGEMENT, ThreadPoolType.SCALING),
        entry(Names.FLUSH, ThreadPoolType.SCALING),
        entry(Names.REFRESH, ThreadPoolType.SCALING),
        entry(Names.WARMER, ThreadPoolType.SCALING),
        entry(Names.SNAPSHOT, ThreadPoolType.SCALING),
        entry(Names.SNAPSHOT_META, ThreadPoolType.SCALING),
        entry(Names.FORCE_MERGE, ThreadPoolType.FIXED),
        entry(Names.FETCH_SHARD_STARTED, ThreadPoolType.SCALING),
        entry(Names.FETCH_SHARD_STORE, ThreadPoolType.SCALING),
        entry(Names.SEARCH_THROTTLED, ThreadPoolType.FIXED),
        entry(Names.SYSTEM_READ, ThreadPoolType.FIXED),
        entry(Names.SYSTEM_WRITE, ThreadPoolType.FIXED),
        entry(Names.SYSTEM_CRITICAL_READ, ThreadPoolType.FIXED),
        entry(Names.SYSTEM_CRITICAL_WRITE, ThreadPoolType.FIXED)
    );

    public static final double searchAutoscalingEWMA = 0.1;

    private final Map<String, ExecutorHolder> executors;

    private final ThreadPoolInfo threadPoolInfo;

    private final CachedTimeThread cachedTimeThread;

    private final ThreadContext threadContext;

    @SuppressWarnings("rawtypes")
    private final Map<String, ExecutorBuilder> builders;

    private final ScheduledThreadPoolExecutor scheduler;

    private final long slowSchedulerWarnThresholdNanos;

    private Map<String, ArrayList<Instrument>> instruments;

    @SuppressWarnings("rawtypes")
    public Collection<ExecutorBuilder> builders() {
        return Collections.unmodifiableCollection(builders.values());
    }

    public static final Setting<TimeValue> ESTIMATED_TIME_INTERVAL_SETTING = Setting.timeSetting(
        "thread_pool.estimated_time_interval",
        TimeValue.timeValueMillis(200),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING = Setting.timeSetting(
        "thread_pool.estimated_time_interval.warn_threshold",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    public static final Setting<TimeValue> SLOW_SCHEDULER_TASK_WARN_THRESHOLD_SETTING = Setting.timeSetting(
        "thread_pool.scheduler.warn_threshold",
        TimeValue.timeValueSeconds(5),
        TimeValue.ZERO,
        Setting.Property.NodeScope
    );

    /**
     * Defines and builds the many thread pools delineated in {@link Names}.
     *
     * @param settings
     * @param meterRegistry
     * @param customBuilders a list of additional thread pool builders that were defined elsewhere (like a Plugin).
     */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public ThreadPool(final Settings settings, MeterRegistry meterRegistry, final ExecutorBuilder<?>... customBuilders) {
        assert Node.NODE_NAME_SETTING.exists(settings);

        final Map<String, ExecutorBuilder> builders = new HashMap<>();
        final int allocatedProcessors = EsExecutors.allocatedProcessors(settings);
        final int halfProc = halfAllocatedProcessors(allocatedProcessors);
        final int halfProcMaxAt5 = halfAllocatedProcessorsMaxFive(allocatedProcessors);
        final int halfProcMaxAt10 = halfAllocatedProcessorsMaxTen(allocatedProcessors);
        final int genericThreadPoolMax = boundedBy(4 * allocatedProcessors, 128, 512);
        final Map<String, ArrayList<Instrument>> instruments = new HashMap<>();

        builders.put(
            Names.GENERIC,
            new ScalingExecutorBuilder(Names.GENERIC, 4, genericThreadPoolMax, TimeValue.timeValueSeconds(30), false)
        );
        builders.put(
            Names.WRITE,
            new FixedExecutorBuilder(settings, Names.WRITE, allocatedProcessors, 10000, new TaskTrackingConfig(true, 0.1))
        );
        int searchOrGetThreadPoolSize = searchOrGetThreadPoolSize(allocatedProcessors);
        builders.put(
            Names.GET,
            new FixedExecutorBuilder(settings, Names.GET, searchOrGetThreadPoolSize, 1000, TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(Names.ANALYZE, new FixedExecutorBuilder(settings, Names.ANALYZE, 1, 16, TaskTrackingConfig.DO_NOT_TRACK));
        builders.put(
            Names.SEARCH,
            new FixedExecutorBuilder(
                settings,
                Names.SEARCH,
                searchOrGetThreadPoolSize,
                1000,
                new TaskTrackingConfig(true, searchAutoscalingEWMA)
            )
        );
        builders.put(
            Names.SEARCH_WORKER,
            new FixedExecutorBuilder(settings, Names.SEARCH_WORKER, searchOrGetThreadPoolSize, -1, TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            Names.SEARCH_COORDINATION,
            new FixedExecutorBuilder(
                settings,
                Names.SEARCH_COORDINATION,
                halfProc,
                1000,
                new TaskTrackingConfig(true, searchAutoscalingEWMA)
            )
        );
        builders.put(
            Names.AUTO_COMPLETE,
            new FixedExecutorBuilder(settings, Names.AUTO_COMPLETE, Math.max(allocatedProcessors / 4, 1), 100, TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            Names.SEARCH_THROTTLED,
            new FixedExecutorBuilder(settings, Names.SEARCH_THROTTLED, 1, 100, TaskTrackingConfig.DEFAULT)
        );
        builders.put(
            Names.MANAGEMENT,
            new ScalingExecutorBuilder(Names.MANAGEMENT, 1, boundedBy(allocatedProcessors, 1, 5), TimeValue.timeValueMinutes(5), false)
        );
        builders.put(Names.FLUSH, new ScalingExecutorBuilder(Names.FLUSH, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5), false));
        // TODO: remove (or refine) this temporary stateless custom refresh pool sizing once ES-7631 is solved.
        final int refreshThreads = DiscoveryNode.isStateless(settings) ? allocatedProcessors : halfProcMaxAt10;
        builders.put(Names.REFRESH, new ScalingExecutorBuilder(Names.REFRESH, 1, refreshThreads, TimeValue.timeValueMinutes(5), false));
        builders.put(Names.WARMER, new ScalingExecutorBuilder(Names.WARMER, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5), false));
        final int maxSnapshotCores = getMaxSnapshotThreadPoolSize(allocatedProcessors);
        builders.put(Names.SNAPSHOT, new ScalingExecutorBuilder(Names.SNAPSHOT, 1, maxSnapshotCores, TimeValue.timeValueMinutes(5), false));
        builders.put(
            Names.SNAPSHOT_META,
            new ScalingExecutorBuilder(
                Names.SNAPSHOT_META,
                1,
                Math.min(allocatedProcessors * 3, 50),
                TimeValue.timeValueSeconds(30L),
                false
            )
        );
        builders.put(
            Names.FETCH_SHARD_STARTED,
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STARTED, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5), false)
        );
        builders.put(
            Names.FORCE_MERGE,
            new FixedExecutorBuilder(
                settings,
                Names.FORCE_MERGE,
                oneEighthAllocatedProcessors(allocatedProcessors),
                -1,
                TaskTrackingConfig.DO_NOT_TRACK
            )
        );
        builders.put(
            Names.CLUSTER_COORDINATION,
            new FixedExecutorBuilder(settings, Names.CLUSTER_COORDINATION, 1, -1, TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            Names.FETCH_SHARD_STORE,
            new ScalingExecutorBuilder(Names.FETCH_SHARD_STORE, 1, 2 * allocatedProcessors, TimeValue.timeValueMinutes(5), false)
        );
        builders.put(
            Names.SYSTEM_READ,
            new FixedExecutorBuilder(settings, Names.SYSTEM_READ, halfProcMaxAt5, 2000, TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            Names.SYSTEM_WRITE,
            new FixedExecutorBuilder(settings, Names.SYSTEM_WRITE, halfProcMaxAt5, 1000, new TaskTrackingConfig(true, 0.1))
        );
        builders.put(
            Names.SYSTEM_CRITICAL_READ,
            new FixedExecutorBuilder(settings, Names.SYSTEM_CRITICAL_READ, halfProcMaxAt5, 2000, TaskTrackingConfig.DO_NOT_TRACK)
        );
        builders.put(
            Names.SYSTEM_CRITICAL_WRITE,
            new FixedExecutorBuilder(settings, Names.SYSTEM_CRITICAL_WRITE, halfProcMaxAt5, 1500, new TaskTrackingConfig(true, 0.1))
        );

        for (final ExecutorBuilder<?> builder : customBuilders) {
            if (builders.containsKey(builder.name())) {
                throw new IllegalArgumentException("builder with name [" + builder.name() + "] already exists");
            }
            builders.put(builder.name(), builder);
        }
        this.builders = Collections.unmodifiableMap(builders);

        threadContext = new ThreadContext(settings);

        // Now that all the thread pools have been defined, actually build them.
        final Map<String, ExecutorHolder> executors = new HashMap<>();
        for (final Map.Entry<String, ExecutorBuilder> entry : builders.entrySet()) {
            final ExecutorBuilder.ExecutorSettings executorSettings = entry.getValue().getSettings(settings);
            final ExecutorHolder executorHolder = entry.getValue().build(executorSettings, threadContext);
            if (executors.containsKey(executorHolder.info.getName())) {
                throw new IllegalStateException("duplicate executors with name [" + executorHolder.info.getName() + "] registered");
            }
            logger.debug("created thread pool: {}", entry.getValue().formatInfo(executorHolder.info));
            executors.put(entry.getKey(), executorHolder);
        }

        this.executors = Map.copyOf(executors);
        this.executors.forEach((k, v) -> instruments.put(k, setupMetrics(meterRegistry, k, v)));
        this.instruments = instruments;
        this.threadPoolInfo = new ThreadPoolInfo(executors.values().stream().map(holder -> holder.info).toList());
        this.scheduler = Scheduler.initScheduler(settings, "scheduler");
        this.slowSchedulerWarnThresholdNanos = SLOW_SCHEDULER_TASK_WARN_THRESHOLD_SETTING.get(settings).nanos();
        this.cachedTimeThread = new CachedTimeThread(
            EsExecutors.threadName(settings, "[timer]"),
            ESTIMATED_TIME_INTERVAL_SETTING.get(settings).millis(),
            LATE_TIME_INTERVAL_WARN_THRESHOLD_SETTING.get(settings).millis()
        );
        this.cachedTimeThread.start();
    }

    private static ArrayList<Instrument> setupMetrics(MeterRegistry meterRegistry, String name, ExecutorHolder holder) {
        Map<String, Object> at = Map.of();
        ArrayList<Instrument> instruments = new ArrayList<>();
        if (holder.executor() instanceof ThreadPoolExecutor threadPoolExecutor) {
            String prefix = THREAD_POOL_METRIC_PREFIX + name;
            instruments.add(
                meterRegistry.registerLongGauge(
                    prefix + THREAD_POOL_METRIC_NAME_CURRENT,
                    "number of threads for " + name,
                    "count",
                    () -> new LongWithAttributes(threadPoolExecutor.getPoolSize(), at)
                )
            );
            instruments.add(
                meterRegistry.registerLongGauge(
                    prefix + THREAD_POOL_METRIC_NAME_QUEUE,
                    "number queue size for " + name,
                    "count",
                    () -> new LongWithAttributes(threadPoolExecutor.getQueue().size(), at)
                )
            );
            instruments.add(
                meterRegistry.registerLongGauge(
                    prefix + THREAD_POOL_METRIC_NAME_ACTIVE,
                    "number of active threads for " + name,
                    "count",
                    () -> new LongWithAttributes(threadPoolExecutor.getActiveCount(), at)
                )
            );
            instruments.add(
                meterRegistry.registerLongGauge(
                    prefix + THREAD_POOL_METRIC_NAME_LARGEST,
                    "largest pool size for " + name,
                    "count",
                    () -> new LongWithAttributes(threadPoolExecutor.getLargestPoolSize(), at)
                )
            );
            instruments.add(
                meterRegistry.registerLongAsyncCounter(
                    prefix + THREAD_POOL_METRIC_NAME_COMPLETED,
                    "number of completed threads for " + name,
                    "count",
                    () -> new LongWithAttributes(threadPoolExecutor.getCompletedTaskCount(), at)
                )
            );
            RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
            if (rejectedExecutionHandler instanceof EsRejectedExecutionHandler handler) {
                handler.registerCounter(meterRegistry, prefix + THREAD_POOL_METRIC_NAME_REJECTED, name);
            }
        }
        return instruments;
    }

    // for subclassing by tests that don't actually use any of the machinery that the regular constructor sets up
    protected ThreadPool() {
        this.builders = Map.of();
        this.executors = Map.of();
        this.cachedTimeThread = null;
        this.threadPoolInfo = new ThreadPoolInfo(List.of());
        this.slowSchedulerWarnThresholdNanos = 0L;
        this.threadContext = new ThreadContext(Settings.EMPTY);
        this.scheduler = null;
    }

    /**
     * Returns a value of milliseconds that may be used for relative time calculations.
     *
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInMillis() {
        return TimeValue.nsecToMSec(relativeTimeInNanos());
    }

    /**
     * Returns a value of nanoseconds that may be used for relative time calculations.
     *
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInNanos() {
        return cachedTimeThread.relativeTimeInNanos();
    }

    /**
     * Returns a value of milliseconds that may be used for relative time calculations. Similar to {@link #relativeTimeInMillis()} except
     * that this method is more expensive: the return value is computed directly from {@link System#nanoTime} and is not cached. You should
     * use {@link #relativeTimeInMillis()} unless the extra accuracy offered by this method is worth the costs.
     *
     * When computing a time interval by comparing relative times in milliseconds, you should make sure that both endpoints use cached
     * values returned from {@link #relativeTimeInMillis()} or that they both use raw values returned from this method. It doesn't really
     * make sense to compare a raw value to a cached value, even if in practice the result of such a comparison will be approximately
     * sensible.
     */
    public long rawRelativeTimeInMillis() {
        return TimeValue.nsecToMSec(System.nanoTime());
    }

    /**
     * Returns the value of milliseconds since UNIX epoch.
     *
     * This method should only be used for exact date/time formatting. For calculating
     * time deltas that should not suffer from negative deltas, which are possible with
     * this method, see {@link #relativeTimeInMillis()}.
     */
    public long absoluteTimeInMillis() {
        return cachedTimeThread.absoluteTimeInMillis();
    }

    @Override
    public ThreadPoolInfo info() {
        return threadPoolInfo;
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
            final String name = holder.info.getName();
            int threads = -1;
            int queue = -1;
            int active = -1;
            long rejected = -1;
            int largest = -1;
            long completed = -1;
            if (holder.executor() instanceof ThreadPoolExecutor threadPoolExecutor) {
                threads = threadPoolExecutor.getPoolSize();
                queue = threadPoolExecutor.getQueue().size();
                active = threadPoolExecutor.getActiveCount();
                largest = threadPoolExecutor.getLargestPoolSize();
                completed = threadPoolExecutor.getCompletedTaskCount();
                RejectedExecutionHandler rejectedExecutionHandler = threadPoolExecutor.getRejectedExecutionHandler();
                if (rejectedExecutionHandler instanceof EsRejectedExecutionHandler handler) {
                    rejected = handler.rejected();
                }
            }
            stats.add(new ThreadPoolStats.Stats(name, threads, queue, active, rejected, largest, completed));
        }
        return new ThreadPoolStats(stats);
    }

    /**
     * Get the generic {@link ExecutorService}. This executor service
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} will not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     */
    public ExecutorService generic() {
        return executor(Names.GENERIC);
    }

    /**
     * Get the {@link ExecutorService} with the given name. This executor service's
     * {@link Executor#execute(Runnable)} method will run the {@link Runnable} it is given in the
     * {@link ThreadContext} of the thread that queues it.
     * <p>
     * Warning: this {@linkplain ExecutorService} might not throw {@link RejectedExecutionException}
     * if you submit a task while it shutdown. It will instead silently queue it and not run it.
     *
     * @param name the name of the executor service to obtain
     * @throws IllegalArgumentException if no executor service with the specified name exists
     */
    public ExecutorService executor(String name) {
        final ExecutorHolder holder = executors.get(name);
        if (holder == null) {
            final var message = "no executor service found for [" + name + "]";
            assert false : message;
            throw new IllegalArgumentException(message);
        }
        return holder.executor();
    }

    /**
     * Schedules a one-shot command to run after a given delay. The command is run in the context of the calling thread.
     *
     * @param command the command to run
     * @param delay delay before the task executes
     * @param executor the name of the thread pool on which to execute this task. SAME means "execute on the scheduler thread" which changes
     *        the meaning of the ScheduledFuture returned by this method. In that case the ScheduledFuture will complete only when the
     *        command completes.
     * @return a ScheduledFuture who's get will return when the task is has been added to its target thread pool and throw an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture will cannot interact with it.
     * @throws org.elasticsearch.common.util.concurrent.EsRejectedExecutionException if the task cannot be scheduled for execution
     */
    @Override
    public ScheduledCancellable schedule(Runnable command, TimeValue delay, Executor executor) {
        final Runnable contextPreservingRunnable = threadContext.preserveContext(command);
        final Runnable toSchedule;
        if (executor != EsExecutors.DIRECT_EXECUTOR_SERVICE) {
            toSchedule = new ThreadedRunnable(contextPreservingRunnable, executor);
        } else if (slowSchedulerWarnThresholdNanos > 0) {
            toSchedule = new Runnable() {
                @Override
                public void run() {
                    final long startTime = ThreadPool.this.relativeTimeInNanos();
                    try {
                        contextPreservingRunnable.run();
                    } finally {
                        final long took = ThreadPool.this.relativeTimeInNanos() - startTime;
                        if (took > slowSchedulerWarnThresholdNanos) {
                            logger.warn(
                                "execution of [{}] took [{}ms] which is above the warn threshold of [{}ms]",
                                contextPreservingRunnable,
                                TimeUnit.NANOSECONDS.toMillis(took),
                                TimeUnit.NANOSECONDS.toMillis(slowSchedulerWarnThresholdNanos)
                            );
                        }
                    }
                }

                @Override
                public String toString() {
                    return contextPreservingRunnable.toString();
                }
            };
        } else {
            toSchedule = contextPreservingRunnable;
        }
        return new ScheduledCancellableAdapter(scheduler.schedule(toSchedule, delay.millis(), TimeUnit.MILLISECONDS));
    }

    public void scheduleUnlessShuttingDown(TimeValue delay, Executor executor, Runnable command) {
        try {
            schedule(command, delay, executor);
        } catch (EsRejectedExecutionException e) {
            if (e.isExecutorShutdown()) {
                logger.debug(
                    () -> format(
                        "could not schedule execution of [%s] after [%s] on [%s] as executor is shut down",
                        command,
                        delay,
                        executor
                    ),
                    e
                );
            } else {
                throw e;
            }
        }
    }

    public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, Executor executor) {
        var runnable = new ReschedulingRunnable(
            command,
            interval,
            executor,
            this,
            e -> logger.debug(() -> format("scheduled task [%s] was rejected on thread pool [%s]", command, executor), e),
            e -> logger.warn(() -> format("failed to run scheduled task [%s] on thread pool [%s]", command, executor), e)
        );
        runnable.start();
        return runnable;
    }

    protected final void stopCachedTimeThread() {
        cachedTimeThread.running = false;
        cachedTimeThread.interrupt();
    }

    private void closeMetrics(ExecutorHolder executor) {
        if (this.instruments.containsKey(executor.info.getName())) {
            this.instruments.get(executor.info.getName()).forEach((instrument) -> {
                if (instrument instanceof LongAsyncCounter longasynccounter) {
                    try {
                        longasynccounter.close();
                    } catch (Exception e) {
                        logger.warn(format("Failed to close LongAsyncCounter for %s. %s", executor.info.getName(), e.getMessage()), e);
                    }
                } else if (instrument instanceof LongGauge longgauge) {
                    try {
                        longgauge.close();
                    } catch (Exception e) {
                        logger.warn(format("Failed to close LongGauge for %s. %s", executor.info.getName(), e.getMessage()), e);
                    }
                }
            });
        }
        this.instruments.remove(executor.info.getName());
    }

    public void shutdown() {
        stopCachedTimeThread();
        scheduler.shutdown();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                closeMetrics(executor);
                executor.executor().shutdown();
            }
        }
    }

    public void shutdownNow() {
        stopCachedTimeThread();
        scheduler.shutdownNow();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                closeMetrics(executor);
                executor.executor().shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                closeMetrics(executor);
                result &= executor.executor().awaitTermination(timeout, unit);
            }
        }
        cachedTimeThread.join(unit.toMillis(timeout));
        return result;
    }

    public ScheduledExecutorService scheduler() {
        return this.scheduler;
    }

    /**
     * Constrains a value between minimum and maximum values
     * (inclusive).
     *
     * @param value the value to constrain
     * @param min   the minimum acceptable value
     * @param max   the maximum acceptable value
     * @return min if value is less than min, max if value is greater
     * than value, otherwise value
     */
    static int boundedBy(int value, int min, int max) {
        assert min < max : min + " vs " + max;
        return Math.min(max, Math.max(min, value));
    }

    static int halfAllocatedProcessors(final int allocatedProcessors) {
        return (allocatedProcessors + 1) / 2;
    }

    static int halfAllocatedProcessorsMaxFive(final int allocatedProcessors) {
        return boundedBy(halfAllocatedProcessors(allocatedProcessors), 1, 5);
    }

    static int halfAllocatedProcessorsMaxTen(final int allocatedProcessors) {
        return boundedBy(halfAllocatedProcessors(allocatedProcessors), 1, 10);
    }

    static int twiceAllocatedProcessors(final int allocatedProcessors) {
        return boundedBy(2 * allocatedProcessors, 2, Integer.MAX_VALUE);
    }

    public static int oneEighthAllocatedProcessors(final int allocatedProcessors) {
        return boundedBy(allocatedProcessors / 8, 1, Integer.MAX_VALUE);
    }

    public static int searchOrGetThreadPoolSize(final int allocatedProcessors) {
        return ((allocatedProcessors * 3) / 2) + 1;
    }

    static int getMaxSnapshotThreadPoolSize(int allocatedProcessors) {
        final ByteSizeValue maxHeapSize = ByteSizeValue.ofBytes(Runtime.getRuntime().maxMemory());
        return getMaxSnapshotThreadPoolSize(allocatedProcessors, maxHeapSize);
    }

    static int getMaxSnapshotThreadPoolSize(int allocatedProcessors, final ByteSizeValue maxHeapSize) {
        // While on larger data nodes, larger snapshot threadpool size improves snapshotting on high latency blob stores,
        // smaller instances can run into OOM issues and need a smaller snapshot threadpool size.
        if (maxHeapSize.compareTo(new ByteSizeValue(750, ByteSizeUnit.MB)) < 0) {
            return halfAllocatedProcessorsMaxFive(allocatedProcessors);
        }
        return 10;
    }

    static class ThreadedRunnable implements Runnable {

        private final Runnable runnable;

        private final Executor executor;

        ThreadedRunnable(Runnable runnable, Executor executor) {
            this.runnable = runnable;
            this.executor = executor;
        }

        @Override
        public void run() {
            try {
                executor.execute(runnable);
            } catch (EsRejectedExecutionException e) {
                if (e.isExecutorShutdown()) {
                    logger.debug(
                        () -> format("could not schedule execution of [%s] on [%s] as executor is shut down", runnable, executor),
                        e
                    );
                } else {
                    throw e;
                }
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

    /**
     * A thread to cache millisecond time values from
     * {@link System#nanoTime()} and {@link System#currentTimeMillis()}.
     *
     * The values are updated at a specified interval.
     */
    static class CachedTimeThread extends Thread {

        final long interval;
        private final TimeChangeChecker timeChangeChecker;

        volatile boolean running = true;
        volatile long relativeNanos;
        volatile long absoluteMillis;

        CachedTimeThread(String name, long intervalMillis, long thresholdMillis) {
            super(name);
            this.interval = intervalMillis;
            this.relativeNanos = System.nanoTime();
            this.absoluteMillis = System.currentTimeMillis();
            this.timeChangeChecker = new TimeChangeChecker(thresholdMillis, absoluteMillis, relativeNanos);
            setDaemon(true);
        }

        /**
         * Return the current time used for relative calculations. This is {@link System#nanoTime()}.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#nanoTime()}
         * whenever called. Typically used for testing.
         */
        long relativeTimeInNanos() {
            if (0 < interval) {
                return relativeNanos;
            }
            return System.nanoTime();
        }

        /**
         * Return the current epoch time, used to find absolute time. This is
         * a cached version of {@link System#currentTimeMillis()}.
         * <p>
         * If {@link ThreadPool#ESTIMATED_TIME_INTERVAL_SETTING} is set to 0
         * then the cache is disabled and the method calls {@link System#currentTimeMillis()}
         * whenever called. Typically used for testing.
         */
        long absoluteTimeInMillis() {
            if (0 < interval) {
                return absoluteMillis;
            }
            return System.currentTimeMillis();
        }

        @Override
        public void run() {
            while (running && 0 < interval) {
                relativeNanos = System.nanoTime();
                absoluteMillis = System.currentTimeMillis();
                timeChangeChecker.check(absoluteMillis, relativeNanos);
                try {
                    // busy-waiting is the whole point!
                    // noinspection BusyWait
                    Thread.sleep(interval);
                } catch (InterruptedException e) {
                    running = false;
                    return;
                }
            }
        }
    }

    static class TimeChangeChecker {

        private final long thresholdMillis;
        private final long thresholdNanos;
        private long absoluteMillis;
        private long relativeNanos;

        TimeChangeChecker(long thresholdMillis, long absoluteMillis, long relativeNanos) {
            this.thresholdMillis = thresholdMillis;
            this.thresholdNanos = TimeValue.timeValueMillis(thresholdMillis).nanos();
            this.absoluteMillis = absoluteMillis;
            this.relativeNanos = relativeNanos;
        }

        void check(long newAbsoluteMillis, long newRelativeNanos) {
            if (thresholdMillis <= 0) {
                return;
            }

            try {
                final long deltaMillis = newAbsoluteMillis - absoluteMillis;
                if (deltaMillis > thresholdMillis) {
                    final TimeValue delta = TimeValue.timeValueMillis(deltaMillis);
                    logger.warn(
                        "timer thread slept for [{}/{}ms] on absolute clock which is above the warn threshold of [{}ms]",
                        delta,
                        deltaMillis,
                        thresholdMillis
                    );
                } else if (deltaMillis < 0) {
                    final TimeValue delta = TimeValue.timeValueMillis(-deltaMillis);
                    logger.warn("absolute clock went backwards by [{}/{}ms] while timer thread was sleeping", delta, -deltaMillis);
                }

                final long deltaNanos = newRelativeNanos - relativeNanos;
                if (deltaNanos > thresholdNanos) {
                    final TimeValue delta = TimeValue.timeValueNanos(deltaNanos);
                    logger.warn(
                        "timer thread slept for [{}/{}ns] on relative clock which is above the warn threshold of [{}ms]",
                        delta,
                        deltaNanos,
                        thresholdMillis
                    );
                } else if (deltaNanos < 0) {
                    final TimeValue delta = TimeValue.timeValueNanos(-deltaNanos);
                    logger.error("relative clock went backwards by [{}/{}ns] while timer thread was sleeping", delta, -deltaNanos);
                    assert false : "System::nanoTime time should be monotonic";
                }
            } finally {
                absoluteMillis = newAbsoluteMillis;
                relativeNanos = newRelativeNanos;
            }
        }
    }

    /**
     * Holds a thread pool and additional ES information ({@link Info}) about that Java thread pool ({@link ExecutorService}) instance.
     *
     * See {@link Names} for a list of thread pools, though there can be more dynamically added via plugins.
     */
    static class ExecutorHolder {
        private final ExecutorService executor;
        public final Info info;

        ExecutorHolder(ExecutorService executor, Info info) {
            assert executor instanceof EsThreadPoolExecutor || executor == EsExecutors.DIRECT_EXECUTOR_SERVICE;
            this.executor = executor;
            this.info = info;
        }

        ExecutorService executor() {
            return executor;
        }
    }

    /**
     * The settings used to create a Java ExecutorService thread pool.
     */
    public static class Info implements Writeable, ToXContentFragment {

        private final String name;
        private final ThreadPoolType type;
        private final int min;
        private final int max;
        private final TimeValue keepAlive;
        private final SizeValue queueSize;

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

        public Info(StreamInput in) throws IOException {
            name = in.readString();
            type = ThreadPoolType.fromType(in.readString());
            min = in.readInt();
            max = in.readInt();
            keepAlive = in.readOptionalTimeValue();
            queueSize = in.readOptionalWriteable(SizeValue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            out.writeString(type.getType());
            out.writeInt(min);
            out.writeInt(max);
            out.writeOptionalTimeValue(keepAlive);
            out.writeOptionalWriteable(queueSize);
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
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(name);
            builder.field("type", type.getType());

            if (type == ThreadPoolType.SCALING) {
                assert min != -1;
                builder.field("core", min);
                assert max != -1;
                builder.field("max", max);
            } else {
                assert max != -1;
                builder.field("size", max);
            }
            if (keepAlive != null) {
                builder.field("keep_alive", keepAlive.toString());
            }
            if (queueSize == null) {
                builder.field("queue_size", -1);
            } else {
                builder.field("queue_size", queueSize.singles());
            }
            builder.endObject();
            return builder;
        }

    }

    /**
     * Returns <code>true</code> if the given service was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ExecutorService service, long timeout, TimeUnit timeUnit) {
        if (service != null) {
            service.shutdown();
            if (awaitTermination(service, timeout, timeUnit)) return true;
            service.shutdownNow();
            return awaitTermination(service, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(final ExecutorService service, final long timeout, final TimeUnit timeUnit) {
        try {
            if (service.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (final InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    /**
     * Returns <code>true</code> if the given pool was terminated successfully. If the termination timed out,
     * the service is <code>null</code> this method will return <code>false</code>.
     */
    public static boolean terminate(ThreadPool pool, long timeout, TimeUnit timeUnit) {
        if (pool != null) {
            // Leverage try-with-resources to close the threadpool
            pool.shutdown();
            if (awaitTermination(pool, timeout, timeUnit)) {
                return true;
            }
            // last resort
            pool.shutdownNow();
            return awaitTermination(pool, timeout, timeUnit);
        }
        return false;
    }

    private static boolean awaitTermination(final ThreadPool threadPool, final long timeout, final TimeUnit timeUnit) {
        try {
            if (threadPool.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    public static boolean assertNotScheduleThread(String reason) {
        assert Thread.currentThread().getName().contains("scheduler") == false
            : "Expected current thread [" + Thread.currentThread() + "] to not be the scheduler thread. Reason: [" + reason + "]";
        return true;
    }

    public static boolean assertCurrentThreadPool(String... permittedThreadPoolNames) {
        final var threadName = Thread.currentThread().getName();
        final var executorName = EsExecutors.executorName(threadName);
        assert threadName.startsWith("TEST-")
            || threadName.startsWith("LuceneTestCase")
            || Arrays.asList(permittedThreadPoolNames).contains(executorName)
            : threadName + " not in " + Arrays.toString(permittedThreadPoolNames) + " nor a test thread";
        return true;
    }

    public static boolean assertInSystemContext(ThreadPool threadPool) {
        final var threadName = Thread.currentThread().getName();
        assert threadName.startsWith("TEST-") || threadName.startsWith("LuceneTestCase") || threadPool.getThreadContext().isSystemContext()
            : threadName + " is not running in the system context nor a test thread";
        return true;
    }

    public static boolean assertCurrentMethodIsNotCalledRecursively() {
        final StackTraceElement[] stackTraceElements = Thread.currentThread().getStackTrace();
        assert stackTraceElements.length >= 3 : stackTraceElements.length;
        assert stackTraceElements[0].getMethodName().equals("getStackTrace") : stackTraceElements[0];
        assert stackTraceElements[1].getMethodName().equals("assertCurrentMethodIsNotCalledRecursively") : stackTraceElements[1];
        final StackTraceElement testingMethod = stackTraceElements[2];
        for (int i = 3; i < stackTraceElements.length; i++) {
            assert stackTraceElements[i].getClassName().equals(testingMethod.getClassName()) == false
                || stackTraceElements[i].getMethodName().equals(testingMethod.getMethodName()) == false
                : testingMethod.getClassName() + "#" + testingMethod.getMethodName() + " is called recursively";
        }
        return true;
    }
}
