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

import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.util.Counter;
import org.apache.lucene.util.IOUtils;
import org.elasticsearch.Version;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.SizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.util.concurrent.XRejectedExecutionHandler;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.node.Node;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
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
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableMap;

public class ThreadPool extends AbstractComponent implements Scheduler, Closeable {

    public static class Names {
        public static final String SAME = "same";
        public static final String GENERIC = "generic";
        public static final String LISTENER = "listener";
        public static final String GET = "get";
        public static final String INDEX = "index";
        public static final String BULK = "bulk";
        public static final String SEARCH = "search";
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
        DIRECT("direct"),
        FIXED("fixed"),
        FIXED_AUTO_QUEUE_SIZE("fixed_auto_queue_size"),
        SCALING("scaling");

        private final String type;

        public String getType() {
            return type;
        }

        ThreadPoolType(String type) {
            this.type = type;
        }

        private static final Map<String, ThreadPoolType> TYPE_MAP;

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

    public static final Map<String, ThreadPoolType> THREAD_POOL_TYPES;

    static {
        HashMap<String, ThreadPoolType> map = new HashMap<>();
        map.put(Names.SAME, ThreadPoolType.DIRECT);
        map.put(Names.GENERIC, ThreadPoolType.SCALING);
        map.put(Names.LISTENER, ThreadPoolType.FIXED);
        map.put(Names.GET, ThreadPoolType.FIXED);
        map.put(Names.INDEX, ThreadPoolType.FIXED);
        map.put(Names.BULK, ThreadPoolType.FIXED);
        map.put(Names.SEARCH, ThreadPoolType.FIXED_AUTO_QUEUE_SIZE);
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

    private Map<String, ExecutorHolder> executors = new HashMap<>();

    private final CachedTimeThread cachedTimeThread;

    static final ExecutorService DIRECT_EXECUTOR = EsExecutors.newDirectExecutorService();

    private final ThreadContext threadContext;

    private final Map<String, ExecutorBuilder> builders;

    private final ScheduledThreadPoolExecutor scheduler;

    public Collection<ExecutorBuilder> builders() {
        return Collections.unmodifiableCollection(builders.values());
    }

    public static Setting<TimeValue> ESTIMATED_TIME_INTERVAL_SETTING =
        Setting.timeSetting("thread_pool.estimated_time_interval", TimeValue.timeValueMillis(200), Setting.Property.NodeScope);

    public ThreadPool(final Settings settings, final ExecutorBuilder<?>... customBuilders) {
        super(settings);

        assert Node.NODE_NAME_SETTING.exists(settings);

        final Map<String, ExecutorBuilder> builders = new HashMap<>();
        final int availableProcessors = EsExecutors.numberOfProcessors(settings);
        final int halfProcMaxAt5 = halfNumberOfProcessorsMaxFive(availableProcessors);
        final int halfProcMaxAt10 = halfNumberOfProcessorsMaxTen(availableProcessors);
        final int genericThreadPoolMax = boundedBy(4 * availableProcessors, 128, 512);
        builders.put(Names.GENERIC, new ScalingExecutorBuilder(Names.GENERIC, 4, genericThreadPoolMax, TimeValue.timeValueSeconds(30)));
        builders.put(Names.INDEX, new FixedExecutorBuilder(settings, Names.INDEX, availableProcessors, 200));
        builders.put(Names.BULK, new FixedExecutorBuilder(settings, Names.BULK, availableProcessors, 200)); // now that we reuse bulk for index/delete ops
        builders.put(Names.GET, new FixedExecutorBuilder(settings, Names.GET, availableProcessors, 1000));
        builders.put(Names.SEARCH, new AutoQueueAdjustingExecutorBuilder(settings,
                        Names.SEARCH, searchThreadPoolSize(availableProcessors), 1000, 1000, 1000, 2000));
        builders.put(Names.MANAGEMENT, new ScalingExecutorBuilder(Names.MANAGEMENT, 1, 5, TimeValue.timeValueMinutes(5)));
        // no queue as this means clients will need to handle rejections on listener queue even if the operation succeeded
        // the assumption here is that the listeners should be very lightweight on the listeners side
        builders.put(Names.LISTENER, new FixedExecutorBuilder(settings, Names.LISTENER, halfProcMaxAt10, -1));
        builders.put(Names.FLUSH, new ScalingExecutorBuilder(Names.FLUSH, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(Names.REFRESH, new ScalingExecutorBuilder(Names.REFRESH, 1, halfProcMaxAt10, TimeValue.timeValueMinutes(5)));
        builders.put(Names.WARMER, new ScalingExecutorBuilder(Names.WARMER, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(Names.SNAPSHOT, new ScalingExecutorBuilder(Names.SNAPSHOT, 1, halfProcMaxAt5, TimeValue.timeValueMinutes(5)));
        builders.put(Names.FETCH_SHARD_STARTED, new ScalingExecutorBuilder(Names.FETCH_SHARD_STARTED, 1, 2 * availableProcessors, TimeValue.timeValueMinutes(5)));
        builders.put(Names.FORCE_MERGE, new FixedExecutorBuilder(settings, Names.FORCE_MERGE, 1, -1));
        builders.put(Names.FETCH_SHARD_STORE, new ScalingExecutorBuilder(Names.FETCH_SHARD_STORE, 1, 2 * availableProcessors, TimeValue.timeValueMinutes(5)));
        for (final ExecutorBuilder<?> builder : customBuilders) {
            if (builders.containsKey(builder.name())) {
                throw new IllegalArgumentException("builder with name [" + builder.name() + "] already exists");
            }
            builders.put(builder.name(), builder);
        }
        this.builders = Collections.unmodifiableMap(builders);

        threadContext = new ThreadContext(settings);

        final Map<String, ExecutorHolder> executors = new HashMap<>();
        for (@SuppressWarnings("unchecked") final Map.Entry<String, ExecutorBuilder> entry : builders.entrySet()) {
            final ExecutorBuilder.ExecutorSettings executorSettings = entry.getValue().getSettings(settings);
            final ExecutorHolder executorHolder = entry.getValue().build(executorSettings, threadContext);
            if (executors.containsKey(executorHolder.info.getName())) {
                throw new IllegalStateException("duplicate executors with name [" + executorHolder.info.getName() + "] registered");
            }
            logger.debug("created thread pool: {}", entry.getValue().formatInfo(executorHolder.info));
            executors.put(entry.getKey(), executorHolder);
        }

        executors.put(Names.SAME, new ExecutorHolder(DIRECT_EXECUTOR, new Info(Names.SAME, ThreadPoolType.DIRECT)));
        this.executors = unmodifiableMap(executors);
        this.scheduler = Scheduler.initScheduler(settings);
        TimeValue estimatedTimeInterval = ESTIMATED_TIME_INTERVAL_SETTING.get(settings);
        this.cachedTimeThread = new CachedTimeThread(EsExecutors.threadName(settings, "[timer]"), estimatedTimeInterval.millis());
        this.cachedTimeThread.start();
    }

    /**
     * Returns a value of milliseconds that may be used for relative time calculations.
     *
     * This method should only be used for calculating time deltas. For an epoch based
     * timestamp, see {@link #absoluteTimeInMillis()}.
     */
    public long relativeTimeInMillis() {
        return cachedTimeThread.relativeTimeInMillis();
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

    public Counter estimatedTimeInMillisCounter() {
        return cachedTimeThread.counter;
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
            throw new IllegalArgumentException("no executor service found for [" + name + "]");
        }
        return holder.executor();
    }

    /**
     * Schedules a one-shot command to run after a given delay. The command is not run in the context of the calling thread. To preserve the
     * context of the calling thread you may call <code>threadPool.getThreadContext().preserveContext</code> on the runnable before passing
     * it to this method.
     *
     * @param delay delay before the task executes
     * @param executor the name of the thread pool on which to execute this task. SAME means "execute on the scheduler thread" which changes the
     *        meaning of the ScheduledFuture returned by this method. In that case the ScheduledFuture will complete only when the command
     *        completes.
     * @param command the command to run
     * @return a ScheduledFuture who's get will return when the task is has been added to its target thread pool and throw an exception if
     *         the task is canceled before it was added to its target thread pool. Once the task has been added to its target thread pool
     *         the ScheduledFuture will cannot interact with it.
     * @throws org.elasticsearch.common.util.concurrent.EsRejectedExecutionException if the task cannot be scheduled for execution
     */
    public ScheduledFuture<?> schedule(TimeValue delay, String executor, Runnable command) {
        if (!Names.SAME.equals(executor)) {
            command = new ThreadedRunnable(command, executor(executor));
        }
        return scheduler.schedule(new ThreadPool.LoggingRunnable(command), delay.millis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, String executor) {
        return new ReschedulingRunnable(command, interval, executor, this,
                (e) -> {
                    if (logger.isDebugEnabled()) {
                        logger.debug((Supplier<?>) () -> new ParameterizedMessage("scheduled task [{}] was rejected on thread pool [{}]",
                                command, executor), e);
                    }
                },
                (e) -> logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to run scheduled task [{}] on thread pool [{}]",
                        command, executor), e));
    }

    public Runnable preserveContext(Runnable command) {
        return getThreadContext().preserveContext(command);
    }

    public void shutdown() {
        cachedTimeThread.running = false;
        cachedTimeThread.interrupt();
        scheduler.shutdown();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                executor.executor().shutdown();
            }
        }
    }

    public void shutdownNow() {
        cachedTimeThread.running = false;
        cachedTimeThread.interrupt();
        scheduler.shutdownNow();
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
                executor.executor().shutdownNow();
            }
        }
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        boolean result = scheduler.awaitTermination(timeout, unit);
        for (ExecutorHolder executor : executors.values()) {
            if (executor.executor() instanceof ThreadPoolExecutor) {
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
        return Math.min(max, Math.max(min, value));
    }

    static int halfNumberOfProcessorsMaxFive(int numberOfProcessors) {
        return boundedBy((numberOfProcessors + 1) / 2, 1, 5);
    }

    static int halfNumberOfProcessorsMaxTen(int numberOfProcessors) {
        return boundedBy((numberOfProcessors + 1) / 2, 1, 10);
    }

    static int twiceNumberOfProcessors(int numberOfProcessors) {
        return boundedBy(2 * numberOfProcessors, 2, Integer.MAX_VALUE);
    }

    public static int searchThreadPoolSize(int availableProcessors) {
        return ((availableProcessors * 3) / 2) + 1;
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
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("failed to run {}", runnable.toString()), e);
                throw e;
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

    /**
     * A thread to cache millisecond time values from
     * {@link System#nanoTime()} and {@link System#currentTimeMillis()}.
     *
     * The values are updated at a specified interval.
     */
    static class CachedTimeThread extends Thread {

        final long interval;
        final TimeCounter counter;
        volatile boolean running = true;
        volatile long relativeMillis;
        volatile long absoluteMillis;

        CachedTimeThread(String name, long interval) {
            super(name);
            this.interval = interval;
            this.relativeMillis = TimeValue.nsecToMSec(System.nanoTime());
            this.absoluteMillis = System.currentTimeMillis();
            this.counter = new TimeCounter();
            setDaemon(true);
        }

        /**
         * Return the current time used for relative calculations. This is
         * {@link System#nanoTime()} truncated to milliseconds.
         */
        long relativeTimeInMillis() {
            return relativeMillis;
        }

        /**
         * Return the current epoch time, used to find absolute time. This is
         * a cached version of {@link System#currentTimeMillis()}.
         */
        long absoluteTimeInMillis() {
            return absoluteMillis;
        }

        @Override
        public void run() {
            while (running) {
                relativeMillis = TimeValue.nsecToMSec(System.nanoTime());
                absoluteMillis = System.currentTimeMillis();
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
                return relativeMillis;
            }
        }
    }

    static class ExecutorHolder {
        private final ExecutorService executor;
        public final Info info;

        ExecutorHolder(ExecutorService executor, Info info) {
            assert executor instanceof EsThreadPoolExecutor || executor == DIRECT_EXECUTOR;
            this.executor = executor;
            this.info = info;
        }

        ExecutorService executor() {
            return executor;
        }
    }

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
            keepAlive = in.readOptionalWriteable(TimeValue::new);
            queueSize = in.readOptionalWriteable(SizeValue::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(name);
            if (type == ThreadPoolType.FIXED_AUTO_QUEUE_SIZE &&
                    out.getVersion().before(Version.V_6_0_0_alpha1)) {
                // 5.x doesn't know about the "fixed_auto_queue_size" thread pool type, just write fixed.
                out.writeString(ThreadPoolType.FIXED.getType());
            } else {
                out.writeString(type.getType());
            }
            out.writeInt(min);
            out.writeInt(max);
            out.writeOptionalWriteable(keepAlive);
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
            static final String TYPE = "type";
            static final String MIN = "min";
            static final String MAX = "max";
            static final String KEEP_ALIVE = "keep_alive";
            static final String QUEUE_SIZE = "queue_size";
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

    private static boolean awaitTermination(
            final ExecutorService service,
            final long timeout,
            final TimeUnit timeUnit) {
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
            try {
                pool.shutdown();
                if (awaitTermination(pool, timeout, timeUnit)) {
                    return true;
                }
                // last resort
                pool.shutdownNow();
                return awaitTermination(pool, timeout, timeUnit);
            } finally {
                IOUtils.closeWhileHandlingException(pool);
            }
        }
        return false;
    }

    private static boolean awaitTermination(
            final ThreadPool threadPool,
            final long timeout,
            final TimeUnit timeUnit) {
        try {
            if (threadPool.awaitTermination(timeout, timeUnit)) {
                return true;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return false;
    }

    @Override
    public void close() throws IOException {
        threadContext.close();
    }

    public ThreadContext getThreadContext() {
        return threadContext;
    }

    public static boolean assertNotScheduleThread(String reason) {
        assert Thread.currentThread().getName().contains("scheduler") == false :
            "Expected current thread [" + Thread.currentThread() + "] to not be the scheduler thread. Reason: [" + reason + "]";
        return true;
    }
}
