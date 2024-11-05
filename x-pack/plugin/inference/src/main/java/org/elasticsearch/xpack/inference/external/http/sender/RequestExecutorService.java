/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.external.http.sender;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.inference.InferenceServiceResults;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.inference.common.AdjustableCapacityBlockingQueue;
import org.elasticsearch.xpack.inference.common.RateLimiter;
import org.elasticsearch.xpack.inference.external.http.RequestExecutor;
import org.elasticsearch.xpack.inference.external.http.retry.RequestSender;
import org.elasticsearch.xpack.inference.services.settings.RateLimitSettings;

import java.time.Clock;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.xpack.inference.InferencePlugin.UTILITY_THREAD_POOL_NAME;

/**
 * A service for queuing and executing {@link RequestTask}. This class is useful because the
 * {@link org.apache.http.impl.nio.conn.PoolingNHttpClientConnectionManager} will block when leasing a connection if no
 * connections are available. To avoid blocking the inference transport threads, this executor will queue up the
 * requests until connections are available.
 *
 * <b>NOTE:</b> It is the responsibility of the class constructing the
 * {@link org.apache.http.client.methods.HttpUriRequest} to set a timeout for how long this executor will wait
 * attempting to execute a task (aka waiting for the connection manager to lease a connection). See
 * {@link org.apache.http.client.config.RequestConfig.Builder#setConnectionRequestTimeout} for more info.
 */
class RequestExecutorService implements RequestExecutor {

    /**
     * Provides dependency injection mainly for testing
     */
    interface Sleeper {
        void sleep(TimeValue sleepTime) throws InterruptedException;
    }

    // default for tests
    static final Sleeper DEFAULT_SLEEPER = sleepTime -> sleepTime.timeUnit().sleep(sleepTime.duration());
    // default for tests
    static final AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> DEFAULT_QUEUE_CREATOR =
        new AdjustableCapacityBlockingQueue.QueueCreator<>() {
            @Override
            public BlockingQueue<RejectableTask> create(int capacity) {
                BlockingQueue<RejectableTask> queue;
                if (capacity <= 0) {
                    queue = create();
                } else {
                    queue = new LinkedBlockingQueue<>(capacity);
                }

                return queue;
            }

            @Override
            public BlockingQueue<RejectableTask> create() {
                return new LinkedBlockingQueue<>();
            }
        };

    /**
     * Provides dependency injection mainly for testing
     */
    interface RateLimiterCreator {
        RateLimiter create(double accumulatedTokensLimit, double tokensPerTimeUnit, TimeUnit unit);
    }

    // default for testing
    static final RateLimiterCreator DEFAULT_RATE_LIMIT_CREATOR = RateLimiter::new;
    private static final Logger logger = LogManager.getLogger(RequestExecutorService.class);
    private static final TimeValue RATE_LIMIT_GROUP_CLEANUP_INTERVAL = TimeValue.timeValueDays(1);

    private final ConcurrentMap<Object, RateLimitingEndpointHandler> rateLimitGroupings = new ConcurrentHashMap<>();
    private final ThreadPool threadPool;
    private final CountDownLatch startupLatch;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final RequestSender requestSender;
    private final RequestExecutorServiceSettings settings;
    private final Clock clock;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> queueCreator;
    private final Sleeper sleeper;
    private final RateLimiterCreator rateLimiterCreator;
    private final AtomicReference<Scheduler.Cancellable> cancellableCleanupTask = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean(false);

    RequestExecutorService(
        ThreadPool threadPool,
        @Nullable CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        RequestSender requestSender
    ) {
        this(
            threadPool,
            DEFAULT_QUEUE_CREATOR,
            startupLatch,
            settings,
            requestSender,
            Clock.systemUTC(),
            DEFAULT_SLEEPER,
            DEFAULT_RATE_LIMIT_CREATOR
        );
    }

    RequestExecutorService(
        ThreadPool threadPool,
        AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> queueCreator,
        @Nullable CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        RequestSender requestSender,
        Clock clock,
        Sleeper sleeper,
        RateLimiterCreator rateLimiterCreator
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.queueCreator = Objects.requireNonNull(queueCreator);
        this.startupLatch = startupLatch;
        this.requestSender = Objects.requireNonNull(requestSender);
        this.settings = Objects.requireNonNull(settings);
        this.clock = Objects.requireNonNull(clock);
        this.sleeper = Objects.requireNonNull(sleeper);
        this.rateLimiterCreator = Objects.requireNonNull(rateLimiterCreator);
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            if (cancellableCleanupTask.get() != null) {
                logger.debug(() -> "Stopping clean up thread");
                cancellableCleanupTask.get().cancel();
            }
        }
    }

    public boolean isShutdown() {
        return shutdown.get();
    }

    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    public boolean isTerminated() {
        return terminationLatch.getCount() == 0;
    }

    public int queueSize() {
        return rateLimitGroupings.values().stream().mapToInt(RateLimitingEndpointHandler::queueSize).sum();
    }

    /**
     * Begin servicing tasks.
     * <p>
     * <b>Note: This should only be called once for the life of the object.</b>
     * </p>
     */
    public void start() {
        try {
            assert started.get() == false : "start() can only be called once";
            started.set(true);

            startCleanupTask();
            signalStartInitiated();

            while (isShutdown() == false) {
                handleTasks();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            shutdown();
            notifyRequestsOfShutdown();
            terminationLatch.countDown();
        }
    }

    private void signalStartInitiated() {
        if (startupLatch != null) {
            startupLatch.countDown();
        }
    }

    private void startCleanupTask() {
        assert cancellableCleanupTask.get() == null : "The clean up task can only be set once";
        cancellableCleanupTask.set(startCleanupThread(RATE_LIMIT_GROUP_CLEANUP_INTERVAL));
    }

    private Scheduler.Cancellable startCleanupThread(TimeValue interval) {
        logger.debug(() -> Strings.format("Clean up task scheduled with interval [%s]", interval));

        return threadPool.scheduleWithFixedDelay(this::removeStaleGroupings, interval, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    // default for testing
    void removeStaleGroupings() {
        var now = Instant.now(clock);
        for (var iter = rateLimitGroupings.values().iterator(); iter.hasNext();) {
            var endpoint = iter.next();

            // if the current time is after the last time the endpoint enqueued a request + allowed stale period then we'll remove it
            if (now.isAfter(endpoint.timeOfLastEnqueue().plus(settings.getRateLimitGroupStaleDuration()))) {
                endpoint.close();
                iter.remove();
            }
        }
    }

    private void handleTasks() throws InterruptedException {
        var timeToWait = settings.getTaskPollFrequency();
        for (var endpoint : rateLimitGroupings.values()) {
            timeToWait = TimeValue.min(endpoint.executeEnqueuedTask(), timeToWait);
        }

        sleeper.sleep(timeToWait);
    }

    private void notifyRequestsOfShutdown() {
        assert isShutdown() : "Requests should only be notified if the executor is shutting down";

        for (var endpoint : rateLimitGroupings.values()) {
            endpoint.notifyRequestsOfShutdown();
        }
    }

    // default for testing
    Integer remainingQueueCapacity(RequestManager requestManager) {
        var endpoint = rateLimitGroupings.get(requestManager.rateLimitGrouping());

        if (endpoint == null) {
            return null;
        }

        return endpoint.remainingCapacity();
    }

    // default for testing
    int numberOfRateLimitGroups() {
        return rateLimitGroupings.size();
    }

    /**
     * Execute the request at some point in the future.
     *
     * @param requestManager the http request to send
     * @param inferenceInputs the inputs to send in the request
     * @param timeout the maximum time to wait for this request to complete (failing or succeeding). Once the time elapses, the
     *                listener::onFailure is called with a {@link org.elasticsearch.ElasticsearchTimeoutException}.
     *                If null, then the request will wait forever
     * @param listener an {@link ActionListener<InferenceServiceResults>} for the response or failure
     */
    public void execute(
        RequestManager requestManager,
        InferenceInputs inferenceInputs,
        @Nullable TimeValue timeout,
        ActionListener<InferenceServiceResults> listener
    ) {
        var task = new RequestTask(
            requestManager,
            inferenceInputs,
            timeout,
            threadPool,
            // TODO when multi-tenancy (as well as batching) is implemented we need to be very careful that we preserve
            // the thread contexts correctly to avoid accidentally retrieving the credentials for the wrong user
            ContextPreservingActionListener.wrapPreservingContext(listener, threadPool.getThreadContext())
        );

        var endpoint = rateLimitGroupings.computeIfAbsent(requestManager.rateLimitGrouping(), key -> {
            var endpointHandler = new RateLimitingEndpointHandler(
                Integer.toString(requestManager.rateLimitGrouping().hashCode()),
                queueCreator,
                settings,
                requestSender,
                clock,
                requestManager.rateLimitSettings(),
                this::isShutdown,
                rateLimiterCreator
            );

            endpointHandler.init();
            return endpointHandler;
        });

        endpoint.enqueue(task);
    }

    /**
     * Provides rate limiting functionality for requests. A single {@link RateLimitingEndpointHandler} governs a group of requests.
     * This allows many requests to be serialized if they are being sent too fast. If the rate limit has not been met they will be sent
     * as soon as a thread is available.
     */
    private static class RateLimitingEndpointHandler {

        private static final TimeValue NO_TASKS_AVAILABLE = TimeValue.MAX_VALUE;
        private static final TimeValue EXECUTED_A_TASK = TimeValue.ZERO;
        private static final Logger logger = LogManager.getLogger(RateLimitingEndpointHandler.class);
        private static final int ACCUMULATED_TOKENS_LIMIT = 1;

        private final AdjustableCapacityBlockingQueue<RejectableTask> queue;
        private final Supplier<Boolean> isShutdownMethod;
        private final RequestSender requestSender;
        private final String id;
        private final AtomicReference<Instant> timeOfLastEnqueue = new AtomicReference<>();
        private final Clock clock;
        private final RateLimiter rateLimiter;
        private final RequestExecutorServiceSettings requestExecutorServiceSettings;

        RateLimitingEndpointHandler(
            String id,
            AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> createQueue,
            RequestExecutorServiceSettings settings,
            RequestSender requestSender,
            Clock clock,
            RateLimitSettings rateLimitSettings,
            Supplier<Boolean> isShutdownMethod,
            RateLimiterCreator rateLimiterCreator
        ) {
            this.requestExecutorServiceSettings = Objects.requireNonNull(settings);
            this.id = Objects.requireNonNull(id);
            this.queue = new AdjustableCapacityBlockingQueue<>(createQueue, settings.getQueueCapacity());
            this.requestSender = Objects.requireNonNull(requestSender);
            this.clock = Objects.requireNonNull(clock);
            this.isShutdownMethod = Objects.requireNonNull(isShutdownMethod);

            Objects.requireNonNull(rateLimitSettings);
            Objects.requireNonNull(rateLimiterCreator);
            rateLimiter = rateLimiterCreator.create(
                ACCUMULATED_TOKENS_LIMIT,
                rateLimitSettings.requestsPerTimeUnit(),
                rateLimitSettings.timeUnit()
            );

        }

        public void init() {
            requestExecutorServiceSettings.registerQueueCapacityCallback(id, this::onCapacityChange);
        }

        private void onCapacityChange(int capacity) {
            logger.debug(() -> Strings.format("Executor service grouping [%s] setting queue capacity to [%s]", id, capacity));

            try {
                queue.setCapacity(capacity);
            } catch (Exception e) {
                logger.warn(format("Executor service grouping [%s] failed to set the capacity of the task queue to [%s]", id, capacity), e);
            }
        }

        public int queueSize() {
            return queue.size();
        }

        public boolean isShutdown() {
            return isShutdownMethod.get();
        }

        public Instant timeOfLastEnqueue() {
            return timeOfLastEnqueue.get();
        }

        public synchronized TimeValue executeEnqueuedTask() {
            try {
                return executeEnqueuedTaskInternal();
            } catch (Exception e) {
                logger.warn(format("Executor service grouping [%s] failed to execute request", id), e);
                // we tried to do some work but failed, so we'll say we did something to try looking for more work
                return EXECUTED_A_TASK;
            }
        }

        private TimeValue executeEnqueuedTaskInternal() {
            var timeBeforeAvailableToken = rateLimiter.timeToReserve(1);
            if (shouldExecuteImmediately(timeBeforeAvailableToken) == false) {
                return timeBeforeAvailableToken;
            }

            var task = queue.poll();

            // TODO Batching - in a situation where no new tasks are queued we'll want to execute any prepared tasks
            // So we'll need to check for null and call a helper method executePreparedTasks()

            if (shouldExecuteTask(task) == false) {
                return NO_TASKS_AVAILABLE;
            }

            // We should never have to wait because we checked above
            var reserveRes = rateLimiter.reserve(1);
            assert shouldExecuteImmediately(reserveRes) : "Reserving request tokens required a sleep when it should not have";

            task.getRequestManager()
                .execute(task.getInferenceInputs(), requestSender, task.getRequestCompletedFunction(), task.getListener());
            return EXECUTED_A_TASK;
        }

        private static boolean shouldExecuteTask(RejectableTask task) {
            return task != null && isNoopRequest(task) == false && task.hasCompleted() == false;
        }

        private static boolean isNoopRequest(InferenceRequest inferenceRequest) {
            return inferenceRequest.getRequestManager() == null
                || inferenceRequest.getInferenceInputs() == null
                || inferenceRequest.getListener() == null;
        }

        private static boolean shouldExecuteImmediately(TimeValue delay) {
            return delay.duration() == 0;
        }

        public void enqueue(RequestTask task) {
            timeOfLastEnqueue.set(Instant.now(clock));

            if (isShutdown()) {
                EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                    format(
                        "Failed to enqueue task for inference id [%s] because the request service [%s] has already shutdown",
                        task.getRequestManager().inferenceEntityId(),
                        id
                    ),
                    true
                );

                task.onRejection(rejected);
                return;
            }

            var addedToQueue = queue.offer(task);

            if (addedToQueue == false) {
                EsRejectedExecutionException rejected = new EsRejectedExecutionException(
                    format(
                        "Failed to execute task for inference id [%s] because the request service [%s] queue is full",
                        task.getRequestManager().inferenceEntityId(),
                        id
                    ),
                    false
                );

                task.onRejection(rejected);
            } else if (isShutdown()) {
                notifyRequestsOfShutdown();
            }
        }

        public synchronized void notifyRequestsOfShutdown() {
            assert isShutdown() : "Requests should only be notified if the executor is shutting down";

            try {
                List<RejectableTask> notExecuted = new ArrayList<>();
                queue.drainTo(notExecuted);

                rejectTasks(notExecuted);
            } catch (Exception e) {
                logger.warn(format("Failed to notify tasks of executor service grouping [%s] shutdown", id));
            }
        }

        private void rejectTasks(List<RejectableTask> tasks) {
            for (var task : tasks) {
                rejectTaskForShutdown(task);
            }
        }

        private void rejectTaskForShutdown(RejectableTask task) {
            try {
                task.onRejection(
                    new EsRejectedExecutionException(
                        format(
                            "Failed to send request, request service [%s] for inference id [%s] has shutdown prior to executing request",
                            id,
                            task.getRequestManager().inferenceEntityId()
                        ),
                        true
                    )
                );
            } catch (Exception e) {
                logger.warn(
                    format(
                        "Failed to notify request for inference id [%s] of rejection after executor service grouping [%s] shutdown",
                        task.getRequestManager().inferenceEntityId(),
                        id
                    )
                );
            }
        }

        public int remainingCapacity() {
            return queue.remainingCapacity();
        }

        public void close() {
            requestExecutorServiceSettings.deregisterQueueCapacityCallback(id);
        }
    }
}
