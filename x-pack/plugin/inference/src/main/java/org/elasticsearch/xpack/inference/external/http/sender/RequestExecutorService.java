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
import org.elasticsearch.inference.InputType;
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
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
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
 *
 * The request flow looks as follows:
 *
 *                     -------------> Add request to fast-path request queue.
 *                     |
 *                     |
 *              request NOT supporting
 *                 rate limiting
 *                     |
 *                     |
 * Request ------------|
 *                     |
 *                     |
 *              request supporting
 *                 rate limiting
 *                     |
 *                     |
 *                     ------------> {Rate Limit Group 1 -> Queue 1, ..., Rate Limit Group N -> Queue N}
 *
 *                                   Explanation: Submit request to the queue for the specific rate limiting group.
 *                                   The rate limiting groups are polled at the same specified interval,
 *                                   which in the worst cases introduces an additional latency of
 *                                   {@link RequestExecutorServiceSettings#getTaskPollFrequency()}.
 */
public class RequestExecutorService implements RequestExecutor {

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

    // TODO: for later (after 8.18)
    // TODO: pass in divisor to RateLimiterCreator
    // TODO: another map for service/task-type-key -> set of RateLimitingEndpointHandler (used for updates; update divisor and then update
    // all endpoint handlers)
    // TODO: one map for service/task-type-key -> divisor (this gets also read when we create an inference endpoint)
    // TODO: divisor value read/writes need to be synchronized in some way

    // default for testing
    static final RateLimiterCreator DEFAULT_RATE_LIMIT_CREATOR = RateLimiter::new;
    private static final Logger logger = LogManager.getLogger(RequestExecutorService.class);
    private static final TimeValue RATE_LIMIT_GROUP_CLEANUP_INTERVAL = TimeValue.timeValueDays(1);

    private final ConcurrentMap<Object, RateLimitingEndpointHandler> rateLimitGroupings = new ConcurrentHashMap<>();
    private final AtomicInteger rateLimitDivisor = new AtomicInteger(1);
    private final ThreadPool threadPool;
    private final CountDownLatch startupLatch;
    private final CountDownLatch terminationLatch = new CountDownLatch(1);
    private final RequestSender requestSender;
    private final RequestExecutorServiceSettings settings;
    private final Clock clock;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> queueCreator;
    private final RateLimiterCreator rateLimiterCreator;
    private final AtomicReference<Scheduler.Cancellable> cancellableCleanupTask = new AtomicReference<>();
    private final AtomicBoolean started = new AtomicBoolean(false);
    private final AdjustableCapacityBlockingQueue<RejectableTask> requestQueue;
    private volatile Future<?> requestQueueTask;

    public RequestExecutorService(
        ThreadPool threadPool,
        @Nullable CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        RequestSender requestSender
    ) {
        this(threadPool, DEFAULT_QUEUE_CREATOR, startupLatch, settings, requestSender, Clock.systemUTC(), DEFAULT_RATE_LIMIT_CREATOR);
    }

    public RequestExecutorService(
        ThreadPool threadPool,
        AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> queueCreator,
        @Nullable CountDownLatch startupLatch,
        RequestExecutorServiceSettings settings,
        RequestSender requestSender,
        Clock clock,
        RateLimiterCreator rateLimiterCreator
    ) {
        this.threadPool = Objects.requireNonNull(threadPool);
        this.queueCreator = Objects.requireNonNull(queueCreator);
        this.startupLatch = startupLatch;
        this.requestSender = Objects.requireNonNull(requestSender);
        this.settings = Objects.requireNonNull(settings);
        this.clock = Objects.requireNonNull(clock);
        this.rateLimiterCreator = Objects.requireNonNull(rateLimiterCreator);
        this.requestQueue = new AdjustableCapacityBlockingQueue<>(queueCreator, settings.getQueueCapacity());
    }

    public void shutdown() {
        if (shutdown.compareAndSet(false, true)) {
            if (requestQueueTask != null) {
                // Wakes up the queue in processRequestQueue
                requestQueue.offer(NOOP_TASK);
            }

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
        return requestQueue.size() + rateLimitGroupings.values().stream().mapToInt(RateLimitingEndpointHandler::queueSize).sum();
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
            startRequestQueueTask();
            signalStartInitiated();
            startHandlingRateLimitedTasks();
        } catch (Exception e) {
            logger.warn("Failed to start request executor", e);
            cleanup(CleanupStrategy.RATE_LIMITED_REQUEST_QUEUES_ONLY);
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

    private void startRequestQueueTask() {
        assert requestQueueTask == null : "The request queue can only be started once";
        requestQueueTask = threadPool.executor(UTILITY_THREAD_POOL_NAME).submit(this::processRequestQueue);
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

    private void scheduleNextHandleTasks(TimeValue timeToWait) {
        if (shutdown.get()) {
            logger.debug("Shutdown requested while scheduling next handle task call, cleaning up");
            cleanup(CleanupStrategy.RATE_LIMITED_REQUEST_QUEUES_ONLY);
            return;
        }

        threadPool.schedule(this::startHandlingRateLimitedTasks, timeToWait, threadPool.executor(UTILITY_THREAD_POOL_NAME));
    }

    private void processRequestQueue() {
        try {
            while (isShutdown() == false) {
                var task = requestQueue.take();

                if (task == NOOP_TASK) {
                    if (isShutdown()) {
                        logger.debug("Shutdown requested, exiting request queue processing");
                        break;
                    }

                    // Skip processing NoopTask
                    continue;
                }

                if (isShutdown()) {
                    logger.debug("Shutdown requested while handling request tasks, cleaning up");
                    rejectNonRateLimitedRequest(task);
                    break;
                }

                executeTaskImmediately(task);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.debug("Inference request queue interrupted, exiting");
        } catch (Exception e) {
            logger.error("Unexpected error processing request queue, terminating", e);
        } finally {
            cleanup(CleanupStrategy.REQUEST_QUEUE_ONLY);
        }
    }

    private void executeTaskImmediately(RejectableTask task) {
        try {
            task.getRequestManager()
                .execute(task.getInferenceInputs(), requestSender, task.getRequestCompletedFunction(), task.getListener());
        } catch (Exception e) {
            logger.warn(
                format("Failed to execute fast-path request for inference id [%s]", task.getRequestManager().inferenceEntityId()),
                e
            );

            var rejectionException = new EsRejectedExecutionException(
                format("Failed to execute request for inference id [%s]", task.getRequestManager().inferenceEntityId()),
                false
            );
            rejectionException.initCause(e);
            task.onRejection(rejectionException);
        }
    }

    // visible for testing
    void submitTaskToRateLimitedExecutionPath(RequestTask task) {
        var requestManager = task.getRequestManager();
        var endpoint = rateLimitGroupings.computeIfAbsent(requestManager.rateLimitGrouping(), key -> {
            var endpointHandler = new RateLimitingEndpointHandler(
                Integer.toString(requestManager.rateLimitGrouping().hashCode()),
                queueCreator,
                settings,
                requestSender,
                clock,
                requestManager.rateLimitSettings(),
                this::isShutdown,
                rateLimiterCreator,
                rateLimitDivisor.get()
            );

            endpointHandler.init();
            return endpointHandler;
        });

        endpoint.enqueue(task);
    }

    private static boolean isEmbeddingsIngestInput(InferenceInputs inputs) {
        return inputs instanceof EmbeddingsInput embeddingsInput && InputType.isIngest(embeddingsInput.getInputType());
    }

    private static boolean rateLimitingEnabled(RateLimitSettings rateLimitSettings) {
        return rateLimitSettings != null && rateLimitSettings.isEnabled();
    }

    private void cleanup(CleanupStrategy cleanupStrategy) {
        try {
            shutdown();

            switch (cleanupStrategy) {
                case RATE_LIMITED_REQUEST_QUEUES_ONLY -> notifyRateLimitedRequestsOfShutdown();
                case REQUEST_QUEUE_ONLY -> rejectRequestsInRequestQueue();
                default -> logger.error(Strings.format("Unknown clean up strategy for request executor: [%s]", cleanupStrategy.toString()));
            }

            terminationLatch.countDown();
        } catch (Exception e) {
            logger.warn("Encountered an error while cleaning up", e);
        }
    }

    private void startHandlingRateLimitedTasks() {
        try {
            TimeValue timeToWait;
            do {
                if (isShutdown()) {
                    logger.debug("Shutdown requested while handling rate limited tasks, cleaning up");
                    cleanup(CleanupStrategy.RATE_LIMITED_REQUEST_QUEUES_ONLY);
                    return;
                }

                timeToWait = settings.getTaskPollFrequency();
                for (var endpoint : rateLimitGroupings.values()) {
                    timeToWait = TimeValue.min(endpoint.executeEnqueuedTask(), timeToWait);
                }
                // if we execute a task the timeToWait will be 0 so we'll immediately look for more work
            } while (timeToWait.compareTo(TimeValue.ZERO) <= 0);

            scheduleNextHandleTasks(timeToWait);
        } catch (Exception e) {
            logger.warn("Encountered an error while handling rate limited tasks", e);
            cleanup(CleanupStrategy.RATE_LIMITED_REQUEST_QUEUES_ONLY);
        }
    }

    private void notifyRateLimitedRequestsOfShutdown() {
        assert isShutdown() : "Requests should only be notified if the executor is shutting down";

        for (var endpoint : rateLimitGroupings.values()) {
            endpoint.notifyRequestsOfShutdown();
        }
    }

    private void rejectRequestsInRequestQueue() {
        assert isShutdown() : "Requests in request queue should only be notified if the executor is shutting down";

        List<RejectableTask> requests = new ArrayList<>();
        requestQueue.drainTo(requests);

        for (var request : requests) {
            // NoopTask does not implement being rejected, therefore we need to skip it
            if (request != NOOP_TASK) {
                rejectNonRateLimitedRequest(request);
            }
        }
    }

    private void rejectNonRateLimitedRequest(RejectableTask task) {
        var inferenceEntityId = task.getRequestManager().inferenceEntityId();

        rejectRequest(
            task,
            format(
                "Failed to send request for inference id [%s] because the request executor service has been shutdown",
                inferenceEntityId
            ),
            format("Failed to notify request for inference id [%s] of rejection after executor service shutdown", inferenceEntityId)
        );
    }

    private static void rejectRequest(RejectableTask task, String rejectionMessage, String rejectionFailedMessage) {
        try {
            task.onRejection(new EsRejectedExecutionException(rejectionMessage, true));
        } catch (Exception e) {
            logger.warn(rejectionFailedMessage);
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

        if (isShutdown()) {
            task.onRejection(
                new EsRejectedExecutionException(
                    format(
                        "Failed to enqueue request task for inference id [%s] because the request executor service has been shutdown",
                        requestManager.inferenceEntityId()
                    ),
                    true
                )
            );
            return;
        }

        if (isEmbeddingsIngestInput(inferenceInputs) || rateLimitingEnabled(requestManager.rateLimitSettings())) {
            submitTaskToRateLimitedExecutionPath(task);
        } else {
            boolean taskAccepted = requestQueue.offer(task);

            if (taskAccepted == false) {
                task.onRejection(
                    new EsRejectedExecutionException(
                        format("Failed to enqueue request task for inference id [%s]", requestManager.inferenceEntityId()),
                        false
                    )
                );
            }
        }
    }

    /**
     * Provides rate limiting functionality for requests. A single {@link RateLimitingEndpointHandler} governs a group of requests.
     * This allows many requests to be serialized if they are being sent too fast. If the rate limit has not been met they will be sent
     * as soon as a thread is available.
     */
    static class RateLimitingEndpointHandler {

        private static final TimeValue NO_TASKS_AVAILABLE = TimeValue.MAX_VALUE;
        private static final TimeValue EXECUTED_A_TASK = TimeValue.ZERO;
        private static final Logger logger = LogManager.getLogger(RateLimitingEndpointHandler.class);
        private static final int ACCUMULATED_TOKENS_LIMIT = 1;

        private final AdjustableCapacityBlockingQueue<RejectableTask> queue;
        private final Supplier<Boolean> isShutdownMethod;
        private final RequestSender requestSender;
        private final String rateLimitGroupingId;
        private final AtomicReference<Instant> timeOfLastEnqueue = new AtomicReference<>();
        private final Clock clock;
        private final RateLimiter rateLimiter;
        private final RequestExecutorServiceSettings requestExecutorServiceSettings;
        private final RateLimitSettings rateLimitSettings;
        private final Long originalRequestsPerTimeUnit;

        RateLimitingEndpointHandler(
            String rateLimitGroupingId,
            AdjustableCapacityBlockingQueue.QueueCreator<RejectableTask> createQueue,
            RequestExecutorServiceSettings settings,
            RequestSender requestSender,
            Clock clock,
            RateLimitSettings rateLimitSettings,
            Supplier<Boolean> isShutdownMethod,
            RateLimiterCreator rateLimiterCreator,
            Integer rateLimitDivisor
        ) {
            this.requestExecutorServiceSettings = Objects.requireNonNull(settings);
            this.rateLimitGroupingId = Objects.requireNonNull(rateLimitGroupingId);
            this.queue = new AdjustableCapacityBlockingQueue<>(createQueue, settings.getQueueCapacity());
            this.requestSender = Objects.requireNonNull(requestSender);
            this.clock = Objects.requireNonNull(clock);
            this.isShutdownMethod = Objects.requireNonNull(isShutdownMethod);
            this.rateLimitSettings = Objects.requireNonNull(rateLimitSettings);
            this.originalRequestsPerTimeUnit = rateLimitSettings.requestsPerTimeUnit();

            Objects.requireNonNull(rateLimitSettings);
            Objects.requireNonNull(rateLimiterCreator);
            rateLimiter = rateLimiterCreator.create(
                ACCUMULATED_TOKENS_LIMIT,
                rateLimitSettings.requestsPerTimeUnit(),
                rateLimitSettings.timeUnit()
            );
        }

        public void init() {
            requestExecutorServiceSettings.registerQueueCapacityCallback(rateLimitGroupingId, this::onCapacityChange);
        }

        private void onCapacityChange(int capacity) {
            logger.debug(
                () -> Strings.format("Executor service grouping [%s] setting queue capacity to [%s]", rateLimitGroupingId, capacity)
            );

            try {
                queue.setCapacity(capacity);
            } catch (Exception e) {
                logger.warn(
                    format(
                        "Executor service grouping [%s] failed to set the capacity of the task queue to [%s]",
                        rateLimitGroupingId,
                        capacity
                    ),
                    e
                );
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
                logger.warn(format("Executor service grouping [%s] failed to execute request", rateLimitGroupingId), e);
                // we tried to do some work but failed, so we'll say we did something to try looking for more work
                return EXECUTED_A_TASK;
            }
        }

        private TimeValue executeEnqueuedTaskInternal() {
            if (rateLimitSettings.isEnabled()) {
                var timeBeforeAvailableToken = rateLimiter.timeToReserve(1);
                if (shouldExecuteImmediately(timeBeforeAvailableToken) == false) {
                    return timeBeforeAvailableToken;
                }
            }

            var task = queue.poll();

            // TODO Batching - in a situation where no new tasks are queued we'll want to execute any prepared tasks
            // So we'll need to check for null and call a helper method executePreparedTasks()

            if (shouldExecuteTask(task) == false) {
                return NO_TASKS_AVAILABLE;
            }

            if (rateLimitSettings.isEnabled()) {
                // We should never have to wait because we checked above
                var reserveRes = rateLimiter.reserve(1);
                assert shouldExecuteImmediately(reserveRes) : "Reserving request tokens required a sleep when it should not have";
            }

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
                        rateLimitGroupingId
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
                        rateLimitGroupingId
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
                logger.warn(format("Failed to notify tasks of executor service grouping [%s] shutdown", rateLimitGroupingId));
            }
        }

        private void rejectTasks(List<RejectableTask> tasks) {
            for (var task : tasks) {
                var inferenceEntityId = task.getRequestManager().inferenceEntityId();

                rejectRequest(
                    task,
                    format(
                        "Failed to send request, request service [%s] for inference id [%s] has shutdown prior to executing request",
                        rateLimitGroupingId,
                        inferenceEntityId
                    ),
                    format(
                        "Failed to notify request for inference id [%s] of rejection after executor service grouping [%s] shutdown",
                        inferenceEntityId,
                        rateLimitGroupingId
                    )
                );
            }
        }

        public int remainingCapacity() {
            return queue.remainingCapacity();
        }

        public void close() {
            requestExecutorServiceSettings.deregisterQueueCapacityCallback(rateLimitGroupingId);
        }
    }

    private static final RejectableTask NOOP_TASK = new RejectableTask() {
        @Override
        public void onRejection(Exception e) {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }

        @Override
        public RequestManager getRequestManager() {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }

        @Override
        public InferenceInputs getInferenceInputs() {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }

        @Override
        public ActionListener<InferenceServiceResults> getListener() {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }

        @Override
        public boolean hasCompleted() {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }

        @Override
        public Supplier<Boolean> getRequestCompletedFunction() {
            throw new UnsupportedOperationException("NoopTask is a pure marker class for signals in the request queue");
        }
    };

    private enum CleanupStrategy {
        REQUEST_QUEUE_ONLY,
        RATE_LIMITED_REQUEST_QUEUES_ONLY
    }
}
