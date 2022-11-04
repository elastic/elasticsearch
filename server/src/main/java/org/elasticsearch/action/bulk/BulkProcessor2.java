/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor2 implements Closeable {

    static final String FLUSH_SCHEDULER_NAME_SUFFIX = "-flush-scheduler";
    static final String RETRY_SCHEDULER_NAME_SUFFIX = "-retry-scheduler";

    /**
     * A listener for the execution.
     */
    public interface Listener {

        /**
         * Callback before the bulk is executed.
         */
        void beforeBulk(long executionId, BulkRequest request);

        /**
         * Callback after a successful execution of bulk request.
         */
        void afterBulk(long executionId, BulkRequest request, BulkResponse response);

        /**
         * Callback after a failed execution of bulk request.
         * <p>
         * Note that in case an instance of <code>InterruptedException</code> is passed, which means that request processing has been
         * cancelled externally, the thread's interruption status has been restored prior to calling this method.
         */
        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final Listener listener;
        private final Scheduler flushScheduler;
        private final Scheduler retryScheduler;
        private final Runnable onClose;
        private int concurrentRequests = 1;
        private int maxRequestsInBulk = 1000;
        private ByteSizeValue maxBulkSizeInBytes = new ByteSizeValue(5, ByteSizeUnit.MB);
        private int maxBulkRequestQueueSize = 1000;
        private int maxBulkRequestRetryQueueSize = 1000;
        private TimeValue queuePollingInterval = TimeValue.timeValueMillis(100);
        private TimeValue flushInterval = null;
        private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();

        private Builder(
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            Listener listener,
            Scheduler flushScheduler,
            Scheduler retryScheduler,
            Runnable onClose
        ) {
            this.consumer = consumer;
            this.listener = listener;
            this.flushScheduler = flushScheduler;
            this.retryScheduler = retryScheduler;
            this.onClose = onClose;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to {@code 1}.
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * {@code 1000}. Can be set to {@code -1} to disable it.
         */
        public Builder setBulkActions(int bulkActions) {
            this.maxRequestsInBulk = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * {@code 5mb}. Can be set to {@code -1} to disable it.
         */
        public Builder setBulkSize(ByteSizeValue maxBulkSizeInBytes) {
            this.maxBulkSizeInBytes = maxBulkSizeInBytes;
            return this;
        }

        /**
         * Sets the maximum size of the queue of BulkRequests kept in memory to be loaded by this processor. Defaults to 1000.
         * @param maxBulkRequestQueueSize
         * @return
         */
        public Builder setMaxBulkRequestQueueSize(int maxBulkRequestQueueSize) {
            this.maxBulkRequestQueueSize = maxBulkRequestQueueSize;
            return this;
        }

        /**
         * Sets the maximum size of the queue of BulkRequests kept in memory that have failed and are to be retried at some point in the
         * future. Defaults to 1000.
         * @param maxBulkRequestRetryQueueSize
         * @return
         */
        public Builder setMaxBulkRequestRetryQueueSize(int maxBulkRequestRetryQueueSize) {
            this.maxBulkRequestRetryQueueSize = maxBulkRequestRetryQueueSize;
            return this;
        }

        /**
         * Sets the interval at which the two queues configured above are to be polled. Defaults to 100ms.
         * @param queuePollingInterval
         * @return
         */
        public Builder setQueuePollingInterval(TimeValue queuePollingInterval) {
            this.queuePollingInterval = queuePollingInterval;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending into a queue if the interval passes. Defaults to not set.
         * <p>
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to {@code -1} with the flush interval set allowing for complete async processing of bulk actions.
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Sets a custom backoff policy. The backoff policy defines how the bulk processor should handle retries of bulk requests internally
         * in case they have failed due to resource constraints (i.e. a thread pool was full).
         *
         * The default is to back off exponentially.
         *
         * @see org.elasticsearch.action.bulk.BackoffPolicy#exponentialBackoff()
         */
        public Builder setBackoffPolicy(BackoffPolicy backoffPolicy) {
            if (backoffPolicy == null) {
                throw new NullPointerException("'backoffPolicy' must not be null. To disable backoff, pass BackoffPolicy.noBackoff()");
            }
            this.backoffPolicy = backoffPolicy;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkProcessor2 build() {
            return new BulkProcessor2(
                consumer,
                backoffPolicy,
                listener,
                concurrentRequests,
                maxRequestsInBulk,
                maxBulkSizeInBytes,
                maxBulkRequestQueueSize,
                maxBulkRequestRetryQueueSize,
                queuePollingInterval,
                flushInterval,
                flushScheduler,
                retryScheduler,
                onClose
            );
        }

    }

    /**
     * @param consumer The consumer that is called to fulfil bulk operations
     * @param listener The BulkProcessor2 listener that gets called on bulk events
     * @param name     The name of this processor, e.g. to identify the scheduler threads
     * @return the builder for BulkProcessor2
     */
    public static Builder builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener, String name) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");
        final ScheduledThreadPoolExecutor flushScheduler = Scheduler.initScheduler(Settings.EMPTY, name + FLUSH_SCHEDULER_NAME_SUFFIX);
        final ScheduledThreadPoolExecutor retryScheduler = Scheduler.initScheduler(Settings.EMPTY, name + RETRY_SCHEDULER_NAME_SUFFIX);
        return new Builder(consumer, listener, buildScheduler(flushScheduler), buildScheduler(retryScheduler), () -> {
            Scheduler.terminate(flushScheduler, 10, TimeUnit.SECONDS);
            Scheduler.terminate(retryScheduler, 10, TimeUnit.SECONDS);
        });
    }

    private static Scheduler buildScheduler(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
        return (command, delay, executor) -> Scheduler.wrapAsScheduledCancellable(
            scheduledThreadPoolExecutor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS)
        );
    }

    private final int bulkActions;
    private final long bulkSize;

    private final Scheduler.Cancellable cancellableFlushTask;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;
    private final BulkRequestHandler2 bulkRequestHandler;
    private final Runnable onClose;

    private volatile boolean closed = false;
    private final ReentrantLock lock = new ReentrantLock();

    BulkProcessor2(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BackoffPolicy backoffPolicy,
        Listener listener,
        int concurrentRequests,
        int bulkActions,
        ByteSizeValue bulkSize,
        int maxBulkRequestQueueSize,
        int maxBulkRequestRetryQueueSize,
        TimeValue queuePollingInterval,
        @Nullable TimeValue flushInterval,
        Scheduler flushScheduler,
        Scheduler retryScheduler,
        Runnable onClose
    ) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.getBytes();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler = new BulkRequestHandler2(
            consumer,
            backoffPolicy,
            listener,
            retryScheduler,
            concurrentRequests,
            maxBulkRequestQueueSize,
            maxBulkRequestRetryQueueSize,
            queuePollingInterval
        );
        // Start period flushing task after everything is setup
        this.cancellableFlushTask = startFlushTask(flushInterval, flushScheduler);
        this.onClose = onClose;
    }

    /**
     * Closes the processor. Waits up to 10ms to clear out any queued requests.
     */
    @Override
    public void close() {
        try {
            awaitClose(10, TimeUnit.MILLISECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the processor. Any remaining bulk actions are flushed if they can be flushed in the given time.
     * <p>
     * Waits for up to the specified timeout for all bulk requests to complete then returns
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit    The time unit of the {@code timeout} argument
     * @throws InterruptedException If the current thread is interrupted
     */
    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        lock.lock();
        try {
            if (closed) {
                return;
            }
            closed = true;

            this.cancellableFlushTask.cancel();

            if (bulkRequest.numberOfActions() > 0) {
                execute();
            }
            try {
                this.bulkRequestHandler.awaitClose(timeout, unit);
            } finally {
                onClose.run();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor2 add(IndexRequest request) {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor2 add(DeleteRequest request) {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * Adds either a delete or an index request.
     */
    public BulkProcessor2 add(DocWriteRequest<?> request) {
        internalAdd(request);
        return this;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private void internalAdd(DocWriteRequest<?> request) {
        // bulkRequest and instance swapping is not threadsafe, so execute the mutations under a lock.
        // once the bulk request is ready to be shipped swap the instance reference unlock and send the local reference to the handler.
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        lock.lock();
        try {
            ensureOpen();
            bulkRequest.add(request);
            bulkRequestToExecute = newBulkRequestIfNeeded();
        } finally {
            lock.unlock();
        }
        // execute sending the local reference outside the lock to allow handler to control the concurrency via it's configuration.
        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
    }

    private Scheduler.Cancellable startFlushTask(TimeValue flushInterval, Scheduler scheduler) {
        if (flushInterval == null) {
            return new Scheduler.Cancellable() {
                @Override
                public boolean cancel() {
                    return false;
                }

                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
        }
        return scheduler.scheduleWithFixedDelay(new Flush(), flushInterval, ThreadPool.Names.GENERIC);
    }

    // needs to be executed under a lock
    private Tuple<BulkRequest, Long> newBulkRequestIfNeeded() {
        ensureOpen();
        if (isOverTheLimit() == false) {
            return null;
        }
        final BulkRequest bulkRequest = this.bulkRequest;
        this.bulkRequest = new BulkRequest();
        return new Tuple<>(bulkRequest, executionIdGen.incrementAndGet());
    }

    // may be executed without a lock
    private void execute(BulkRequest bulkRequest, long executionId) {
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    // needs to be executed under a lock
    private void execute() {
        final BulkRequest bulkRequest = this.bulkRequest;
        final long executionId = executionIdGen.incrementAndGet();

        this.bulkRequest = new BulkRequest();
        execute(bulkRequest, executionId);
    }

    // needs to be executed under a lock
    private boolean isOverTheLimit() {
        if (bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) {
            return true;
        }
        return bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize;
    }

    /**
     * Flush pending delete or index requests.
     */
    public void flush() {
        lock.lock();
        try {
            ensureOpen();
            if (bulkRequest.numberOfActions() > 0) {
                execute();
            }
        } finally {
            lock.unlock();
        }
    }

    class Flush implements Runnable {
        @Override
        public void run() {
            lock.lock();
            try {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            } finally {
                lock.unlock();
            }
        }
    }
}
