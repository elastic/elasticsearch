/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor2 implements Closeable {

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
        void afterBulk(long executionId, BulkRequest request, Exception failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final Listener listener;
        private final ThreadPool threadPool;
        private int maxRequestsInBulk = 1000;
        private ByteSizeValue maxBulkSizeInBytes = new ByteSizeValue(5, ByteSizeUnit.MB);
        private ByteSizeValue maxBytesInFlight = new ByteSizeValue(50, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;
        private int maxNumberOfRetries = 3;

        private Builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener, ThreadPool threadPool) {
            this.consumer = consumer;
            this.listener = listener;
            this.threadPool = threadPool;
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
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
         * <p>
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to {@code -1} with the flush interval set allowing for complete async processing of bulk actions.
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Sets the maximum number of times a BulkRequest will be retried if it fails.
         */
        public Builder setMaxNumberOfRetries(int maxNumberOfRetries) {
            assert maxNumberOfRetries >= 0;
            this.maxNumberOfRetries = maxNumberOfRetries;
            return this;
        }

        /**
         * Sets the maximum number of bytes allowed in in-flight requests (both the BulkRequest being built up by the BulkProcessor and
         * any BulkRequests sent to Retry2 that have not yet completed) before subsequent calls to add()result in
         * EsRejectedExecutionException. Defaults to 50mb.
         */
        public Builder setMaxBytesInFlight(ByteSizeValue maxBytesInFlight) {
            this.maxBytesInFlight = maxBytesInFlight;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkProcessor2 build() {
            return new BulkProcessor2(
                consumer,
                maxNumberOfRetries,
                listener,
                maxRequestsInBulk,
                maxBulkSizeInBytes,
                maxBytesInFlight,
                flushInterval,
                threadPool
            );
        }
    }

    /**
     * @param consumer The consumer that is called to fulfil bulk operations. This consumer _must_ operate either very fast or
     *                 asynchronously.
     * @param listener The BulkProcessor2 listener that gets called on bulk events
     * @param threadPool The threadpool used to schedule the flush task for this bulk processor, if flushInterval is not null.
     * @return the builder for BulkProcessor2
     */
    public static Builder builder(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        Listener listener,
        ThreadPool threadPool
    ) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");
        return new Builder(consumer, listener, threadPool);
    }

    private final int maxActionsPerBulkRequest;
    private final long maxBulkSizeBytes;
    private final ByteSizeValue maxBytesInFlight;
    /*
     * This is the approximate total number of bytes in in-flight requests, both in the BulkRequest that it is building up and in all of
     * the BulkRequests that it has sent to Retry2 that have not completed yet. If this number would exceeds maxBytesInFlight, then calls
     * to add() will throw EsRejectedExecutionExceptions.
     */
    private final AtomicLong totalBytesInFlight = new AtomicLong(0);

    /**
     * This is a task (which might be null) that is scheduled at some pont in the future to flush the bulk request and start a new bulk
     * request. This variable is read and written to from multiple threads, and is protected by mutex.
     */
    private volatile Scheduler.Cancellable cancellableFlushTask = null;

    private final AtomicLong executionIdGen = new AtomicLong();

    private static final Logger logger = LogManager.getLogger(BulkProcessor2.class);

    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
    private final Listener listener;

    private final Retry2 retry;

    private final TimeValue flushInterval;

    private final ThreadPool threadPool;

    /*
     * This is the BulkRequest that is being built up by this class in calls to the various add methods.
     */
    private BulkRequest bulkRequestUnderConstruction;

    private volatile boolean closed = false;
    /*
     * This mutex is used to protect two things related to the bulkRequest object: (1) it makes sure that two threads do not add requests
     * to the BulkRequest at the same time since BulkRequest is not threadsafe and (2) it makes sure that no other thread is writing to
     * the BulkRequest when we swap the bulkRequest variable over to a new BulkRequest object. It also protects access to
     * cancellableFlushTask.
     */
    private final Object mutex = new Object();

    /*
     * This Lock and Condition are used to throttle calls to bulkProcessor.addWithBackpressure when the bulkProcessor already has too many
     * bytes in flight and cannot accept more data until already-in-flight requests complete.
     */
    private final Lock backpressureLock = new ReentrantLock();
    private final Condition backpressureNotRequiredCondition = backpressureLock.newCondition();

    BulkProcessor2(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        int maxNumberOfRetries,
        Listener listener,
        int maxActionsPerBulkRequest,
        ByteSizeValue maxBulkSize,
        ByteSizeValue maxBytesInFlight,
        @Nullable TimeValue flushInterval,
        ThreadPool threadPool
    ) {
        this.maxActionsPerBulkRequest = maxActionsPerBulkRequest;
        this.maxBulkSizeBytes = maxBulkSize.getBytes();
        this.maxBytesInFlight = maxBytesInFlight;
        this.bulkRequestUnderConstruction = new BulkRequest();
        this.consumer = consumer;
        this.listener = listener;
        this.retry = new Retry2(maxNumberOfRetries);
        this.flushInterval = flushInterval;
        this.threadPool = threadPool;
    }

    /**
     * Closes the processor. Any remaining bulk actions are flushed if they can be flushed in the given time.
     * <p>
     * Waits for up to the specified timeout for all bulk requests to complete then returns
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit    The time unit of the {@code timeout} argument
     * @return True if the bulk processor was able to be closed in the given time, false otherwise
     * @throws InterruptedException If the current thread is interrupted
     */
    public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        synchronized (mutex) {
            if (closed) {
                return true;
            }
            closed = true;

            if (cancellableFlushTask != null) {
                cancellableFlushTask.cancel();
            }

            if (bulkRequestUnderConstruction.numberOfActions() > 0) {
                execute();
            }
            return this.retry.awaitClose(timeout, unit);
        }
    }

    /**
     * This method flushes any requests, prevents any future retries, closes this processor, and returns once any pending requests complete.
     */
    public void close() {
        try {
            awaitClose(30, TimeUnit.SECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     * @throws EsRejectedExecutionException if adding the approximate size in bytes of the request to totalBytesInFlight would exceed
     * maxBytesInFlight
     */
    public BulkProcessor2 add(IndexRequest request) throws EsRejectedExecutionException {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * This method is similar to {@link #add(IndexRequest) add()}, except that if adding the approximate size in bytes of the request to
     * totalBytesInFlight would exceed maxBytesInFlight then this method will block until the request can be added without exceeding
     * maxBytesInFlight (or until shouldAbort returns false). This method should be used carefully, ideally from a single thread. This is
     * because calling it with multiple threads would either hang up all the threads in the pool (in the case of a bounded thread pool) or
     * effectively create an unbounded queue (in the case of an unbounded thread pool).
     * @param request The request to add to a batch to be consumed
     * @param shouldAbort If this returns true then this method bails out with an EsRejectedExecutionException
     * @return this BulkProcessor2
     * @throws EsRejectedExecutionException if shouldAbort returns true before the request has been added to a batch
     */
    public BulkProcessor2 addWithBackpressure(IndexRequest request, Supplier<Boolean> shouldAbort) throws EsRejectedExecutionException {
        return addWithBackpressure((DocWriteRequest<?>) request, shouldAbort);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     * @throws EsRejectedExecutionException if adding the approximate size in bytes of the request to totalBytesInFlight would exceed
     * maxBytesInFlight
     */
    public BulkProcessor2 add(DeleteRequest request) throws EsRejectedExecutionException {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * Adds either a delete or an index request.
     * @throws EsRejectedExecutionException if the total bytes already in flight exceeds maxBytesInFlight. In this case, the request will
     * not be retried and it is on the client to decide whether to wait and try later.
     */
    private BulkProcessor2 add(DocWriteRequest<?> request) throws EsRejectedExecutionException {
        internalAdd(request);
        return this;
    }

    private BulkProcessor2 addWithBackpressure(DocWriteRequest<?> request, Supplier<Boolean> shouldAbort)
        throws EsRejectedExecutionException {
        /*
         * We want this method to block until the bulkProcessor accepts the request. Otherwise, the subsequent calls to this method will
         * likely continue rejecting. BulkProcessor2.add does not have the ability to exert backpressure.
         * So we have to catch the EsRejectedExecutionException that is thrown when it already has too many bytes in flight. We then
         * wait until some bulk has been completed, reducing the amount of data in flight and (probably) making room for this request.
         */
        boolean successfullyAdded = false;
        while (successfullyAdded == false) {
            if (shouldAbort.get()) {
                throw new EsRejectedExecutionException("Rejecting request because bulk add has been cancelled by the caller");
            }
            try {
                add(request);
                successfullyAdded = true;
            } catch (EsRejectedExecutionException e) {
                logger.trace("Attempt to add request to batch rejected because too many bytes are in flight already. Will try again.");
                /*
                 * Note: It is possible that signalAll was called between the call to add above and acquiring this lock.
                 * But in that case, either we wait 500ms, or another batch completes and another call to signalAll wakes us up.
                 * Either way is preferable to requiring this lock for the whole try/catch block. That is why we ignore the
                 * result of the call to await() -- either way we are going to try calling bulkProcessor.add() again.
                 */
                backpressureLock.lock();
                try {
                    backpressureNotRequiredCondition.await(500, TimeUnit.MILLISECONDS);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(ex);
                } finally {
                    backpressureLock.unlock();
                }
            }
        }
        return this;
    }

    /*
     * Exposed for unit testing
     */
    long getTotalBytesInFlight() {
        return totalBytesInFlight.get();
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private void internalAdd(DocWriteRequest<?> request) throws EsRejectedExecutionException {
        // bulkRequest and instance swapping is not threadsafe, so execute the mutations under a mutex.
        // once the bulk request is ready to be shipped swap the instance reference unlock and send the local reference to the handler.
        Tuple<BulkRequest, Long> bulkRequestToExecute;
        synchronized (mutex) {
            ensureOpen();
            if (totalBytesInFlight.get() >= maxBytesInFlight.getBytes()) {
                throw new EsRejectedExecutionException(
                    "Cannot index request of size "
                        + bulkRequestUnderConstruction.estimatedSizeInBytes()
                        + " because "
                        + totalBytesInFlight.get()
                        + " bytes are already in flight and the max is "
                        + maxBytesInFlight
                );
            }
            long bytesBeforeNewRequest = bulkRequestUnderConstruction.estimatedSizeInBytes();
            bulkRequestUnderConstruction.add(request);
            totalBytesInFlight.addAndGet(bulkRequestUnderConstruction.estimatedSizeInBytes() - bytesBeforeNewRequest);
            bulkRequestToExecute = newBulkRequestIfNeeded();
        }
        // execute sending the local reference outside the lock to allow handler to control the concurrency via it's configuration.
        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
        /*
         * We could have the flush task running nonstop, checking every flushInterval whether there was data to flush. But there is
         * likely to not be data almost all of the time, so this would waste a thread's time. So instead we schedule a flush task
         * whenever we add data. If a task is already scheduled, it does nothing. Since both the cancellableFlushTask and the
         * bulkRequestUnderConstruction are protected by the same mutex, there is no risk that a request will be left hanging.
         */
        scheduleFlushTask();
    }

    /**
     * This method schedules a flush task to run flushInterval in the future if flushInterval is not null and if there is not already a
     * flush task scheduled.
     */
    private void scheduleFlushTask() {
        if (flushInterval == null) {
            return;
        }
        /*
         * This method is called from multiple threads. We synchronize on mutex here so that we are sure that cancellableFlushTask is not
         * changed between when we check it and when we set it (whether that is a transition from null -> not null from another thread
         * in this method or a change from not null -> null from the scheduled task).
         */
        synchronized (mutex) {
            if (cancellableFlushTask == null) {
                cancellableFlushTask = threadPool.schedule(() -> {
                    synchronized (mutex) {
                        if (closed == false && bulkRequestUnderConstruction.numberOfActions() > 0) {
                            execute();
                        }
                        cancellableFlushTask = null;
                    }
                }, flushInterval, ThreadPool.Names.GENERIC);
            }
        }
    }

    private Tuple<BulkRequest, Long> newBulkRequestIfNeeded() {
        assert Thread.holdsLock(mutex);
        ensureOpen();
        if (bulkRequestExceedsLimits() || totalBytesInFlight.get() >= maxBytesInFlight.getBytes()) {
            final BulkRequest bulkRequest = this.bulkRequestUnderConstruction;
            this.bulkRequestUnderConstruction = new BulkRequest();
            return new Tuple<>(bulkRequest, executionIdGen.incrementAndGet());
        }
        return null;
    }

    /**
     * This method sends the bulkRequest to the consumer up to maxNumberOfRetries times. The executionId is used to notify the listener
     * both before and after the request.
     * @param bulkRequest
     * @param executionId
     */
    private void execute(BulkRequest bulkRequest, long executionId) {
        try {
            listener.beforeBulk(executionId, bulkRequest);
            retry.consumeRequestWithRetries(consumer, bulkRequest, new ActionListener<>() {
                @Override
                public void onResponse(BulkResponse response) {
                    totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
                    maybeNoLongerInExcessofMaxBytesInFlight();
                    listener.afterBulk(executionId, bulkRequest, response);
                }

                @Override
                public void onFailure(Exception e) {
                    totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
                    maybeNoLongerInExcessofMaxBytesInFlight();
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            });
        } catch (Exception e) {
            logger.warn(() -> "Failed to execute bulk request " + executionId + ".", e);
            totalBytesInFlight.addAndGet(-1 * bulkRequest.estimatedSizeInBytes());
            maybeNoLongerInExcessofMaxBytesInFlight();
            listener.afterBulk(executionId, bulkRequest, e);
        }
    }

    /*
     * This method is to be called whenever we deduct from totalBytesInFlight, signalling that it's worth retrying any requests blocked in
     * addWithBackpressure.
     */
    private void maybeNoLongerInExcessofMaxBytesInFlight() {
        // Signal in case any rejected docs were waiting for the space in bulkProcessor in addWithBackPressure
        backpressureLock.lock();
        try {
            backpressureNotRequiredCondition.signalAll();
        } finally {
            backpressureLock.unlock();
        }
    }

    private void execute() {
        assert Thread.holdsLock(mutex);
        final BulkRequest bulkRequest = this.bulkRequestUnderConstruction;
        final long executionId = executionIdGen.incrementAndGet();
        this.bulkRequestUnderConstruction = new BulkRequest();
        execute(bulkRequest, executionId);
    }

    private boolean bulkRequestExceedsLimits() {
        assert Thread.holdsLock(mutex);
        if (maxActionsPerBulkRequest != -1 && bulkRequestUnderConstruction.numberOfActions() >= maxActionsPerBulkRequest) {
            return true;
        }
        return maxBulkSizeBytes != -1 && bulkRequestUnderConstruction.estimatedSizeInBytes() >= maxBulkSizeBytes;
    }
}
