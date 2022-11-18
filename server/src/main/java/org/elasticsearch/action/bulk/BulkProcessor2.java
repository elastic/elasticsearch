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
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

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
        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
        private final Listener listener;
        private final ThreadPool threadPool;
        private final Runnable onClose;
        private int maxRequestsInBulk = 1000;
        private ByteSizeValue maxBulkSizeInBytes = new ByteSizeValue(5, ByteSizeUnit.MB);
        private ByteSizeValue maxBytesInFlight = new ByteSizeValue(50, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;
        int maxNumberOfRetries = 3;

        private Builder(
            BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
            Listener listener,
            Runnable onClose,
            ThreadPool threadPool
        ) {
            this.consumer = consumer;
            this.listener = listener;
            this.threadPool = threadPool;
            this.onClose = onClose;
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
         * Sets the maximum number of times a BulkLoad will be retried if it fails.
         */
        public Builder setMaxNumberOfRetries(int maxNumberOfRetries) {
            assert maxNumberOfRetries >= 0;
            this.maxNumberOfRetries = maxNumberOfRetries;
            return this;
        }

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
                threadPool,
                onClose
            );
        }
    }

    /**
     * @param consumer The consumer that is called to fulfil bulk operations
     * @param listener The BulkProcessor2 listener that gets called on bulk events
     * @param threadPool The threadpool to use for this bulk processor
     * @return the builder for BulkProcessor2
     */
    public static Builder builder(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        Listener listener,
        ThreadPool threadPool
    ) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");
        return new Builder(consumer, listener, () -> {
            // TODO: stop the flush
        }, threadPool);
    }

    private final int bulkActions;
    private final long bulkSize;
    private final long maxBytesInFlight;

    private final Scheduler.Cancellable cancellableFlushTask;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;
    private final BulkRequestHandler2 bulkRequestHandler;
    private final Runnable onClose;

    private volatile boolean closed = false;
    /*
     * This mutex is used to protect two things related to the bulkRequest object: (1) it makes sure that two threads do not add requests
     * to the BulkRequest at the same time since BulkRequest is not threadsafe and (2) it makes sure that no other thread is writing to
     * the BulkRequest when we swap the bulkRequest variable over to a new BulkRequest object.
     */
    private final Object mutex = new Object();

    BulkProcessor2(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        int maxNumberOfRetries,
        Listener listener,
        int bulkActions,
        ByteSizeValue bulkSize,
        ByteSizeValue maxBytesInFlight,
        @Nullable TimeValue flushInterval,
        ThreadPool threadPool,
        Runnable onClose
    ) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.getBytes();
        this.maxBytesInFlight = maxBytesInFlight.getBytes();
        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler = new BulkRequestHandler2(consumer, maxNumberOfRetries, maxBytesInFlight, listener);
        // Start period flushing task after everything is setup
        this.cancellableFlushTask = startFlushTask(flushInterval, threadPool);
        this.onClose = onClose;
    }

    /**
     * Closes the processor. Waits up to 1s to clear out any queued requests.
     */
    @Override
    public void close() {
        try {
            awaitClose(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        } catch (TimeoutException e) {
            throw new RuntimeException(e);
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
    public void awaitClose(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        synchronized (mutex) {
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
        // bulkRequest and instance swapping is not threadsafe, so execute the mutations under a mutex.
        // once the bulk request is ready to be shipped swap the instance reference unlock and send the local reference to the handler.
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        synchronized (mutex) {
            ensureOpen();
            bulkRequest.add(request);
            bulkRequestToExecute = newBulkRequestIfNeeded();
        }
        // execute sending the local reference outside the lock to allow handler to control the concurrency via it's configuration.
        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
    }

    private Scheduler.Cancellable startFlushTask(TimeValue flushInterval, ThreadPool threadPool) {
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
        return threadPool.scheduleWithFixedDelay(() -> {
            synchronized (mutex) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            }
        }, flushInterval, ThreadPool.Names.GENERIC);
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
}
