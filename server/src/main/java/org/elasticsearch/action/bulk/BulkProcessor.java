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
import org.elasticsearch.client.Client;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.RestApiVersion;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
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
public class BulkProcessor implements Closeable {

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
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;
        private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();
        private String globalIndex;
        private String globalRouting;
        private String globalPipeline;

        private Builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener,
                        Scheduler flushScheduler, Scheduler retryScheduler, Runnable onClose) {
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
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * {@code 5mb}. Can be set to {@code -1} to disable it.
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
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

        public Builder setGlobalIndex(String globalIndex) {
            this.globalIndex = globalIndex;
            return this;
        }

        public Builder setGlobalRouting(String globalRouting) {
            this.globalRouting = globalRouting;
            return this;
        }

        public Builder setGlobalPipeline(String globalPipeline) {
            this.globalPipeline = globalPipeline;
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
        public BulkProcessor build() {
            return new BulkProcessor(consumer, backoffPolicy, listener, concurrentRequests, bulkActions,
                bulkSize, flushInterval, flushScheduler, retryScheduler, onClose, createBulkRequestWithGlobalDefaults());
        }

        private Supplier<BulkRequest> createBulkRequestWithGlobalDefaults() {
            return () -> new BulkRequest(globalIndex)
                .pipeline(globalPipeline)
                .routing(globalRouting);
        }
    }

    /**
     * @param client The client that executes the bulk operations
     * @param listener The BulkProcessor listener that gets called on bulk events
     * @param flushScheduler The scheduler that is used to flush
     * @param retryScheduler The scheduler that is used for retries
     * @param onClose The runnable instance that is executed on close. Consumers are required to clean up the schedulers.
     * @return the builder for BulkProcessor
     */
    public static Builder builder(Client client, Listener listener, Scheduler flushScheduler, Scheduler retryScheduler, Runnable onClose) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(listener, "listener");
        return new Builder(client::bulk, listener, flushScheduler, retryScheduler, onClose);
    }


    /**
     * @param client The client that executes the bulk operations
     * @param listener The BulkProcessor listener that gets called on bulk events
     * @return the builder for BulkProcessor
     * @deprecated Use {@link #builder(BiConsumer, Listener, String)}
     * with client::bulk as the first argument, or {@link #builder(org.elasticsearch.client.Client,
     * org.elasticsearch.action.bulk.BulkProcessor.Listener, org.elasticsearch.threadpool.Scheduler,
     * org.elasticsearch.threadpool.Scheduler, java.lang.Runnable)} and manage the flush and retry schedulers explicitly
     */
    @Deprecated
    public static Builder builder(Client client, Listener listener) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(listener, "listener");
        return new Builder(client::bulk, listener, client.threadPool(), client.threadPool(), () -> {});
    }

    /**
     * @param consumer The consumer that is called to fulfil bulk operations
     * @param listener The BulkProcessor listener that gets called on bulk events
     * @return the builder for BulkProcessor
     * @deprecated use {@link #builder(BiConsumer, Listener, String)} instead
     */
    @Deprecated
    public static Builder builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener) {
        return builder(consumer, listener, "anonymous-bulk-processor");
    }

    /**
     * @param consumer The consumer that is called to fulfil bulk operations
     * @param listener The BulkProcessor listener that gets called on bulk events
     * @param name     The name of this processor, e.g. to identify the scheduler threads
     * @return the builder for BulkProcessor
     */
    public static Builder builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener, String name) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");
        final ScheduledThreadPoolExecutor flushScheduler = Scheduler.initScheduler(Settings.EMPTY, name + FLUSH_SCHEDULER_NAME_SUFFIX);
        final ScheduledThreadPoolExecutor retryScheduler = Scheduler.initScheduler(Settings.EMPTY, name + RETRY_SCHEDULER_NAME_SUFFIX);
        return new Builder(consumer, listener,
            buildScheduler(flushScheduler),
            buildScheduler(retryScheduler),
            () ->
            {
                Scheduler.terminate(flushScheduler, 10, TimeUnit.SECONDS);
                Scheduler.terminate(retryScheduler, 10, TimeUnit.SECONDS);
            });
    }

    private static Scheduler buildScheduler(ScheduledThreadPoolExecutor scheduledThreadPoolExecutor) {
        return (command, delay, executor) ->
            Scheduler.wrapAsScheduledCancellable(scheduledThreadPoolExecutor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS));
    }

    private final int bulkActions;
    private final long bulkSize;

    private final Scheduler.Cancellable cancellableFlushTask;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;
    private final Supplier<BulkRequest> bulkRequestSupplier;
    private final BulkRequestHandler bulkRequestHandler;
    private final Runnable onClose;

    private volatile boolean closed = false;
    private final ReentrantLock lock = new ReentrantLock();

    BulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy, Listener listener,
                  int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval,
                  Scheduler flushScheduler, Scheduler retryScheduler, Runnable onClose, Supplier<BulkRequest> bulkRequestSupplier) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.getBytes();
        this.bulkRequest = bulkRequestSupplier.get();
        this.bulkRequestSupplier = bulkRequestSupplier;
        this.bulkRequestHandler = new BulkRequestHandler(consumer, backoffPolicy, listener, retryScheduler, concurrentRequests);
        // Start period flushing task after everything is setup
        this.cancellableFlushTask = startFlushTask(flushInterval, flushScheduler);
        this.onClose = onClose;
    }

    /**
     * @deprecated use the {@link BulkProcessor} constructor which uses separate schedulers for flush and retry
     */
    @Deprecated
    BulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy, Listener listener,
                  int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval,
                  Scheduler scheduler, Runnable onClose, Supplier<BulkRequest> bulkRequestSupplier) {
        this(consumer, backoffPolicy, listener, concurrentRequests, bulkActions, bulkSize, flushInterval,
            scheduler, scheduler, onClose, bulkRequestSupplier);
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     */
    @Override
    public void close() {
        try {
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch (InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     * <p>
     * If concurrent requests are not enabled, returns {@code true} immediately.
     * If concurrent requests are enabled, waits for up to the specified timeout for all bulk requests to complete then returns {@code true}
     * If the specified waiting time elapses before all bulk requests complete, {@code false} is returned.
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit    The time unit of the {@code timeout} argument
     * @return {@code true} if all bulk requests completed and {@code false} if the waiting time elapsed before all the bulk requests
     * completed
     * @throws InterruptedException If the current thread is interrupted
     */
    public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        lock.lock();
        try {
            if (closed) {
                return true;
            }
            closed = true;

            this.cancellableFlushTask.cancel();

            if (bulkRequest.numberOfActions() > 0) {
                bulkRequestToExecute = newBulkRequest();
            }
        } finally {
            lock.unlock();
        }

        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }

        try {
            return this.bulkRequestHandler.awaitClose(timeout, unit);
        } finally {
            onClose.run();
        }
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(IndexRequest request) {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((DocWriteRequest<?>) request);
    }

    /**
     * Adds either a delete or an index request.
     */
    public BulkProcessor add(DocWriteRequest<?> request) {
        internalAdd(request);
        return this;
    }

    boolean isOpen() {
        return closed == false;
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("bulk process already closed");
        }
    }

    private void internalAdd(DocWriteRequest<?> request) {
        //bulkRequest and instance swapping is not threadsafe, so execute the mutations under a lock.
        //once the bulk request is ready to be shipped swap the instance reference unlock and send the local reference to the handler.
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        lock.lock();
        try {
            ensureOpen();
            bulkRequest.add(request);
            bulkRequestToExecute = newBulkRequestIfNeeded();
        } finally {
            lock.unlock();
        }

        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
    }

    /**
     * Adds the data from the bytes to be processed by the bulk processor
     */
    public BulkProcessor add(BytesReference data, @Nullable String defaultIndex,
                             @Nullable String defaultPipeline, XContentType xContentType) throws Exception {
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        lock.lock();
        try {
            ensureOpen();
            bulkRequest.add(data, defaultIndex, null, null, defaultPipeline, null,
                true, xContentType, RestApiVersion.current());
            bulkRequestToExecute = newBulkRequestIfNeeded();
        } finally {
            lock.unlock();
        }

        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
        return this;
    }

    private Tuple<BulkRequest, Long> newBulkRequest() {
        assert lock.isHeldByCurrentThread();
        final BulkRequest bulkRequest = this.bulkRequest;
        this.bulkRequest = bulkRequestSupplier.get();
        return new Tuple<>(bulkRequest, executionIdGen.incrementAndGet());
    }

    private Tuple<BulkRequest, Long> newBulkRequestIfNeeded() {
        assert lock.isHeldByCurrentThread();
        if (isOverTheLimit() == false) {
            return null;
        }
        return newBulkRequest();
    }

    private void execute(BulkRequest bulkRequest, long executionId) {
        assert lock.isHeldByCurrentThread() == false; // TRICKY
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    private boolean isOverTheLimit() {
        assert lock.isHeldByCurrentThread();
        if (bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) {
            return true;
        }
        if (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize) {
            return true;
        }
        return false;
    }

    private void flush(boolean ensureOpen) {
        Tuple<BulkRequest, Long> bulkRequestToExecute = null;
        lock.lock();
        try {
            if (ensureOpen) {
                ensureOpen();
            }
            if (closed) {
                return;
            }
            if (bulkRequest.numberOfActions() == 0) {
                return;
            }
            bulkRequestToExecute = newBulkRequest();
        } finally {
            lock.unlock();
        }

        if (bulkRequestToExecute != null) {
            execute(bulkRequestToExecute.v1(), bulkRequestToExecute.v2());
        }
    }

    /**
     * Flush pending delete or index requests.
     */
    public void flush() {
        flush(true);
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
        return scheduler.scheduleWithFixedDelay(() -> flush(false), flushInterval, ThreadPool.Names.GENERIC);
    }
}
