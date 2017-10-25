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

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.Objects;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor implements Closeable {

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
        private final Scheduler scheduler;
        private final Runnable onClose;
        private int concurrentRequests = 1;
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;
        private BackoffPolicy backoffPolicy = BackoffPolicy.exponentialBackoff();

        private Builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener,
                        Scheduler scheduler, Runnable onClose) {
            this.consumer = consumer;
            this.listener = listener;
            this.scheduler = scheduler;
            this.onClose = onClose;
        }

        /**
         * Sets the number of concurrent requests allowed to be executed. A value of 0 means that only a single
         * request will be allowed to be executed. A value of 1 means 1 concurrent request is allowed to be executed
         * while accumulating new bulk requests. Defaults to <tt>1</tt>.
         */
        public Builder setConcurrentRequests(int concurrentRequests) {
            this.concurrentRequests = concurrentRequests;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the number of actions currently added. Defaults to
         * <tt>1000</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkActions(int bulkActions) {
            this.bulkActions = bulkActions;
            return this;
        }

        /**
         * Sets when to flush a new bulk request based on the size of actions currently added. Defaults to
         * <tt>5mb</tt>. Can be set to <tt>-1</tt> to disable it.
         */
        public Builder setBulkSize(ByteSizeValue bulkSize) {
            this.bulkSize = bulkSize;
            return this;
        }

        /**
         * Sets a flush interval flushing *any* bulk actions pending if the interval passes. Defaults to not set.
         * <p>
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to <tt>-1</tt> with the flush interval set allowing for complete async processing of bulk actions.
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
        public BulkProcessor build() {
            return new BulkProcessor(consumer, backoffPolicy, listener, concurrentRequests, bulkActions, bulkSize, flushInterval,
                    scheduler, onClose);
        }
    }

    public static Builder builder(Client client, Listener listener) {
        Objects.requireNonNull(client, "client");
        Objects.requireNonNull(listener, "listener");
        return new Builder(client::bulk, listener, client.threadPool(), () -> {});
    }

    public static Builder builder(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, Listener listener) {
        Objects.requireNonNull(consumer, "consumer");
        Objects.requireNonNull(listener, "listener");
        final ScheduledThreadPoolExecutor scheduledThreadPoolExecutor = Scheduler.initScheduler(Settings.EMPTY);
        return new Builder(consumer, listener,
                (delay, executor, command) -> scheduledThreadPoolExecutor.schedule(command, delay.millis(), TimeUnit.MILLISECONDS),
                () -> Scheduler.terminate(scheduledThreadPoolExecutor, 10, TimeUnit.SECONDS));
    }

    private final int bulkActions;
    private final long bulkSize;

    private final Scheduler.Cancellable cancellableFlushTask;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;
    private final BulkRequestHandler bulkRequestHandler;
    private final Scheduler scheduler;
    private final Runnable onClose;

    private volatile boolean closed = false;

    BulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy, Listener listener,
                  int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval,
                  Scheduler scheduler, Runnable onClose) {
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.getBytes();
        this.bulkRequest = new BulkRequest();
        this.scheduler = scheduler;
        this.bulkRequestHandler = new BulkRequestHandler(consumer, backoffPolicy, listener, scheduler, concurrentRequests);
        // Start period flushing task after everything is setup
        this.cancellableFlushTask = startFlushTask(flushInterval, scheduler);
        this.onClose = onClose;
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
        onClose.run();
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     * <p>
     * If concurrent requests are not enabled, returns {@code true} immediately.
     * If concurrent requests are enabled, waits for up to the specified timeout for all bulk requests to complete then returns {@code true},
     * If the specified waiting time elapses before all bulk requests complete, {@code false} is returned.
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit    The time unit of the {@code timeout} argument
     * @return {@code true} if all bulk requests completed and {@code false} if the waiting time elapsed before all the bulk requests completed
     * @throws InterruptedException If the current thread is interrupted
     */
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return true;
        }
        closed = true;

        this.cancellableFlushTask.cancel();

        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        return this.bulkRequestHandler.awaitClose(timeout, unit);
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(IndexRequest request) {
        return add((DocWriteRequest) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((DocWriteRequest) request);
    }

    /**
     * Adds either a delete or an index request.
     */
    public BulkProcessor add(DocWriteRequest request) {
        return add(request, null);
    }

    public BulkProcessor add(DocWriteRequest request, @Nullable Object payload) {
        internalAdd(request, payload);
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

    private synchronized void internalAdd(DocWriteRequest request, @Nullable Object payload) {
        ensureOpen();
        bulkRequest.add(request, payload);
        executeIfNeeded();
    }

    /**
     * Adds the data from the bytes to be processed by the bulk processor
     */
    public BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType,
                             XContentType xContentType) throws Exception {
        return add(data, defaultIndex, defaultType, null, null, xContentType);
    }

    /**
     * Adds the data from the bytes to be processed by the bulk processor
     */
    public synchronized BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType,
                                          @Nullable String defaultPipeline, @Nullable Object payload, XContentType xContentType) throws Exception {
        bulkRequest.add(data, defaultIndex, defaultType, null, null, null, defaultPipeline, payload, true, xContentType);
        executeIfNeeded();
        return this;
    }

    private Scheduler.Cancellable startFlushTask(TimeValue flushInterval, Scheduler scheduler) {
        if (flushInterval == null) {
            return new Scheduler.Cancellable() {
                @Override
                public void cancel() {}

                @Override
                public boolean isCancelled() {
                    return true;
                }
            };
        }
        final Runnable flushRunnable = scheduler.preserveContext(new Flush());
        return scheduler.scheduleWithFixedDelay(flushRunnable, flushInterval, ThreadPool.Names.GENERIC);
    }

    private void executeIfNeeded() {
        ensureOpen();
        if (!isOverTheLimit()) {
            return;
        }
        execute();
    }

    // (currently) needs to be executed under a lock
    private void execute() {
        final BulkRequest bulkRequest = this.bulkRequest;
        final long executionId = executionIdGen.incrementAndGet();

        this.bulkRequest = new BulkRequest();
        this.bulkRequestHandler.execute(bulkRequest, executionId);
    }

    private boolean isOverTheLimit() {
        if (bulkActions != -1 && bulkRequest.numberOfActions() >= bulkActions) {
            return true;
        }
        if (bulkSize != -1 && bulkRequest.estimatedSizeInBytes() >= bulkSize) {
            return true;
        }
        return false;
    }

    /**
     * Flush pending delete or index requests.
     */
    public synchronized void flush() {
        ensureOpen();
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
    }

    class Flush implements Runnable {

        @Override
        public void run() {
            synchronized (BulkProcessor.this) {
                if (closed) {
                    return;
                }
                if (bulkRequest.numberOfActions() == 0) {
                    return;
                }
                execute();
            }
        }
    }
}
