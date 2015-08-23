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
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.FutureUtils;

import java.io.Closeable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A bulk processor is a thread safe bulk processing class, allowing to easily set when to "flush" a new bulk request
 * (either based on number of actions, based on the size, or time), and to easily control the number of concurrent bulk
 * requests allowed to be executed in parallel.
 * <p/>
 * In order to create a new bulk processor, use the {@link Builder}.
 */
public class BulkProcessor implements Closeable {

    /**
     * A listener for the execution.
     */
    public static interface Listener {

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
         */
        void afterBulk(long executionId, BulkRequest request, Throwable failure);
    }

    /**
     * A builder used to create a build an instance of a bulk processor.
     */
    public static class Builder {

        private final Client client;
        private final Listener listener;

        private String name;
        private int concurrentRequests = 1;
        private int bulkActions = 1000;
        private ByteSizeValue bulkSize = new ByteSizeValue(5, ByteSizeUnit.MB);
        private TimeValue flushInterval = null;

        /**
         * Creates a builder of bulk processor with the client to use and the listener that will be used
         * to be notified on the completion of bulk requests.
         */
        public Builder(Client client, Listener listener) {
            this.client = client;
            this.listener = listener;
        }

        /**
         * Sets an optional name to identify this bulk processor.
         */
        public Builder setName(String name) {
            this.name = name;
            return this;
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
         * <p/>
         * Note, both {@link #setBulkActions(int)} and {@link #setBulkSize(org.elasticsearch.common.unit.ByteSizeValue)}
         * can be set to <tt>-1</tt> with the flush interval set allowing for complete async processing of bulk actions.
         */
        public Builder setFlushInterval(TimeValue flushInterval) {
            this.flushInterval = flushInterval;
            return this;
        }

        /**
         * Builds a new bulk processor.
         */
        public BulkProcessor build() {
            return new BulkProcessor(client, listener, name, concurrentRequests, bulkActions, bulkSize, flushInterval);
        }
    }

    public static Builder builder(Client client, Listener listener) {
        if (client == null) {
            throw new NullPointerException("The client you specified while building a BulkProcessor is null");
        }
        
        return new Builder(client, listener);
    }

    private final Client client;
    private final Listener listener;

    private final String name;

    private final int concurrentRequests;
    private final int bulkActions;
    private final long bulkSize;
    private final TimeValue flushInterval;

    private final Semaphore semaphore;
    private final ScheduledThreadPoolExecutor scheduler;
    private final ScheduledFuture scheduledFuture;

    private final AtomicLong executionIdGen = new AtomicLong();

    private BulkRequest bulkRequest;

    private volatile boolean closed = false;

    BulkProcessor(Client client, Listener listener, @Nullable String name, int concurrentRequests, int bulkActions, ByteSizeValue bulkSize, @Nullable TimeValue flushInterval) {
        this.client = client;
        this.listener = listener;
        this.name = name;
        this.concurrentRequests = concurrentRequests;
        this.bulkActions = bulkActions;
        this.bulkSize = bulkSize.bytes();

        this.semaphore = new Semaphore(concurrentRequests);
        this.bulkRequest = new BulkRequest();

        this.flushInterval = flushInterval;
        if (flushInterval != null) {
            this.scheduler = (ScheduledThreadPoolExecutor) Executors.newScheduledThreadPool(1, EsExecutors.daemonThreadFactory(client.settings(), (name != null ? "[" + name + "]" : "") + "bulk_processor"));
            this.scheduler.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
            this.scheduler.setContinueExistingPeriodicTasksAfterShutdownPolicy(false);
            this.scheduledFuture = this.scheduler.scheduleWithFixedDelay(new Flush(), flushInterval.millis(), flushInterval.millis(), TimeUnit.MILLISECONDS);
        } else {
            this.scheduler = null;
            this.scheduledFuture = null;
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     */
    @Override
    public void close() {
        try {
            awaitClose(0, TimeUnit.NANOSECONDS);
        } catch(InterruptedException exc) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Closes the processor. If flushing by time is enabled, then it's shutdown. Any remaining bulk actions are flushed.
     *
     * If concurrent requests are not enabled, returns {@code true} immediately.
     * If concurrent requests are enabled, waits for up to the specified timeout for all bulk requests to complete then returns {@code true},
     * If the specified waiting time elapses before all bulk requests complete, {@code false} is returned.
     *
     * @param timeout The maximum time to wait for the bulk requests to complete
     * @param unit The time unit of the {@code timeout} argument
     * @return {@code true} if all bulk requests completed and {@code false} if the waiting time elapsed before all the bulk requests completed
     * @throws InterruptedException If the current thread is interrupted
     */
    public synchronized boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (closed) {
            return true;
        }
        closed = true;
        if (this.scheduledFuture != null) {
            FutureUtils.cancel(this.scheduledFuture);
            this.scheduler.shutdown();
        }
        if (bulkRequest.numberOfActions() > 0) {
            execute();
        }
        if (this.concurrentRequests < 1) {
            return true;
        }
        if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
            semaphore.release(this.concurrentRequests);
            return true;
        }
        return false;
    }

    /**
     * Adds an {@link IndexRequest} to the list of actions to execute. Follows the same behavior of {@link IndexRequest}
     * (for example, if no id is provided, one will be generated, or usage of the create flag).
     */
    public BulkProcessor add(IndexRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds an {@link DeleteRequest} to the list of actions to execute.
     */
    public BulkProcessor add(DeleteRequest request) {
        return add((ActionRequest) request);
    }

    /**
     * Adds either a delete or an index request.
     */
    public BulkProcessor add(ActionRequest request) {
        return add(request, null);
    }

    public BulkProcessor add(ActionRequest request, @Nullable Object payload) {
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

    private synchronized void internalAdd(ActionRequest request, @Nullable Object payload) {
        ensureOpen();
        bulkRequest.add(request, payload);
        executeIfNeeded();
    }

    public BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType) throws Exception {
        return add(data, defaultIndex, defaultType, null);
    }

    public synchronized BulkProcessor add(BytesReference data, @Nullable String defaultIndex, @Nullable String defaultType, @Nullable Object payload) throws Exception {
        bulkRequest.add(data, defaultIndex, defaultType, null, null, payload, true);
        executeIfNeeded();
        return this;
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

        if (concurrentRequests == 0) {
            // execute in a blocking fashion...
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkItemResponses = client.bulk(bulkRequest).actionGet();
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkItemResponses);
            } catch (Exception e) {
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }
        } else {
            boolean success = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                client.bulk(bulkRequest, new ActionListener<BulkResponse>() {
                    @Override
                    public void onResponse(BulkResponse response) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, response);
                        } finally {
                            semaphore.release();
                        }
                    }

                    @Override
                    public void onFailure(Throwable e) {
                        try {
                            listener.afterBulk(executionId, bulkRequest, e);
                        } finally {
                            semaphore.release();
                        }
                    }
                });
                success = true;
            } catch (InterruptedException e) {
                Thread.interrupted();
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Throwable t) {
                listener.afterBulk(executionId, bulkRequest, t);
            } finally {
                 if (!success) {  // if we fail on client.bulk() release the semaphore
                     semaphore.release();
                 }
            }
        }
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
