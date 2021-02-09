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
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.common.collect.PersistentStack;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

public class ConcurrentBulkProcessor implements AutoCloseable {

    private static final State EMPTY_STATE = new State(PersistentStack.empty(), 0);

    private final ConcurrentBulkRequestHandler bulkRequestHandler;
    private final ByteSizeValue bulkSize;
    private final long bulkActions;
    private final Runnable onClose;
    private final Supplier<BulkRequest> bulkRequestSupplier;
    private final AtomicInteger executionIdGen = new AtomicInteger();
    private final AtomicReference<State> state = new AtomicReference<>(EMPTY_STATE);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Scheduler.Cancellable cancellableFlushTask;

    public ConcurrentBulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy,
                                   BulkProcessor.Listener listener, int concurrentRequests, long bulkActions, ByteSizeValue bulkSize,
                                   TimeValue flushInterval, Scheduler flushScheduler, Scheduler retryScheduler, Runnable onClose,
                                   Supplier<BulkRequest> bulkRequestSupplier) {
        this.bulkSize = bulkSize;
        this.bulkActions = bulkActions;
        this.onClose = onClose;
        this.bulkRequestSupplier = bulkRequestSupplier;
        this.bulkRequestHandler = new ConcurrentBulkRequestHandler(concurrentRequests, consumer, backoffPolicy, retryScheduler, listener);
        this.cancellableFlushTask = startFlushTask(flushInterval, flushScheduler);
    }

    public void add(IndexRequest request) {
        updateState(request, BulkRequest.getSize(request));
    }

    public void add(DeleteRequest request) {
        updateState(request, BulkRequest.getSize(request));
    }

    public void add(UpdateRequest request) {
        updateState(request, BulkRequest.getSize(request));
    }

    private void updateState(DocWriteRequest<?> request, long finalSizeInBytes) {
        if (closed.get()) {
            throw new IllegalStateException("bulk process already closed");
        }
        State newState = state.updateAndGet(s -> new State(s.actions.push(request), s.size + finalSizeInBytes));
        if (closed.get() || newState.actions.size() >= bulkActions || new ByteSizeValue(newState.size).compareTo(bulkSize) >= 0) {
            flush();
        }
    }

    public void flush() {
        BulkRequest bulkRequest = bulkRequestSupplier.get();
        for (DocWriteRequest<?> action : state.getAndSet(EMPTY_STATE).actions) {
            bulkRequest.add(action);
        }
        if (bulkRequest.numberOfActions() > 0) {
            bulkRequestHandler.execute(bulkRequest, executionIdGen.getAndIncrement());
        }
    }

    @Override
    public void close() {
        closed.set(true);
        cancellableFlushTask.cancel();
        flush();
        onClose.run();
    }

    public boolean awaitClose(long value, TimeUnit units) throws InterruptedException {
        close();
        bulkRequestHandler.awaitClose(value, units);
        return false;
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
        return scheduler.scheduleWithFixedDelay(this::flush, flushInterval, ThreadPool.Names.GENERIC);
    }

    private static final class State {
        private final PersistentStack<DocWriteRequest<?>> actions;
        private final long size;

        private State(PersistentStack<DocWriteRequest<?>> actions, long size) {
            this.actions = actions;
            this.size = size;
        }
    }
}
