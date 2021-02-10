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

    private static final State EMPTY_STATE = new State(PersistentStack.empty(), null, 0);

    private final ConcurrentBulkRequestHandler bulkRequestHandler;
    private final long bulkSize;
    private final long bulkActions;
    private final Runnable onClose;
    private final Supplier<BulkRequest> bulkRequestSupplier;
    private final AtomicInteger executionIdGen = new AtomicInteger();
    private final AtomicReference<State> state = new AtomicReference<>(EMPTY_STATE);
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Scheduler.Cancellable cancellableFlushTask;

    public ConcurrentBulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy,
                                   BulkProcessor.Listener listener, int concurrentRequests, long bulkActions, ByteSizeValue bulkSize,
                                   TimeValue flushInterval, Scheduler scheduler, Runnable onClose,
                                   Supplier<BulkRequest> bulkRequestSupplier) {
        this(consumer, backoffPolicy, listener, concurrentRequests, bulkActions, bulkSize, flushInterval, scheduler, scheduler, onClose,
            bulkRequestSupplier);
    }

    public ConcurrentBulkProcessor(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy,
                                   BulkProcessor.Listener listener, int concurrentRequests, long bulkActions, ByteSizeValue bulkSize,
                                   TimeValue flushInterval, Scheduler flushScheduler, Scheduler retryScheduler, Runnable onClose,
                                   Supplier<BulkRequest> bulkRequestSupplier) {
        this.bulkSize = bulkSize.getBytes();
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
        State newState = state.updateAndGet(s -> {
            long size = s.size + finalSizeInBytes;
            PersistentStack<DocWriteRequest<?>> actions = s.actions.push(request);
            if (actions.size() >= bulkActions || size >= bulkSize) {
                return new State(PersistentStack.empty(), actions, 0);
            } else {
                return new State(actions, null, size);
            }
        });

        if (newState.toFlush != null) {
            flush(newState.toFlush);
        }
        if (closed.get()) {
            flush();
        }
    }

    public void flush() {
        flush(state.getAndSet(EMPTY_STATE).actions);
    }

    private void flush(PersistentStack<DocWriteRequest<?>> actions) {
        BulkRequest bulkRequest = bulkRequestSupplier.get();
        for (DocWriteRequest<?> action : actions) {
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
        private final PersistentStack<DocWriteRequest<?>> toFlush;
        private final long size;

        private State(PersistentStack<DocWriteRequest<?>> actions, PersistentStack<DocWriteRequest<?>> toFlush, long size) {
            this.actions = actions;
            this.toFlush = toFlush;
            this.size = size;
        }
    }
}
