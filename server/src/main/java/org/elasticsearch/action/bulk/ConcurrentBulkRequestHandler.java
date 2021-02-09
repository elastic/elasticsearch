/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.collect.PersistentQueue;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.threadpool.Scheduler;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

public class ConcurrentBulkRequestHandler {

    private final int maxConcurrentRequests;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
    private final BulkProcessor.Listener listener;
    private final Retry retry;
    private final AtomicReference<State> state = new AtomicReference<>(new State(PersistentQueue.empty(), 0));

    public ConcurrentBulkRequestHandler(int maxConcurrentRequests, BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
                                        BackoffPolicy backoffPolicy, Scheduler retryScheduler, BulkProcessor.Listener listener) {
        this.maxConcurrentRequests = Math.max(maxConcurrentRequests, 1);
        this.consumer = consumer;
        this.listener = listener;
        this.retry = new Retry(backoffPolicy, retryScheduler);
    }

    public void execute(BulkRequest request, long executionId) {
        Execution execution = new Execution(request, executionId);
        State newState = state.updateAndGet(s -> s.concurrentRequests == maxConcurrentRequests ?
            new State(s.pendingTask.add(execution), s.concurrentRequests) :
            new State(s.pendingTask, s.concurrentRequests + 1));
        if (newState.pendingTask.isEmpty()) {
            innerExecute(request, executionId);
        }
    }

    private void innerExecute(BulkRequest request, long executionId) {
        while (true) {
            listener.beforeBulk(executionId, request);
            AtomicBoolean shouldContinueHere = new AtomicBoolean();
            try {
                retry.withBackoff(consumer, request, ActionListener.runAfter(new BulkActionListener(executionId, request), () -> {
                    if (shouldContinueHere.getAndSet(true)) {
                        Optional<Execution> nextExecution = updateStateAfterBulk();
                        nextExecution.ifPresent(execution -> innerExecute(execution.request, execution.executionId));
                    }
                }));
            } catch (Exception e) {
                listener.afterBulk(executionId, request, e);
                shouldContinueHere.set(true);
            }
            if (shouldContinueHere.getAndSet(true) == false) {
                return;
            }
            Optional<Execution> nextExecution = updateStateAfterBulk();
            if (nextExecution.isEmpty()) {
                return;
            }
            request = nextExecution.get().request;
            executionId = nextExecution.get().executionId;
        }
    }

    private Optional<Execution> updateStateAfterBulk() {
        State newState = state.updateAndGet(s -> {
            if (s.pendingTask.isEmpty()) {
                return new State(s.pendingTask, s.concurrentRequests - 1);
            } else {
                Tuple<Execution, PersistentQueue<Execution>> remove = s.pendingTask.remove();
                return new State(remove.v2(), s.concurrentRequests, remove.v1());
            }
        });
        return Optional.ofNullable(newState.toExecute);
    }

    public boolean awaitClose(long value, TimeUnit units) throws InterruptedException {
        long millis = units.toMillis(value);
        for (int i = 0; i < 100; i++) {
            if (state.get().concurrentRequests == 0) {
                return true;
            }
            Thread.sleep(millis / 100);
        }
        return false;
    }

    private static final class State {
        private final PersistentQueue<Execution> pendingTask;
        private final int concurrentRequests;
        private final Execution toExecute;

        private State(PersistentQueue<Execution> pendingTask, int concurrentRequests, Execution toExecute) {
            this.pendingTask = pendingTask;
            this.concurrentRequests = concurrentRequests;
            this.toExecute = toExecute;
        }

        private State(PersistentQueue<Execution> pendingTask, int concurrentRequests) {
            this(pendingTask, concurrentRequests, null);
        }
    }

    private static final class Execution {
        private final BulkRequest request;
        private final long executionId;

        private Execution(BulkRequest request, long executionId) {
            this.request = request;
            this.executionId = executionId;
        }
    }

    private class BulkActionListener implements ActionListener<BulkResponse> {
        private final long executionId;
        private final BulkRequest request;

        private BulkActionListener(long executionId, BulkRequest request) {
            this.executionId = executionId;
            this.request = request;
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            listener.afterBulk(executionId, request, bulkItemResponses);
        }

        @Override
        public void onFailure(Exception e) {
            listener.afterBulk(executionId, request, e);
        }
    }
}
