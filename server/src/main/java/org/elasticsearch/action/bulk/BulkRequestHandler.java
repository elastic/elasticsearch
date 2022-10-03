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
import org.elasticsearch.threadpool.Scheduler;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Implements the low-level details of bulk request handling
 */
public final class BulkRequestHandler {
    private final Logger logger;
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
    private final BulkProcessor.Listener listener;
    private final Semaphore semaphore;
    private final Set<String> sempahoreThreadNames = ConcurrentHashMap.newKeySet();
    private final Retry retry;
    private final int concurrentRequests;

    BulkRequestHandler(
        BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer,
        BackoffPolicy backoffPolicy,
        BulkProcessor.Listener listener,
        Scheduler scheduler,
        int concurrentRequests
    ) {
        assert concurrentRequests >= 0;
        this.logger = LogManager.getLogger(getClass());
        this.consumer = consumer;
        this.listener = listener;
        this.concurrentRequests = concurrentRequests;
        this.retry = new Retry(backoffPolicy, scheduler);
        this.semaphore = new Semaphore(concurrentRequests > 0 ? concurrentRequests : 1);
    }

    Set<String> getNamesOfThreadsThatHaveAcquiredSemaphore() {
        return sempahoreThreadNames;
    }

    private void acquireSemaphore() throws InterruptedException {
        logger.trace("Requesting BulkRequestHandler semaphore");
        semaphore.acquire();
        if (logger.isTraceEnabled()) {
            sempahoreThreadNames.add(Thread.currentThread().getName());
            logger.trace("Acquired BulkRequestHandler semaphore");
        }
    }

    private boolean tryAcquireSemaphore(long timeout, TimeUnit unit) throws InterruptedException {
        logger.trace("Requesting BulkRequestHandler semaphore");
        boolean acquired = semaphore.tryAcquire(this.concurrentRequests, timeout, unit);
        if (logger.isTraceEnabled()) {
            if (acquired) {
                sempahoreThreadNames.add(Thread.currentThread().getName());
                logger.trace("Acquired BulkRequestHandler semaphore");
            } else {
                logger.trace("Did not acquire BulkRequestHandler semaphore within given time");
            }
        }
        return acquired;
    }

    private void releaseSemaphore() {
        if (logger.isTraceEnabled()) {
            sempahoreThreadNames.remove(Thread.currentThread().getName());
            logger.trace("Releasing BulkRequestHandler semaphore");
        }
        semaphore.release();
    }

    private void releaseSemaphore(int count) {
        if (logger.isTraceEnabled()) {
            sempahoreThreadNames.remove(Thread.currentThread().getName());
            logger.trace("Releasing BulkRequestHandler semaphore");
        }
        semaphore.release(count);
    }

    public void execute(BulkRequest bulkRequest, long executionId) {
        try {
            listener.beforeBulk(executionId, bulkRequest);
            CountDownLatch latch = new CountDownLatch(1);
            final BiConsumer<BulkRequest, ActionListener<BulkResponse>> semaphoreAcquiringConsumer = (
                bulkRequest1,
                bulkResponseActionListener) -> {
                try {
                    acquireSemaphore();
                    consumer.accept(bulkRequest1, bulkResponseActionListener);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    releaseSemaphore();
                }
            };
            retry.withBackoff(semaphoreAcquiringConsumer, bulkRequest, ActionListener.runAfter(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    listener.afterBulk(executionId, bulkRequest, response);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }, () -> { latch.countDown(); }));
            if (concurrentRequests == 0) {
                latch.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info(() -> "Bulk request " + executionId + " has been cancelled.", e);
            listener.afterBulk(executionId, bulkRequest, e);
        } catch (Exception e) {
            logger.warn(() -> "Failed to execute bulk request " + executionId + ".", e);
            listener.afterBulk(executionId, bulkRequest, e);
        }
    }

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (tryAcquireSemaphore(timeout, unit)) {
            releaseSemaphore(this.concurrentRequests);
            return true;
        }
        return false;
    }
}
