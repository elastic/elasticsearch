/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.action.bulk;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.BackoffPolicy;
import org.elasticsearch.threadpool.Scheduler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

/**
 * Implements the low-level details of bulk request handling
 */
public final class BulkRequestHandler {
    private static final Logger logger = LogManager.getLogger(BulkRequestHandler.class);
    private final BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer;
    private final BulkProcessor.Listener listener;
    private final Semaphore semaphore;
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
        this.consumer = consumer;
        this.listener = listener;
        this.concurrentRequests = concurrentRequests;
        this.retry = new Retry(backoffPolicy, scheduler);
        this.semaphore = new Semaphore(concurrentRequests > 0 ? concurrentRequests : 1);
    }

    public void execute(BulkRequest bulkRequest, long executionId) {
        Runnable toRelease = () -> {};
        boolean bulkRequestSetupSuccessful = false;
        try {
            listener.beforeBulk(executionId, bulkRequest);
            semaphore.acquire();
            toRelease = semaphore::release;
            CountDownLatch latch = new CountDownLatch(1);
            retry.withBackoff(consumer, bulkRequest, ActionListener.runAfter(new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    listener.afterBulk(executionId, bulkRequest, response);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }, () -> {
                semaphore.release();
                latch.countDown();
            }));
            bulkRequestSetupSuccessful = true;
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
        } finally {
            if (bulkRequestSetupSuccessful == false) {  // if we fail on client.bulk() release the semaphore
                toRelease.run();
            }
        }
    }

    boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
        if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
            semaphore.release(this.concurrentRequests);
            return true;
        }
        return false;
    }
}
