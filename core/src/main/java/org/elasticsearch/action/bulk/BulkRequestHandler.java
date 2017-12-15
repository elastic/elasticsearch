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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.threadpool.Scheduler;

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
    private final Retry retry;
    private final int concurrentRequests;

    BulkRequestHandler(BiConsumer<BulkRequest, ActionListener<BulkResponse>> consumer, BackoffPolicy backoffPolicy,
                       BulkProcessor.Listener listener, Scheduler scheduler, int concurrentRequests) {
        assert concurrentRequests >= 0;
        this.logger = Loggers.getLogger(getClass());
        this.consumer = consumer;
        this.listener = listener;
        this.concurrentRequests = concurrentRequests;
        this.retry = new Retry(EsRejectedExecutionException.class, backoffPolicy, scheduler);
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
            retry.withBackoff(consumer, bulkRequest, new ActionListener<BulkResponse>() {
                @Override
                public void onResponse(BulkResponse response) {
                    try {
                        listener.afterBulk(executionId, bulkRequest, response);
                    } finally {
                        semaphore.release();
                        latch.countDown();
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        listener.afterBulk(executionId, bulkRequest, e);
                    } finally {
                        semaphore.release();
                        latch.countDown();
                    }
                }
            }, Settings.EMPTY);
            bulkRequestSetupSuccessful = true;
            if (concurrentRequests == 0) {
                latch.await();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.info((Supplier<?>) () -> new ParameterizedMessage("Bulk request {} has been cancelled.", executionId), e);
            listener.afterBulk(executionId, bulkRequest, e);
        } catch (Exception e) {
            logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to execute bulk request {}.", executionId), e);
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
