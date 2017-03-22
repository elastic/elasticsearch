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
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;

import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

/**
 * Abstracts the low-level details of bulk request handling
 */
abstract class BulkRequestHandler {
    protected final Logger logger;
    protected final Client client;

    protected BulkRequestHandler(Client client) {
        this.client = client;
        this.logger = Loggers.getLogger(getClass(), client.settings());
    }


    public abstract void execute(BulkRequest bulkRequest, long executionId);

    public abstract boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException;


    public static BulkRequestHandler syncHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener) {
        return new SyncBulkRequestHandler(client, backoffPolicy, listener);
    }

    public static BulkRequestHandler asyncHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener, int concurrentRequests) {
        return new AsyncBulkRequestHandler(client, backoffPolicy, listener, concurrentRequests);
    }

    private static class SyncBulkRequestHandler extends BulkRequestHandler {
        private final BulkProcessor.Listener listener;
        private final BackoffPolicy backoffPolicy;

        SyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            this.listener = listener;
        }

        @Override
        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean afterCalled = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                BulkResponse bulkResponse = Retry
                        .on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withSyncBackoff(client, bulkRequest);
                afterCalled = true;
                listener.afterBulk(executionId, bulkRequest, bulkResponse);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info((Supplier<?>) () -> new ParameterizedMessage("Bulk request {} has been cancelled.", executionId), e);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to execute bulk request {}.", executionId), e);
                if (!afterCalled) {
                    listener.afterBulk(executionId, bulkRequest, e);
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            // we are "closed" immediately as there is no request in flight
            return true;
        }
    }

    private static class AsyncBulkRequestHandler extends BulkRequestHandler {
        private final BackoffPolicy backoffPolicy;
        private final BulkProcessor.Listener listener;
        private final Semaphore semaphore;
        private final int concurrentRequests;

        private AsyncBulkRequestHandler(Client client, BackoffPolicy backoffPolicy, BulkProcessor.Listener listener, int concurrentRequests) {
            super(client);
            this.backoffPolicy = backoffPolicy;
            assert concurrentRequests > 0;
            this.listener = listener;
            this.concurrentRequests = concurrentRequests;
            this.semaphore = new Semaphore(concurrentRequests);
        }

        @Override
        public void execute(BulkRequest bulkRequest, long executionId) {
            boolean bulkRequestSetupSuccessful = false;
            boolean acquired = false;
            try {
                listener.beforeBulk(executionId, bulkRequest);
                semaphore.acquire();
                acquired = true;
                Retry.on(EsRejectedExecutionException.class)
                        .policy(backoffPolicy)
                        .withAsyncBackoff(client, bulkRequest, new ActionListener<BulkResponse>() {
                            @Override
                            public void onResponse(BulkResponse response) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, response);
                                } finally {
                                    semaphore.release();
                                }
                            }

                            @Override
                            public void onFailure(Exception e) {
                                try {
                                    listener.afterBulk(executionId, bulkRequest, e);
                                } finally {
                                    semaphore.release();
                                }
                            }
                        });
                bulkRequestSetupSuccessful = true;
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.info((Supplier<?>) () -> new ParameterizedMessage("Bulk request {} has been cancelled.", executionId), e);
                listener.afterBulk(executionId, bulkRequest, e);
            } catch (Exception e) {
                logger.warn((Supplier<?>) () -> new ParameterizedMessage("Failed to execute bulk request {}.", executionId), e);
                listener.afterBulk(executionId, bulkRequest, e);
            } finally {
                if (!bulkRequestSetupSuccessful && acquired) {  // if we fail on client.bulk() release the semaphore
                    semaphore.release();
                }
            }
        }

        @Override
        public boolean awaitClose(long timeout, TimeUnit unit) throws InterruptedException {
            if (semaphore.tryAcquire(this.concurrentRequests, timeout, unit)) {
                semaphore.release(this.concurrentRequests);
                return true;
            }
            return false;
        }
    }
}
