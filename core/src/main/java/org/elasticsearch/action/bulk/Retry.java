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

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.concurrent.FutureUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ScheduledFuture;
import java.util.function.Predicate;

/**
 * Encapsulates synchronous and asynchronous retry logic.
 */
public class Retry {
    private final Class<? extends Throwable> retryOnThrowable;

    private BackoffPolicy backoffPolicy;

    public static Retry on(Class<? extends Throwable> retryOnThrowable) {
        return new Retry(retryOnThrowable);
    }

    /**
     * @param backoffPolicy The backoff policy that defines how long and how often to wait for retries.
     */
    public Retry policy(BackoffPolicy backoffPolicy) {
        this.backoffPolicy = backoffPolicy;
        return this;
    }

    Retry(Class<? extends Throwable> retryOnThrowable) {
        this.retryOnThrowable = retryOnThrowable;
    }

    /**
     * Invokes #bulk(BulkRequest, ActionListener) on the provided client. Backs off on the provided exception and delegates results to the
     * provided listener.
     *
     * @param client      Client invoking the bulk request.
     * @param bulkRequest The bulk request that should be executed.
     * @param listener    A listener that is invoked when the bulk request finishes or completes with an exception. The listener is not
     */
    public void withAsyncBackoff(Client client, BulkRequest bulkRequest, ActionListener<BulkResponse> listener) {
        AsyncRetryHandler r = new AsyncRetryHandler(retryOnThrowable, backoffPolicy, client, listener);
        r.execute(bulkRequest);

    }

    /**
     * Invokes #bulk(BulkRequest) on the provided client. Backs off on the provided exception.
     *
     * @param client      Client invoking the bulk request.
     * @param bulkRequest The bulk request that should be executed.
     * @return the bulk response as returned by the client.
     * @throws Exception Any exception thrown by the callable.
     */
    public BulkResponse withSyncBackoff(Client client, BulkRequest bulkRequest) throws Exception {
        return SyncRetryHandler
                .create(retryOnThrowable, backoffPolicy, client)
                .executeBlocking(bulkRequest)
                .actionGet();
    }

    static class AbstractRetryHandler implements ActionListener<BulkResponse> {
        private final ESLogger logger;
        private final Client client;
        private final ActionListener<BulkResponse> listener;
        private final Iterator<TimeValue> backoff;
        private final Class<? extends Throwable> retryOnThrowable;
        // Access only when holding a client-side lock, see also #addResponses()
        private final List<BulkItemResponse> responses = new ArrayList<>();
        private final long startTimestampNanos;
        // needed to construct the next bulk request based on the response to the previous one
        // volatile as we're called from a scheduled thread
        private volatile BulkRequest currentBulkRequest;
        private volatile ScheduledFuture<?> scheduledRequestFuture;

        public AbstractRetryHandler(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy, Client client, ActionListener<BulkResponse> listener) {
            this.retryOnThrowable = retryOnThrowable;
            this.backoff = backoffPolicy.iterator();
            this.client = client;
            this.listener = listener;
            this.logger = Loggers.getLogger(getClass(), client.settings());
            // in contrast to System.currentTimeMillis(), nanoTime() uses a monotonic clock under the hood
            this.startTimestampNanos = System.nanoTime();
        }

        @Override
        public void onResponse(BulkResponse bulkItemResponses) {
            if (!bulkItemResponses.hasFailures()) {
                // we're done here, include all responses
                addResponses(bulkItemResponses, (r -> true));
                finishHim();
            } else {
                if (canRetry(bulkItemResponses)) {
                    addResponses(bulkItemResponses, (r -> !r.isFailed()));
                    retry(createBulkRequestForRetry(bulkItemResponses));
                } else {
                    addResponses(bulkItemResponses, (r -> true));
                    finishHim();
                }
            }
        }

        @Override
        public void onFailure(Throwable e) {
            try {
                listener.onFailure(e);
            } finally {
                FutureUtils.cancel(scheduledRequestFuture);
            }
        }

        private void retry(BulkRequest bulkRequestForRetry) {
            assert backoff.hasNext();
            TimeValue next = backoff.next();
            logger.trace("Retry of bulk request scheduled in {} ms.", next.millis());
            scheduledRequestFuture = client.threadPool().schedule(next, ThreadPool.Names.SAME, (() -> this.execute(bulkRequestForRetry)));
        }

        private BulkRequest createBulkRequestForRetry(BulkResponse bulkItemResponses) {
            BulkRequest requestToReissue = new BulkRequest();
            int index = 0;
            for (BulkItemResponse bulkItemResponse : bulkItemResponses.getItems()) {
                if (bulkItemResponse.isFailed()) {
                    requestToReissue.add(currentBulkRequest.requests().get(index));
                }
                index++;
            }
            return requestToReissue;
        }

        private boolean canRetry(BulkResponse bulkItemResponses) {
            if (!backoff.hasNext()) {
                return false;
            }
            for (BulkItemResponse bulkItemResponse : bulkItemResponses) {
                if (bulkItemResponse.isFailed()) {
                    Throwable cause = bulkItemResponse.getFailure().getCause();
                    Throwable rootCause = ExceptionsHelper.unwrapCause(cause);
                    if (!rootCause.getClass().equals(retryOnThrowable)) {
                        return false;
                    }
                }
            }
            return true;
        }

        private void finishHim() {
            try {
                listener.onResponse(getAccumulatedResponse());
            } finally {
                FutureUtils.cancel(scheduledRequestFuture);
            }
        }

        private void addResponses(BulkResponse response, Predicate<BulkItemResponse> filter) {
            for (BulkItemResponse bulkItemResponse : response) {
                if (filter.test(bulkItemResponse)) {
                    // Use client-side lock here to avoid visibility issues. This method may be called multiple times
                    // (based on how many retries we have to issue) and relying that the response handling code will be
                    // scheduled on the same thread is fragile.
                    synchronized (responses) {
                        responses.add(bulkItemResponse);
                    }
                }
            }
        }

        private BulkResponse getAccumulatedResponse() {
            BulkItemResponse[] itemResponses;
            synchronized (responses) {
                itemResponses = responses.toArray(new BulkItemResponse[1]);
            }
            long stopTimestamp = System.nanoTime();
            long totalLatencyMs = TimeValue.timeValueNanos(stopTimestamp - startTimestampNanos).millis();
            return new BulkResponse(itemResponses, totalLatencyMs);
        }

        public void execute(BulkRequest bulkRequest) {
            this.currentBulkRequest = bulkRequest;
            client.bulk(bulkRequest, this);
        }
    }

    static class AsyncRetryHandler extends AbstractRetryHandler {
        public AsyncRetryHandler(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy, Client client, ActionListener<BulkResponse> listener) {
            super(retryOnThrowable, backoffPolicy, client, listener);
        }
    }

    static class SyncRetryHandler extends AbstractRetryHandler {
        private final PlainActionFuture<BulkResponse> actionFuture;

        public static SyncRetryHandler create(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy, Client client) {
            PlainActionFuture<BulkResponse> actionFuture = PlainActionFuture.newFuture();
            return new SyncRetryHandler(retryOnThrowable, backoffPolicy, client, actionFuture);
        }

        public SyncRetryHandler(Class<? extends Throwable> retryOnThrowable, BackoffPolicy backoffPolicy, Client client, PlainActionFuture<BulkResponse> actionFuture) {
            super(retryOnThrowable, backoffPolicy, client, actionFuture);
            this.actionFuture = actionFuture;
        }

        public ActionFuture<BulkResponse> executeBlocking(BulkRequest bulkRequest) {
            super.execute(bulkRequest);
            return actionFuture;
        }
    }
}
