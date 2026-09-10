/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Flow;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared helpers for integration tests that exercise streaming ES|QL execution
 * ({@code POST /_query?streaming=true}).
 */
public final class StreamQueryTestUtils {

    private StreamQueryTestUtils() {}

    public static EsqlStreamQueryAction.StreamStart executeStreamRequest(
        Client client,
        EsqlQueryRequest source,
        CountingStreamSubscriber subscriber
    ) throws Exception {
        int batchSize = ESTestCase.randomIntBetween(1, 10);
        AtomicReference<EsqlStreamQueryAction.StreamStart> startRef = new AtomicReference<>();
        ActionFuture<ActionResponse.Empty> future = client.execute(
            EsqlStreamQueryAction.INSTANCE,
            EsqlStreamQueryRequest.from(source, ActionListener.wrap(start -> {
                startRef.set(start);
                start.publisher().subscribe(subscriber);
            }, subscriber.failure::set), false, batchSize)
        );
        future.actionGet(TimeValue.timeValueSeconds(60));
        EsqlStreamQueryAction.StreamStart streamStart = startRef.get();
        ESTestCase.assertNotNull(
            "streamStartListener was never called — no HTTP response would have been sent for this query",
            streamStart
        );
        ESTestCase.assertTrue(
            "subscriber terminal signal (onComplete or onError) never fired — the stream was never terminated",
            subscriber.completed.await(60, TimeUnit.SECONDS)
        );
        subscriber.rethrowIfFailed();
        return streamStart;
    }

    public static class CountingStreamSubscriber implements Flow.Subscriber<Page> {

        public final AtomicInteger rowCount = new AtomicInteger();
        public final AtomicReference<Throwable> failure = new AtomicReference<>();
        public final CountDownLatch completed = new CountDownLatch(1);
        private volatile Flow.Subscription subscription;

        @Override
        public void onSubscribe(Flow.Subscription subscription) {
            this.subscription = subscription;
            subscription.request(1);
        }

        @Override
        public void onNext(Page page) {
            try {
                rowCount.addAndGet(page.getPositionCount());
                page.releaseBlocks();
            } finally {
                subscription.request(1);
            }
        }

        @Override
        public void onError(Throwable throwable) {
            failure.set(throwable);
            completed.countDown();
        }

        @Override
        public void onComplete() {
            completed.countDown();
        }

        public void rethrowIfFailed() throws Exception {
            Throwable t = failure.get();
            if (t instanceof Exception e) {
                throw e;
            } else if (t != null) {
                throw new AssertionError("subscriber received unexpected error", t);
            }
        }
    }
}
