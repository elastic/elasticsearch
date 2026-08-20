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

import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Shared helpers for integration tests that exercise the streaming {@code /_query/stream} endpoint.
 */
public final class StreamQueryTestUtils {

    private StreamQueryTestUtils() {}

    public static void executeStreamRequest(Client client, EsqlQueryRequest source, CountingStreamSubscriber subscriber) throws Exception {
        if (source.pageSize() == null) {
            source.pageSize(ESTestCase.randomIntBetween(1, 10));
        }
        ActionFuture<ActionResponse.Empty> future = client.execute(
            EsqlStreamQueryAction.INSTANCE,
            EsqlStreamQueryRequest.from(
                source,
                ActionListener.wrap(start -> start.publisher().subscribe(subscriber), subscriber.failure::set),
                false
            )
        );
        future.actionGet(TimeValue.timeValueSeconds(60));
        subscriber.rethrowIfFailed();
    }

    public static class CountingStreamSubscriber implements Flow.Subscriber<Page> {

        public final AtomicInteger rowCount = new AtomicInteger();
        public final AtomicReference<Throwable> failure = new AtomicReference<>();
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
        }

        @Override
        public void onComplete() {}

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
