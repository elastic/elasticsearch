/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalException;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

/**
 * Runs a query attempt again once when an external object was rewritten under it. The failed attempt has already dropped the
 * listing and file-metadata caches, so the second attempt lists the object again and reads its new version.
 * <p>
 * The retry is bounded: a second object-changed failure is returned as it is. The retry is skipped when the query was cancelled,
 * and when the executor rejects it, in which case the original failure is returned with the rejection suppressed. The thread
 * context is restored to its state before the first attempt, so warnings and response headers from the failed attempt do not
 * reach the response of the retry.
 */
final class ObjectChangedRetry {

    private ObjectChangedRetry() {}

    /**
     * @param attempt starts one attempt of the query and completes {@code listener} with its outcome
     * @param retryable whether a failure may be retried at all; false for queries whose state outlives the attempt
     * @param executor runs the second attempt, so that planning starts on the pool it asserts on
     * @param cancelled consulted before retrying, so a cancelled query is not planned again
     */
    static <T> void run(
        Consumer<ActionListener<T>> attempt,
        ActionListener<T> listener,
        ThreadContext threadContext,
        Executor executor,
        BooleanSupplier cancelled,
        boolean retryable
    ) {
        if (retryable == false) {
            attempt.accept(listener);
            return;
        }
        ThreadContext.StoredContext initial = threadContext.newStoredContext();
        attempt.accept(listener.delegateResponse((l, ex) -> {
            if (ExternalException.hasCondition(ex, ExternalException.Condition.OBJECT_CHANGED) == false || cancelled.getAsBoolean()) {
                l.onFailure(ex);
                return;
            }
            AbstractRunnable retry = new AbstractRunnable() {
                @Override
                protected void doRun() {
                    initial.restore();
                    attempt.accept(l);
                }

                @Override
                public void onFailure(Exception e) {
                    l.onFailure(e);
                }

                // An Elasticsearch pool does not throw a rejection for an AbstractRunnable; it reports it here instead.
                @Override
                public void onRejection(Exception rejected) {
                    ex.addSuppressed(rejected);
                    l.onFailure(ex);
                }
            };
            try {
                executor.execute(retry);
            } catch (EsRejectedExecutionException rejected) {
                ex.addSuppressed(rejected);
                l.onFailure(ex);
            }
        }));
    }
}
