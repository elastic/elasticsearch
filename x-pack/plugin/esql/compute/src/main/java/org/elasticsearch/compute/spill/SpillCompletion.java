/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.compute.operator.IsBlockedResult;
import org.elasticsearch.core.Nullable;

/**
 * The completion of one storage operation, usable by anything that holds a listener: the operator that issued it, or the
 * response cursor that reads a result back. It hands the backend a listener and remembers what came back; anyone may
 * subscribe to the outcome, and {@link #blocked} turns it into the driver's blocked state, which completes when the
 * operation does, success or failure alike, so that the driver runs again and the issuer reads the outcome.
 */
public final class SpillCompletion<T> {

    private final SubscribableListener<T> outcome = new SubscribableListener<>();
    private final SubscribableListener<Void> done = new SubscribableListener<>();
    @Nullable
    private volatile T result;
    @Nullable
    private volatile Exception failure;
    private final ActionListener<T> listener = ActionListener.assertOnce(new ActionListener<>() {
        @Override
        public void onResponse(T response) {
            result = response;
            outcome.onResponse(response);
            done.onResponse(null);
        }

        @Override
        public void onFailure(Exception e) {
            failure = e;
            outcome.onFailure(e);
            done.onResponse(null);
        }
    });

    /**
     * The listener to hand to the backend. It may complete inline.
     */
    public ActionListener<T> listener() {
        return listener;
    }

    public boolean isDone() {
        return outcome.isDone();
    }

    public boolean isSuccess() {
        return outcome.isSuccess();
    }

    /**
     * The result, once {@link #isSuccess()}; null before completion or after a failure.
     */
    @Nullable
    public T result() {
        return outcome.isSuccess() ? result : null;
    }

    /**
     * The failure, once the operation failed; null otherwise.
     */
    @Nullable
    public Exception failure() {
        return outcome.isDone() && outcome.isSuccess() == false ? failure : null;
    }

    /**
     * Subscribes to the outcome; called inline when it is already known.
     */
    public void addListener(ActionListener<T> subscriber) {
        outcome.addListener(subscriber);
    }

    /**
     * The driver's blocked state while this operation runs: not blocked once it has completed either way.
     */
    public IsBlockedResult blocked(String reason) {
        return new IsBlockedResult(done, reason);
    }
}
