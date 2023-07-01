/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ListenableActionFuture;
import org.elasticsearch.action.support.SubscribableListener;

import java.util.concurrent.ExecutionException;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableActionFuture} and {@link SubscribableListener} except for its handling of exceptions: if this listener is
 * completed exceptionally with a checked exception then it wraps the exception in an {@link UncategorizedExecutionException} whose cause is
 * an {@link ExecutionException}, whose cause in turn is the checked exception. This matches the behaviour of {@link FutureUtils#get}.
 */
// The name {@link ListenableFuture} dates back a long way and could be improved - TODO find a better name
public final class ListenableFuture<T> extends SubscribableListener<T> {
    public T result() {
        try {
            return super.rawResult();
        } catch (Exception e) {
            throw wrapAsExecutionException(e);
        }
    }

    @Override
    protected RuntimeException wrapException(Exception exception) {
        return wrapAsExecutionException(exception);
    }
}
