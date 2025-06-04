/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.support;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.exception.ElasticsearchWrapperException;
import org.elasticsearch.exception.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;

import java.util.concurrent.ExecutionException;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableFuture} and {@link SubscribableListener} except for its handling of exceptions: if this listener is completed
 * exceptionally with an {@link ElasticsearchException} that is also an {@link ElasticsearchWrapperException} then it is unwrapped using
 * {@link ExceptionsHelper#unwrapCause}; if the resulting exception is a checked exception then it is wrapped in an {@link
 * UncategorizedExecutionException}. Moreover if this listener is completed exceptionally with a checked exception then it wraps the
 * exception in an {@link UncategorizedExecutionException} whose cause is an {@link ExecutionException}, whose cause in turn is the checked
 * exception. This matches the behaviour of {@link PlainActionFuture#actionGet}.
 */
// The name {@link ListenableActionFuture} dates back a long way and could be improved - TODO find a better name
public final class ListenableActionFuture<T> extends SubscribableListener<T> {
    public T actionResult() {
        try {
            return super.rawResult();
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override
    protected RuntimeException wrapException(Exception exception) {
        if (exception instanceof ElasticsearchException elasticsearchException) {
            final var rootCause = ExceptionsHelper.unwrapCause(elasticsearchException);
            if (rootCause instanceof RuntimeException runtimeException) {
                return runtimeException;
            } else {
                return new UncategorizedExecutionException("Failed execution", rootCause);
            }
        } else {
            return wrapAsExecutionException(exception);
        }
    }
}
