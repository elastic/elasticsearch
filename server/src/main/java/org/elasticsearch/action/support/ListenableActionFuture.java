/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchWrapperException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ListenableFuture;

/**
 * An {@link ActionListener} which allows for the result to fan out to a (dynamic) collection of other listeners, added using {@link
 * #addListener}. Listeners added before completion are retained until completion; listeners added after completion are completed
 * immediately.
 *
 * Similar to {@link ListenableFuture}, except that if this listener is completed exceptionally then it unwraps any layers of {@link
 * ElasticsearchWrapperException} layers before converting the exception to a {@link RuntimeException}.
 */
// The name {@link ListenableActionFuture} dates back a long way and could be improved - TODO find a better name
public final class ListenableActionFuture<T> extends FanOutListener<T> {
    public T actionResult() {
        try {
            return super.rawResult();
        } catch (Exception e) {
            throw wrapException(e);
        }
    }

    @Override
    protected RuntimeException wrapException(Exception exception) {
        return wrapAsExecutionException(ExceptionsHelper.unwrapCause(exception));
    }
}
