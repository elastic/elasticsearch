/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.core.RefCounted;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.logging.LogManager;
import org.elasticsearch.logging.Logger;

/**
 * Adds wrappers around listeners for refcounted responses which retain a ref to the response until this {@link ResponseLifetime} is
 * closed. This is useful when running async actions which return refcounted responses that may not be processed immediately (e.g. via
 * {@link PlainActionFuture} or {@link SubscribableListener}).
 */
public class ResponseLifetime implements Releasable {

    private static final Logger logger = LogManager.getLogger(ResponseLifetime.class);

    private final SubscribableListener<Void> closeListener = new SubscribableListener<>();

    public <T extends RefCounted> ActionListener<T> retainResponse(ActionListener<T> listener) {
        return listener.delegateFailure((l, response) -> {
            if (response.tryIncRef()) {
                try {
                    l.onResponse(response);
                } finally {
                    closeListener.addListener(ActionListener.releasing(response::decRef));
                }
            } else {
                final var exception = new IllegalStateException(
                    Strings.format("could not retain already-released response [%s] for listener [%s]", response, l)
                );
                logger.error("could not retain already-released response", exception);
                assert false : exception;
                l.onFailure(exception);
            }
        });
    }

    @Override
    public void close() {
        closeListener.onResponse(null);
    }
}
