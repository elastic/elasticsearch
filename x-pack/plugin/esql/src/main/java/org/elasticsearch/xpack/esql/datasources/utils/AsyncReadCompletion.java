/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.utils;

import org.elasticsearch.ExceptionsHelper;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Wraps a {@link CompletableFuture#whenComplete} callback so a fatal {@link Error} it throws is rethrown to
 * the uncaught-exception handler rather than swallowed by the (discarded) future {@code whenComplete} returns.
 * Native external reads bridge a foreign client's {@code CompletableFuture} to an {@code ActionListener} this
 * way, so without this an {@code Error} raised while delivering the result would vanish.
 */
public final class AsyncReadCompletion {

    private AsyncReadCompletion() {}

    /** See the class javadoc. Non-fatal throwables are re-raised unchanged. */
    public static <T> BiConsumer<T, Throwable> errorSafe(BiConsumer<T, Throwable> action) {
        return (result, failure) -> {
            try {
                action.accept(result, failure);
            } catch (Throwable t) {
                ExceptionsHelper.maybeDieOnAnotherThread(t);
                throw t;
            }
        };
    }
}
