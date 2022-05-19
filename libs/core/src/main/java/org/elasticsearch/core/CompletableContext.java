/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.core;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * A thread-safe completable context that allows listeners to be attached. This class relies on the
 * {@link CompletableFuture} for the concurrency logic. However, it does not accept {@link Throwable} as
 * an exceptional result. This allows attaching listeners that only handle {@link Exception}.
 *
 * @param <T> the result type
 */
public class CompletableContext<T> {

    public CompletableContext() {}

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    public void addListener(BiConsumer<T, ? super Exception> listener) {
        BiConsumer<T, Throwable> castThrowable = (v, t) -> {
            if (t == null) {
                listener.accept(v, null);
            } else {
                assert (t instanceof Error) == false : "Cannot be error";
                listener.accept(v, (Exception) t);
            }
        };
        completableFuture.whenComplete(castThrowable);
    }

    public boolean isDone() {
        return completableFuture.isDone();
    }

    public boolean isCompletedExceptionally() {
        return completableFuture.isCompletedExceptionally();
    }

    public boolean completeExceptionally(Exception ex) {
        return completableFuture.completeExceptionally(ex);
    }

    public boolean complete(T value) {
        return completableFuture.complete(value);
    }
}
