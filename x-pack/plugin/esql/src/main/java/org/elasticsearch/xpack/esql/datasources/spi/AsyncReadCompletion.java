/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources.spi;

import org.elasticsearch.ExceptionsHelper;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

/**
 * Helpers for bridging a foreign async framework's {@link CompletableFuture} to an
 * {@link org.elasticsearch.action.ActionListener}.
 *
 * <p>Native async storage reads are handed a {@code CompletableFuture} by the underlying client
 * (the AWS SDK's {@code AsyncResponseTransformer.prepare()}, {@code java.net.http.HttpClient.sendAsync},
 * the Azure SDK's reactive {@code .toFuture()}), so the future itself is not ours to replace. We bridge
 * its completion to an {@code ActionListener} with {@link CompletableFuture#whenComplete} and discard the
 * future it returns.
 *
 * <p>That discard is where {@code CompletableFuture} is trappy: it captures any {@link Throwable} the
 * completion callback raises into the (discarded) returned future instead of letting it propagate. A fatal
 * {@link Error} — {@code OutOfMemoryError}, {@code AssertionError} from a test, a {@code StackOverflowError}
 * — thrown while delivering the result to the listener would therefore vanish silently rather than reaching
 * the uncaught-exception handler. {@link #errorSafe} closes that hole.
 */
public final class AsyncReadCompletion {

    private AsyncReadCompletion() {}

    /**
     * Wrap a {@link CompletableFuture#whenComplete} callback so that a fatal {@link Error} escaping it is
     * dispatched to the uncaught-exception handler via {@link ExceptionsHelper#maybeDieOnAnotherThread}
     * rather than being captured and swallowed by the completing {@code CompletableFuture}.
     *
     * <p>Non-fatal throwables are re-raised unchanged: a well-behaved {@code ActionListener} handles its own
     * exceptions, so anything a callback still throws is a listener-contract violation, not a condition to
     * recover from here. The behaviour for {@code Error} is the only change — and the one that matters,
     * because that is the throwable {@code CompletableFuture} silently eats.
     */
    public static <T> BiConsumer<T, Throwable> errorSafe(BiConsumer<T, Throwable> action) {
        return (result, failure) -> {
            try {
                action.accept(result, failure);
            } catch (Throwable t) {
                // whenComplete would otherwise capture this into the future it returns, which we discard.
                // Fork a thread to rethrow any fatal Error so it reaches the uncaught-exception handler.
                ExceptionsHelper.maybeDieOnAnotherThread(t);
                throw t;
            }
        };
    }
}
