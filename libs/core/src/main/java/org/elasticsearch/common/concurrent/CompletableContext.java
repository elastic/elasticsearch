/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.concurrent;

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

    private final CompletableFuture<T> completableFuture = new CompletableFuture<>();

    public void addListener(BiConsumer<T, ? super Exception> listener) {
        BiConsumer<T, Throwable> castThrowable = (v, t) -> {
            if (t == null) {
                listener.accept(v, null);
            } else {
                assert !(t instanceof Error) : "Cannot be error";
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
