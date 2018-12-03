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

package org.elasticsearch.common.util.concurrent;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.cluster.service.ClusterApplierService;
import org.elasticsearch.cluster.service.MasterService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.Transports;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Wraps a CompletableFuture and ensures Errors are properly bubbled up
 * TODO:
 * - add CompletableFuture class and methods to forbidden APIs:
 *   - blacklist constructor methods of CompletableFuture and static methods that create CompletableFuture instances
 *   - blacklist methods that return MinimalStage (completedStage, failedStage, minimalCompletionStage(possibly override this one))
 *   - blacklist static methods on CompletableFuture (failedFuture, allOf, anyOf, 2 * supplyAsync, 2 * runAsync, completedFuture)
 * - provide corresponding methods for the forbidden static ones on BaseFuture
 */
public class BaseFuture<V> implements Future<V>, CompletionStage<V> {

    private final CompletableFuture<V> wrapped;

    public BaseFuture() {
        this(new CompletableFuture<>());
    }

    private BaseFuture(CompletableFuture<V> fut) {
        wrapped = fut;
        wrapped.exceptionally(t -> {
            ExceptionsHelper.maybeDieOnAnotherThread(t);
            return null;
        });
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException {
        assert timeout <= 0 || blockingAllowed();
        return wrapped.get(timeout, unit);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        assert blockingAllowed();
        return wrapped.get();
    }

    public V join() {
        assert blockingAllowed();
        return wrapped.join();
    }

    private static final String BLOCKING_OP_REASON = "Blocking operation";

    private static boolean blockingAllowed() {
        return Transports.assertNotTransportThread(BLOCKING_OP_REASON) &&
            ThreadPool.assertNotScheduleThread(BLOCKING_OP_REASON) &&
            ClusterApplierService.assertNotClusterStateUpdateThread(BLOCKING_OP_REASON) &&
            MasterService.assertNotMasterUpdateThread(BLOCKING_OP_REASON);
    }

    public V getNow(V valueIfAbsent) {
        return wrapped.getNow(valueIfAbsent);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return wrapped.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return wrapped.isCancelled();
    }

    @Override
    public boolean isDone() {
        return wrapped.isDone();
    }

    public boolean isCompletedExceptionally() {
        return wrapped.isCompletedExceptionally();
    }

    public boolean complete(V value) {
        return wrapped.complete(value);
    }

    public boolean completeExceptionally(Throwable ex) {
        return wrapped.completeExceptionally(ex);
    }

    @Override
    public String toString() {
        return "BaseFuture{" + wrapped + "}";
    }

    public static BaseFuture<Void> allOf(BaseFuture<?>... cfs) {
        return new BaseFuture<>(CompletableFuture.allOf(Arrays.stream(cfs).map(bf -> bf.wrapped).toArray(CompletableFuture[]::new)));
    }

    public static BaseFuture<Object> anyOf(BaseFuture<?>... cfs) {
        return new BaseFuture<>(CompletableFuture.anyOf(Arrays.stream(cfs).map(bf -> bf.wrapped).toArray(CompletableFuture[]::new)));
    }

    public static <U> BaseFuture<U> supplyAsync(Supplier<U> supplier, Executor executor) {
        return new BaseFuture<>(CompletableFuture.supplyAsync(supplier, executor));
    }

    public static BaseFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return new BaseFuture<>(CompletableFuture.runAsync(runnable, executor));
    }

    public static <U> BaseFuture<U> completedFuture(U value) {
        return new BaseFuture<>(CompletableFuture.completedFuture(value));
    }

    public static <U> BaseFuture<U> failedFuture(Throwable ex) {
        final BaseFuture fut = new BaseFuture<>();
        fut.completeExceptionally(ex);
        return fut;
    }

    @Override
    public <U> BaseFuture<U> thenApply(Function<? super V,? extends U> fn) {
        return new BaseFuture<>(wrapped.thenApply(fn));
    }

    @Override
    public <U> CompletionStage<U> thenApplyAsync(Function<? super V, ? extends U> fn) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U> BaseFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return new BaseFuture<>(wrapped.thenApplyAsync(fn, executor));
    }

    @Override
    public BaseFuture<Void> thenAccept(Consumer<? super V> action) {
        return new BaseFuture<>(wrapped.thenAccept(action));
    }

    @Override
    public CompletionStage<Void> thenAcceptAsync(Consumer<? super V> action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return new BaseFuture<>(wrapped.thenAcceptAsync(action, executor));
    }

    @Override
    public BaseFuture<Void> thenRun(Runnable action) {
        return new BaseFuture<>(wrapped.thenRun(action));
    }

    @Override
    public CompletionStage<Void> thenRunAsync(Runnable action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return new BaseFuture<>(wrapped.thenRunAsync(action, executor));
    }

    @Override
    public <U, T> BaseFuture<T> thenCombine(CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends T> fn) {
        return new BaseFuture<>(wrapped.thenCombine(other, fn));
    }

    @Override
    public <U, T> CompletionStage<T> thenCombineAsync(CompletionStage<? extends U> other,
                                                      BiFunction<? super V, ? super U, ? extends T> fn) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U, T> BaseFuture<T> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends T> fn, Executor executor) {
        return new BaseFuture<>(wrapped.thenCombineAsync(other, fn, executor));
    }

    @Override
    public <U> BaseFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return new BaseFuture<>(wrapped.thenAcceptBoth(other, action));
    }

    @Override
    public <U> CompletionStage<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U> BaseFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action,
                                                           Executor executor) {
        return new BaseFuture<>(wrapped.thenAcceptBothAsync(other, action, executor));
    }

    @Override
    public BaseFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return new BaseFuture<>(wrapped.runAfterBoth(other, action));
    }

    @Override
    public CompletionStage<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return new BaseFuture<>(wrapped.runAfterBothAsync(other, action, executor));
    }

    @Override
    public <U> BaseFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return new BaseFuture<>(wrapped.applyToEither(other, fn));
    }

    @Override
    public <U> CompletionStage<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U> BaseFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn, Executor executor) {
        return new BaseFuture<>(wrapped.applyToEitherAsync(other, fn, executor));
    }

    @Override
    public BaseFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return new BaseFuture<>(wrapped.acceptEither(other, action));
    }

    @Override
    public CompletionStage<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action, Executor executor) {
        return new BaseFuture<>(wrapped.acceptEitherAsync(other, action, executor));
    }

    @Override
    public BaseFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return new BaseFuture<>(wrapped.runAfterEither(other, action));
    }

    @Override
    public CompletionStage<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return new BaseFuture<>(wrapped.runAfterEitherAsync(other, action, executor));
    }

    @Override
    public <U> BaseFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return new BaseFuture<>(wrapped.thenCompose(fn));
    }

    @Override
    public <U> CompletionStage<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U> BaseFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return new BaseFuture<>(wrapped.thenComposeAsync(fn, executor));
    }

    @Override
    public BaseFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return new BaseFuture<>(wrapped.whenComplete(action));
    }

    @Override
    public CompletionStage<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public BaseFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return new BaseFuture<>(wrapped.whenCompleteAsync(action, executor));
    }

    @Override
    public <U> BaseFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return new BaseFuture<>(wrapped.handle(fn));
    }

    @Override
    public <U> CompletionStage<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn) {
        throw new UnsupportedOperationException("specify executor");
    }

    @Override
    public <U> BaseFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return new BaseFuture<>(wrapped.handleAsync(fn, executor));
    }

    @Override
    public BaseFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return new BaseFuture<>(wrapped.exceptionally(fn));
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return wrapped.toCompletableFuture();
    }
}
