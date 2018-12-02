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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * CompletableFuture implementation that properly bubbles up errors. How does it work?
 * The main idea is to override newIncompleteFuture() and provide a BaseFuture instance. This means that all CompletionStage methods
 * properly instantiate BaseFuture instances. The newIncompleteFuture() method was only introduced in JDK9.
 * This means that we can't guarantee for older JDKs to return our subclass for the CompletionStage methods. It's ok though,
 * because ES 7.0 will require JDK 11 to run. There are other methods on CompletableFuture that are unsafe to use, and we will have
 * to ban them. I envision the following plan for them:
 * - add CompletableFuture methods to forbidden APIs:
 *   - blacklist all methods on CompletableFuture / CompletableStage that take the default (forkjoinpool) executor
 *   - blacklist constructor methods of CompletableFuture and static methods that create CompletableFuture instances
 *     (failedFuture, allOf, anyOf, 2 * supplyAsync, 2 * runAsync, completedFuture)
 *   - blacklist methods that return MinimalStage (completedStage, failedStage, minimalCompletionStage(possibly override this one))
 *   - blacklist methods we cannot wrap because they only exist on JDK9 (2 * completeAsync, orTimeout, completeOnTimeout,
 *     2 * delayedExecutor,
 * - provide corresponding methods for the forbidden static ones on BaseFuture
 * - possibly return BaseFuture on overridden methods such as whenComplete etc. This makes users able to program against BaseFuture
 *   instead of CompletableFuture. Would only be possible once we switch to JDK 9+ as JDK8 would get a classcastexception otherwise.
 */
public class BaseFuture<V> extends CompletableFuture<V> {

    private static final String BLOCKING_OP_REASON = "Blocking operation";

    // Unfortunately this method was only introduced in JDK 9. This means that we can't guarantee for older JDKs to return our
    // subclass here for the CompletionStage methods (bummer)
    // it's ok though, because ES 7.0 will require JDK 11 to run
    public <U> CompletableFuture<U> newIncompleteFuture() {
        return new BaseFuture<>();
    }

    // method only provided since JDK9 so cannot specify Override annotation here
    public Executor defaultExecutor() {
        throw new IllegalStateException("default executor");
    }

    // method only provided since JDK9 so cannot specify Override annotation here
    public CompletionStage<V> minimalCompletionStage() {
        throw new IllegalStateException("minimalCompletionStage");
    }

    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException {
        assert timeout <= 0 || blockingAllowed();
        return super.get(timeout, unit);
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
        assert blockingAllowed();
        return super.get();
    }

    private static boolean blockingAllowed() {
        return Transports.assertNotTransportThread(BLOCKING_OP_REASON) &&
            ThreadPool.assertNotScheduleThread(BLOCKING_OP_REASON) &&
            ClusterApplierService.assertNotClusterStateUpdateThread(BLOCKING_OP_REASON) &&
            MasterService.assertNotMasterUpdateThread(BLOCKING_OP_REASON);
    }

    @Override
    public boolean completeExceptionally(Throwable ex) {
        ExceptionsHelper.maybeDieOnAnotherThread(ex);
        return super.completeExceptionally(ex);
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<? super V,? extends U> fn) {
        return super.thenApply(wrap(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenApplyAsync(Function<? super V, ? extends U> fn, Executor executor) {
        return super.thenApplyAsync(wrap(fn), executor);
    }

    @Override
    public CompletableFuture<Void> thenAccept(Consumer<? super V> action) {
        return super.thenAccept(wrap(action));
    }

    @Override
    public CompletableFuture<Void> thenAcceptAsync(Consumer<? super V> action, Executor executor) {
        return super.thenAcceptAsync(wrap(action), executor);
    }

    @Override
    public CompletableFuture<Void> thenRun(Runnable action) {
        return super.thenRun(wrap(action));
    }

    @Override
    public CompletableFuture<Void> thenRunAsync(Runnable action, Executor executor) {
        return super.thenRunAsync(wrap(action), executor);
    }

    @Override
    public <U, T> CompletableFuture<T> thenCombine(CompletionStage<? extends U> other, BiFunction<? super V, ? super U, ? extends T> fn) {
        return super.thenCombine(other, wrap(fn));
    }

    @Override
    public <U, T> CompletableFuture<T> thenCombineAsync(CompletionStage<? extends U> other,
                                                        BiFunction<? super V, ? super U, ? extends T> fn, Executor executor) {
        return super.thenCombineAsync(other, wrap(fn), executor);
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action) {
        return super.thenAcceptBoth(other, wrap(action));
    }

    @Override
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletionStage<? extends U> other, BiConsumer<? super V, ? super U> action,
                                                           Executor executor) {
        return super.thenAcceptBothAsync(other, wrap(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterBoth(CompletionStage<?> other, Runnable action) {
        return super.runAfterBoth(other, wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterBothAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterBothAsync(other, wrap(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> applyToEither(CompletionStage<? extends V> other, Function<? super V, U> fn) {
        return super.applyToEither(other, wrap(fn));
    }

    @Override
    public <U> CompletableFuture<U> applyToEitherAsync(CompletionStage<? extends V> other, Function<? super V, U> fn, Executor executor) {
        return super.applyToEitherAsync(other, wrap(fn), executor);
    }

    @Override
    public CompletableFuture<Void> acceptEither(CompletionStage<? extends V> other, Consumer<? super V> action) {
        return super.acceptEither(other, wrap(action));
    }

    @Override
    public CompletableFuture<Void> acceptEitherAsync(CompletionStage<? extends V> other, Consumer<? super V> action, Executor executor) {
        return super.acceptEitherAsync(other, wrap(action), executor);
    }

    @Override
    public CompletableFuture<Void> runAfterEither(CompletionStage<?> other, Runnable action) {
        return super.runAfterEither(other, wrap(action));
    }

    @Override
    public CompletableFuture<Void> runAfterEitherAsync(CompletionStage<?> other, Runnable action, Executor executor) {
        return super.runAfterEitherAsync(other, wrap(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> thenCompose(Function<? super V, ? extends CompletionStage<U>> fn) {
        return super.thenCompose(wrap(fn));
    }

    @Override
    public <U> CompletableFuture<U> thenComposeAsync(Function<? super V, ? extends CompletionStage<U>> fn, Executor executor) {
        return super.thenComposeAsync(wrap(fn), executor);
    }

    @Override
    public CompletableFuture<V> whenComplete(BiConsumer<? super V, ? super Throwable> action) {
        return super.whenComplete(wrap2(action));
    }

    @Override
    public CompletableFuture<V> whenCompleteAsync(BiConsumer<? super V, ? super Throwable> action, Executor executor) {
        return super.whenCompleteAsync(wrap2(action), executor);
    }

    @Override
    public <U> CompletableFuture<U> handle(BiFunction<? super V, Throwable, ? extends U> fn) {
        return super.handle(wrap2(fn));
    }

    @Override
    public <U> CompletableFuture<U> handleAsync(BiFunction<? super V, Throwable, ? extends U> fn, Executor executor) {
        return super.handleAsync(wrap2(fn), executor);
    }

    @Override
    public CompletableFuture<V> toCompletableFuture() {
        return super.toCompletableFuture();
    }

    @Override
    public CompletableFuture<V> exceptionally(Function<Throwable, ? extends V> fn) {
        return super.exceptionally(wrap(fn));
    }

    private BiConsumer<? super V, ? super Throwable> wrap2(BiConsumer<? super V, ? super Throwable> action) {
        return (v, t) -> {
            if (t != null) {
                ExceptionsHelper.maybeDieOnAnotherThread(t);
            }
            try {
                action.accept(v, t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private static <X, Y> BiConsumer<X, Y> wrap(BiConsumer<X, Y> action) {
        return (v, t) -> {
            try {
                action.accept(v, t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private <U> BiFunction<? super V, Throwable, ? extends U> wrap2(BiFunction<? super V, Throwable, ? extends U> fn) {
        return (v, t) -> {
            if (t != null) {
                ExceptionsHelper.maybeDieOnAnotherThread(t);
            }
            try {
                return fn.apply(v, t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private static <X, Y, Z> BiFunction<X, Y, Z> wrap(BiFunction<X, Y, Z> fn) {
        return (v, t) -> {
            try {
                return fn.apply(v, t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private static <T, U> Function<T, U> wrap(Function<T, U> fn) {
        return t -> {
            if (t instanceof Throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread((Throwable) t);
            }
            try {
                return fn.apply(t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private static <T> Consumer<T> wrap(Consumer<T> consumer) {
        return t -> {
            try {
                consumer.accept(t);
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

    private static Runnable wrap(Runnable runnable) {
        return () -> {
            try {
                runnable.run();
            } catch (Throwable throwable) {
                ExceptionsHelper.maybeDieOnAnotherThread(throwable);
                throw throwable;
            }
        };
    }

}
