/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasources;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Nullable;

import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;
import java.util.function.Supplier;

/**
 * Wraps discovery/resolution IO executors so submitted tasks restore the caller's
 * {@link ThreadContext} and {@link StorageRetryCancellation} scope, while keeping
 * {@link AbstractRunnable} identity.
 *
 * <p>{@link EsThreadPoolExecutor#execute} only calls {@link AbstractRunnable#onRejection} when the
 * submitted task is an {@link AbstractRunnable}. Wrapping work in a lambda makes a pool rejection
 * throw instead; {@code SubscribableListener} swallows that throw so waiters hang, and
 * {@code ThrottledIterator} never runs {@code onAfter} so its ref count sticks. This helper wraps
 * an {@link AbstractRunnable} as an {@link AbstractRunnable}: restore context (and optional cancel)
 * in {@code doRun}, call {@code in.run()} on the success path so the inner {@code onFailure}/
 * {@code onAfter} fire once, forward {@code onRejection} and {@code isForceExecution}, and forward
 * {@code onAfter} only when the inner never ran (pool rejection).
 */
public final class ExternalIoExecutors {

    private ExternalIoExecutors() {}

    /**
     * Restores {@code restorableContext} (when non-null) and installs {@code isCancelled} as the
     * ambient {@link StorageRetryCancellation} scope (when non-null) around every task
     * {@code executor} runs.
     */
    public static Executor restoring(
        Executor executor,
        @Nullable Supplier<ThreadContext.StoredContext> restorableContext,
        @Nullable BooleanSupplier isCancelled
    ) {
        return preserving(executor, command -> restoreAndRun(restorableContext, isCancelled, command));
    }

    /**
     * Runs {@code around} around every task, preserving {@link AbstractRunnable} so a throwing or
     * rejecting inner executor still delivers {@code onRejection}/{@code onAfter}.
     */
    public static Executor preserving(Executor executor, Consumer<Runnable> around) {
        return command -> {
            Runnable wrapped = wrap(command, around);
            try {
                executor.execute(wrapped);
            } catch (Exception e) {
                if (wrapped instanceof AbstractRunnable abstractRunnable) {
                    try {
                        abstractRunnable.onRejection(e);
                    } finally {
                        abstractRunnable.onAfter();
                    }
                } else {
                    throw ExceptionsHelper.convertToRuntime(e);
                }
            }
        };
    }

    private static Runnable wrap(Runnable command, Consumer<Runnable> around) {
        if (command instanceof AbstractRunnable inner) {
            return new AbstractRunnable() {
                private boolean ran;

                @Override
                public boolean isForceExecution() {
                    return inner.isForceExecution();
                }

                @Override
                protected void doRun() {
                    around.accept(() -> {
                        ran = true;
                        inner.run();
                    });
                }

                @Override
                public void onFailure(Exception e) {
                    if (ran == false) {
                        inner.onFailure(e);
                    } else {
                        ExceptionsHelper.reThrowIfNotNull(e);
                    }
                }

                @Override
                public void onRejection(Exception e) {
                    inner.onRejection(e);
                }

                @Override
                public void onAfter() {
                    if (ran == false) {
                        inner.onAfter();
                    }
                }
            };
        }
        return () -> around.accept(command);
    }

    private static void restoreAndRun(
        @Nullable Supplier<ThreadContext.StoredContext> restorableContext,
        @Nullable BooleanSupplier isCancelled,
        Runnable command
    ) {
        if (restorableContext == null) {
            runWithOptionalCancel(isCancelled, command);
            return;
        }
        try (ThreadContext.StoredContext ignored = restorableContext.get()) {
            runWithOptionalCancel(isCancelled, command);
        }
    }

    private static void runWithOptionalCancel(@Nullable BooleanSupplier isCancelled, Runnable command) {
        if (isCancelled == null) {
            command.run();
            return;
        }
        StorageRetryCancellation.runWithCancellation(isCancelled, command::run);
    }
}
