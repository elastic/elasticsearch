/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.Objects;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * A listener for a {@link Releasable} which races against a timeout, but which does not leak any {@link Releasable} with which it is
 * eventually completed.
 */
public class TimeoutReleasableListener implements ActionListener<Releasable> {

    private final Supplier<ActionListener<Releasable>> listenerSupplier;
    private final Runnable timeoutCanceller;

    public static TimeoutReleasableListener create(
        ActionListener<Releasable> delegate,
        TimeValue timeout,
        ThreadPool threadPool,
        Executor timeoutExecutor
    ) {
        final var listenerSupplier = readOnce(delegate);
        return new TimeoutReleasableListener(listenerSupplier, scheduleTimeout(timeout, threadPool, timeoutExecutor, listenerSupplier));
    }

    private static Supplier<ActionListener<Releasable>> readOnce(ActionListener<Releasable> listener) {
        final var listenerRef = new AtomicReference<>(ActionListener.assertOnce(listener));
        return new Supplier<>() {
            @Override
            public ActionListener<Releasable> get() {
                return listenerRef.getAndSet(null);
            }

            @Override
            public String toString() {
                return Objects.toString(listenerRef.get());
            }
        };
    }

    private static Runnable scheduleTimeout(
        TimeValue timeout,
        ThreadPool threadPool,
        Executor timeoutExecutor,
        Supplier<ActionListener<Releasable>> listenerSupplier
    ) {
        try {
            final var cancellable = threadPool.schedule(() -> {
                final var listener = listenerSupplier.get();
                if (listener != null) {
                    listener.onFailure(new ElasticsearchTimeoutException("timed out after [" + timeout + "/" + timeout.millis() + "ms]"));
                }
            }, timeout, timeoutExecutor);
            return () -> {
                try {
                    cancellable.cancel();
                } catch (Exception e) {
                    // should not happen, we cannot reasonably do anything with an exception here anyway
                    assert false : e;
                }
            };
        } catch (Exception e) {
            listenerSupplier.get().onFailure(e);
            return () -> {};
        }
    }

    private TimeoutReleasableListener(Supplier<ActionListener<Releasable>> listenerSupplier, Runnable timeoutCanceller) {
        this.listenerSupplier = listenerSupplier;
        this.timeoutCanceller = timeoutCanceller;
    }

    @Override
    public void onResponse(Releasable releasable) {
        final var listener = listenerSupplier.get();
        if (listener == null) {
            Releasables.closeExpectNoException(releasable);
        } else {
            timeoutCanceller.run();
            listener.onResponse(releasable);
        }
    }

    @Override
    public void onFailure(Exception e) {
        final var listener = listenerSupplier.get();
        if (listener != null) {
            timeoutCanceller.run();
            listener.onFailure(e);
        }
    }

    @Override
    public String toString() {
        return "TimeoutReleasableListener[" + listenerSupplier + "]";
    }
}
