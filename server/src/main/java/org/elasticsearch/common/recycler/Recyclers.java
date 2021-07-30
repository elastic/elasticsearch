/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.recycler;

import com.carrotsearch.hppc.BitMixer;

import java.util.ArrayDeque;

public enum Recyclers {
    ;

    /**
     * Return a {@link Recycler} that never recycles entries.
     */
    public static <T> Recycler<T> none(Recycler.C<T> c) {
        return new NoneRecycler<>(c);
    }

    /**
     * Return a concurrent recycler based on a deque.
     */
    public static <T> Recycler<T> concurrentDeque(Recycler.C<T> c, int limit) {
        return new ConcurrentDequeRecycler<>(c, limit);
    }

    /**
     * Return a recycler based on a deque.
     */
    public static <T> Recycler<T> deque(Recycler.C<T> c, int limit) {
        return new DequeRecycler<>(c, new ArrayDeque<>(), limit);
    }

    /**
     * Return a recycler based on a deque.
     */
    public static <T> Recycler.Factory<T> dequeFactory(final Recycler.C<T> c, final int limit) {
        return () -> deque(c, limit);
    }

    /**
     * Wrap the provided recycler so that calls to {@link Recycler#obtain()} and {@link Recycler.V#close()} are protected by
     * a lock.
     */
    public static <T> Recycler<T> locked(final Recycler<T> recycler) {
        return new FilterRecycler<T>() {

            private final Object lock;

            {
                this.lock = new Object();
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recycler;
            }

            @Override
            public Recycler.V<T> obtain() {
                synchronized (lock) {
                    return super.obtain();
                }
            }

            @Override
            protected Recycler.V<T> wrap(final Recycler.V<T> delegate) {
                return new Recycler.V<T>() {

                    @Override
                    public void close() {
                        synchronized (lock) {
                            delegate.close();
                        }
                    }

                    @Override
                    public T v() {
                        return delegate.v();
                    }

                    @Override
                    public boolean isRecycled() {
                        return delegate.isRecycled();
                    }

                };
            }

        };
    }

    /**
     * Create a concurrent implementation that can support concurrent access from
     * <code>concurrencyLevel</code> threads with little contention.
     */
    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory, final int concurrencyLevel) {
        if (concurrencyLevel < 1) {
            throw new IllegalArgumentException("concurrencyLevel must be >= 1");
        }
        if (concurrencyLevel == 1) {
            return locked(factory.build());
        }
        return new FilterRecycler<T>() {

            private final Recycler<T>[] recyclers;

            {
                @SuppressWarnings({"rawtypes", "unchecked"})
                final Recycler<T>[] recyclers = new Recycler[concurrencyLevel];
                this.recyclers = recyclers;
                for (int i = 0; i < concurrencyLevel; ++i) {
                    recyclers[i] = locked(factory.build());
                }
            }

            int slot() {
                final long id = Thread.currentThread().getId();
                // don't trust Thread.hashCode to have equiprobable low bits
                int slot = (int) BitMixer.mix64(id);
                // make positive, otherwise % may return negative numbers
                slot &= 0x7FFFFFFF;
                slot %= concurrencyLevel;
                return slot;
            }

            @Override
            protected Recycler<T> getDelegate() {
                return recyclers[slot()];
            }

        };
    }

    public static <T> Recycler<T> concurrent(final Recycler.Factory<T> factory) {
        return concurrent(factory, Runtime.getRuntime().availableProcessors());
    }
}
