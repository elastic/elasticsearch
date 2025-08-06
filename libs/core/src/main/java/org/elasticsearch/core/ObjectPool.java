/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.core;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.time.Duration;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

// FIXME: Build thread local shim for this to toggle using FeatureFlag
public interface ObjectPool<T> {
    static <T> ObjectPool<T> withInitial(Supplier<T> supplier, Duration timeout) {
        return new HybridPool<>(supplier, timeout.toNanos());
    }

    static <T> ObjectPool<T> withInitial(Supplier<T> supplier) {
        return withInitial(supplier, UnboundedObjectPool.DEFAULT_TIMEOUT);
    }

    interface PooledObject<T> extends AutoCloseable {
        T get();

        void close();
    }

    PooledObject<T> acquire();

    final class HybridPool<T> implements ObjectPool<T> {
        private static final class EsThreadLocal<T> extends ThreadLocal<T> implements PooledObject<T> {
            private final Supplier<T> supplier;

            EsThreadLocal(Supplier<T> supplier) {
                this.supplier = supplier;
            }

            @Override
            protected T initialValue() {
                return supplier.get();
            }

            @Override
            public void close() {
                // noop
            }
        }

        private final PooledObject<T> threadLocal;
        private final UnboundedObjectPool<T> unboundedPool;

        private HybridPool(Supplier<T> supplier, long timeoutNanos) {
            this.threadLocal = new EsThreadLocal<>(supplier);
            this.unboundedPool = new UnboundedObjectPool<>(supplier, timeoutNanos);
        }

        @Override
        public PooledObject<T> acquire() {
            if (Thread.currentThread().isVirtual()) {
                return unboundedPool.acquire();
            } else {
                unboundedPool.checkTimeouts();
                return threadLocal;
            }
        }
    }

    final class UnboundedObjectPool<T> implements ObjectPool<T> {
        private static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(30);
        private static final long TIMEOUT_CHECK_INTERVAL_NANOS = TimeUnit.SECONDS.toNanos(5);

        private static final ThreadFactory VTHREAD_FACTORY = Thread.ofVirtual().name("object-pool-timeout-", 0).factory();
        private static final VarHandle VAR_HANDLE;

        static {
            try {
                VAR_HANDLE = MethodHandles.lookup().findVarHandle(ObjectPool.class, "lastTimeoutCheckNanos", long.class);
            } catch (ReflectiveOperationException e) {
                throw new ExceptionInInitializerError(e);
            }
        }

        // FIXME ArrayBlockingQueue and dynamically resize when necessary? That could minimize allocations when releasing objects
        private final Queue<PooledObjectImpl> pool = new ConcurrentLinkedQueue<>();
        private final Runnable timeoutChecker = this::runTimeoutCheck;

        private final Supplier<T> supplier;
        private final long timeoutNanos;

        private volatile long lastTimeoutCheckNanos = 0;

        private UnboundedObjectPool(Supplier<T> supplier, long timeoutNanos) {
            this.supplier = supplier;
            this.timeoutNanos = timeoutNanos;
        }

        public PooledObject<T> acquire() {
            PooledObjectImpl obj = pool.poll();
            if (obj != null) {
                obj.acquire();
                return obj;
            }
            return new PooledObjectImpl(supplier.get(), System.nanoTime());
        }

        private boolean hasNextTimedOut() {
            var next = pool.peek();
            return next != null && next.hasTimedOut();
        }

        private void checkTimeouts() {
            long nowNanos = System.nanoTime();
            long lastNanos = lastTimeoutCheckNanos;
            if (nowNanos - lastNanos < TIMEOUT_CHECK_INTERVAL_NANOS) {
                return; // no need to check timeouts
            }

            if (VAR_HANDLE.compareAndSet(this, lastNanos, nowNanos) && hasNextTimedOut()) {
                // fork timeout check onto a virtual thread
                VTHREAD_FACTORY.newThread(timeoutChecker).start();
            }
        }

        private void runTimeoutCheck() {
            do {
                PooledObjectImpl obj = pool.poll();
                if (obj == null) {
                    return; // no objects to check
                } else if (obj.hasTimedOut() == false) {
                    obj.release(); // return to pool and reset release time to keep it in order
                    return;
                }
                // otherwise just drop
                lastTimeoutCheckNanos = System.nanoTime();
            } while (hasNextTimedOut());
        }

        private class PooledObjectImpl implements PooledObject<T> {
            private final T object;
            // if negative: release time, if positive: acquire time
            private volatile long nanoTime;

            PooledObjectImpl(T object, long nanoTime) {
                this.object = object;
                this.nanoTime = nanoTime;
            }

            @Override
            public T get() {
                assert nanoTime > 0 : "Object must be acquired before use";
                return object;
            }

            boolean hasTimedOut() {
                var current = nanoTime;
                return current < 0 && System.nanoTime() + current > timeoutNanos;
            }

            void acquire() {
                this.nanoTime = System.nanoTime();
            }

            void release() {
                this.nanoTime = -System.nanoTime();
                pool.offer(PooledObjectImpl.this);
            }

            @Override
            public void close() {
                release();
                checkTimeouts();
            }
        }
    }
}
