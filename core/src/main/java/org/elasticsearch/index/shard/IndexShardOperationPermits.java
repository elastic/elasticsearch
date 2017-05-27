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

package org.elasticsearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Assertions;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.CheckedRunnable;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Tracks shard operation permits. Each operation on the shard obtains a permit. When we need to block operations (e.g., to transition
 * between terms) we immediately delay all operations to a queue, obtain all available permits, and wait for outstanding operations to drain
 * and return their permits. Delayed operations will acquire permits and be completed after the operation that blocked all operations has
 * completed.
 */
final class IndexShardOperationPermits implements Closeable {

    private final ShardId shardId;
    private final Logger logger;
    private final ThreadPool threadPool;

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    final Semaphore semaphore = new Semaphore(Integer.MAX_VALUE, true); // fair to ensure a blocking thread is not starved
    @Nullable private List<ActionListener<Releasable>> delayedOperations; // operations that are delayed
    private volatile boolean closed;
    private boolean delayed; // does not need to be volatile as all accesses are done under a lock on this

    /**
     * Construct operation permits for the specified shards.
     *
     * @param shardId    the shard
     * @param logger     the logger for the shard
     * @param threadPool the thread pool (used to execute delayed operations)
     */
    IndexShardOperationPermits(final ShardId shardId, final Logger logger, final ThreadPool threadPool) {
        this.shardId = shardId;
        this.logger = logger;
        this.threadPool = threadPool;
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Wait for in-flight operations to finish and executes {@code onBlocked} under the guarantee that no new operations are started. Queues
     * operations that are occurring in the meanwhile and runs them once {@code onBlocked} has executed.
     *
     * @param timeout   the maximum time to wait for the in-flight operations block
     * @param timeUnit  the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @param <E>       the type of checked exception thrown by {@code onBlocked}
     * @throws InterruptedException      if calling thread is interrupted
     * @throws TimeoutException          if timed out waiting for in-flight operations to finish
     * @throws IndexShardClosedException if operation permit has been closed
     */
    <E extends Exception> void syncBlockOperations(
            final long timeout,
            final TimeUnit timeUnit,
            final CheckedRunnable<E> onBlocked) throws InterruptedException, TimeoutException, E {
        if (closed) {
            throw new IndexShardClosedException(shardId);
        }
        delayOperations();
        doBlockOperations(timeout, timeUnit, onBlocked);
    }

    /**
     * Immediately delays operations and on another thread waits for in-flight operations to finish and then executes {@code onBlocked}
     * under the guarantee that no new operations are started. Delayed operations are run after {@code onBlocked} has executed. After
     * operations are delayed and the blocking is forked to another thread, returns to the caller.
     *
     * @param timeout   the maximum time to wait for the in-flight operations block
     * @param timeUnit  the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @param <E>       the type of checked exception thrown by {@code onBlocked} (not thrown on the calling thread)
     */
    <E extends Exception> void asyncBlockOperations(final long timeout, final TimeUnit timeUnit, final CheckedRunnable<E> onBlocked) {
        delayOperations();
        threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
            try {
                doBlockOperations(timeout, timeUnit, onBlocked);
            } catch (final Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    private void delayOperations() {
        synchronized (this) {
            if (delayed) {
                throw new IllegalStateException("operations are already delayed");
            } else {
                delayed = true;
            }
        }
    }

    private <E extends Exception> void doBlockOperations(
            final long timeout,
            final TimeUnit timeUnit,
            final CheckedRunnable<E> onBlocked) throws InterruptedException, TimeoutException, E {
        if (Assertions.ENABLED) {
            synchronized (this) {
                assert delayed;
            }
        }
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, timeout, timeUnit)) {
                assert semaphore.availablePermits() == 0;
                try {
                    onBlocked.run();
                } finally {
                    semaphore.release(TOTAL_PERMITS);
                }
            } else {
                throw new TimeoutException("timed out during blockOperations");
            }
        } finally {
            final List<ActionListener<Releasable>> queuedActions;
            synchronized (this) {
                queuedActions = delayedOperations;
                delayedOperations = null;
                delayed = false;
            }
            if (queuedActions != null) {
                // Try acquiring permits on fresh thread (for two reasons):
                // - blockOperations can be called on recovery thread which can be expected to be interrupted when recovery is cancelled.
                //   Interruptions are bad here as permit acquisition will throw an InterruptedException which will be swallowed by
                //   ThreadedActionListener if the queue of the thread pool on which it submits is full.
                // - if permit is acquired and queue of the thread pool which the ThreadedActionListener uses is full, the onFailure
                //   handler is executed on the calling thread. This should not be the recovery thread as it would delay the recovery.
                threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                    for (ActionListener<Releasable> queuedAction : queuedActions) {
                        acquire(queuedAction, null, false);
                    }
                });
            }
        }
    }

    /**
     * Acquires a permit whenever permit acquisition is not blocked. If the permit is directly available, the provided
     * {@link ActionListener} will be called on the calling thread. During calls of
     * {@link #syncBlockOperations(long, TimeUnit, CheckedRunnable)}, permit acquisition can be delayed. The provided {@link ActionListener}
     * will then be called using the provided executor once operations are no longer blocked.
     *
     * @param onAcquired      {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param executorOnDelay executor to use for delayed call
     * @param forceExecution  whether the runnable should force its execution in case it gets rejected
     */
    public void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution) {
        if (closed) {
            onAcquired.onFailure(new IndexShardClosedException(shardId));
            return;
        }
        final Releasable releasable;
        try {
            synchronized (this) {
                releasable = tryAcquire();
                if (releasable == null) {
                    assert delayed;
                    // operations are delayed, this operation will be retried by doBlockOperations once the delay is remoked
                    if (delayedOperations == null) {
                        delayedOperations = new ArrayList<>();
                    }
                    final Supplier<StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(false);
                    if (executorOnDelay != null) {
                        delayedOperations.add(
                            new ThreadedActionListener<>(logger, threadPool, executorOnDelay,
                                new ContextPreservingActionListener<>(contextSupplier, onAcquired), forceExecution));
                    } else {
                        delayedOperations.add(new ContextPreservingActionListener<>(contextSupplier, onAcquired));
                    }
                    return;
                }
            }
        } catch (final InterruptedException e) {
            onAcquired.onFailure(e);
            return;
        }
        onAcquired.onResponse(releasable);
    }

    @Nullable private Releasable tryAcquire() throws InterruptedException {
        assert Thread.holdsLock(this);
        if (!delayed && semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the untimed tryAcquire methods do not honor the fairness setting
            final AtomicBoolean closed = new AtomicBoolean();
            return () -> {
                if (closed.compareAndSet(false, true)) {
                    semaphore.release(1);
                }
            };
        }
        return null;
    }

    /**
     * Obtain the active operation count, or zero if all permits are held (even if there are outstanding operations in flight).
     *
     * @return the active operation count, or zero when all permits ar eheld
     */
    int getActiveOperationsCount() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            /*
             * This occurs when either doBlockOperations is holding all the permits or there are outstanding operations in flight and the
             * remainder of the permits are held by doBlockOperations. We do not distinguish between these two cases and simply say that
             * the active operations count is zero.
             */
            return 0;
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }

}
