/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.shard;

import org.elasticsearch.Assertions;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.action.support.ContextPreservingActionListener;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.ThreadContext.StoredContext;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;

/**
 * Tracks shard operation permits. Each operation on the shard obtains a permit. When we need to block operations (e.g., to transition
 * between terms) we immediately delay all operations to a queue, obtain all available permits, and wait for outstanding operations to drain
 * and return their permits. Delayed operations will acquire permits and be completed after the operation that blocked all operations has
 * completed.
 */
final class IndexShardOperationPermits implements Closeable {

    private final ShardId shardId;
    private final ThreadPool threadPool;

    static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    final Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true); // fair to ensure a blocking thread is not starved
    private final List<DelayedOperation> delayedOperations = new ArrayList<>(); // operations that are delayed
    private volatile boolean closed;
    private int queuedBlockOperations; // does not need to be volatile as all accesses are done under a lock on this

    // only valid when assertions are enabled. Key is AtomicBoolean associated with each permit to ensure close once semantics.
    // Value is a tuple, with a some debug information supplied by the caller and a stack trace of the acquiring thread
    private final Map<AtomicBoolean, Tuple<String, StackTraceElement[]>> issuedPermits;

    /**
     * Construct operation permits for the specified shards.
     *
     * @param shardId    the shard
     * @param threadPool the thread pool (used to execute delayed operations)
     */
    IndexShardOperationPermits(final ShardId shardId, final ThreadPool threadPool) {
        this.shardId = shardId;
        this.threadPool = threadPool;
        if (Assertions.ENABLED) {
            issuedPermits = new ConcurrentHashMap<>();
        } else {
            issuedPermits = null;
        }
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Immediately delays operations and uses the {@code executor} to wait for in-flight operations to finish and then acquires all
     * permits. When all permits are acquired, the provided {@link ActionListener} is called under the guarantee that no new operations are
     * started. Delayed operations are run once the {@link Releasable} is released or if a failure occurs while acquiring all permits; in
     * this case the {@code onFailure} handler will be invoked after delayed operations are released.
     *
     * @param onAcquired {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param timeout    the maximum time to wait for the in-flight operations block
     * @param timeUnit   the time unit of the {@code timeout} argument
     * @param executor   executor on which to wait for in-flight operations to finish and acquire all permits
     */
    public void blockOperations(final ActionListener<Releasable> onAcquired, final long timeout, final TimeUnit timeUnit,
                                String executor)  {
        delayOperations();
        threadPool.executor(executor).execute(new AbstractRunnable() {

            final Releasable released = Releasables.releaseOnce(() -> releaseDelayedOperations());

            @Override
            public void onFailure(final Exception e) {
                try {
                    released.close(); // resume delayed operations as soon as possible
                } finally {
                    onAcquired.onFailure(e);
                }
            }

            @Override
            protected void doRun() throws Exception {
                final Releasable releasable = acquireAll(timeout, timeUnit);
                onAcquired.onResponse(() -> Releasables.close(releasable, released));
            }
        });
    }

    private void delayOperations() {
        if (closed) {
            throw new IndexShardClosedException(shardId);
        }
        synchronized (this) {
            assert queuedBlockOperations > 0 || delayedOperations.isEmpty();
            queuedBlockOperations++;
        }
    }

    private Releasable acquireAll(final long timeout, final TimeUnit timeUnit) throws InterruptedException, TimeoutException {
        if (Assertions.ENABLED) {
            // since delayed is not volatile, we have to synchronize even here for visibility
            synchronized (this) {
                assert queuedBlockOperations > 0;
            }
        }
        if (semaphore.tryAcquire(TOTAL_PERMITS, timeout, timeUnit)) {
            final Releasable release = Releasables.releaseOnce(() -> {
                assert semaphore.availablePermits() == 0;
                semaphore.release(TOTAL_PERMITS);
            });
            return release;
        } else {
            throw new TimeoutException("timeout while blocking operations");
        }
    }

    private void releaseDelayedOperations() {
        final List<DelayedOperation> queuedActions;
        synchronized (this) {
            assert queuedBlockOperations > 0;
            queuedBlockOperations--;
            if (queuedBlockOperations == 0) {
                queuedActions = new ArrayList<>(delayedOperations);
                delayedOperations.clear();
            } else {
                queuedActions = Collections.emptyList();
            }
        }
        if (queuedActions.isEmpty() == false) {
            /*
             * Try acquiring permits on fresh thread (for two reasons):
             *   - blockOperations can be called on a recovery thread which can be expected to be interrupted when recovery is cancelled;
             *     interruptions are bad here as permit acquisition will throw an interrupted exception which will be swallowed by
             *     the threaded action listener if the queue of the thread pool on which it submits is full
             *   - if a permit is acquired and the queue of the thread pool which the threaded action listener uses is full, the
             *     onFailure handler is executed on the calling thread; this should not be the recovery thread as it would delay the
             *     recovery
             */
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> {
                for (DelayedOperation queuedAction : queuedActions) {
                    acquire(queuedAction.listener, null, false, queuedAction.debugInfo, queuedAction.stackTrace);
                }
            });
        }
    }

    /**
     * Acquires a permit whenever permit acquisition is not blocked. If the permit is directly available, the provided
     * {@link ActionListener} will be called on the calling thread.
     * The {@link ActionListener#onResponse(Object)} method will then be called using the provided executor once operations are no
     * longer blocked. Note that the executor will not be used for {@link ActionListener#onFailure(Exception)} calls. Those will run
     * directly on the calling thread, which in case of delays, will be a generic thread. Callers should thus make sure
     * that the {@link ActionListener#onFailure(Exception)} method provided here only contains lightweight operations.
     *
     * @param onAcquired      {@link ActionListener} that is invoked once acquisition is successful or failed
     * @param executorOnDelay executor to use for the possibly delayed {@link ActionListener#onResponse(Object)} call
     * @param forceExecution  whether the runnable should force its execution in case it gets rejected
     * @param debugInfo       an extra information that can be useful when tracing an unreleased permit. When assertions are enabled
     *                        the tracing will capture the supplied object's {@link Object#toString()} value. Otherwise the object
     *                        isn't used
     *
     */
    public void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo) {
        final StackTraceElement[] stackTrace;
        if (Assertions.ENABLED) {
            stackTrace = Thread.currentThread().getStackTrace();
        } else {
            stackTrace = null;
        }
        acquire(onAcquired, executorOnDelay, forceExecution, debugInfo, stackTrace);
    }

    private void acquire(final ActionListener<Releasable> onAcquired, final String executorOnDelay, final boolean forceExecution,
                        final Object debugInfo, final StackTraceElement[] stackTrace) {
        if (closed) {
            onAcquired.onFailure(new IndexShardClosedException(shardId));
            return;
        }
        final Releasable releasable;
        try {
            synchronized (this) {
                if (queuedBlockOperations > 0) {
                    final Supplier<StoredContext> contextSupplier = threadPool.getThreadContext().newRestorableContext(false);
                    final ActionListener<Releasable> wrappedListener;
                    if (executorOnDelay != null) {
                        wrappedListener = new ContextPreservingActionListener<>(contextSupplier, onAcquired).delegateFailure(
                            (l, r) -> threadPool.executor(executorOnDelay).execute(new ActionRunnable<>(l) {
                                @Override
                                public boolean isForceExecution() {
                                    return forceExecution;
                                }

                                @Override
                                protected void doRun() {
                                    listener.onResponse(r);
                                }

                                @Override
                                public void onRejection(Exception e) {
                                    IOUtils.closeWhileHandlingException(r);
                                    super.onRejection(e);
                                }
                            }));
                    } else {
                        wrappedListener = new ContextPreservingActionListener<>(contextSupplier, onAcquired);
                    }
                    delayedOperations.add(new DelayedOperation(wrappedListener, debugInfo, stackTrace));
                    return;
                } else {
                    releasable = acquire(debugInfo, stackTrace);
                }
            }
        } catch (final InterruptedException e) {
            onAcquired.onFailure(e);
            return;
        }
        // execute this outside the synchronized block!
        onAcquired.onResponse(releasable);
    }

    private Releasable acquire(Object debugInfo, StackTraceElement[] stackTrace) throws InterruptedException {
        assert Thread.holdsLock(this);
        if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the un-timed tryAcquire methods do not honor the fairness setting
            final AtomicBoolean closed = new AtomicBoolean();
            final Releasable releasable = () -> {
                if (closed.compareAndSet(false, true)) {
                    if (Assertions.ENABLED) {
                        Tuple<String, StackTraceElement[]> existing = issuedPermits.remove(closed);
                        assert existing != null;
                    }
                    semaphore.release(1);
                }
            };
            if (Assertions.ENABLED) {
                issuedPermits.put(closed, new Tuple<>(debugInfo.toString(), stackTrace));
            }
            return releasable;
        } else {
            // this should never happen, if it does something is deeply wrong
            throw new IllegalStateException("failed to obtain permit but operations are not delayed");
        }
    }

    /**
     * Obtain the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} if all permits are held.
     *
     * @return the active operation count, or {@link IndexShard#OPERATIONS_BLOCKED} when all permits are held.
     */
    int getActiveOperationsCount() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            return IndexShard.OPERATIONS_BLOCKED; // This occurs when blockOperations() has acquired all the permits.
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }


    synchronized boolean isBlocked() {
        return queuedBlockOperations > 0;
    }

    /**
     * @return a list of describing each permit that wasn't released yet. The description consist of the debugInfo supplied
     *         when the permit was acquired plus a stack traces that was captured when the permit was request.
     */
    List<String> getActiveOperations() {
        return issuedPermits.values().stream().map(
            t -> t.v1() + "\n" + ExceptionsHelper.formatStackTrace(t.v2()))
            .collect(Collectors.toList());
    }

    private static class DelayedOperation {
        private final ActionListener<Releasable> listener;
        private final String debugInfo;
        private final StackTraceElement[] stackTrace;

        private DelayedOperation(ActionListener<Releasable> listener, Object debugInfo, StackTraceElement[] stackTrace) {
            this.listener = listener;
            if (Assertions.ENABLED) {
                this.debugInfo = "[delayed] " + debugInfo;
                this.stackTrace = stackTrace;
            } else {
                this.debugInfo = null;
                this.stackTrace = null;
            }
        }
    }
}
