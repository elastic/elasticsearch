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
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ThreadedActionListener;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexShardOperationsLock implements Closeable {
    private final ShardId shardId;
    private final Logger logger;
    private final ThreadPool threadPool;

    private static final int TOTAL_PERMITS = Integer.MAX_VALUE;
    // fair semaphore to ensure that blockOperations() does not starve under thread contention
    final Semaphore semaphore = new Semaphore(TOTAL_PERMITS, true);
    @Nullable private List<ActionListener<Releasable>> delayedOperations; // operations that are delayed due to relocation hand-off
    private volatile boolean closed;

    private final Set<ActiveOperation> operationsTracker; // tracks active operations when assertions are enabled

    public IndexShardOperationsLock(ShardId shardId, Logger logger, ThreadPool threadPool, boolean trackActiveOperations) {
        this.shardId = shardId;
        this.logger = logger;
        this.threadPool = threadPool;
        this.operationsTracker = trackActiveOperations ? Collections.newSetFromMap(new IdentityHashMap<>()) : null;
    }

    @Override
    public void close() {
        closed = true;
    }

    /**
     * Wait for in-flight operations to finish and executes onBlocked under the guarantee that no new operations are started. Queues
     * operations that are occurring in the meanwhile and runs them once onBlocked has executed.
     *
     * @param timeout the maximum time to wait for the in-flight operations block
     * @param timeUnit the time unit of the {@code timeout} argument
     * @param onBlocked the action to run once the block has been acquired
     * @throws InterruptedException if calling thread is interrupted
     * @throws TimeoutException if timed out waiting for in-flight operations to finish
     * @throws IndexShardClosedException if operation lock has been closed
     */
    public void blockOperations(long timeout, TimeUnit timeUnit, Runnable onBlocked) throws InterruptedException, TimeoutException {
        if (closed) {
            throw new IndexShardClosedException(shardId);
        }
        try {
            if (semaphore.tryAcquire(TOTAL_PERMITS, timeout, timeUnit)) {
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
            }
            if (queuedActions != null) {
                // Try acquiring permits on fresh thread (for two reasons):
                // - blockOperations is called on recovery thread which can be expected to be interrupted when recovery is cancelled.
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
     * Acquires a lock whenever lock acquisition is not blocked. If the lock is directly available, the provided
     * ActionListener will be called on the calling thread. During calls of {@link #blockOperations(long, TimeUnit, Runnable)}, lock
     * acquisition can be delayed. The provided ActionListener will then be called using the provided executor once blockOperations
     * terminates.
     * @param onAcquired ActionListener that is invoked once acquisition is successful or failed
     * @param executorOnDelay executor to use for delayed call
     * @param forceExecution whether the runnable should force its execution in case it gets rejected
     */
    public void acquire(ActionListener<Releasable> onAcquired, String executorOnDelay, boolean forceExecution) {
        if (closed) {
            onAcquired.onFailure(new IndexShardClosedException(shardId));
            return;
        }
        Releasable releasable;
        try {
            synchronized (this) {
                releasable = tryAcquire(onAcquired);
                if (releasable == null) {
                    // blockOperations is executing, this operation will be retried by blockOperations once it finishes
                    if (delayedOperations == null) {
                        delayedOperations = new ArrayList<>();
                    }
                    if (executorOnDelay != null) {
                        delayedOperations.add(
                            new ThreadedActionListener<>(logger, threadPool, executorOnDelay, onAcquired, forceExecution));
                    } else {
                        delayedOperations.add(onAcquired);
                    }
                    return;
                }
            }
        } catch (InterruptedException e) {
            onAcquired.onFailure(e);
            return;
        }
        onAcquired.onResponse(releasable);
    }

    @Nullable private Releasable tryAcquire(ActionListener<Releasable> onAcquired) throws InterruptedException {
        if (semaphore.tryAcquire(1, 0, TimeUnit.SECONDS)) { // the untimed tryAcquire methods do not honor the fairness setting
            AtomicBoolean closed = new AtomicBoolean();
            if (operationsTracker == null) {
                return () -> {
                    if (closed.compareAndSet(false, true)) {
                        semaphore.release(1);
                    }
                };
            } else {
                Throwable throwable = new Throwable(); // generate caller stack trace
                ActiveOperation activeOperation = new ActiveOperation() {
                    @Override
                    public Throwable getCallerStackTrace() {
                        return throwable;
                    }

                    @Override
                    public ActionListener<Releasable> getOperation() {
                        return onAcquired;
                    }

                    @Override
                    public void close() {
                        if (closed.compareAndSet(false, true)) {
                            if (operationsTracker != null) {
                                operationsTracker.remove(this);
                            }
                            semaphore.release(1);
                        }
                    }
                };
                operationsTracker.add(activeOperation);
                return activeOperation;
            }
        }
        return null;
    }

    public int getActiveOperationsCount() {
        int availablePermits = semaphore.availablePermits();
        if (availablePermits == 0) {
            // when blockOperations is holding all permits
            return 0;
        } else {
            return TOTAL_PERMITS - availablePermits;
        }
    }

    /**
     * Throws an IllegalStateException if not all operations have been released. Uses the toString() of the ActionListener that was passed
     * to the acquire method to provide additional context information.
     */
    public void ensureNoActiveOperations() {
        int numActiveOperations = getActiveOperationsCount();
        if (numActiveOperations > 0) {
            if (operationsTracker != null) {
                for (ActiveOperation activeOperation : operationsTracker) {
                    Throwable callerStackTrace = activeOperation.getCallerStackTrace();
                    ActionListener<Releasable> operation = activeOperation.getOperation();
                    throw new IllegalStateException(numActiveOperations + " active operations in progress, one of which is: " + operation,
                        callerStackTrace);
                }
            }
            throw new IllegalStateException(numActiveOperations + " active operations in progress");
        }
    }

    interface ActiveOperation extends Releasable {
        Throwable getCallerStackTrace();
        ActionListener<Releasable> getOperation();
    }
}
