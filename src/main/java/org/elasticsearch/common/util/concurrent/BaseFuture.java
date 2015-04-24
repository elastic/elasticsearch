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

import com.google.common.annotations.Beta;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.transport.Transports;

import java.util.concurrent.*;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An abstract implementation of the {@link com.google.common.util.concurrent.ListenableFuture} interface. This
 * class is preferable to {@link java.util.concurrent.FutureTask} for two
 * reasons: It implements {@code ListenableFuture}, and it does not implement
 * {@code Runnable}. (If you want a {@code Runnable} implementation of {@code
 * ListenableFuture}, create a {@link com.google.common.util.concurrent.ListenableFutureTask}, or submit your
 * tasks to a {@link com.google.common.util.concurrent.ListeningExecutorService}.)
 * <p/>
 * <p>This class implements all methods in {@code ListenableFuture}.
 * Subclasses should provide a way to set the result of the computation through
 * the protected methods {@link #set(Object)} and
 * {@link #setException(Throwable)}. Subclasses may also override {@link
 * #interruptTask()}, which will be invoked automatically if a call to {@link
 * #cancel(boolean) cancel(true)} succeeds in canceling the future.
 * <p/>
 * <p>{@code AbstractFuture} uses an {@link AbstractQueuedSynchronizer} to deal
 * with concurrency issues and guarantee thread safety.
 * <p/>
 * <p>The state changing methods all return a boolean indicating success or
 * failure in changing the future's state.  Valid states are running,
 * completed, failed, or cancelled.
 * <p/>
 * <p>This class uses an {@link com.google.common.util.concurrent.ExecutionList} to guarantee that all registered
 * listeners will be executed, either when the future finishes or, for listeners
 * that are added after the future completes, immediately.
 * {@code Runnable}-{@code Executor} pairs are stored in the execution list but
 * are not necessarily executed in the order in which they were added.  (If a
 * listener is added after the Future is complete, it will be executed
 * immediately, even if earlier listeners have not been executed. Additionally,
 * executors need not guarantee FIFO execution, or different listeners may run
 * in different executors.)
 *
 * @author Sven Mawson
 * @since 1.0
 */
// Same as AbstractFuture from Guava, but without the listeners and with
// additional assertions
public abstract class BaseFuture<V> implements Future<V> {

    /**
     * Synchronization control for AbstractFutures.
     */
    private final Sync<V> sync = new Sync<>();

    /*
    * Improve the documentation of when InterruptedException is thrown. Our
    * behavior matches the JDK's, but the JDK's documentation is misleading.
    */

    /**
     * {@inheritDoc}
     * <p/>
     * <p>The default {@link BaseFuture} implementation throws {@code
     * InterruptedException} if the current thread is interrupted before or during
     * the call, even if the value is already available.
     *
     * @throws InterruptedException  if the current thread was interrupted before
     *                               or during the call (optional but recommended).
     * @throws CancellationException {@inheritDoc}
     */
    @Override
    public V get(long timeout, TimeUnit unit) throws InterruptedException,
            TimeoutException, ExecutionException {
        Transports.assertNotTransportThread("Blocking operation");
        return sync.get(unit.toNanos(timeout));
    }

    /*
    * Improve the documentation of when InterruptedException is thrown. Our
    * behavior matches the JDK's, but the JDK's documentation is misleading.
    */

    /**
     * {@inheritDoc}
     * <p/>
     * <p>The default {@link BaseFuture} implementation throws {@code
     * InterruptedException} if the current thread is interrupted before or during
     * the call, even if the value is already available.
     *
     * @throws InterruptedException  if the current thread was interrupted before
     *                               or during the call (optional but recommended).
     * @throws CancellationException {@inheritDoc}
     */
    @Override
    public V get() throws InterruptedException, ExecutionException {
        Transports.assertNotTransportThread("Blocking operation");
        return sync.get();
    }

    @Override
    public boolean isDone() {
        return sync.isDone();
    }

    @Override
    public boolean isCancelled() {
        return sync.isCancelled();
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        if (!sync.cancel()) {
            return false;
        }
        done();
        if (mayInterruptIfRunning) {
            interruptTask();
        }
        return true;
    }

    /**
     * Subclasses can override this method to implement interruption of the
     * future's computation. The method is invoked automatically by a successful
     * call to {@link #cancel(boolean) cancel(true)}.
     * <p/>
     * <p>The default implementation does nothing.
     *
     * @since 10.0
     */
    protected void interruptTask() {
    }

    /**
     * Subclasses should invoke this method to set the result of the computation
     * to {@code value}.  This will set the state of the future to
     * {@link BaseFuture.Sync#COMPLETED} and call {@link #done()} if the
     * state was successfully changed.
     *
     * @param value the value that was the result of the task.
     * @return true if the state was successfully changed.
     */
    protected boolean set(@Nullable V value) {
        boolean result = sync.set(value);
        if (result) {
            done();
        }
        return result;
    }

    /**
     * Subclasses should invoke this method to set the result of the computation
     * to an error, {@code throwable}.  This will set the state of the future to
     * {@link BaseFuture.Sync#COMPLETED} and call {@link #done()} if the
     * state was successfully changed.
     *
     * @param throwable the exception that the task failed with.
     * @return true if the state was successfully changed.
     * @throws Error if the throwable was an {@link Error}.
     */
    protected boolean setException(Throwable throwable) {
        boolean result = sync.setException(checkNotNull(throwable));
        if (result) {
            done();
        }

        // If it's an Error, we want to make sure it reaches the top of the
        // call stack, so we rethrow it.

        // we want to notify the listeners we have with errors as well, as it breaks
        // how we work in ES in terms of using assertions
//        if (throwable instanceof Error) {
//            throw (Error) throwable;
//        }
        return result;
    }

    @Beta
    protected void done() {
    }

    /**
     * <p>Following the contract of {@link AbstractQueuedSynchronizer} we create a
     * private subclass to hold the synchronizer.  This synchronizer is used to
     * implement the blocking and waiting calls as well as to handle state changes
     * in a thread-safe manner.  The current state of the future is held in the
     * Sync state, and the lock is released whenever the state changes to either
     * {@link #COMPLETED} or {@link #CANCELLED}.
     * <p/>
     * <p>To avoid races between threads doing release and acquire, we transition
     * to the final state in two steps.  One thread will successfully CAS from
     * RUNNING to COMPLETING, that thread will then set the result of the
     * computation, and only then transition to COMPLETED or CANCELLED.
     * <p/>
     * <p>We don't use the integer argument passed between acquire methods so we
     * pass around a -1 everywhere.
     */
    static final class Sync<V> extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 0L;

        /* Valid states. */
        static final int RUNNING = 0;
        static final int COMPLETING = 1;
        static final int COMPLETED = 2;
        static final int CANCELLED = 4;

        private V value;
        private Throwable exception;

        /*
        * Acquisition succeeds if the future is done, otherwise it fails.
        */
        @Override
        protected int tryAcquireShared(int ignored) {
            if (isDone()) {
                return 1;
            }
            return -1;
        }

        /*
        * We always allow a release to go through, this means the state has been
        * successfully changed and the result is available.
        */
        @Override
        protected boolean tryReleaseShared(int finalState) {
            setState(finalState);
            return true;
        }

        /**
         * Blocks until the task is complete or the timeout expires.  Throws a
         * {@link TimeoutException} if the timer expires, otherwise behaves like
         * {@link #get()}.
         */
        V get(long nanos) throws TimeoutException, CancellationException,
                ExecutionException, InterruptedException {

            // Attempt to acquire the shared lock with a timeout.
            if (!tryAcquireSharedNanos(-1, nanos)) {
                throw new TimeoutException("Timeout waiting for task.");
            }

            return getValue();
        }

        /**
         * Blocks until {@link #complete(Object, Throwable, int)} has been
         * successfully called.  Throws a {@link CancellationException} if the task
         * was cancelled, or a {@link ExecutionException} if the task completed with
         * an error.
         */
        V get() throws CancellationException, ExecutionException,
                InterruptedException {

            // Acquire the shared lock allowing interruption.
            acquireSharedInterruptibly(-1);
            return getValue();
        }

        /**
         * Implementation of the actual value retrieval.  Will return the value
         * on success, an exception on failure, a cancellation on cancellation, or
         * an illegal state if the synchronizer is in an invalid state.
         */
        private V getValue() throws CancellationException, ExecutionException {
            int state = getState();
            switch (state) {
                case COMPLETED:
                    if (exception != null) {
                        throw new ExecutionException(exception);
                    } else {
                        return value;
                    }

                case CANCELLED:
                    throw new CancellationException("Task was cancelled.");

                default:
                    throw new IllegalStateException(
                            "Error, synchronizer in invalid state: " + state);
            }
        }

        /**
         * Checks if the state is {@link #COMPLETED} or {@link #CANCELLED}.
         */
        boolean isDone() {
            return (getState() & (COMPLETED | CANCELLED)) != 0;
        }

        /**
         * Checks if the state is {@link #CANCELLED}.
         */
        boolean isCancelled() {
            return getState() == CANCELLED;
        }

        /**
         * Transition to the COMPLETED state and set the value.
         */
        boolean set(@Nullable V v) {
            return complete(v, null, COMPLETED);
        }

        /**
         * Transition to the COMPLETED state and set the exception.
         */
        boolean setException(Throwable t) {
            return complete(null, t, COMPLETED);
        }

        /**
         * Transition to the CANCELLED state.
         */
        boolean cancel() {
            return complete(null, null, CANCELLED);
        }

        /**
         * Implementation of completing a task.  Either {@code v} or {@code t} will
         * be set but not both.  The {@code finalState} is the state to change to
         * from {@link #RUNNING}.  If the state is not in the RUNNING state we
         * return {@code false} after waiting for the state to be set to a valid
         * final state ({@link #COMPLETED} or {@link #CANCELLED}).
         *
         * @param v          the value to set as the result of the computation.
         * @param t          the exception to set as the result of the computation.
         * @param finalState the state to transition to.
         */
        private boolean complete(@Nullable V v, @Nullable Throwable t,
                                 int finalState) {
            boolean doCompletion = compareAndSetState(RUNNING, COMPLETING);
            if (doCompletion) {
                // If this thread successfully transitioned to COMPLETING, set the value
                // and exception and then release to the final state.
                this.value = v;
                this.exception = t;
                releaseShared(finalState);
            } else if (getState() == COMPLETING) {
                // If some other thread is currently completing the future, block until
                // they are done so we can guarantee completion.
                acquireShared(-1);
            }
            return doCompletion;
        }
    }
}
