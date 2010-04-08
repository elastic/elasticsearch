/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/licenses/publicdomain
 */

package org.elasticsearch.util.concurrent.jsr166y;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.*;

/**
 * Abstract base class for tasks that run within a {@link ForkJoinPool}.
 * A {@code ForkJoinTask} is a thread-like entity that is much
 * lighter weight than a normal thread.  Huge numbers of tasks and
 * subtasks may be hosted by a small number of actual threads in a
 * ForkJoinPool, at the price of some usage limitations.
 *
 * <p>A "main" {@code ForkJoinTask} begins execution when submitted
 * to a {@link ForkJoinPool}.  Once started, it will usually in turn
 * start other subtasks.  As indicated by the name of this class,
 * many programs using {@code ForkJoinTask} employ only methods
 * {@link #fork} and {@link #join}, or derivatives such as {@link
 * #invokeAll}.  However, this class also provides a number of other
 * methods that can come into play in advanced usages, as well as
 * extension mechanics that allow support of new forms of fork/join
 * processing.
 *
 * <p>A {@code ForkJoinTask} is a lightweight form of {@link Future}.
 * The efficiency of {@code ForkJoinTask}s stems from a set of
 * restrictions (that are only partially statically enforceable)
 * reflecting their intended use as computational tasks calculating
 * pure functions or operating on purely isolated objects.  The
 * primary coordination mechanisms are {@link #fork}, that arranges
 * asynchronous execution, and {@link #join}, that doesn't proceed
 * until the task's result has been computed.  Computations should
 * avoid {@code synchronized} methods or blocks, and should minimize
 * other blocking synchronization apart from joining other tasks or
 * using synchronizers such as Phasers that are advertised to
 * cooperate with fork/join scheduling. Tasks should also not perform
 * blocking IO, and should ideally access variables that are
 * completely independent of those accessed by other running
 * tasks. Minor breaches of these restrictions, for example using
 * shared output streams, may be tolerable in practice, but frequent
 * use may result in poor performance, and the potential to
 * indefinitely stall if the number of threads not waiting for IO or
 * other external synchronization becomes exhausted. This usage
 * restriction is in part enforced by not permitting checked
 * exceptions such as {@code IOExceptions} to be thrown. However,
 * computations may still encounter unchecked exceptions, that are
 * rethrown to callers attempting to join them. These exceptions may
 * additionally include {@link RejectedExecutionException} stemming
 * from internal resource exhaustion, such as failure to allocate
 * internal task queues.
 *
 * <p>The primary method for awaiting completion and extracting
 * results of a task is {@link #join}, but there are several variants:
 * The {@link Future#get} methods support interruptible and/or timed
 * waits for completion and report results using {@code Future}
 * conventions. Method {@link #helpJoin} enables callers to actively
 * execute other tasks while awaiting joins, which is sometimes more
 * efficient but only applies when all subtasks are known to be
 * strictly tree-structured. Method {@link #invoke} is semantically
 * equivalent to {@code fork(); join()} but always attempts to begin
 * execution in the current thread. The "<em>quiet</em>" forms of
 * these methods do not extract results or report exceptions. These
 * may be useful when a set of tasks are being executed, and you need
 * to delay processing of results or exceptions until all complete.
 * Method {@code invokeAll} (available in multiple versions)
 * performs the most common form of parallel invocation: forking a set
 * of tasks and joining them all.
 *
 * <p>The execution status of tasks may be queried at several levels
 * of detail: {@link #isDone} is true if a task completed in any way
 * (including the case where a task was cancelled without executing);
 * {@link #isCompletedNormally} is true if a task completed without
 * cancellation or encountering an exception; {@link #isCancelled} is
 * true if the task was cancelled (in which case {@link #getException}
 * returns a {@link java.util.concurrent.CancellationException}); and
 * {@link #isCompletedAbnormally} is true if a task was either
 * cancelled or encountered an exception, in which case {@link
 * #getException} will return either the encountered exception or
 * {@link java.util.concurrent.CancellationException}.
 *
 * <p>The ForkJoinTask class is not usually directly subclassed.
 * Instead, you subclass one of the abstract classes that support a
 * particular style of fork/join processing, typically {@link
 * RecursiveAction} for computations that do not return results, or
 * {@link RecursiveTask} for those that do.  Normally, a concrete
 * ForkJoinTask subclass declares fields comprising its parameters,
 * established in a constructor, and then defines a {@code compute}
 * method that somehow uses the control methods supplied by this base
 * class. While these methods have {@code public} access (to allow
 * instances of different task subclasses to call each other's
 * methods), some of them may only be called from within other
 * ForkJoinTasks (as may be determined using method {@link
 * #inForkJoinPool}).  Attempts to invoke them in other contexts
 * result in exceptions or errors, possibly including
 * ClassCastException.
 *
 * <p>Most base support methods are {@code final}, to prevent
 * overriding of implementations that are intrinsically tied to the
 * underlying lightweight task scheduling framework.  Developers
 * creating new basic styles of fork/join processing should minimally
 * implement {@code protected} methods {@link #exec}, {@link
 * #setRawResult}, and {@link #getRawResult}, while also introducing
 * an abstract computational method that can be implemented in its
 * subclasses, possibly relying on other {@code protected} methods
 * provided by this class.
 *
 * <p>ForkJoinTasks should perform relatively small amounts of
 * computation. Large tasks should be split into smaller subtasks,
 * usually via recursive decomposition. As a very rough rule of thumb,
 * a task should perform more than 100 and less than 10000 basic
 * computational steps. If tasks are too big, then parallelism cannot
 * improve throughput. If too small, then memory and internal task
 * maintenance overhead may overwhelm processing.
 *
 * <p>This class provides {@code adapt} methods for {@link Runnable}
 * and {@link Callable}, that may be of use when mixing execution of
 * {@code ForkJoinTasks} with other kinds of tasks. When all tasks
 * are of this form, consider using a pool in
 * {@linkplain ForkJoinPool#setAsyncMode async mode}.
 *
 * <p>ForkJoinTasks are {@code Serializable}, which enables them to be
 * used in extensions such as remote execution frameworks. It is
 * sensible to serialize tasks only before or after, but not during,
 * execution. Serialization is not relied on during execution itself.
 *
 * @author Doug Lea
 * @since 1.7
 */
public abstract class ForkJoinTask<V> implements Future<V>, Serializable {

    /*
     * See the internal documentation of class ForkJoinPool for a
     * general implementation overview.  ForkJoinTasks are mainly
     * responsible for maintaining their "status" field amidst relays
     * to methods in ForkJoinWorkerThread and ForkJoinPool. The
     * methods of this class are more-or-less layered into (1) basic
     * status maintenance (2) execution and awaiting completion (3)
     * user-level methods that additionally report results. This is
     * sometimes hard to see because this file orders exported methods
     * in a way that flows well in javadocs.
     */

    /**
     * Run control status bits packed into a single int to minimize
     * footprint and to ensure atomicity (via CAS).  Status is
     * initially zero, and takes on nonnegative values until
     * completed, upon which status holds COMPLETED. CANCELLED, or
     * EXCEPTIONAL, which use the top 3 bits.  Tasks undergoing
     * blocking waits by other threads have SIGNAL_MASK bits set --
     * bit 15 for external (nonFJ) waits, and the rest a count of
     * waiting FJ threads.  (This representation relies on
     * ForkJoinPool max thread limits). Signal counts are not directly
     * incremented by ForkJoinTask methods, but instead via a call to
     * requestSignal within ForkJoinPool.preJoin, once their need is
     * established.
     *
     * Completion of a stolen task with SIGNAL_MASK bits set awakens
     * any waiters via notifyAll. Even though suboptimal for some
     * purposes, we use basic builtin wait/notify to take advantage of
     * "monitor inflation" in JVMs that we would otherwise need to
     * emulate to avoid adding further per-task bookkeeping overhead.
     * We want these monitors to be "fat", i.e., not use biasing or
     * thin-lock techniques, so use some odd coding idioms that tend
     * to avoid them.
     *
     * Note that bits 16-28 are currently unused. Also value
     * 0x80000000 is available as spare completion value.
     */
    volatile int status; // accessed directly by pool and workers

    private static final int COMPLETION_MASK = 0xe0000000;
    private static final int NORMAL = 0xe0000000; // == mask
    private static final int CANCELLED = 0xc0000000;
    private static final int EXCEPTIONAL = 0xa0000000;
    private static final int SIGNAL_MASK = 0x0000ffff;
    private static final int INTERNAL_SIGNAL_MASK = 0x00007fff;
    private static final int EXTERNAL_SIGNAL = 0x00008000;

    /**
     * Table of exceptions thrown by tasks, to enable reporting by
     * callers. Because exceptions are rare, we don't directly keep
     * them with task objects, but instead use a weak ref table.  Note
     * that cancellation exceptions don't appear in the table, but are
     * instead recorded as status values.
     * TODO: Use ConcurrentReferenceHashMap
     */
    static final Map<ForkJoinTask<?>, Throwable> exceptionMap =
            Collections.synchronizedMap
                    (new WeakHashMap<ForkJoinTask<?>, Throwable>());

    // Maintaining completion status

    /**
     * Marks completion and wakes up threads waiting to join this task,
     * also clearing signal request bits.
     *
     * @param completion one of NORMAL, CANCELLED, EXCEPTIONAL
     */
    private void setCompletion(int completion) {
        int s;
        while ((s = status) >= 0) {
            if (UNSAFE.compareAndSwapInt(this, statusOffset, s, completion)) {
                if ((s & SIGNAL_MASK) != 0) {
                    Thread t = Thread.currentThread();
                    if (t instanceof ForkJoinWorkerThread)
                        ((ForkJoinWorkerThread) t).pool.updateRunningCount
                                (s & INTERNAL_SIGNAL_MASK);
                    synchronized (this) {
                        notifyAll();
                    }
                }
                return;
            }
        }
    }

    /**
     * Record exception and set exceptional completion
     */
    private void setDoneExceptionally(Throwable rex) {
        exceptionMap.put(this, rex);
        setCompletion(EXCEPTIONAL);
    }

    /**
     * Main internal execution method: Unless done, calls exec and
     * records completion.
     *
     * @return true if ran and completed normally
     */
    final boolean tryExec() {
        try {
            if (status < 0 || !exec())
                return false;
        } catch (Throwable rex) {
            setDoneExceptionally(rex);
            return false;
        }
        setCompletion(NORMAL); // must be outside try block
        return true;
    }

    /**
     * Increments internal signal count (thus requesting signal upon
     * completion) unless already done.  Call only once per join.
     * Used by ForkJoinPool.preJoin.
     *
     * @return status
     */
    final int requestSignal() {
        int s;
        do {
        } while ((s = status) >= 0 &&
                !UNSAFE.compareAndSwapInt(this, statusOffset, s, s + 1));
        return s;
    }

    /**
     * Sets external signal request unless already done.
     *
     * @return status
     */
    private int requestExternalSignal() {
        int s;
        do {
        } while ((s = status) >= 0 &&
                !UNSAFE.compareAndSwapInt(this, statusOffset,
                        s, s | EXTERNAL_SIGNAL));
        return s;
    }

    /*
     * Awaiting completion. The four versions, internal vs external X
     * untimed vs timed, have the same overall structure but differ
     * from each other enough to defy simple integration.
     */

    /**
     * Blocks a worker until this task is done, also maintaining pool
     * and signal counts
     */
    private void awaitDone(ForkJoinWorkerThread w) {
        if (status >= 0) {
            w.pool.preJoin(this);
            while (status >= 0) {
                try { // minimize lock scope
                    synchronized (this) {
                        if (status >= 0)
                            wait();
                        else { // help release; also helps avoid lock-biasing
                            notifyAll();
                            break;
                        }
                    }
                } catch (InterruptedException ie) {
                    cancelIfTerminating();
                }
            }
        }
    }

    /**
     * Blocks a non-ForkJoin thread until this task is done.
     */
    private void externalAwaitDone() {
        if (requestExternalSignal() >= 0) {
            boolean interrupted = false;
            while (status >= 0) {
                try {
                    synchronized (this) {
                        if (status >= 0)
                            wait();
                        else {
                            notifyAll();
                            break;
                        }
                    }
                } catch (InterruptedException ie) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    /**
     * Blocks a worker until this task is done or timeout elapses
     */
    private void timedAwaitDone(ForkJoinWorkerThread w, long nanos) {
        if (status >= 0) {
            long startTime = System.nanoTime();
            ForkJoinPool pool = w.pool;
            pool.preJoin(this);
            while (status >= 0) {
                long nt = nanos - (System.nanoTime() - startTime);
                if (nt > 0) {
                    long ms = nt / 1000000;
                    int ns = (int) (nt % 1000000);
                    try {
                        synchronized (this) {
                            if (status >= 0) wait(ms, ns);
                        }
                    } catch (InterruptedException ie) {
                        cancelIfTerminating();
                    }
                } else {
                    int s; // adjust running count on timeout
                    while ((s = status) >= 0 &&
                            (s & INTERNAL_SIGNAL_MASK) != 0) {
                        if (UNSAFE.compareAndSwapInt(this, statusOffset,
                                s, s - 1)) {
                            pool.updateRunningCount(1);
                            break;
                        }
                    }
                    break;
                }
            }
        }
    }

    /**
     * Blocks a non-ForkJoin thread until this task is done or timeout elapses
     */
    private void externalTimedAwaitDone(long nanos) {
        if (requestExternalSignal() >= 0) {
            long startTime = System.nanoTime();
            boolean interrupted = false;
            while (status >= 0) {
                long nt = nanos - (System.nanoTime() - startTime);
                if (nt <= 0)
                    break;
                long ms = nt / 1000000;
                int ns = (int) (nt % 1000000);
                try {
                    synchronized (this) {
                        if (status >= 0) wait(ms, ns);
                    }
                } catch (InterruptedException ie) {
                    interrupted = true;
                }
            }
            if (interrupted)
                Thread.currentThread().interrupt();
        }
    }

    // reporting results

    /**
     * Returns result or throws the exception associated with status.
     * Uses Unsafe as a workaround for javac not allowing rethrow of
     * unchecked exceptions.
     */
    private V reportResult() {
        if ((status & COMPLETION_MASK) < NORMAL) {
            Throwable ex = getException();
            if (ex != null)
                UNSAFE.throwException(ex);
        }
        return getRawResult();
    }

    /**
     * Returns result or throws exception using j.u.c.Future conventions.
     * Only call when {@code isDone} known to be true or thread known
     * to be interrupted.
     */
    private V reportFutureResult()
            throws InterruptedException, ExecutionException {
        if (Thread.interrupted())
            throw new InterruptedException();
        int s = status & COMPLETION_MASK;
        if (s < NORMAL) {
            Throwable ex;
            if (s == CANCELLED)
                throw new CancellationException();
            if (s == EXCEPTIONAL && (ex = exceptionMap.get(this)) != null)
                throw new ExecutionException(ex);
        }
        return getRawResult();
    }

    /**
     * Returns result or throws exception using j.u.c.Future conventions
     * with timeouts.
     */
    private V reportTimedFutureResult()
            throws InterruptedException, ExecutionException, TimeoutException {
        if (Thread.interrupted())
            throw new InterruptedException();
        Throwable ex;
        int s = status & COMPLETION_MASK;
        if (s == NORMAL)
            return getRawResult();
        else if (s == CANCELLED)
            throw new CancellationException();
        else if (s == EXCEPTIONAL && (ex = exceptionMap.get(this)) != null)
            throw new ExecutionException(ex);
        else
            throw new TimeoutException();
    }

    // public methods

    /**
     * Arranges to asynchronously execute this task.  While it is not
     * necessarily enforced, it is a usage error to fork a task more
     * than once unless it has completed and been reinitialized.
     * Subsequent modifications to the state of this task or any data
     * it operates on are not necessarily consistently observable by
     * any thread other than the one executing it unless preceded by a
     * call to {@link #join} or related methods, or a call to {@link
     * #isDone} returning {@code true}.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return {@code this}, to simplify usage
     */
    public final ForkJoinTask<V> fork() {
        ((ForkJoinWorkerThread) Thread.currentThread())
                .pushTask(this);
        return this;
    }

    /**
     * Returns the result of the computation when it {@link #isDone is done}.
     * This method differs from {@link #get()} in that
     * abnormal completion results in {@code RuntimeException} or
     * {@code Error}, not {@code ExecutionException}.
     *
     * @return the computed result
     */
    public final V join() {
        quietlyJoin();
        return reportResult();
    }

    /**
     * Commences performing this task, awaits its completion if
     * necessary, and return its result, or throws an (unchecked)
     * exception if the underlying computation did so.
     *
     * @return the computed result
     */
    public final V invoke() {
        if (!tryExec())
            quietlyJoin();
        return reportResult();
    }

    /**
     * Forks the given tasks, returning when {@code isDone} holds for
     * each task or an (unchecked) exception is encountered, in which
     * case the exception is rethrown.  If either task encounters an
     * exception, the other one may be, but is not guaranteed to be,
     * cancelled.  If both tasks throw an exception, then this method
     * throws one of them.  The individual status of each task may be
     * checked using {@link #getException()} and related methods.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @param t1 the first task
     * @param t2 the second task
     * @throws NullPointerException if any task is null
     */
    public static void invokeAll(ForkJoinTask<?> t1, ForkJoinTask<?> t2) {
        t2.fork();
        t1.invoke();
        t2.join();
    }

    /**
     * Forks the given tasks, returning when {@code isDone} holds for
     * each task or an (unchecked) exception is encountered, in which
     * case the exception is rethrown. If any task encounters an
     * exception, others may be, but are not guaranteed to be,
     * cancelled.  If more than one task encounters an exception, then
     * this method throws any one of these exceptions.  The individual
     * status of each task may be checked using {@link #getException()}
     * and related methods.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @param tasks the tasks
     * @throws NullPointerException if any task is null
     */
    public static void invokeAll(ForkJoinTask<?>... tasks) {
        Throwable ex = null;
        int last = tasks.length - 1;
        for (int i = last; i >= 0; --i) {
            ForkJoinTask<?> t = tasks[i];
            if (t == null) {
                if (ex == null)
                    ex = new NullPointerException();
            } else if (i != 0)
                t.fork();
            else {
                t.quietlyInvoke();
                if (ex == null)
                    ex = t.getException();
            }
        }
        for (int i = 1; i <= last; ++i) {
            ForkJoinTask<?> t = tasks[i];
            if (t != null) {
                if (ex != null)
                    t.cancel(false);
                else {
                    t.quietlyJoin();
                    if (ex == null)
                        ex = t.getException();
                }
            }
        }
        if (ex != null)
            UNSAFE.throwException(ex);
    }

    /**
     * Forks all tasks in the specified collection, returning when
     * {@code isDone} holds for each task or an (unchecked) exception
     * is encountered.  If any task encounters an exception, others
     * may be, but are not guaranteed to be, cancelled.  If more than
     * one task encounters an exception, then this method throws any
     * one of these exceptions.  The individual status of each task
     * may be checked using {@link #getException()} and related
     * methods.  The behavior of this operation is undefined if the
     * specified collection is modified while the operation is in
     * progress.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @param tasks the collection of tasks
     * @return the tasks argument, to simplify usage
     * @throws NullPointerException if tasks or any element are null
     */
    public static <T extends ForkJoinTask<?>> Collection<T> invokeAll(Collection<T> tasks) {
        if (!(tasks instanceof RandomAccess) || !(tasks instanceof List<?>)) {
            invokeAll(tasks.toArray(new ForkJoinTask<?>[tasks.size()]));
            return tasks;
        }
        @SuppressWarnings("unchecked")
        List<? extends ForkJoinTask<?>> ts =
                (List<? extends ForkJoinTask<?>>) tasks;
        Throwable ex = null;
        int last = ts.size() - 1;
        for (int i = last; i >= 0; --i) {
            ForkJoinTask<?> t = ts.get(i);
            if (t == null) {
                if (ex == null)
                    ex = new NullPointerException();
            } else if (i != 0)
                t.fork();
            else {
                t.quietlyInvoke();
                if (ex == null)
                    ex = t.getException();
            }
        }
        for (int i = 1; i <= last; ++i) {
            ForkJoinTask<?> t = ts.get(i);
            if (t != null) {
                if (ex != null)
                    t.cancel(false);
                else {
                    t.quietlyJoin();
                    if (ex == null)
                        ex = t.getException();
                }
            }
        }
        if (ex != null)
            UNSAFE.throwException(ex);
        return tasks;
    }

    /**
     * Attempts to cancel execution of this task. This attempt will
     * fail if the task has already completed, has already been
     * cancelled, or could not be cancelled for some other reason. If
     * successful, and this task has not started when cancel is
     * called, execution of this task is suppressed, {@link
     * #isCancelled} will report true, and {@link #join} will result
     * in a {@code CancellationException} being thrown.
     *
     * <p>This method may be overridden in subclasses, but if so, must
     * still ensure that these minimal properties hold. In particular,
     * the {@code cancel} method itself must not throw exceptions.
     *
     * <p>This method is designed to be invoked by <em>other</em>
     * tasks. To terminate the current task, you can just return or
     * throw an unchecked exception from its computation method, or
     * invoke {@link #completeExceptionally}.
     *
     * @param mayInterruptIfRunning this value is ignored in the
     *                              default implementation because tasks are not
     *                              cancelled via interruption
     * @return {@code true} if this task is now cancelled
     */
    public boolean cancel(boolean mayInterruptIfRunning) {
        setCompletion(CANCELLED);
        return (status & COMPLETION_MASK) == CANCELLED;
    }

    /**
     * Cancels, ignoring any exceptions it throws. Used during worker
     * and pool shutdown.
     */
    final void cancelIgnoringExceptions() {
        try {
            cancel(false);
        } catch (Throwable ignore) {
        }
    }

    /**
     * Cancels ignoring exceptions if worker is terminating
     */
    private void cancelIfTerminating() {
        Thread t = Thread.currentThread();
        if ((t instanceof ForkJoinWorkerThread) &&
                ((ForkJoinWorkerThread) t).isTerminating()) {
            try {
                cancel(false);
            } catch (Throwable ignore) {
            }
        }
    }

    public final boolean isDone() {
        return status < 0;
    }

    public final boolean isCancelled() {
        return (status & COMPLETION_MASK) == CANCELLED;
    }

    /**
     * Returns {@code true} if this task threw an exception or was cancelled.
     *
     * @return {@code true} if this task threw an exception or was cancelled
     */
    public final boolean isCompletedAbnormally() {
        return (status & COMPLETION_MASK) < NORMAL;
    }

    /**
     * Returns {@code true} if this task completed without throwing an
     * exception and was not cancelled.
     *
     * @return {@code true} if this task completed without throwing an
     *         exception and was not cancelled
     */
    public final boolean isCompletedNormally() {
        return (status & COMPLETION_MASK) == NORMAL;
    }

    /**
     * Returns the exception thrown by the base computation, or a
     * {@code CancellationException} if cancelled, or {@code null} if
     * none or if the method has not yet completed.
     *
     * @return the exception, or {@code null} if none
     */
    public final Throwable getException() {
        int s = status & COMPLETION_MASK;
        return ((s >= NORMAL) ? null :
                (s == CANCELLED) ? new CancellationException() :
                        exceptionMap.get(this));
    }

    /**
     * Completes this task abnormally, and if not already aborted or
     * cancelled, causes it to throw the given exception upon
     * {@code join} and related operations. This method may be used
     * to induce exceptions in asynchronous tasks, or to force
     * completion of tasks that would not otherwise complete.  Its use
     * in other situations is discouraged.  This method is
     * overridable, but overridden versions must invoke {@code super}
     * implementation to maintain guarantees.
     *
     * @param ex the exception to throw. If this exception is not a
     *           {@code RuntimeException} or {@code Error}, the actual exception
     *           thrown will be a {@code RuntimeException} with cause {@code ex}.
     */
    public void completeExceptionally(Throwable ex) {
        setDoneExceptionally((ex instanceof RuntimeException) ||
                (ex instanceof Error) ? ex :
                new RuntimeException(ex));
    }

    /**
     * Completes this task, and if not already aborted or cancelled,
     * returning a {@code null} result upon {@code join} and related
     * operations. This method may be used to provide results for
     * asynchronous tasks, or to provide alternative handling for
     * tasks that would not otherwise complete normally. Its use in
     * other situations is discouraged. This method is
     * overridable, but overridden versions must invoke {@code super}
     * implementation to maintain guarantees.
     *
     * @param value the result value for this task
     */
    public void complete(V value) {
        try {
            setRawResult(value);
        } catch (Throwable rex) {
            setDoneExceptionally(rex);
            return;
        }
        setCompletion(NORMAL);
    }

    public final V get() throws InterruptedException, ExecutionException {
        quietlyJoin();
        return reportFutureResult();
    }

    public final V get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        long nanos = unit.toNanos(timeout);
        Thread t = Thread.currentThread();
        if (t instanceof ForkJoinWorkerThread) {
            ForkJoinWorkerThread w = (ForkJoinWorkerThread) t;
            if (!w.unpushTask(this) || !tryExec())
                timedAwaitDone(w, nanos);
        } else
            externalTimedAwaitDone(nanos);
        return reportTimedFutureResult();
    }

    /**
     * Possibly executes other tasks until this task {@link #isDone is
     * done}, then returns the result of the computation.  This method
     * may be more efficient than {@code join}, but is only applicable
     * when there are no potential dependencies between continuation
     * of the current task and that of any other task that might be
     * executed while helping. (This usually holds for pure
     * divide-and-conquer tasks).
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return the computed result
     */
    public final V helpJoin() {
        quietlyHelpJoin();
        return reportResult();
    }

    /**
     * Possibly executes other tasks until this task {@link #isDone is
     * done}.  This method may be useful when processing collections
     * of tasks when some have been cancelled or otherwise known to
     * have aborted.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     */
    public final void quietlyHelpJoin() {
        ForkJoinWorkerThread w = (ForkJoinWorkerThread) Thread.currentThread();
        if (!w.unpushTask(this) || !tryExec()) {
            while (status >= 0) {
                ForkJoinTask<?> t = w.scanWhileJoining(this);
                if (t == null) {
                    if (status >= 0)
                        awaitDone(w);
                    break;
                }
                t.tryExec();
            }
        }
    }

    /**
     * Joins this task, without returning its result or throwing an
     * exception. This method may be useful when processing
     * collections of tasks when some have been cancelled or otherwise
     * known to have aborted.
     */
    public final void quietlyJoin() {
        Thread t = Thread.currentThread();
        if (t instanceof ForkJoinWorkerThread) {
            ForkJoinWorkerThread w = (ForkJoinWorkerThread) t;
            if (!w.unpushTask(this) || !tryExec())
                awaitDone(w);
        } else
            externalAwaitDone();
    }

    /**
     * Commences performing this task and awaits its completion if
     * necessary, without returning its result or throwing an
     * exception. This method may be useful when processing
     * collections of tasks when some have been cancelled or otherwise
     * known to have aborted.
     */
    public final void quietlyInvoke() {
        if (!tryExec())
            quietlyJoin();
    }

    /**
     * Possibly executes tasks until the pool hosting the current task
     * {@link ForkJoinPool#isQuiescent is quiescent}. This method may
     * be of use in designs in which many tasks are forked, but none
     * are explicitly joined, instead executing them until all are
     * processed.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     */
    public static void helpQuiesce() {
        ((ForkJoinWorkerThread) Thread.currentThread())
                .helpQuiescePool();
    }

    /**
     * Resets the internal bookkeeping state of this task, allowing a
     * subsequent {@code fork}. This method allows repeated reuse of
     * this task, but only if reuse occurs when this task has either
     * never been forked, or has been forked, then completed and all
     * outstanding joins of this task have also completed. Effects
     * under any other usage conditions are not guaranteed.
     * This method may be useful when executing
     * pre-constructed trees of subtasks in loops.
     */
    public void reinitialize() {
        if ((status & COMPLETION_MASK) == EXCEPTIONAL)
            exceptionMap.remove(this);
        status = 0;
    }

    /**
     * Returns the pool hosting the current task execution, or null
     * if this task is executing outside of any ForkJoinPool.
     *
     * @return the pool, or {@code null} if none
     * @see #inForkJoinPool
     */
    public static ForkJoinPool getPool() {
        Thread t = Thread.currentThread();
        return (t instanceof ForkJoinWorkerThread) ?
                ((ForkJoinWorkerThread) t).pool : null;
    }

    /**
     * Returns {@code true} if the current thread is executing as a
     * ForkJoinPool computation.
     *
     * @return {@code true} if the current thread is executing as a
     *         ForkJoinPool computation, or false otherwise
     */
    public static boolean inForkJoinPool() {
        return Thread.currentThread() instanceof ForkJoinWorkerThread;
    }

    /**
     * Tries to unschedule this task for execution. This method will
     * typically succeed if this task is the most recently forked task
     * by the current thread, and has not commenced executing in
     * another thread.  This method may be useful when arranging
     * alternative local processing of tasks that could have been, but
     * were not, stolen.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return {@code true} if unforked
     */
    public boolean tryUnfork() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .unpushTask(this);
    }

    /**
     * Returns an estimate of the number of tasks that have been
     * forked by the current worker thread but not yet executed. This
     * value may be useful for heuristic decisions about whether to
     * fork other tasks.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return the number of tasks
     */
    public static int getQueuedTaskCount() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .getQueueSize();
    }

    /**
     * Returns an estimate of how many more locally queued tasks are
     * held by the current worker thread than there are other worker
     * threads that might steal them.  This value may be useful for
     * heuristic decisions about whether to fork other tasks. In many
     * usages of ForkJoinTasks, at steady state, each worker should
     * aim to maintain a small constant surplus (for example, 3) of
     * tasks, and to process computations locally if this threshold is
     * exceeded.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return the surplus number of tasks, which may be negative
     */
    public static int getSurplusQueuedTaskCount() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .getEstimatedSurplusTaskCount();
    }

    // Extension methods

    /**
     * Returns the result that would be returned by {@link #join}, even
     * if this task completed abnormally, or {@code null} if this task
     * is not known to have been completed.  This method is designed
     * to aid debugging, as well as to support extensions. Its use in
     * any other context is discouraged.
     *
     * @return the result, or {@code null} if not completed
     */
    public abstract V getRawResult();

    /**
     * Forces the given value to be returned as a result.  This method
     * is designed to support extensions, and should not in general be
     * called otherwise.
     *
     * @param value the value
     */
    protected abstract void setRawResult(V value);

    /**
     * Immediately performs the base action of this task.  This method
     * is designed to support extensions, and should not in general be
     * called otherwise. The return value controls whether this task
     * is considered to be done normally. It may return false in
     * asynchronous actions that require explicit invocations of
     * {@link #complete} to become joinable. It may also throw an
     * (unchecked) exception to indicate abnormal exit.
     *
     * @return {@code true} if completed normally
     */
    protected abstract boolean exec();

    /**
     * Returns, but does not unschedule or execute, a task queued by
     * the current thread but not yet executed, if one is immediately
     * available. There is no guarantee that this task will actually
     * be polled or executed next. Conversely, this method may return
     * null even if a task exists but cannot be accessed without
     * contention with other threads.  This method is designed
     * primarily to support extensions, and is unlikely to be useful
     * otherwise.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return the next task, or {@code null} if none are available
     */
    protected static ForkJoinTask<?> peekNextLocalTask() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .peekTask();
    }

    /**
     * Unschedules and returns, without executing, the next task
     * queued by the current thread but not yet executed.  This method
     * is designed primarily to support extensions, and is unlikely to
     * be useful otherwise.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return the next task, or {@code null} if none are available
     */
    protected static ForkJoinTask<?> pollNextLocalTask() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .pollLocalTask();
    }

    /**
     * Unschedules and returns, without executing, the next task
     * queued by the current thread but not yet executed, if one is
     * available, or if not available, a task that was forked by some
     * other thread, if available. Availability may be transient, so a
     * {@code null} result does not necessarily imply quiescence
     * of the pool this task is operating in.  This method is designed
     * primarily to support extensions, and is unlikely to be useful
     * otherwise.
     *
     * <p>This method may be invoked only from within {@code
     * ForkJoinTask} computations (as may be determined using method
     * {@link #inForkJoinPool}).  Attempts to invoke in other contexts
     * result in exceptions or errors, possibly including {@code
     * ClassCastException}.
     *
     * @return a task, or {@code null} if none are available
     */
    protected static ForkJoinTask<?> pollTask() {
        return ((ForkJoinWorkerThread) Thread.currentThread())
                .pollTask();
    }

    /**
     * Adaptor for Runnables. This implements RunnableFuture
     * to be compliant with AbstractExecutorService constraints
     * when used in ForkJoinPool.
     */
    static final class AdaptedRunnable<T> extends ForkJoinTask<T>
            implements RunnableFuture<T> {
        final Runnable runnable;
        final T resultOnCompletion;
        T result;

        AdaptedRunnable(Runnable runnable, T result) {
            if (runnable == null) throw new NullPointerException();
            this.runnable = runnable;
            this.resultOnCompletion = result;
        }

        public T getRawResult() {
            return result;
        }

        public void setRawResult(T v) {
            result = v;
        }

        public boolean exec() {
            runnable.run();
            result = resultOnCompletion;
            return true;
        }

        public void run() {
            invoke();
        }

        private static final long serialVersionUID = 5232453952276885070L;
    }

    /**
     * Adaptor for Callables
     */
    static final class AdaptedCallable<T> extends ForkJoinTask<T>
            implements RunnableFuture<T> {
        final Callable<? extends T> callable;
        T result;

        AdaptedCallable(Callable<? extends T> callable) {
            if (callable == null) throw new NullPointerException();
            this.callable = callable;
        }

        public T getRawResult() {
            return result;
        }

        public void setRawResult(T v) {
            result = v;
        }

        public boolean exec() {
            try {
                result = callable.call();
                return true;
            } catch (Error err) {
                throw err;
            } catch (RuntimeException rex) {
                throw rex;
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public void run() {
            invoke();
        }

        private static final long serialVersionUID = 2838392045355241008L;
    }

    /**
     * Returns a new {@code ForkJoinTask} that performs the {@code run}
     * method of the given {@code Runnable} as its action, and returns
     * a null result upon {@link #join}.
     *
     * @param runnable the runnable action
     * @return the task
     */
    public static ForkJoinTask<?> adapt(Runnable runnable) {
        return new AdaptedRunnable<Void>(runnable, null);
    }

    /**
     * Returns a new {@code ForkJoinTask} that performs the {@code run}
     * method of the given {@code Runnable} as its action, and returns
     * the given result upon {@link #join}.
     *
     * @param runnable the runnable action
     * @param result   the result upon completion
     * @return the task
     */
    public static <T> ForkJoinTask<T> adapt(Runnable runnable, T result) {
        return new AdaptedRunnable<T>(runnable, result);
    }

    /**
     * Returns a new {@code ForkJoinTask} that performs the {@code call}
     * method of the given {@code Callable} as its action, and returns
     * its result upon {@link #join}, translating any checked exceptions
     * encountered into {@code RuntimeException}.
     *
     * @param callable the callable action
     * @return the task
     */
    public static <T> ForkJoinTask<T> adapt(Callable<? extends T> callable) {
        return new AdaptedCallable<T>(callable);
    }

    // Serialization support

    private static final long serialVersionUID = -7721805057305804111L;

    /**
     * Saves the state to a stream.
     *
     * @param s the stream
     * @serialData the current run status and the exception thrown
     * during execution, or {@code null} if none
     */
    private void writeObject(java.io.ObjectOutputStream s)
            throws java.io.IOException {
        s.defaultWriteObject();
        s.writeObject(getException());
    }

    /**
     * Reconstitutes the instance from a stream.
     *
     * @param s the stream
     */
    private void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException {
        s.defaultReadObject();
        status &= ~INTERNAL_SIGNAL_MASK; // clear internal signal counts
        status |= EXTERNAL_SIGNAL; // conservatively set external signal
        Object ex = s.readObject();
        if (ex != null)
            setDoneExceptionally((Throwable) ex);
    }

    // Unsafe mechanics

    private static final sun.misc.Unsafe UNSAFE = getUnsafe();
    private static final long statusOffset =
            objectFieldOffset("status", ForkJoinTask.class);

    private static long objectFieldOffset(String field, Class<?> klazz) {
        try {
            return UNSAFE.objectFieldOffset(klazz.getDeclaredField(field));
        } catch (NoSuchFieldException e) {
            // Convert Exception to corresponding Error
            NoSuchFieldError error = new NoSuchFieldError(field);
            error.initCause(e);
            throw error;
        }
    }

    /**
     * Returns a sun.misc.Unsafe.  Suitable for use in a 3rd party package.
     * Replace with a simple call to Unsafe.getUnsafe when integrating
     * into a jdk.
     *
     * @return a sun.misc.Unsafe
     */
    private static sun.misc.Unsafe getUnsafe() {
        try {
            return sun.misc.Unsafe.getUnsafe();
        } catch (SecurityException se) {
            try {
                return java.security.AccessController.doPrivileged
                        (new java.security
                                .PrivilegedExceptionAction<sun.misc.Unsafe>() {
                            public sun.misc.Unsafe run() throws Exception {
                                java.lang.reflect.Field f = sun.misc
                                        .Unsafe.class.getDeclaredField("theUnsafe");
                                f.setAccessible(true);
                                return (sun.misc.Unsafe) f.get(null);
                            }
                        });
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics",
                        e.getCause());
            }
        }
    }
}
