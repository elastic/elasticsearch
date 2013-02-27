/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e;
import jsr166y.ThreadLocalRandom;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * A {@link Future} that may be explicitly completed (setting its
 * value and status), and may include dependent functions and actions
 * that trigger upon its completion.  Methods are available for adding
 * those based on Functions, Blocks, and Runnables, depending on
 * whether they require arguments and/or produce results, as well as
 * those triggered after either or both the current and another
 * CompletableFuture complete.  Functions and actions supplied for
 * dependent completions (mainly using methods with prefix {@code
 * then}) may be performed by the thread that completes the current
 * CompletableFuture, or by any other caller of these methods.  There
 * are no guarantees about the order of processing completions unless
 * constrained by these methods.
 *
 * <p>When two or more threads attempt to {@link #complete} or {@link
 * #completeExceptionally} a CompletableFuture, only one of them
 * succeeds.
 *
 * <p>Upon exceptional completion, or when a completion entails
 * computation of a function or action, and it terminates abruptly
 * with an (unchecked) exception or error, then further completions
 * act as {@code completeExceptionally} with a {@link
 * CompletionException} holding that exception as its cause.  If a
 * CompletableFuture completes exceptionally, and is not followed by a
 * {@link #exceptionally} or {@link #handle} completion, then all of
 * its dependents (and their dependents) also complete exceptionally
 * with CompletionExceptions holding the ultimate cause.  In case of a
 * CompletionException, methods {@link #get()} and {@link #get(long,
 * TimeUnit)} throw an {@link ExecutionException} with the same cause
 * as would be held in the corresponding CompletionException. However,
 * in these cases, methods {@link #join()} and {@link #getNow} throw
 * the CompletionException, which simplifies usage especially within
 * other completion functions.
 *
 * <p>CompletableFutures themselves do not execute asynchronously.
 * However, the {@code async} methods provide commonly useful ways to
 * commence asynchronous processing, using either a given {@link
 * Executor} or by default the {@link ForkJoinPool#commonPool()}, of a
 * function or action that will result in the completion of a new
 * CompletableFuture. To simplify monitoring, debugging, and tracking,
 * all generated asynchronous tasks are instances of the tagging
 * interface {@link AsynchronousCompletionTask}.
 *
 * <p><em>jsr166e note: During transition, this class
 * uses nested functional interfaces with different names but the
 * same forms as those expected for JDK8.</em>
 *
 * @author Doug Lea
 * @since 1.8
 */
public class CompletableFuture<T> implements Future<T> {
    // jsr166e nested interfaces

    /** Interface describing a void action of one argument */
    public interface Action<A> { void accept(A a); }
    /** Interface describing a void action of two arguments */
    public interface BiAction<A,B> { void accept(A a, B b); }
    /** Interface describing a function of one argument */
    public interface Fun<A,T> { T apply(A a); }
    /** Interface describing a function of two arguments */
    public interface BiFun<A,B,T> { T apply(A a, B b); }
    /** Interface describing a function of no arguments */
    public interface Generator<T> { T get(); }


    /*
     * Overview:
     *
     * 1. Non-nullness of field result (set via CAS) indicates done.
     * An AltResult is used to box null as a result, as well as to
     * hold exceptions.  Using a single field makes completion fast
     * and simple to detect and trigger, at the expense of a lot of
     * encoding and decoding that infiltrates many methods. One minor
     * simplification relies on the (static) NIL (to box null results)
     * being the only AltResult with a null exception field, so we
     * don't usually need explicit comparisons with NIL. The CF
     * exception propagation mechanics surrounding decoding rely on
     * unchecked casts of decoded results really being unchecked,
     * where user type errors are caught at point of use, as is
     * currently the case in Java. These are highlighted by using
     * SuppressWarnings-annotated temporaries.
     *
     * 2. Waiters are held in a Treiber stack similar to the one used
     * in FutureTask, Phaser, and SynchronousQueue. See their
     * internal documentation for algorithmic details.
     *
     * 3. Completions are also kept in a list/stack, and pulled off
     * and run when completion is triggered. (We could even use the
     * same stack as for waiters, but would give up the potential
     * parallelism obtained because woken waiters help release/run
     * others -- see method postComplete).  Because post-processing
     * may race with direct calls, class Completion opportunistically
     * extends AtomicInteger so callers can claim the action via
     * compareAndSet(0, 1).  The Completion.run methods are all
     * written a boringly similar uniform way (that sometimes includes
     * unnecessary-looking checks, kept to maintain uniformity). There
     * are enough dimensions upon which they differ that factoring to
     * use common code isn't worthwhile.
     *
     * 4. The exported then/and/or methods do support a bit of
     * factoring (see doThenApply etc). They must cope with the
     * intrinsic races surrounding addition of a dependent action
     * versus performing the action directly because the task is
     * already complete.  For example, a CF may not be complete upon
     * entry, so a dependent completion is added, but by the time it
     * is added, the target CF is complete, so must be directly
     * executed. This is all done while avoiding unnecessary object
     * construction in safe-bypass cases.
     */

    // preliminaries

    static final class AltResult {
        final Throwable ex; // null only for NIL
        AltResult(Throwable ex) { this.ex = ex; }
    }

    static final AltResult NIL = new AltResult(null);

    // Fields

    volatile Object result;    // Either the result or boxed AltResult
    volatile WaitNode waiters; // Treiber stack of threads blocked on get()
    volatile CompletionNode completions; // list (Treiber stack) of completions

    // Basic utilities for triggering and processing completions

    /**
     * Removes and signals all waiting threads and runs all completions.
     */
    final void postComplete() {
        WaitNode q; Thread t;
        while ((q = waiters) != null) {
            if (UNSAFE.compareAndSwapObject(this, WAITERS, q, q.next) &&
                (t = q.thread) != null) {
                q.thread = null;
                LockSupport.unpark(t);
            }
        }

        CompletionNode h; Completion c;
        while ((h = completions) != null) {
            if (UNSAFE.compareAndSwapObject(this, COMPLETIONS, h, h.next) &&
                (c = h.completion) != null)
                c.run();
        }
    }

    /**
     * Triggers completion with the encoding of the given arguments:
     * if the exception is non-null, encodes it as a wrapped
     * CompletionException unless it is one already.  Otherwise uses
     * the given result, boxed as NIL if null.
     */
    final void internalComplete(Object v, Throwable ex) {
        if (result == null)
            UNSAFE.compareAndSwapObject
                (this, RESULT, null,
                 (ex == null) ? (v == null) ? NIL : v :
                 new AltResult((ex instanceof CompletionException) ? ex :
                               new CompletionException(ex)));
        postComplete(); // help out even if not triggered
    }

    /**
     * If triggered, helps release and/or process completions.
     */
    final void helpPostComplete() {
        if (result != null)
            postComplete();
    }

    /* ------------- waiting for completions -------------- */

    /**
     * Heuristic spin value for waitingGet() before blocking on
     * multiprocessors
     */
    static final int WAITING_GET_SPINS = 256;

    /**
     * Linked nodes to record waiting threads in a Treiber stack.  See
     * other classes such as Phaser and SynchronousQueue for more
     * detailed explanation. This class implements ManagedBlocker to
     * avoid starvation when blocking actions pile up in
     * ForkJoinPools.
     */
    static final class WaitNode implements ForkJoinPool.ManagedBlocker {
        long nanos;          // wait time if timed
        final long deadline; // non-zero if timed
        volatile int interruptControl; // > 0: interruptible, < 0: interrupted
        volatile Thread thread;
        volatile WaitNode next;
        WaitNode(boolean interruptible, long nanos, long deadline) {
            this.thread = Thread.currentThread();
            this.interruptControl = interruptible ? 1 : 0;
            this.nanos = nanos;
            this.deadline = deadline;
        }
        public boolean isReleasable() {
            if (thread == null)
                return true;
            if (Thread.interrupted()) {
                int i = interruptControl;
                interruptControl = -1;
                if (i > 0)
                    return true;
            }
            if (deadline != 0L &&
                (nanos <= 0L || (nanos = deadline - System.nanoTime()) <= 0L)) {
                thread = null;
                return true;
            }
            return false;
        }
        public boolean block() {
            if (isReleasable())
                return true;
            else if (deadline == 0L)
                LockSupport.park(this);
            else if (nanos > 0L)
                LockSupport.parkNanos(this, nanos);
            return isReleasable();
        }
    }

    /**
     * Returns raw result after waiting, or null if interruptible and
     * interrupted.
     */
    private Object waitingGet(boolean interruptible) {
        WaitNode q = null;
        boolean queued = false;
        int h = 0, spins = 0;
        for (Object r;;) {
            if ((r = result) != null) {
                if (q != null) { // suppress unpark
                    q.thread = null;
                    if (q.interruptControl < 0) {
                        if (interruptible) {
                            removeWaiter(q);
                            return null;
                        }
                        Thread.currentThread().interrupt();
                    }
                }
                postComplete(); // help release others
                return r;
            }
            else if (h == 0) {
                h = ThreadLocalRandom.current().nextInt();
                if (Runtime.getRuntime().availableProcessors() > 1)
                    spins = WAITING_GET_SPINS;
            }
            else if (spins > 0) {
                h ^= h << 1;  // xorshift
                h ^= h >>> 3;
                if ((h ^= h << 10) >= 0)
                    --spins;
            }
            else if (q == null)
                q = new WaitNode(interruptible, 0L, 0L);
            else if (!queued)
                queued = UNSAFE.compareAndSwapObject(this, WAITERS,
                                                     q.next = waiters, q);
            else if (interruptible && q.interruptControl < 0) {
                removeWaiter(q);
                return null;
            }
            else if (q.thread != null && result == null) {
                try {
                    ForkJoinPool.managedBlock(q);
                } catch (InterruptedException ex) {
                    q.interruptControl = -1;
                }
            }
        }
    }

    /**
     * Awaits completion or aborts on interrupt or timeout.
     *
     * @param nanos time to wait
     * @return raw result
     */
    private Object timedAwaitDone(long nanos)
        throws InterruptedException, TimeoutException {
        WaitNode q = null;
        boolean queued = false;
        for (Object r;;) {
            if ((r = result) != null) {
                if (q != null) {
                    q.thread = null;
                    if (q.interruptControl < 0) {
                        removeWaiter(q);
                        throw new InterruptedException();
                    }
                }
                postComplete();
                return r;
            }
            else if (q == null) {
                if (nanos <= 0L)
                    throw new TimeoutException();
                long d = System.nanoTime() + nanos;
                q = new WaitNode(true, nanos, d == 0L ? 1L : d); // avoid 0
            }
            else if (!queued)
                queued = UNSAFE.compareAndSwapObject(this, WAITERS,
                                                     q.next = waiters, q);
            else if (q.interruptControl < 0) {
                removeWaiter(q);
                throw new InterruptedException();
            }
            else if (q.nanos <= 0L) {
                if (result == null) {
                    removeWaiter(q);
                    throw new TimeoutException();
                }
            }
            else if (q.thread != null && result == null) {
                try {
                    ForkJoinPool.managedBlock(q);
                } catch (InterruptedException ex) {
                    q.interruptControl = -1;
                }
            }
        }
    }

    /**
     * Tries to unlink a timed-out or interrupted wait node to avoid
     * accumulating garbage.  Internal nodes are simply unspliced
     * without CAS since it is harmless if they are traversed anyway
     * by releasers.  To avoid effects of unsplicing from already
     * removed nodes, the list is retraversed in case of an apparent
     * race.  This is slow when there are a lot of nodes, but we don't
     * expect lists to be long enough to outweigh higher-overhead
     * schemes.
     */
    private void removeWaiter(WaitNode node) {
        if (node != null) {
            node.thread = null;
            retry:
            for (;;) {          // restart on removeWaiter race
                for (WaitNode pred = null, q = waiters, s; q != null; q = s) {
                    s = q.next;
                    if (q.thread != null)
                        pred = q;
                    else if (pred != null) {
                        pred.next = s;
                        if (pred.thread == null) // check for race
                            continue retry;
                    }
                    else if (!UNSAFE.compareAndSwapObject(this, WAITERS, q, s))
                        continue retry;
                }
                break;
            }
        }
    }

    /* ------------- Async tasks -------------- */

    /**
     * A tagging interface identifying asynchronous tasks produced by
     * {@code async} methods. This may be useful for monitoring,
     * debugging, and tracking asynchronous activities.
     */
    public static interface AsynchronousCompletionTask {
    }

    /** Base class can act as either FJ or plain Runnable */
    abstract static class Async extends ForkJoinTask<Void>
        implements Runnable, AsynchronousCompletionTask {
        public final Void getRawResult() { return null; }
        public final void setRawResult(Void v) { }
        public final void run() { exec(); }
    }

    static final class AsyncRun extends Async {
        final Runnable fn;
        final CompletableFuture<Void> dst;
        AsyncRun(Runnable fn, CompletableFuture<Void> dst) {
            this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<Void> d; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    fn.run();
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                }
                d.internalComplete(null, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AsyncSupply<U> extends Async {
        final Generator<U> fn;
        final CompletableFuture<U> dst;
        AsyncSupply(Generator<U> fn, CompletableFuture<U> dst) {
            this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<U> d; U u; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    u = fn.get();
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                    u = null;
                }
                d.internalComplete(u, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AsyncApply<T,U> extends Async {
        final Fun<? super T,? extends U> fn;
        final T arg;
        final CompletableFuture<U> dst;
        AsyncApply(T arg, Fun<? super T,? extends U> fn,
                      CompletableFuture<U> dst) {
            this.arg = arg; this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<U> d; U u; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    u = fn.apply(arg);
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                    u = null;
                }
                d.internalComplete(u, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AsyncBiApply<T,U,V> extends Async {
        final BiFun<? super T,? super U,? extends V> fn;
        final T arg1;
        final U arg2;
        final CompletableFuture<V> dst;
        AsyncBiApply(T arg1, U arg2,
                        BiFun<? super T,? super U,? extends V> fn,
                        CompletableFuture<V> dst) {
            this.arg1 = arg1; this.arg2 = arg2; this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<V> d; V v; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    v = fn.apply(arg1, arg2);
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                    v = null;
                }
                d.internalComplete(v, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AsyncAccept<T> extends Async {
        final Action<? super T> fn;
        final T arg;
        final CompletableFuture<Void> dst;
        AsyncAccept(T arg, Action<? super T> fn,
                   CompletableFuture<Void> dst) {
            this.arg = arg; this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<Void> d; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    fn.accept(arg);
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                }
                d.internalComplete(null, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AsyncBiAccept<T,U> extends Async {
        final BiAction<? super T,? super U> fn;
        final T arg1;
        final U arg2;
        final CompletableFuture<Void> dst;
        AsyncBiAccept(T arg1, U arg2,
                     BiAction<? super T,? super U> fn,
                     CompletableFuture<Void> dst) {
            this.arg1 = arg1; this.arg2 = arg2; this.fn = fn; this.dst = dst;
        }
        public final boolean exec() {
            CompletableFuture<Void> d; Throwable ex;
            if ((d = this.dst) != null && d.result == null) {
                try {
                    fn.accept(arg1, arg2);
                    ex = null;
                } catch (Throwable rex) {
                    ex = rex;
                }
                d.internalComplete(null, ex);
            }
            return true;
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    /* ------------- Completions -------------- */

    /**
     * Simple linked list nodes to record completions, used in
     * basically the same way as WaitNodes. (We separate nodes from
     * the Completions themselves mainly because for the And and Or
     * methods, the same Completion object resides in two lists.)
     */
    static final class CompletionNode {
        final Completion completion;
        volatile CompletionNode next;
        CompletionNode(Completion completion) { this.completion = completion; }
    }

    // Opportunistically subclass AtomicInteger to use compareAndSet to claim.
    abstract static class Completion extends AtomicInteger implements Runnable {
    }

    static final class ApplyCompletion<T,U> extends Completion {
        final CompletableFuture<? extends T> src;
        final Fun<? super T,? extends U> fn;
        final CompletableFuture<U> dst;
        final Executor executor;
        ApplyCompletion(CompletableFuture<? extends T> src,
                        Fun<? super T,? extends U> fn,
                        CompletableFuture<U> dst, Executor executor) {
            this.src = src; this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final Fun<? super T,? extends U> fn;
            final CompletableFuture<U> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                Executor e = executor;
                U u = null;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncApply<T,U>(t, fn, dst));
                        else
                            u = fn.apply(t);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(u, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class AcceptCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final Action<? super T> fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        AcceptCompletion(CompletableFuture<? extends T> src,
                         Action<? super T> fn,
                         CompletableFuture<Void> dst, Executor executor) {
            this.src = src; this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final Action<? super T> fn;
            final CompletableFuture<Void> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncAccept<T>(t, fn, dst));
                        else
                            fn.accept(t);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class RunCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final Runnable fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        RunCompletion(CompletableFuture<? extends T> src,
                      Runnable fn,
                      CompletableFuture<Void> dst,
                      Executor executor) {
            this.src = src; this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final Runnable fn;
            final CompletableFuture<Void> dst;
            Object r; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult)
                    ex = ((AltResult)r).ex;
                else
                    ex = null;
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncRun(fn, dst));
                        else
                            fn.run();
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class BiApplyCompletion<T,U,V> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<? extends U> snd;
        final BiFun<? super T,? super U,? extends V> fn;
        final CompletableFuture<V> dst;
        final Executor executor;
        BiApplyCompletion(CompletableFuture<? extends T> src,
                          CompletableFuture<? extends U> snd,
                          BiFun<? super T,? super U,? extends V> fn,
                          CompletableFuture<V> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<? extends U> b;
            final BiFun<? super T,? super U,? extends V> fn;
            final CompletableFuture<V> dst;
            Object r, s; T t; U u; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                (b = this.snd) != null &&
                (s = b.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                if (ex != null)
                    u = null;
                else if (s instanceof AltResult) {
                    ex = ((AltResult)s).ex;
                    u = null;
                }
                else {
                    @SuppressWarnings("unchecked") U us = (U) s;
                    u = us;
                }
                Executor e = executor;
                V v = null;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncBiApply<T,U,V>(t, u, fn, dst));
                        else
                            v = fn.apply(t, u);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(v, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class BiAcceptCompletion<T,U> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<? extends U> snd;
        final BiAction<? super T,? super U> fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        BiAcceptCompletion(CompletableFuture<? extends T> src,
                           CompletableFuture<? extends U> snd,
                           BiAction<? super T,? super U> fn,
                           CompletableFuture<Void> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<? extends U> b;
            final BiAction<? super T,? super U> fn;
            final CompletableFuture<Void> dst;
            Object r, s; T t; U u; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                (b = this.snd) != null &&
                (s = b.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                if (ex != null)
                    u = null;
                else if (s instanceof AltResult) {
                    ex = ((AltResult)s).ex;
                    u = null;
                }
                else {
                    @SuppressWarnings("unchecked") U us = (U) s;
                    u = us;
                }
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncBiAccept<T,U>(t, u, fn, dst));
                        else
                            fn.accept(t, u);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class BiRunCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<?> snd;
        final Runnable fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        BiRunCompletion(CompletableFuture<? extends T> src,
                        CompletableFuture<?> snd,
                        Runnable fn,
                        CompletableFuture<Void> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<?> b;
            final Runnable fn;
            final CompletableFuture<Void> dst;
            Object r, s; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                (b = this.snd) != null &&
                (s = b.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult)
                    ex = ((AltResult)r).ex;
                else
                    ex = null;
                if (ex == null && (s instanceof AltResult))
                    ex = ((AltResult)s).ex;
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncRun(fn, dst));
                        else
                            fn.run();
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class OrApplyCompletion<T,U> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<? extends T> snd;
        final Fun<? super T,? extends U> fn;
        final CompletableFuture<U> dst;
        final Executor executor;
        OrApplyCompletion(CompletableFuture<? extends T> src,
                          CompletableFuture<? extends T> snd,
                          Fun<? super T,? extends U> fn,
                          CompletableFuture<U> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<? extends T> b;
            final Fun<? super T,? extends U> fn;
            final CompletableFuture<U> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (((a = this.src) != null && (r = a.result) != null) ||
                 ((b = this.snd) != null && (r = b.result) != null)) &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                Executor e = executor;
                U u = null;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncApply<T,U>(t, fn, dst));
                        else
                            u = fn.apply(t);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(u, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class OrAcceptCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<? extends T> snd;
        final Action<? super T> fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        OrAcceptCompletion(CompletableFuture<? extends T> src,
                           CompletableFuture<? extends T> snd,
                           Action<? super T> fn,
                           CompletableFuture<Void> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<? extends T> b;
            final Action<? super T> fn;
            final CompletableFuture<Void> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (((a = this.src) != null && (r = a.result) != null) ||
                 ((b = this.snd) != null && (r = b.result) != null)) &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncAccept<T>(t, fn, dst));
                        else
                            fn.accept(t);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class OrRunCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<?> snd;
        final Runnable fn;
        final CompletableFuture<Void> dst;
        final Executor executor;
        OrRunCompletion(CompletableFuture<? extends T> src,
                        CompletableFuture<?> snd,
                        Runnable fn,
                        CompletableFuture<Void> dst, Executor executor) {
            this.src = src; this.snd = snd;
            this.fn = fn; this.dst = dst;
            this.executor = executor;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<?> b;
            final Runnable fn;
            final CompletableFuture<Void> dst;
            Object r; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (((a = this.src) != null && (r = a.result) != null) ||
                 ((b = this.snd) != null && (r = b.result) != null)) &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult)
                    ex = ((AltResult)r).ex;
                else
                    ex = null;
                Executor e = executor;
                if (ex == null) {
                    try {
                        if (e != null)
                            e.execute(new AsyncRun(fn, dst));
                        else
                            fn.run();
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (e == null || ex != null)
                    dst.internalComplete(null, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class ExceptionCompletion<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final Fun<? super Throwable, ? extends T> fn;
        final CompletableFuture<T> dst;
        ExceptionCompletion(CompletableFuture<? extends T> src,
                            Fun<? super Throwable, ? extends T> fn,
                            CompletableFuture<T> dst) {
            this.src = src; this.fn = fn; this.dst = dst;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final Fun<? super Throwable, ? extends T> fn;
            final CompletableFuture<T> dst;
            Object r; T t = null; Throwable ex, dx = null;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if ((r instanceof AltResult) &&
                    (ex = ((AltResult)r).ex) != null)  {
                    try {
                        t = fn.apply(ex);
                    } catch (Throwable rex) {
                        dx = rex;
                    }
                }
                else {
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                dst.internalComplete(t, dx);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class ThenCopy<T> extends Completion {
        final CompletableFuture<? extends T> src;
        final CompletableFuture<T> dst;
        ThenCopy(CompletableFuture<? extends T> src,
                 CompletableFuture<T> dst) {
            this.src = src; this.dst = dst;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final CompletableFuture<T> dst;
            Object r; Object t; Throwable ex;
            if ((dst = this.dst) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    t = r;
                }
                dst.internalComplete(t, ex);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class HandleCompletion<T,U> extends Completion {
        final CompletableFuture<? extends T> src;
        final BiFun<? super T, Throwable, ? extends U> fn;
        final CompletableFuture<U> dst;
        HandleCompletion(CompletableFuture<? extends T> src,
                         BiFun<? super T, Throwable, ? extends U> fn,
                         final CompletableFuture<U> dst) {
            this.src = src; this.fn = fn; this.dst = dst;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final BiFun<? super T, Throwable, ? extends U> fn;
            final CompletableFuture<U> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                U u = null; Throwable dx = null;
                try {
                    u = fn.apply(t, ex);
                } catch (Throwable rex) {
                    dx = rex;
                }
                dst.internalComplete(u, dx);
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    static final class ComposeCompletion<T,U> extends Completion {
        final CompletableFuture<? extends T> src;
        final Fun<? super T, CompletableFuture<U>> fn;
        final CompletableFuture<U> dst;
        ComposeCompletion(CompletableFuture<? extends T> src,
                          Fun<? super T, CompletableFuture<U>> fn,
                          final CompletableFuture<U> dst) {
            this.src = src; this.fn = fn; this.dst = dst;
        }
        public final void run() {
            final CompletableFuture<? extends T> a;
            final Fun<? super T, CompletableFuture<U>> fn;
            final CompletableFuture<U> dst;
            Object r; T t; Throwable ex;
            if ((dst = this.dst) != null &&
                (fn = this.fn) != null &&
                (a = this.src) != null &&
                (r = a.result) != null &&
                compareAndSet(0, 1)) {
                if (r instanceof AltResult) {
                    ex = ((AltResult)r).ex;
                    t = null;
                }
                else {
                    ex = null;
                    @SuppressWarnings("unchecked") T tr = (T) r;
                    t = tr;
                }
                CompletableFuture<U> c = null;
                U u = null;
                boolean complete = false;
                if (ex == null) {
                    try {
                        c = fn.apply(t);
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                }
                if (ex != null || c == null) {
                    if (ex == null)
                        ex = new NullPointerException();
                }
                else {
                    ThenCopy<U> d = null;
                    Object s;
                    if ((s = c.result) == null) {
                        CompletionNode p = new CompletionNode
                            (d = new ThenCopy<U>(c, dst));
                        while ((s = c.result) == null) {
                            if (UNSAFE.compareAndSwapObject
                                (c, COMPLETIONS, p.next = c.completions, p))
                                break;
                        }
                    }
                    if (s != null && (d == null || d.compareAndSet(0, 1))) {
                        complete = true;
                        if (s instanceof AltResult) {
                            ex = ((AltResult)s).ex;  // no rewrap
                            u = null;
                        }
                        else {
                            @SuppressWarnings("unchecked") U us = (U) s;
                            u = us;
                        }
                    }
                }
                if (complete || ex != null)
                    dst.internalComplete(u, ex);
                if (c != null)
                    c.helpPostComplete();
            }
        }
        private static final long serialVersionUID = 5232453952276885070L;
    }

    // public methods

    /**
     * Creates a new incomplete CompletableFuture.
     */
    public CompletableFuture() {
    }

    /**
     * Asynchronously executes in the {@link
     * ForkJoinPool#commonPool()}, a task that completes the returned
     * CompletableFuture with the result of the given Supplier.
     *
     * @param supplier a function returning the value to be used
     * to complete the returned CompletableFuture
     * @return the CompletableFuture
     */
    public static <U> CompletableFuture<U> supplyAsync(Generator<U> supplier) {
        if (supplier == null) throw new NullPointerException();
        CompletableFuture<U> f = new CompletableFuture<U>();
        ForkJoinPool.commonPool().
            execute((ForkJoinTask<?>)new AsyncSupply<U>(supplier, f));
        return f;
    }

    /**
     * Asynchronously executes using the given executor, a task that
     * completes the returned CompletableFuture with the result of the
     * given Supplier.
     *
     * @param supplier a function returning the value to be used
     * to complete the returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the CompletableFuture
     */
    public static <U> CompletableFuture<U> supplyAsync(Generator<U> supplier,
                                                       Executor executor) {
        if (executor == null || supplier == null)
            throw new NullPointerException();
        CompletableFuture<U> f = new CompletableFuture<U>();
        executor.execute(new AsyncSupply<U>(supplier, f));
        return f;
    }

    /**
     * Asynchronously executes in the {@link
     * ForkJoinPool#commonPool()} a task that runs the given action,
     * and then completes the returned CompletableFuture.
     *
     * @param runnable the action to run before completing the
     * returned CompletableFuture
     * @return the CompletableFuture
     */
    public static CompletableFuture<Void> runAsync(Runnable runnable) {
        if (runnable == null) throw new NullPointerException();
        CompletableFuture<Void> f = new CompletableFuture<Void>();
        ForkJoinPool.commonPool().
            execute((ForkJoinTask<?>)new AsyncRun(runnable, f));
        return f;
    }

    /**
     * Asynchronously executes using the given executor, a task that
     * runs the given action, and then completes the returned
     * CompletableFuture.
     *
     * @param runnable the action to run before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the CompletableFuture
     */
    public static CompletableFuture<Void> runAsync(Runnable runnable,
                                                   Executor executor) {
        if (executor == null || runnable == null)
            throw new NullPointerException();
        CompletableFuture<Void> f = new CompletableFuture<Void>();
        executor.execute(new AsyncRun(runnable, f));
        return f;
    }

    /**
     * Returns {@code true} if completed in any fashion: normally,
     * exceptionally, or via cancellation.
     *
     * @return {@code true} if completed
     */
    public boolean isDone() {
        return result != null;
    }

    /**
     * Waits if necessary for the computation to complete, and then
     * retrieves its result.
     *
     * @return the computed result
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     */
    public T get() throws InterruptedException, ExecutionException {
        Object r; Throwable ex, cause;
        if ((r = result) == null && (r = waitingGet(true)) == null)
            throw new InterruptedException();
        if (!(r instanceof AltResult)) {
            @SuppressWarnings("unchecked") T tr = (T) r;
            return tr;
        }
        if ((ex = ((AltResult)r).ex) == null)
            return null;
        if (ex instanceof CancellationException)
            throw (CancellationException)ex;
        if ((ex instanceof CompletionException) &&
            (cause = ex.getCause()) != null)
            ex = cause;
        throw new ExecutionException(ex);
    }

    /**
     * Waits if necessary for at most the given time for completion,
     * and then retrieves its result, if available.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return the computed result
     * @throws CancellationException if the computation was cancelled
     * @throws ExecutionException if the computation threw an
     * exception
     * @throws InterruptedException if the current thread was interrupted
     * while waiting
     * @throws TimeoutException if the wait timed out
     */
    public T get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
        Object r; Throwable ex, cause;
        long nanos = unit.toNanos(timeout);
        if (Thread.interrupted())
            throw new InterruptedException();
        if ((r = result) == null)
            r = timedAwaitDone(nanos);
        if (!(r instanceof AltResult)) {
            @SuppressWarnings("unchecked") T tr = (T) r;
            return tr;
        }
        if ((ex = ((AltResult)r).ex) == null)
            return null;
        if (ex instanceof CancellationException)
            throw (CancellationException)ex;
        if ((ex instanceof CompletionException) &&
            (cause = ex.getCause()) != null)
            ex = cause;
        throw new ExecutionException(ex);
    }

    /**
     * Returns the result value when complete, or throws an
     * (unchecked) exception if completed exceptionally. To better
     * conform with the use of common functional forms, if a
     * computation involved in the completion of this
     * CompletableFuture threw an exception, this method throws an
     * (unchecked) {@link CompletionException} with the underlying
     * exception as its cause.
     *
     * @return the result value
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if a completion computation threw
     * an exception
     */
    public T join() {
        Object r; Throwable ex;
        if ((r = result) == null)
            r = waitingGet(false);
        if (!(r instanceof AltResult)) {
            @SuppressWarnings("unchecked") T tr = (T) r;
            return tr;
        }
        if ((ex = ((AltResult)r).ex) == null)
            return null;
        if (ex instanceof CancellationException)
            throw (CancellationException)ex;
        if (ex instanceof CompletionException)
            throw (CompletionException)ex;
        throw new CompletionException(ex);
    }

    /**
     * Returns the result value (or throws any encountered exception)
     * if completed, else returns the given valueIfAbsent.
     *
     * @param valueIfAbsent the value to return if not completed
     * @return the result value, if completed, else the given valueIfAbsent
     * @throws CancellationException if the computation was cancelled
     * @throws CompletionException if a completion computation threw
     * an exception
     */
    public T getNow(T valueIfAbsent) {
        Object r; Throwable ex;
        if ((r = result) == null)
            return valueIfAbsent;
        if (!(r instanceof AltResult)) {
            @SuppressWarnings("unchecked") T tr = (T) r;
            return tr;
        }
        if ((ex = ((AltResult)r).ex) == null)
            return null;
        if (ex instanceof CancellationException)
            throw (CancellationException)ex;
        if (ex instanceof CompletionException)
            throw (CompletionException)ex;
        throw new CompletionException(ex);
    }

    /**
     * If not already completed, sets the value returned by {@link
     * #get()} and related methods to the given value.
     *
     * @param value the result value
     * @return {@code true} if this invocation caused this CompletableFuture
     * to transition to a completed state, else {@code false}
     */
    public boolean complete(T value) {
        boolean triggered = result == null &&
            UNSAFE.compareAndSwapObject(this, RESULT, null,
                                        value == null ? NIL : value);
        postComplete();
        return triggered;
    }

    /**
     * If not already completed, causes invocations of {@link #get()}
     * and related methods to throw the given exception.
     *
     * @param ex the exception
     * @return {@code true} if this invocation caused this CompletableFuture
     * to transition to a completed state, else {@code false}
     */
    public boolean completeExceptionally(Throwable ex) {
        if (ex == null) throw new NullPointerException();
        boolean triggered = result == null &&
            UNSAFE.compareAndSwapObject(this, RESULT, null, new AltResult(ex));
        postComplete();
        return triggered;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the result of the given function of this CompletableFuture.
     * If this CompletableFuture completes exceptionally,
     * then the returned CompletableFuture also does so,
     * with a CompletionException holding this exception as
     * its cause.
     *
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> thenApply(Fun<? super T,? extends U> fn) {
        return doThenApply(fn, null);
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the {@link ForkJoinPool#commonPool()} with the
     * result of the given function of this CompletableFuture.  If
     * this CompletableFuture completes exceptionally, then the
     * returned CompletableFuture also does so, with a
     * CompletionException holding this exception as its cause.
     *
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> thenApplyAsync(Fun<? super T,? extends U> fn) {
        return doThenApply(fn, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the given executor with the result of the given
     * function of this CompletableFuture.  If this CompletableFuture
     * completes exceptionally, then the returned CompletableFuture
     * also does so, with a CompletionException holding this exception as
     * its cause.
     *
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> thenApplyAsync(Fun<? super T,? extends U> fn,
                                                   Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenApply(fn, executor);
    }

    private <U> CompletableFuture<U> doThenApply(Fun<? super T,? extends U> fn,
                                                 Executor e) {
        if (fn == null) throw new NullPointerException();
        CompletableFuture<U> dst = new CompletableFuture<U>();
        ApplyCompletion<T,U> d = null;
        Object r;
        if ((r = result) == null) {
            CompletionNode p = new CompletionNode
                (d = new ApplyCompletion<T,U>(this, fn, dst, e));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject
                    (this, COMPLETIONS, p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            U u = null;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncApply<T,U>(t, fn, dst));
                    else
                        u = fn.apply(t);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(u, ex);
        }
        helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed after
     * performing the given action with this CompletableFuture's
     * result when it completes.  If this CompletableFuture
     * completes exceptionally, then the returned CompletableFuture
     * also does so, with a CompletionException holding this exception as
     * its cause.
     *
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenAccept(Action<? super T> block) {
        return doThenAccept(block, null);
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the {@link ForkJoinPool#commonPool()} with this
     * CompletableFuture's result when it completes.  If this
     * CompletableFuture completes exceptionally, then the returned
     * CompletableFuture also does so, with a CompletionException holding
     * this exception as its cause.
     *
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenAcceptAsync(Action<? super T> block) {
        return doThenAccept(block, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the given executor with this
     * CompletableFuture's result when it completes.  If this
     * CompletableFuture completes exceptionally, then the returned
     * CompletableFuture also does so, with a CompletionException holding
     * this exception as its cause.
     *
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenAcceptAsync(Action<? super T> block,
                                                   Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenAccept(block, executor);
    }

    private CompletableFuture<Void> doThenAccept(Action<? super T> fn,
                                                 Executor e) {
        if (fn == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        AcceptCompletion<T> d = null;
        Object r;
        if ((r = result) == null) {
            CompletionNode p = new CompletionNode
                (d = new AcceptCompletion<T>(this, fn, dst, e));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject
                    (this, COMPLETIONS, p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncAccept<T>(t, fn, dst));
                    else
                        fn.accept(t);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed after
     * performing the given action when this CompletableFuture
     * completes.  If this CompletableFuture completes exceptionally,
     * then the returned CompletableFuture also does so, with a
     * CompletionException holding this exception as its cause.
     *
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenRun(Runnable action) {
        return doThenRun(action, null);
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the {@link ForkJoinPool#commonPool()} after
     * performing the given action when this CompletableFuture
     * completes.  If this CompletableFuture completes exceptionally,
     * then the returned CompletableFuture also does so, with a
     * CompletionException holding this exception as its cause.
     *
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenRunAsync(Runnable action) {
        return doThenRun(action, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the given executor after performing the given
     * action when this CompletableFuture completes.  If this
     * CompletableFuture completes exceptionally, then the returned
     * CompletableFuture also does so, with a CompletionException holding
     * this exception as its cause.
     *
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> thenRunAsync(Runnable action,
                                                Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenRun(action, executor);
    }

    private CompletableFuture<Void> doThenRun(Runnable action,
                                              Executor e) {
        if (action == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        RunCompletion<T> d = null;
        Object r;
        if ((r = result) == null) {
            CompletionNode p = new CompletionNode
                (d = new RunCompletion<T>(this, action, dst, e));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject
                    (this, COMPLETIONS, p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            Throwable ex;
            if (r instanceof AltResult)
                ex = ((AltResult)r).ex;
            else
                ex = null;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncRun(action, dst));
                    else
                        action.run();
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the result of the given function of this and the other given
     * CompletableFuture's results when both complete.  If this or
     * the other CompletableFuture complete exceptionally, then the
     * returned CompletableFuture also does so, with a
     * CompletionException holding the exception as its cause.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U,V> CompletableFuture<V> thenCombine(CompletableFuture<? extends U> other,
                                                  BiFun<? super T,? super U,? extends V> fn) {
        return doThenBiApply(other, fn, null);
    }

    /**
     * Creates and returns a CompletableFuture that is asynchronously
     * completed using the {@link ForkJoinPool#commonPool()} with
     * the result of the given function of this and the other given
     * CompletableFuture's results when both complete.  If this or
     * the other CompletableFuture complete exceptionally, then the
     * returned CompletableFuture also does so, with a
     * CompletionException holding the exception as its cause.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U,V> CompletableFuture<V> thenCombineAsync(CompletableFuture<? extends U> other,
                                                       BiFun<? super T,? super U,? extends V> fn) {
        return doThenBiApply(other, fn, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is
     * asynchronously completed using the given executor with the
     * result of the given function of this and the other given
     * CompletableFuture's results when both complete.  If this or
     * the other CompletableFuture complete exceptionally, then the
     * returned CompletableFuture also does so, with a
     * CompletionException holding the exception as its cause.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public <U,V> CompletableFuture<V> thenCombineAsync(CompletableFuture<? extends U> other,
                                                       BiFun<? super T,? super U,? extends V> fn,
                                                       Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenBiApply(other, fn, executor);
    }

    private <U,V> CompletableFuture<V> doThenBiApply(CompletableFuture<? extends U> other,
                                                     BiFun<? super T,? super U,? extends V> fn,
                                                     Executor e) {
        if (other == null || fn == null) throw new NullPointerException();
        CompletableFuture<V> dst = new CompletableFuture<V>();
        BiApplyCompletion<T,U,V> d = null;
        Object r, s = null;
        if ((r = result) == null || (s = other.result) == null) {
            d = new BiApplyCompletion<T,U,V>(this, other, fn, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r == null && (r = result) == null) ||
                   (s == null && (s = other.result) == null)) {
                if (q != null) {
                    if (s != null ||
                        UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (r != null ||
                         UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p)) {
                    if (s != null)
                        break;
                    q = new CompletionNode(d);
                }
            }
        }
        if (r != null && s != null && (d == null || d.compareAndSet(0, 1))) {
            T t; U u; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            if (ex != null)
                u = null;
            else if (s instanceof AltResult) {
                ex = ((AltResult)s).ex;
                u = null;
            }
            else {
                @SuppressWarnings("unchecked") U us = (U) s;
                u = us;
            }
            V v = null;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncBiApply<T,U,V>(t, u, fn, dst));
                    else
                        v = fn.apply(t, u);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(v, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the results of this and the other given CompletableFuture if
     * both complete.  If this and/or the other CompletableFuture
     * complete exceptionally, then the returned CompletableFuture
     * also does so, with a CompletionException holding one of these
     * exceptions as its cause.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<Void> thenAcceptBoth(CompletableFuture<? extends U> other,
                                                      BiAction<? super T, ? super U> block) {
        return doThenBiAccept(other, block, null);
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the {@link ForkJoinPool#commonPool()} with
     * the results of this and the other given CompletableFuture when
     * both complete.  If this and/or the other CompletableFuture
     * complete exceptionally, then the returned CompletableFuture
     * also does so, with a CompletionException holding one of these
     * exceptions as its cause.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletableFuture<? extends U> other,
                                                           BiAction<? super T, ? super U> block) {
        return doThenBiAccept(other, block, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the given executor with the results of
     * this and the other given CompletableFuture when both complete.
     * If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture also does
     * so, with a CompletionException holding one of these exceptions as
     * its cause.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<Void> thenAcceptBothAsync(CompletableFuture<? extends U> other,
                                                           BiAction<? super T, ? super U> block,
                                                           Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenBiAccept(other, block, executor);
    }

    private <U> CompletableFuture<Void> doThenBiAccept(CompletableFuture<? extends U> other,
                                                       BiAction<? super T,? super U> fn,
                                                       Executor e) {
        if (other == null || fn == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        BiAcceptCompletion<T,U> d = null;
        Object r, s = null;
        if ((r = result) == null || (s = other.result) == null) {
            d = new BiAcceptCompletion<T,U>(this, other, fn, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r == null && (r = result) == null) ||
                   (s == null && (s = other.result) == null)) {
                if (q != null) {
                    if (s != null ||
                        UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (r != null ||
                         UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p)) {
                    if (s != null)
                        break;
                    q = new CompletionNode(d);
                }
            }
        }
        if (r != null && s != null && (d == null || d.compareAndSet(0, 1))) {
            T t; U u; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            if (ex != null)
                u = null;
            else if (s instanceof AltResult) {
                ex = ((AltResult)s).ex;
                u = null;
            }
            else {
                @SuppressWarnings("unchecked") U us = (U) s;
                u = us;
            }
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncBiAccept<T,U>(t, u, fn, dst));
                    else
                        fn.accept(t, u);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * when this and the other given CompletableFuture both
     * complete.  If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture also does
     * so, with a CompletionException holding one of these exceptions as
     * its cause.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterBoth(CompletableFuture<?> other,
                                                Runnable action) {
        return doThenBiRun(other, action, null);
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the {@link ForkJoinPool#commonPool()}
     * when this and the other given CompletableFuture both
     * complete.  If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture also does
     * so, with a CompletionException holding one of these exceptions as
     * its cause.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterBothAsync(CompletableFuture<?> other,
                                                     Runnable action) {
        return doThenBiRun(other, action, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the given executor
     * when this and the other given CompletableFuture both
     * complete.  If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture also does
     * so, with a CompletionException holding one of these exceptions as
     * its cause.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterBothAsync(CompletableFuture<?> other,
                                                     Runnable action,
                                                     Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doThenBiRun(other, action, executor);
    }

    private CompletableFuture<Void> doThenBiRun(CompletableFuture<?> other,
                                                Runnable action,
                                                Executor e) {
        if (other == null || action == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        BiRunCompletion<T> d = null;
        Object r, s = null;
        if ((r = result) == null || (s = other.result) == null) {
            d = new BiRunCompletion<T>(this, other, action, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r == null && (r = result) == null) ||
                   (s == null && (s = other.result) == null)) {
                if (q != null) {
                    if (s != null ||
                        UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (r != null ||
                         UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p)) {
                    if (s != null)
                        break;
                    q = new CompletionNode(d);
                }
            }
        }
        if (r != null && s != null && (d == null || d.compareAndSet(0, 1))) {
            Throwable ex;
            if (r instanceof AltResult)
                ex = ((AltResult)r).ex;
            else
                ex = null;
            if (ex == null && (s instanceof AltResult))
                ex = ((AltResult)s).ex;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncRun(action, dst));
                    else
                        action.run();
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the result of the given function of either this or the other
     * given CompletableFuture's results when either complete.  If
     * this and/or the other CompletableFuture complete exceptionally,
     * then the returned CompletableFuture may also do so, with a
     * CompletionException holding one of these exceptions as its cause.
     * No guarantees are made about which result or exception is used
     * in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> applyToEither(CompletableFuture<? extends T> other,
                                                  Fun<? super T, U> fn) {
        return doOrApply(other, fn, null);
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the {@link ForkJoinPool#commonPool()} with
     * the result of the given function of either this or the other
     * given CompletableFuture's results when either complete.  If
     * this and/or the other CompletableFuture complete exceptionally,
     * then the returned CompletableFuture may also do so, with a
     * CompletionException holding one of these exceptions as its cause.
     * No guarantees are made about which result or exception is used
     * in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> applyToEitherAsync(CompletableFuture<? extends T> other,
                                                       Fun<? super T, U> fn) {
        return doOrApply(other, fn, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the given executor with the result of the
     * given function of either this or the other given
     * CompletableFuture's results when either complete.  If this
     * and/or the other CompletableFuture complete exceptionally, then
     * the returned CompletableFuture may also do so, with a
     * CompletionException holding one of these exceptions as its cause.
     * No guarantees are made about which result or exception is used
     * in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param fn the function to use to compute the value of
     * the returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> applyToEitherAsync(CompletableFuture<? extends T> other,
                                                       Fun<? super T, U> fn,
                                                       Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doOrApply(other, fn, executor);
    }

    private <U> CompletableFuture<U> doOrApply(CompletableFuture<? extends T> other,
                                               Fun<? super T, U> fn,
                                               Executor e) {
        if (other == null || fn == null) throw new NullPointerException();
        CompletableFuture<U> dst = new CompletableFuture<U>();
        OrApplyCompletion<T,U> d = null;
        Object r;
        if ((r = result) == null && (r = other.result) == null) {
            d = new OrApplyCompletion<T,U>(this, other, fn, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r = result) == null && (r = other.result) == null) {
                if (q != null) {
                    if (UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p))
                    q = new CompletionNode(d);
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            U u = null;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncApply<T,U>(t, fn, dst));
                    else
                        u = fn.apply(t);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(u, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed after
     * performing the given action with the result of either this or the
     * other given CompletableFuture's result, when either complete.
     * If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture may also do
     * so, with a CompletionException holding one of these exceptions as
     * its cause.  No guarantees are made about which exception is
     * used in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> acceptEither(CompletableFuture<? extends T> other,
                                                Action<? super T> block) {
        return doOrAccept(other, block, null);
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the {@link ForkJoinPool#commonPool()},
     * performing the given action with the result of either this or
     * the other given CompletableFuture's result, when either
     * complete.  If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture may also do
     * so, with a CompletionException holding one of these exceptions as
     * its cause.  No guarantees are made about which exception is
     * used in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> acceptEitherAsync(CompletableFuture<? extends T> other,
                                                     Action<? super T> block) {
        return doOrAccept(other, block, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the given executor,
     * performing the given action with the result of either this or
     * the other given CompletableFuture's result, when either
     * complete.  If this and/or the other CompletableFuture complete
     * exceptionally, then the returned CompletableFuture may also do
     * so, with a CompletionException holding one of these exceptions as
     * its cause.  No guarantees are made about which exception is
     * used in the returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param block the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> acceptEitherAsync(CompletableFuture<? extends T> other,
                                                     Action<? super T> block,
                                                     Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doOrAccept(other, block, executor);
    }

    private CompletableFuture<Void> doOrAccept(CompletableFuture<? extends T> other,
                                               Action<? super T> fn,
                                               Executor e) {
        if (other == null || fn == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        OrAcceptCompletion<T> d = null;
        Object r;
        if ((r = result) == null && (r = other.result) == null) {
            d = new OrAcceptCompletion<T>(this, other, fn, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r = result) == null && (r = other.result) == null) {
                if (q != null) {
                    if (UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p))
                    q = new CompletionNode(d);
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncAccept<T>(t, fn, dst));
                    else
                        fn.accept(t);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * after this or the other given CompletableFuture complete.  If
     * this and/or the other CompletableFuture complete exceptionally,
     * then the returned CompletableFuture may also do so, with a
     * CompletionException holding one of these exceptions as its cause.
     * No guarantees are made about which exception is used in the
     * returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterEither(CompletableFuture<?> other,
                                                  Runnable action) {
        return doOrRun(other, action, null);
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the {@link ForkJoinPool#commonPool()}
     * after this or the other given CompletableFuture complete.  If
     * this and/or the other CompletableFuture complete exceptionally,
     * then the returned CompletableFuture may also do so, with a
     * CompletionException holding one of these exceptions as its cause.
     * No guarantees are made about which exception is used in the
     * returned CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterEitherAsync(CompletableFuture<?> other,
                                                       Runnable action) {
        return doOrRun(other, action, ForkJoinPool.commonPool());
    }

    /**
     * Creates and returns a CompletableFuture that is completed
     * asynchronously using the given executor after this or the other
     * given CompletableFuture complete.  If this and/or the other
     * CompletableFuture complete exceptionally, then the returned
     * CompletableFuture may also do so, with a CompletionException
     * holding one of these exceptions as its cause.  No guarantees are
     * made about which exception is used in the returned
     * CompletableFuture.
     *
     * @param other the other CompletableFuture
     * @param action the action to perform before completing the
     * returned CompletableFuture
     * @param executor the executor to use for asynchronous execution
     * @return the new CompletableFuture
     */
    public CompletableFuture<Void> runAfterEitherAsync(CompletableFuture<?> other,
                                                       Runnable action,
                                                       Executor executor) {
        if (executor == null) throw new NullPointerException();
        return doOrRun(other, action, executor);
    }

    private CompletableFuture<Void> doOrRun(CompletableFuture<?> other,
                                            Runnable action,
                                            Executor e) {
        if (other == null || action == null) throw new NullPointerException();
        CompletableFuture<Void> dst = new CompletableFuture<Void>();
        OrRunCompletion<T> d = null;
        Object r;
        if ((r = result) == null && (r = other.result) == null) {
            d = new OrRunCompletion<T>(this, other, action, dst, e);
            CompletionNode q = null, p = new CompletionNode(d);
            while ((r = result) == null && (r = other.result) == null) {
                if (q != null) {
                    if (UNSAFE.compareAndSwapObject
                        (other, COMPLETIONS, q.next = other.completions, q))
                        break;
                }
                else if (UNSAFE.compareAndSwapObject
                         (this, COMPLETIONS, p.next = completions, p))
                    q = new CompletionNode(d);
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            Throwable ex;
            if (r instanceof AltResult)
                ex = ((AltResult)r).ex;
            else
                ex = null;
            if (ex == null) {
                try {
                    if (e != null)
                        e.execute(new AsyncRun(action, dst));
                    else
                        action.run();
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (e == null || ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        other.helpPostComplete();
        return dst;
    }

    /**
     * Returns a CompletableFuture (or an equivalent one) produced by
     * the given function of the result of this CompletableFuture when
     * completed.  If this CompletableFuture completes exceptionally,
     * then the returned CompletableFuture also does so, with a
     * CompletionException holding this exception as its cause.
     *
     * @param fn the function returning a new CompletableFuture
     * @return the CompletableFuture, that {@code isDone()} upon
     * return if completed by the given function, or an exception
     * occurs
     */
    public <U> CompletableFuture<U> thenCompose(Fun<? super T,
                                                CompletableFuture<U>> fn) {
        if (fn == null) throw new NullPointerException();
        CompletableFuture<U> dst = null;
        ComposeCompletion<T,U> d = null;
        Object r;
        if ((r = result) == null) {
            dst = new CompletableFuture<U>();
            CompletionNode p = new CompletionNode
                (d = new ComposeCompletion<T,U>(this, fn, dst));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject
                    (this, COMPLETIONS, p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            if (ex == null) {
                try {
                    dst = fn.apply(t);
                } catch (Throwable rex) {
                    ex = rex;
                }
            }
            if (dst == null) {
                dst = new CompletableFuture<U>();
                if (ex == null)
                    ex = new NullPointerException();
            }
            if (ex != null)
                dst.internalComplete(null, ex);
        }
        helpPostComplete();
        dst.helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the result of the given function of the exception triggering
     * this CompletableFuture's completion when it completes
     * exceptionally; Otherwise, if this CompletableFuture completes
     * normally, then the returned CompletableFuture also completes
     * normally with the same value.
     *
     * @param fn the function to use to compute the value of the
     * returned CompletableFuture if this CompletableFuture completed
     * exceptionally
     * @return the new CompletableFuture
     */
    public CompletableFuture<T> exceptionally(Fun<Throwable, ? extends T> fn) {
        if (fn == null) throw new NullPointerException();
        CompletableFuture<T> dst = new CompletableFuture<T>();
        ExceptionCompletion<T> d = null;
        Object r;
        if ((r = result) == null) {
            CompletionNode p =
                new CompletionNode(d = new ExceptionCompletion<T>(this, fn, dst));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject(this, COMPLETIONS,
                                                p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t = null; Throwable ex, dx = null;
            if (r instanceof AltResult) {
                if ((ex = ((AltResult)r).ex) != null)  {
                    try {
                        t = fn.apply(ex);
                    } catch (Throwable rex) {
                        dx = rex;
                    }
                }
            }
            else {
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            dst.internalComplete(t, dx);
        }
        helpPostComplete();
        return dst;
    }

    /**
     * Creates and returns a CompletableFuture that is completed with
     * the result of the given function of the result and exception of
     * this CompletableFuture's completion when it completes.  The
     * given function is invoked with the result (or {@code null} if
     * none) and the exception (or {@code null} if none) of this
     * CompletableFuture when complete.
     *
     * @param fn the function to use to compute the value of the
     * returned CompletableFuture
     * @return the new CompletableFuture
     */
    public <U> CompletableFuture<U> handle(BiFun<? super T, Throwable, ? extends U> fn) {
        if (fn == null) throw new NullPointerException();
        CompletableFuture<U> dst = new CompletableFuture<U>();
        HandleCompletion<T,U> d = null;
        Object r;
        if ((r = result) == null) {
            CompletionNode p =
                new CompletionNode(d = new HandleCompletion<T,U>(this, fn, dst));
            while ((r = result) == null) {
                if (UNSAFE.compareAndSwapObject(this, COMPLETIONS,
                                                p.next = completions, p))
                    break;
            }
        }
        if (r != null && (d == null || d.compareAndSet(0, 1))) {
            T t; Throwable ex;
            if (r instanceof AltResult) {
                ex = ((AltResult)r).ex;
                t = null;
            }
            else {
                ex = null;
                @SuppressWarnings("unchecked") T tr = (T) r;
                t = tr;
            }
            U u; Throwable dx;
            try {
                u = fn.apply(t, ex);
                dx = null;
            } catch (Throwable rex) {
                dx = rex;
                u = null;
            }
            dst.internalComplete(u, dx);
        }
        helpPostComplete();
        return dst;
    }

    /**
     * Attempts to complete this CompletableFuture with
     * a {@link CancellationException}.
     *
     * @param mayInterruptIfRunning this value has no effect in this
     * implementation because interrupts are not used to control
     * processing.
     *
     * @return {@code true} if this task is now cancelled
     */
    public boolean cancel(boolean mayInterruptIfRunning) {
        Object r;
        while ((r = result) == null) {
            r = new AltResult(new CancellationException());
            if (UNSAFE.compareAndSwapObject(this, RESULT, null, r)) {
                postComplete();
                return true;
            }
        }
        return ((r instanceof AltResult) &&
                (((AltResult)r).ex instanceof CancellationException));
    }

    /**
     * Returns {@code true} if this CompletableFuture was cancelled
     * before it completed normally.
     *
     * @return {@code true} if this CompletableFuture was cancelled
     * before it completed normally
     */
    public boolean isCancelled() {
        Object r;
        return ((r = result) instanceof AltResult) &&
            (((AltResult)r).ex instanceof CancellationException);
    }

    /**
     * Forcibly sets or resets the value subsequently returned by
     * method {@link #get()} and related methods, whether or not
     * already completed. This method is designed for use only in
     * error recovery actions, and even in such situations may result
     * in ongoing dependent completions using established versus
     * overwritten outcomes.
     *
     * @param value the completion value
     */
    public void obtrudeValue(T value) {
        result = (value == null) ? NIL : value;
        postComplete();
    }

    /**
     * Forcibly causes subsequent invocations of method {@link #get()}
     * and related methods to throw the given exception, whether or
     * not already completed. This method is designed for use only in
     * recovery actions, and even in such situations may result in
     * ongoing dependent completions using established versus
     * overwritten outcomes.
     *
     * @param ex the exception
     */
    public void obtrudeException(Throwable ex) {
        if (ex == null) throw new NullPointerException();
        result = new AltResult(ex);
        postComplete();
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe UNSAFE;
    private static final long RESULT;
    private static final long WAITERS;
    private static final long COMPLETIONS;
    static {
        try {
            UNSAFE = getUnsafe();
            Class<?> k = CompletableFuture.class;
            RESULT = UNSAFE.objectFieldOffset
                (k.getDeclaredField("result"));
            WAITERS = UNSAFE.objectFieldOffset
                (k.getDeclaredField("waiters"));
            COMPLETIONS = UNSAFE.objectFieldOffset
                (k.getDeclaredField("completions"));
        } catch (Exception e) {
            throw new Error(e);
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
        } catch (SecurityException tryReflectionInstead) {}
        try {
            return java.security.AccessController.doPrivileged
            (new java.security.PrivilegedExceptionAction<sun.misc.Unsafe>() {
                public sun.misc.Unsafe run() throws Exception {
                    Class<sun.misc.Unsafe> k = sun.misc.Unsafe.class;
                    for (java.lang.reflect.Field f : k.getDeclaredFields()) {
                        f.setAccessible(true);
                        Object x = f.get(null);
                        if (k.isInstance(x))
                            return k.cast(x);
                    }
                    throw new NoSuchFieldError("the Unsafe");
                }});
        } catch (java.security.PrivilegedActionException e) {
            throw new RuntimeException("Could not initialize intrinsics",
                                       e.getCause());
        }
    }
}
