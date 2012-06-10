/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166y;

/**
 * A resultless {@link ForkJoinTask} with a completion action
 * performed when triggered and there are no remaining pending
 * actions. Uses of CountedCompleter are similar to those of other
 * completion based components (such as {@link
 * java.nio.channels.CompletionHandler}) except that multiple
 * <em>pending</em> completions may be necessary to trigger the {@link
 * #onCompletion} action, not just one. Unless initialized otherwise,
 * the {@link #getPendingCount pending count} starts at zero, but may
 * be (atomically) changed using methods {@link #setPendingCount},
 * {@link #addToPendingCount}, and {@link
 * #compareAndSetPendingCount}. Upon invocation of {@link
 * #tryComplete}, if the pending action count is nonzero, it is
 * decremented; otherwise, the completion action is performed, and if
 * this completer itself has a completer, the process is continued
 * with its completer.  As is the case with related synchronization
 * components such as {@link Phaser} and {@link
 * java.util.concurrent.Semaphore} these methods affect only internal
 * counts; they do not establish any further internal bookkeeping. In
 * particular, the identities of pending tasks are not maintained. As
 * illustrated below, you can create subclasses that do record some or
 * all pended tasks or their results when needed.
 *
 * <p>A concrete CountedCompleter class must define method {@link
 * #compute}, that should, in almost all use cases, invoke {@code
 * tryComplete()} once before returning. The class may also optionally
 * override method {@link #onCompletion} to perform an action upon
 * normal completion, and method {@link #onExceptionalCompletion} to
 * perform an action upon any exception.
 *
 * <p>A CountedCompleter that does not itself have a completer (i.e.,
 * one for which {@link #getCompleter} returns {@code null}) can be
 * used as a regular ForkJoinTask with this added functionality.
 * However, any completer that in turn has another completer serves
 * only as an internal helper for other computations, so its own task
 * status (as reported in methods such as {@link ForkJoinTask#isDone})
 * is arbitrary; this status changes only upon explicit invocations of
 * {@link #complete}, {@link ForkJoinTask#cancel}, {@link
 * ForkJoinTask#completeExceptionally} or upon exceptional completion
 * of method {@code compute}. Upon any exceptional completion, the
 * exception may be relayed to a task's completer (and its completer,
 * and so on), if one exists and it has not otherwise already
 * completed.
 *
 * <p><b>Sample Usages.</b>
 *
 * <p><b>Parallel recursive decomposition.</b> CountedCompleters may
 * be arranged in trees similar to those often used with {@link
 * RecursiveAction}s, although the constructions involved in setting
 * them up typically vary. Even though they entail a bit more
 * bookkeeping, CountedCompleters may be better choices when applying
 * a possibly time-consuming operation (that cannot be further
 * subdivided) to each element of an array or collection; especially
 * when the operation takes a significantly different amount of time
 * to complete for some elements than others, either because of
 * intrinsic variation (for example IO) or auxiliary effects such as
 * garbage collection.  Because CountedCompleters provide their own
 * continuations, other threads need not block waiting to perform
 * them.
 *
 * <p> For example, here is an initial version of a class that uses
 * divide-by-two recursive decomposition to divide work into single
 * pieces (leaf tasks). Even when work is split into individual calls,
 * tree-based techniques are usually preferable to directly forking
 * leaf tasks, because they reduce inter-thread communication and
 * improve load balancing. In the recursive case, the second of each
 * pair of subtasks to finish triggers completion of its parent
 * (because no result combination is performed, the default no-op
 * implementation of method {@code onCompletion} is not overridden). A
 * static utility method sets up the base task and invokes it:
 *
 * <pre> {@code
 * class MyOperation<E> { void apply(E e) { ... }  }
 *
 * class ForEach<E> extends CountedCompleter {
 *
 *     public static <E> void forEach(ForkJoinPool pool, E[] array, MyOperation<E> op) {
 *         pool.invoke(new ForEach<E>(null, array, op, 0, array.length));
 *     }
 *
 *     final E[] array; final MyOperation<E> op; final int lo, hi;
 *     ForEach(CountedCompleter p, E[] array, MyOperation<E> op, int lo, int hi) {
 *         super(p);
 *         this.array = array; this.op = op; this.lo = lo; this.hi = hi;
 *     }
 *
 *     public void compute() { // version 1
 *         if (hi - lo >= 2) {
 *             int mid = (lo + hi) >>> 1;
 *             setPendingCount(2); // must set pending count before fork
 *             new ForEach(this, array, op, mid, hi).fork(); // right child
 *             new ForEach(this, array, op, lo, mid).fork(); // left child
 *         }
 *         else if (hi > lo)
 *             op.apply(array[lo]);
 *         tryComplete();
 *     }
 * } }</pre>
 *
 * This design can be improved by noticing that in the recursive case,
 * the task has nothing to do after forking its right task, so can
 * directly invoke its left task before returning. (This is an analog
 * of tail recursion removal.)  Also, because the task returns upon
 * executing its left task (rather than falling through to invoke
 * tryComplete) the pending count is set to one:
 *
 * <pre> {@code
 * class ForEach<E> ...
 *     public void compute() { // version 2
 *         if (hi - lo >= 2) {
 *             int mid = (lo + hi) >>> 1;
 *             setPendingCount(1); // only one pending
 *             new ForEach(this, array, op, mid, hi).fork(); // right child
 *             new ForEach(this, array, op, lo, mid).compute(); // direct invoke
 *         }
 *         else {
 *             if (hi > lo)
 *                 op.apply(array[lo]);
 *             tryComplete();
 *         }
 *     }
 * }</pre>
 *
 * As a further improvement, notice that the left task need not even
 * exist.  Instead of creating a new one, we can iterate using the
 * original task, and add a pending count for each fork:
 *
 * <pre> {@code
 * class ForEach<E> ...
 *     public void compute() { // version 3
 *         int l = lo,  h = hi;
 *         while (h - l >= 2) {
 *             int mid = (l + h) >>> 1;
 *             addToPendingCount(1);
 *             new ForEach(this, array, op, mid, h).fork(); // right child
 *             h = mid;
 *         }
 *         if (h > l)
 *             op.apply(array[l]);
 *         tryComplete();
 *     }
 * }</pre>
 *
 * Additional improvements of such classes might entail precomputing
 * pending counts so that they can be established in constructors,
 * specializing classes for leaf steps, subdividing by say, four,
 * instead of two per iteration, and using an adaptive threshold
 * instead of always subdividing down to single elements.
 *
 * <p><b>Recording subtasks.</b> CountedCompleter tasks that combine
 * results of multiple subtasks usually need to access these results
 * in method {@link #onCompletion}. As illustrated in the following
 * class (that performs a simplified form of map-reduce where mappings
 * and reductions are all of type {@code E}), one way to do this in
 * divide and conquer designs is to have each subtask record its
 * sibling, so that it can be accessed in method {@code onCompletion}.
 * For clarity, this class uses explicit left and right subtasks, but
 * variants of other streamlinings seen in the above example may also
 * apply.
 *
 * <pre> {@code
 * class MyMapper<E> { E apply(E v) {  ...  } }
 * class MyReducer<E> { E apply(E x, E y) {  ...  } }
 * class MapReducer<E> extends CountedCompleter {
 *     final E[] array; final MyMapper<E> mapper;
 *     final MyReducer<E> reducer; final int lo, hi;
 *     MapReducer sibling;
 *     E result;
 *     MapReducer(CountedCompleter p, E[] array, MyMapper<E> mapper,
 *                MyReducer<E> reducer, int lo, int hi) {
 *         super(p);
 *         this.array = array; this.mapper = mapper;
 *         this.reducer = reducer; this.lo = lo; this.hi = hi;
 *     }
 *     public void compute() {
 *         if (hi - lo >= 2) {
 *             int mid = (lo + hi) >>> 1;
 *             MapReducer<E> left = new MapReducer(this, array, mapper, reducer, lo, mid);
 *             MapReducer<E> right = new MapReducer(this, array, mapper, reducer, mid, hi);
 *             left.sibling = right;
 *             right.sibling = left;
 *             setPendingCount(1); // only right is pending
 *             right.fork();
 *             left.compute();     // directly execute left
 *         }
 *         else {
 *             if (hi > lo)
 *                 result = mapper.apply(array[lo]);
 *             tryComplete();
 *         }
 *     }
 *     public void onCompletion(CountedCompleter caller) {
 *         if (caller != this) {
 *            MapReducer<E> child = (MapReducer<E>)caller;
 *            MapReducer<E> sib = child.sibling;
 *            if (sib == null || sib.result == null)
 *                result = child.result;
 *            else
 *                result = reducer.apply(child.result, sib.result);
 *         }
 *     }
 *
 *     public static <E> E mapReduce(ForkJoinPool pool, E[] array,
 *                                   MyMapper<E> mapper, MyReducer<E> reducer) {
 *         MapReducer<E> mr = new MapReducer<E>(null, array, mapper,
 *                                              reducer, 0, array.length);
 *         pool.invoke(mr);
 *         return mr.result;
 *     }
 * } }</pre>
 *
 * <p><b>Triggers.</b> Some CountedCompleters are themselves never
 * forked, but instead serve as bits of plumbing in other designs;
 * including those in which the completion of one of more async tasks
 * triggers another async task. For example:
 *
 * <pre> {@code
 * class HeaderBuilder extends CountedCompleter { ... }
 * class BodyBuilder extends CountedCompleter { ... }
 * class PacketSender extends CountedCompleter {
 *     PacketSender(...) { super(null, 1); ... } // trigger on second completion
 *     public void compute() { } // never called
 *     public void onCompletion(CountedCompleter caller) { sendPacket(); }
 * }
 * // sample use:
 * PacketSender p = new PacketSender();
 * new HeaderBuilder(p, ...).fork();
 * new BodyBuilder(p, ...).fork();
 * }</pre>
 *
 * @since 1.8
 * @author Doug Lea
 */
public abstract class CountedCompleter extends ForkJoinTask<Void> {
    private static final long serialVersionUID = 5232453752276485070L;

    /** This task's completer, or null if none */
    final CountedCompleter completer;
    /** The number of pending tasks until completion */
    volatile int pending;

    /**
     * Creates a new CountedCompleter with the given completer
     * and initial pending count.
     *
     * @param completer this tasks completer, or {@code null} if none
     * @param initialPendingCount the initial pending count
     */
    protected CountedCompleter(CountedCompleter completer,
                               int initialPendingCount) {
        this.completer = completer;
        this.pending = initialPendingCount;
    }

    /**
     * Creates a new CountedCompleter with the given completer
     * and an initial pending count of zero.
     *
     * @param completer this tasks completer, or {@code null} if none
     */
    protected CountedCompleter(CountedCompleter completer) {
        this.completer = completer;
    }

    /**
     * Creates a new CountedCompleter with no completer
     * and an initial pending count of zero.
     */
    protected CountedCompleter() {
        this.completer = null;
    }

    /**
     * The main computation performed by this task.
     */
    public abstract void compute();

    /**
     * Performs an action when method {@link #tryComplete} is invoked
     * and there are no pending counts, or when the unconditional
     * method {@link #complete} is invoked.  By default, this method
     * does nothing.
     *
     * @param caller the task invoking this method (which may
     * be this task itself).
     */
    public void onCompletion(CountedCompleter caller) {
    }

    /**
     * Performs an action when method {@link #completeExceptionally}
     * is invoked or method {@link #compute} throws an exception, and
     * this task has not otherwise already completed normally. On
     * entry to this method, this task {@link
     * ForkJoinTask#isCompletedAbnormally}.  The return value of this
     * method controls further propagation: If {@code true} and this
     * task has a completer, then this completer is also completed
     * exceptionally.  The default implementation of this method does
     * nothing except return {@code true}.
     *
     * @param ex the exception
     * @param caller the task invoking this method (which may
     * be this task itself).
     * @return true if this exception should be propagated to this
     * tasks completer, if one exists.
     */
    public boolean onExceptionalCompletion(Throwable ex, CountedCompleter caller) {
        return true;
    }

    /**
     * Returns the completer established in this task's constructor,
     * or {@code null} if none.
     *
     * @return the completer
     */
    public final CountedCompleter getCompleter() {
        return completer;
    }

    /**
     * Returns the current pending count.
     *
     * @return the current pending count
     */
    public final int getPendingCount() {
        return pending;
    }

    /**
     * Sets the pending count to the given value.
     *
     * @param count the count
     */
    public final void setPendingCount(int count) {
        pending = count;
    }

    /**
     * Adds (atomically) the given value to the pending count.
     *
     * @param delta the value to add
     */
    public final void addToPendingCount(int delta) {
        int c; // note: can replace with intrinsic in jdk8
        do {} while (!U.compareAndSwapInt(this, PENDING, c = pending, c+delta));
    }

    /**
     * Sets (atomically) the pending count to the given count only if
     * it currently holds the given expected value.
     *
     * @param expected the expected value
     * @param count the new value
     * @return true is successful
     */
    public final boolean compareAndSetPendingCount(int expected, int count) {
        return U.compareAndSwapInt(this, PENDING, expected, count);
    }

    /**
     * If the pending count is nonzero, decrements the count;
     * otherwise invokes {@link #onCompletion} and then similarly
     * tries to complete this task's completer, if one exists,
     * else marks this task as complete.
     */
    public final void tryComplete() {
        CountedCompleter a = this, s = a;
        for (int c;;) {
            if ((c = a.pending) == 0) {
                a.onCompletion(s);
                if ((a = (s = a).completer) == null) {
                    s.quietlyComplete();
                    return;
                }
            }
            else if (U.compareAndSwapInt(a, PENDING, c, c - 1))
                return;
        }
    }

    /**
     * Regardless of pending count, invokes {@link #onCompletion},
     * marks this task as complete with a {@code null} return value,
     * and further triggers {@link #tryComplete} on this task's
     * completer, if one exists. This method may be useful when
     * forcing completion as soon as any one (versus all) of several
     * subtask results are obtained.
     *
     * @param mustBeNull the {@code null} completion value
     */
    public void complete(Void mustBeNull) {
        CountedCompleter p;
        onCompletion(this);
        quietlyComplete();
        if ((p = completer) != null)
            p.tryComplete();
    }

    /**
     * Support for FJT exception propagation
     */
    void internalPropagateException(Throwable ex) {
        CountedCompleter a = this, s = a;
        while (a.onExceptionalCompletion(ex, s) &&
               (a = (s = a).completer) != null && a.status >= 0)
            a.recordExceptionalCompletion(ex);
    }

    /**
     * Implements execution conventions for CountedCompleters
     */
    protected final boolean exec() {
        compute();
        return false;
    }

    /**
     * Always returns {@code null}.
     *
     * @return {@code null} always
     */
    public final Void getRawResult() { return null; }

    /**
     * Requires null completion value.
     */
    protected final void setRawResult(Void mustBeNull) { }

    // Unsafe mechanics
    private static final sun.misc.Unsafe U;
    private static final long PENDING;
    static {
        try {
            U = getUnsafe();
            PENDING = U.objectFieldOffset
                (CountedCompleter.class.getDeclaredField("pending"));
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
                        }});
            } catch (java.security.PrivilegedActionException e) {
                throw new RuntimeException("Could not initialize intrinsics",
                                           e.getCause());
            }
        }
    }

}
