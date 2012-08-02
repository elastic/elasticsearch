/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166y;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;

/**
 * An {@link ExecutorService} for running {@link ForkJoinTask}s.
 * A {@code ForkJoinPool} provides the entry point for submissions
 * from non-{@code ForkJoinTask} clients, as well as management and
 * monitoring operations.
 *
 * <p>A {@code ForkJoinPool} differs from other kinds of {@link
 * ExecutorService} mainly by virtue of employing
 * <em>work-stealing</em>: all threads in the pool attempt to find and
 * execute tasks submitted to the pool and/or created by other active
 * tasks (eventually blocking waiting for work if none exist). This
 * enables efficient processing when most tasks spawn other subtasks
 * (as do most {@code ForkJoinTask}s), as well as when many small
 * tasks are submitted to the pool from external clients.  Especially
 * when setting <em>asyncMode</em> to true in constructors, {@code
 * ForkJoinPool}s may also be appropriate for use with event-style
 * tasks that are never joined.
 *
 * <p>A {@code ForkJoinPool} is constructed with a given target
 * parallelism level; by default, equal to the number of available
 * processors. The pool attempts to maintain enough active (or
 * available) threads by dynamically adding, suspending, or resuming
 * internal worker threads, even if some tasks are stalled waiting to
 * join others. However, no such adjustments are guaranteed in the
 * face of blocked IO or other unmanaged synchronization. The nested
 * {@link ManagedBlocker} interface enables extension of the kinds of
 * synchronization accommodated.
 *
 * <p>In addition to execution and lifecycle control methods, this
 * class provides status check methods (for example
 * {@link #getStealCount}) that are intended to aid in developing,
 * tuning, and monitoring fork/join applications. Also, method
 * {@link #toString} returns indications of pool state in a
 * convenient form for informal monitoring.
 *
 * <p> As is the case with other ExecutorServices, there are three
 * main task execution methods summarized in the following table.
 * These are designed to be used primarily by clients not already
 * engaged in fork/join computations in the current pool.  The main
 * forms of these methods accept instances of {@code ForkJoinTask},
 * but overloaded forms also allow mixed execution of plain {@code
 * Runnable}- or {@code Callable}- based activities as well.  However,
 * tasks that are already executing in a pool should normally instead
 * use the within-computation forms listed in the table unless using
 * async event-style tasks that are not usually joined, in which case
 * there is little difference among choice of methods.
 *
 * <table BORDER CELLPADDING=3 CELLSPACING=1>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER> <b>Call from non-fork/join clients</b></td>
 *    <td ALIGN=CENTER> <b>Call from within fork/join computations</b></td>
 *  </tr>
 *  <tr>
 *    <td> <b>Arrange async execution</td>
 *    <td> {@link #execute(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#fork}</td>
 *  </tr>
 *  <tr>
 *    <td> <b>Await and obtain result</td>
 *    <td> {@link #invoke(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#invoke}</td>
 *  </tr>
 *  <tr>
 *    <td> <b>Arrange exec and obtain Future</td>
 *    <td> {@link #submit(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#fork} (ForkJoinTasks <em>are</em> Futures)</td>
 *  </tr>
 * </table>
 *
 * <p><b>Sample Usage.</b> Normally a single {@code ForkJoinPool} is
 * used for all parallel task execution in a program or subsystem.
 * Otherwise, use would not usually outweigh the construction and
 * bookkeeping overhead of creating a large set of threads. For
 * example, a common pool could be used for the {@code SortTasks}
 * illustrated in {@link RecursiveAction}. Because {@code
 * ForkJoinPool} uses threads in {@linkplain java.lang.Thread#isDaemon
 * daemon} mode, there is typically no need to explicitly {@link
 * #shutdown} such a pool upon program exit.
 *
 *  <pre> {@code
 * static final ForkJoinPool mainPool = new ForkJoinPool();
 * ...
 * public void sort(long[] array) {
 *   mainPool.invoke(new SortTask(array, 0, array.length));
 * }}</pre>
 *
 * <p><b>Implementation notes</b>: This implementation restricts the
 * maximum number of running threads to 32767. Attempts to create
 * pools with greater than the maximum number result in
 * {@code IllegalArgumentException}.
 *
 * <p>This implementation rejects submitted tasks (that is, by throwing
 * {@link RejectedExecutionException}) only when the pool is shut down
 * or internal resources have been exhausted.
 *
 * @since 1.7
 * @author Doug Lea
 */
public class ForkJoinPool extends AbstractExecutorService {

    /*
     * Implementation Overview
     *
     * This class and its nested classes provide the main
     * functionality and control for a set of worker threads:
     * Submissions from non-FJ threads enter into submission queues.
     * Workers take these tasks and typically split them into subtasks
     * that may be stolen by other workers.  Preference rules give
     * first priority to processing tasks from their own queues (LIFO
     * or FIFO, depending on mode), then to randomized FIFO steals of
     * tasks in other queues.
     *
     * WorkQueues
     * ==========
     *
     * Most operations occur within work-stealing queues (in nested
     * class WorkQueue).  These are special forms of Deques that
     * support only three of the four possible end-operations -- push,
     * pop, and poll (aka steal), under the further constraints that
     * push and pop are called only from the owning thread (or, as
     * extended here, under a lock), while poll may be called from
     * other threads.  (If you are unfamiliar with them, you probably
     * want to read Herlihy and Shavit's book "The Art of
     * Multiprocessor programming", chapter 16 describing these in
     * more detail before proceeding.)  The main work-stealing queue
     * design is roughly similar to those in the papers "Dynamic
     * Circular Work-Stealing Deque" by Chase and Lev, SPAA 2005
     * (http://research.sun.com/scalable/pubs/index.html) and
     * "Idempotent work stealing" by Michael, Saraswat, and Vechev,
     * PPoPP 2009 (http://portal.acm.org/citation.cfm?id=1504186).
     * The main differences ultimately stem from GC requirements that
     * we null out taken slots as soon as we can, to maintain as small
     * a footprint as possible even in programs generating huge
     * numbers of tasks. To accomplish this, we shift the CAS
     * arbitrating pop vs poll (steal) from being on the indices
     * ("base" and "top") to the slots themselves.  So, both a
     * successful pop and poll mainly entail a CAS of a slot from
     * non-null to null.  Because we rely on CASes of references, we
     * do not need tag bits on base or top.  They are simple ints as
     * used in any circular array-based queue (see for example
     * ArrayDeque).  Updates to the indices must still be ordered in a
     * way that guarantees that top == base means the queue is empty,
     * but otherwise may err on the side of possibly making the queue
     * appear nonempty when a push, pop, or poll have not fully
     * committed. Note that this means that the poll operation,
     * considered individually, is not wait-free. One thief cannot
     * successfully continue until another in-progress one (or, if
     * previously empty, a push) completes.  However, in the
     * aggregate, we ensure at least probabilistic non-blockingness.
     * If an attempted steal fails, a thief always chooses a different
     * random victim target to try next. So, in order for one thief to
     * progress, it suffices for any in-progress poll or new push on
     * any empty queue to complete. (This is why we normally use
     * method pollAt and its variants that try once at the apparent
     * base index, else consider alternative actions, rather than
     * method poll.)
     *
     * This approach also enables support of a user mode in which local
     * task processing is in FIFO, not LIFO order, simply by using
     * poll rather than pop.  This can be useful in message-passing
     * frameworks in which tasks are never joined.  However neither
     * mode considers affinities, loads, cache localities, etc, so
     * rarely provide the best possible performance on a given
     * machine, but portably provide good throughput by averaging over
     * these factors.  (Further, even if we did try to use such
     * information, we do not usually have a basis for exploiting it.
     * For example, some sets of tasks profit from cache affinities,
     * but others are harmed by cache pollution effects.)
     *
     * WorkQueues are also used in a similar way for tasks submitted
     * to the pool. We cannot mix these tasks in the same queues used
     * for work-stealing (this would contaminate lifo/fifo
     * processing). Instead, we loosely associate submission queues
     * with submitting threads, using a form of hashing.  The
     * ThreadLocal Submitter class contains a value initially used as
     * a hash code for choosing existing queues, but may be randomly
     * repositioned upon contention with other submitters.  In
     * essence, submitters act like workers except that they never
     * take tasks, and they are multiplexed on to a finite number of
     * shared work queues. However, classes are set up so that future
     * extensions could allow submitters to optionally help perform
     * tasks as well. Insertion of tasks in shared mode requires a
     * lock (mainly to protect in the case of resizing) but we use
     * only a simple spinlock (using bits in field runState), because
     * submitters encountering a busy queue move on to try or create
     * other queues -- they block only when creating and registering
     * new queues.
     *
     * Management
     * ==========
     *
     * The main throughput advantages of work-stealing stem from
     * decentralized control -- workers mostly take tasks from
     * themselves or each other. We cannot negate this in the
     * implementation of other management responsibilities. The main
     * tactic for avoiding bottlenecks is packing nearly all
     * essentially atomic control state into two volatile variables
     * that are by far most often read (not written) as status and
     * consistency checks.
     *
     * Field "ctl" contains 64 bits holding all the information needed
     * to atomically decide to add, inactivate, enqueue (on an event
     * queue), dequeue, and/or re-activate workers.  To enable this
     * packing, we restrict maximum parallelism to (1<<15)-1 (which is
     * far in excess of normal operating range) to allow ids, counts,
     * and their negations (used for thresholding) to fit into 16bit
     * fields.
     *
     * Field "runState" contains 32 bits needed to register and
     * deregister WorkQueues, as well as to enable shutdown. It is
     * only modified under a lock (normally briefly held, but
     * occasionally protecting allocations and resizings) but even
     * when locked remains available to check consistency.
     *
     * Recording WorkQueues.  WorkQueues are recorded in the
     * "workQueues" array that is created upon pool construction and
     * expanded if necessary.  Updates to the array while recording
     * new workers and unrecording terminated ones are protected from
     * each other by a lock but the array is otherwise concurrently
     * readable, and accessed directly.  To simplify index-based
     * operations, the array size is always a power of two, and all
     * readers must tolerate null slots. Shared (submission) queues
     * are at even indices, worker queues at odd indices. Grouping
     * them together in this way simplifies and speeds up task
     * scanning.
     *
     * All worker thread creation is on-demand, triggered by task
     * submissions, replacement of terminated workers, and/or
     * compensation for blocked workers. However, all other support
     * code is set up to work with other policies.  To ensure that we
     * do not hold on to worker references that would prevent GC, ALL
     * accesses to workQueues are via indices into the workQueues
     * array (which is one source of some of the messy code
     * constructions here). In essence, the workQueues array serves as
     * a weak reference mechanism. Thus for example the wait queue
     * field of ctl stores indices, not references.  Access to the
     * workQueues in associated methods (for example signalWork) must
     * both index-check and null-check the IDs. All such accesses
     * ignore bad IDs by returning out early from what they are doing,
     * since this can only be associated with termination, in which
     * case it is OK to give up.  All uses of the workQueues array
     * also check that it is non-null (even if previously
     * non-null). This allows nulling during termination, which is
     * currently not necessary, but remains an option for
     * resource-revocation-based shutdown schemes. It also helps
     * reduce JIT issuance of uncommon-trap code, which tends to
     * unnecessarily complicate control flow in some methods.
     *
     * Event Queuing. Unlike HPC work-stealing frameworks, we cannot
     * let workers spin indefinitely scanning for tasks when none can
     * be found immediately, and we cannot start/resume workers unless
     * there appear to be tasks available.  On the other hand, we must
     * quickly prod them into action when new tasks are submitted or
     * generated. In many usages, ramp-up time to activate workers is
     * the main limiting factor in overall performance (this is
     * compounded at program start-up by JIT compilation and
     * allocation). So we try to streamline this as much as possible.
     * We park/unpark workers after placing in an event wait queue
     * when they cannot find work. This "queue" is actually a simple
     * Treiber stack, headed by the "id" field of ctl, plus a 15bit
     * counter value (that reflects the number of times a worker has
     * been inactivated) to avoid ABA effects (we need only as many
     * version numbers as worker threads). Successors are held in
     * field WorkQueue.nextWait.  Queuing deals with several intrinsic
     * races, mainly that a task-producing thread can miss seeing (and
     * signalling) another thread that gave up looking for work but
     * has not yet entered the wait queue. We solve this by requiring
     * a full sweep of all workers (via repeated calls to method
     * scan()) both before and after a newly waiting worker is added
     * to the wait queue. During a rescan, the worker might release
     * some other queued worker rather than itself, which has the same
     * net effect. Because enqueued workers may actually be rescanning
     * rather than waiting, we set and clear the "parker" field of
     * WorkQueues to reduce unnecessary calls to unpark.  (This
     * requires a secondary recheck to avoid missed signals.)  Note
     * the unusual conventions about Thread.interrupts surrounding
     * parking and other blocking: Because interrupts are used solely
     * to alert threads to check termination, which is checked anyway
     * upon blocking, we clear status (using Thread.interrupted)
     * before any call to park, so that park does not immediately
     * return due to status being set via some other unrelated call to
     * interrupt in user code.
     *
     * Signalling.  We create or wake up workers only when there
     * appears to be at least one task they might be able to find and
     * execute.  When a submission is added or another worker adds a
     * task to a queue that previously had fewer than two tasks, they
     * signal waiting workers (or trigger creation of new ones if
     * fewer than the given parallelism level -- see signalWork).
     * These primary signals are buttressed by signals during rescans;
     * together these cover the signals needed in cases when more
     * tasks are pushed but untaken, and improve performance compared
     * to having one thread wake up all workers.
     *
     * Trimming workers. To release resources after periods of lack of
     * use, a worker starting to wait when the pool is quiescent will
     * time out and terminate if the pool has remained quiescent for
     * SHRINK_RATE nanosecs. This will slowly propagate, eventually
     * terminating all workers after long periods of non-use.
     *
     * Shutdown and Termination. A call to shutdownNow atomically sets
     * a runState bit and then (non-atomically) sets each worker's
     * runState status, cancels all unprocessed tasks, and wakes up
     * all waiting workers.  Detecting whether termination should
     * commence after a non-abrupt shutdown() call requires more work
     * and bookkeeping. We need consensus about quiescence (i.e., that
     * there is no more work). The active count provides a primary
     * indication but non-abrupt shutdown still requires a rechecking
     * scan for any workers that are inactive but not queued.
     *
     * Joining Tasks
     * =============
     *
     * Any of several actions may be taken when one worker is waiting
     * to join a task stolen (or always held) by another.  Because we
     * are multiplexing many tasks on to a pool of workers, we can't
     * just let them block (as in Thread.join).  We also cannot just
     * reassign the joiner's run-time stack with another and replace
     * it later, which would be a form of "continuation", that even if
     * possible is not necessarily a good idea since we sometimes need
     * both an unblocked task and its continuation to progress.
     * Instead we combine two tactics:
     *
     *   Helping: Arranging for the joiner to execute some task that it
     *      would be running if the steal had not occurred.
     *
     *   Compensating: Unless there are already enough live threads,
     *      method tryCompensate() may create or re-activate a spare
     *      thread to compensate for blocked joiners until they unblock.
     *
     * A third form (implemented in tryRemoveAndExec and
     * tryPollForAndExec) amounts to helping a hypothetical
     * compensator: If we can readily tell that a possible action of a
     * compensator is to steal and execute the task being joined, the
     * joining thread can do so directly, without the need for a
     * compensation thread (although at the expense of larger run-time
     * stacks, but the tradeoff is typically worthwhile).
     *
     * The ManagedBlocker extension API can't use helping so relies
     * only on compensation in method awaitBlocker.
     *
     * The algorithm in tryHelpStealer entails a form of "linear"
     * helping: Each worker records (in field currentSteal) the most
     * recent task it stole from some other worker. Plus, it records
     * (in field currentJoin) the task it is currently actively
     * joining. Method tryHelpStealer uses these markers to try to
     * find a worker to help (i.e., steal back a task from and execute
     * it) that could hasten completion of the actively joined task.
     * In essence, the joiner executes a task that would be on its own
     * local deque had the to-be-joined task not been stolen. This may
     * be seen as a conservative variant of the approach in Wagner &
     * Calder "Leapfrogging: a portable technique for implementing
     * efficient futures" SIGPLAN Notices, 1993
     * (http://portal.acm.org/citation.cfm?id=155354). It differs in
     * that: (1) We only maintain dependency links across workers upon
     * steals, rather than use per-task bookkeeping.  This sometimes
     * requires a linear scan of workQueues array to locate stealers,
     * but often doesn't because stealers leave hints (that may become
     * stale/wrong) of where to locate them.  A stealHint is only a
     * hint because a worker might have had multiple steals and the
     * hint records only one of them (usually the most current).
     * Hinting isolates cost to when it is needed, rather than adding
     * to per-task overhead.  (2) It is "shallow", ignoring nesting
     * and potentially cyclic mutual steals.  (3) It is intentionally
     * racy: field currentJoin is updated only while actively joining,
     * which means that we miss links in the chain during long-lived
     * tasks, GC stalls etc (which is OK since blocking in such cases
     * is usually a good idea).  (4) We bound the number of attempts
     * to find work (see MAX_HELP) and fall back to suspending the
     * worker and if necessary replacing it with another.
     *
     * It is impossible to keep exactly the target parallelism number
     * of threads running at any given time.  Determining the
     * existence of conservatively safe helping targets, the
     * availability of already-created spares, and the apparent need
     * to create new spares are all racy, so we rely on multiple
     * retries of each.  Compensation in the apparent absence of
     * helping opportunities is challenging to control on JVMs, where
     * GC and other activities can stall progress of tasks that in
     * turn stall out many other dependent tasks, without us being
     * able to determine whether they will ever require compensation.
     * Even though work-stealing otherwise encounters little
     * degradation in the presence of more threads than cores,
     * aggressively adding new threads in such cases entails risk of
     * unwanted positive feedback control loops in which more threads
     * cause more dependent stalls (as well as delayed progress of
     * unblocked threads to the point that we know they are available)
     * leading to more situations requiring more threads, and so
     * on. This aspect of control can be seen as an (analytically
     * intractable) game with an opponent that may choose the worst
     * (for us) active thread to stall at any time.  We take several
     * precautions to bound losses (and thus bound gains), mainly in
     * methods tryCompensate and awaitJoin: (1) We only try
     * compensation after attempting enough helping steps (measured
     * via counting and timing) that we have already consumed the
     * estimated cost of creating and activating a new thread.  (2) We
     * allow up to 50% of threads to be blocked before initially
     * adding any others, and unless completely saturated, check that
     * some work is available for a new worker before adding. Also, we
     * create up to only 50% more threads until entering a mode that
     * only adds a thread if all others are possibly blocked.  All
     * together, this means that we might be half as fast to react,
     * and create half as many threads as possible in the ideal case,
     * but present vastly fewer anomalies in all other cases compared
     * to both more aggressive and more conservative alternatives.
     *
     * Style notes: There is a lot of representation-level coupling
     * among classes ForkJoinPool, ForkJoinWorkerThread, and
     * ForkJoinTask.  The fields of WorkQueue maintain data structures
     * managed by ForkJoinPool, so are directly accessed.  There is
     * little point trying to reduce this, since any associated future
     * changes in representations will need to be accompanied by
     * algorithmic changes anyway. Several methods intrinsically
     * sprawl because they must accumulate sets of consistent reads of
     * volatiles held in local variables.  Methods signalWork() and
     * scan() are the main bottlenecks, so are especially heavily
     * micro-optimized/mangled.  There are lots of inline assignments
     * (of form "while ((local = field) != 0)") which are usually the
     * simplest way to ensure the required read orderings (which are
     * sometimes critical). This leads to a "C"-like style of listing
     * declarations of these locals at the heads of methods or blocks.
     * There are several occurrences of the unusual "do {} while
     * (!cas...)"  which is the simplest way to force an update of a
     * CAS'ed variable. There are also other coding oddities that help
     * some methods perform reasonably even when interpreted (not
     * compiled).
     *
     * The order of declarations in this file is:
     * (1) Static utility functions
     * (2) Nested (static) classes
     * (3) Static fields
     * (4) Fields, along with constants used when unpacking some of them
     * (5) Internal control methods
     * (6) Callbacks and other support for ForkJoinTask methods
     * (7) Exported methods
     * (8) Static block initializing statics in minimally dependent order
     */

    // Static utilities

    /**
     * If there is a security manager, makes sure caller has
     * permission to modify threads.
     */
    private static void checkPermission() {
        SecurityManager security = System.getSecurityManager();
        if (security != null)
            security.checkPermission(modifyThreadPermission);
    }

    // Nested classes

    /**
     * Factory for creating new {@link ForkJoinWorkerThread}s.
     * A {@code ForkJoinWorkerThreadFactory} must be defined and used
     * for {@code ForkJoinWorkerThread} subclasses that extend base
     * functionality or initialize threads with different contexts.
     */
    public static interface ForkJoinWorkerThreadFactory {
        /**
         * Returns a new worker thread operating in the given pool.
         *
         * @param pool the pool this thread works in
         * @throws NullPointerException if the pool is null
         */
        public ForkJoinWorkerThread newThread(ForkJoinPool pool);
    }

    /**
     * Default ForkJoinWorkerThreadFactory implementation; creates a
     * new ForkJoinWorkerThread.
     */
    static class DefaultForkJoinWorkerThreadFactory
        implements ForkJoinWorkerThreadFactory {
        public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            return new ForkJoinWorkerThread(pool);
        }
    }

    /**
     * A simple non-reentrant lock used for exclusion when managing
     * queues and workers. We use a custom lock so that we can readily
     * probe lock state in constructions that check among alternative
     * actions. The lock is normally only very briefly held, and
     * sometimes treated as a spinlock, but other usages block to
     * reduce overall contention in those cases where locked code
     * bodies perform allocation/resizing.
     */
    static final class Mutex extends AbstractQueuedSynchronizer {
        public final boolean tryAcquire(int ignore) {
            return compareAndSetState(0, 1);
        }
        public final boolean tryRelease(int ignore) {
            setState(0);
            return true;
        }
        public final void lock() { acquire(0); }
        public final void unlock() { release(0); }
        public final boolean isHeldExclusively() { return getState() == 1; }
        public final Condition newCondition() { return new ConditionObject(); }
    }

    /**
     * Class for artificial tasks that are used to replace the target
     * of local joins if they are removed from an interior queue slot
     * in WorkQueue.tryRemoveAndExec. We don't need the proxy to
     * actually do anything beyond having a unique identity.
     */
    static final class EmptyTask extends ForkJoinTask<Void> {
        EmptyTask() { status = ForkJoinTask.NORMAL; } // force done
        public final Void getRawResult() { return null; }
        public final void setRawResult(Void x) {}
        public final boolean exec() { return true; }
    }

    /**
     * Queues supporting work-stealing as well as external task
     * submission. See above for main rationale and algorithms.
     * Implementation relies heavily on "Unsafe" intrinsics
     * and selective use of "volatile":
     *
     * Field "base" is the index (mod array.length) of the least valid
     * queue slot, which is always the next position to steal (poll)
     * from if nonempty. Reads and writes require volatile orderings
     * but not CAS, because updates are only performed after slot
     * CASes.
     *
     * Field "top" is the index (mod array.length) of the next queue
     * slot to push to or pop from. It is written only by owner thread
     * for push, or under lock for trySharedPush, and accessed by
     * other threads only after reading (volatile) base.  Both top and
     * base are allowed to wrap around on overflow, but (top - base)
     * (or more commonly -(base - top) to force volatile read of base
     * before top) still estimates size.
     *
     * The array slots are read and written using the emulation of
     * volatiles/atomics provided by Unsafe. Insertions must in
     * general use putOrderedObject as a form of releasing store to
     * ensure that all writes to the task object are ordered before
     * its publication in the queue. (Although we can avoid one case
     * of this when locked in trySharedPush.) All removals entail a
     * CAS to null.  The array is always a power of two. To ensure
     * safety of Unsafe array operations, all accesses perform
     * explicit null checks and implicit bounds checks via
     * power-of-two masking.
     *
     * In addition to basic queuing support, this class contains
     * fields described elsewhere to control execution. It turns out
     * to work better memory-layout-wise to include them in this
     * class rather than a separate class.
     *
     * Performance on most platforms is very sensitive to placement of
     * instances of both WorkQueues and their arrays -- we absolutely
     * do not want multiple WorkQueue instances or multiple queue
     * arrays sharing cache lines. (It would be best for queue objects
     * and their arrays to share, but there is nothing available to
     * help arrange that).  Unfortunately, because they are recorded
     * in a common array, WorkQueue instances are often moved to be
     * adjacent by garbage collectors. To reduce impact, we use field
     * padding that works OK on common platforms; this effectively
     * trades off slightly slower average field access for the sake of
     * avoiding really bad worst-case access. (Until better JVM
     * support is in place, this padding is dependent on transient
     * properties of JVM field layout rules.)  We also take care in
     * allocating, sizing and resizing the array. Non-shared queue
     * arrays are initialized (via method growArray) by workers before
     * use. Others are allocated on first use.
     */
    static final class WorkQueue {
        /**
         * Capacity of work-stealing queue array upon initialization.
         * Must be a power of two; at least 4, but should be larger to
         * reduce or eliminate cacheline sharing among queues.
         * Currently, it is much larger, as a partial workaround for
         * the fact that JVMs often place arrays in locations that
         * share GC bookkeeping (especially cardmarks) such that
         * per-write accesses encounter serious memory contention.
         */
        static final int INITIAL_QUEUE_CAPACITY = 1 << 13;

        /**
         * Maximum size for queue arrays. Must be a power of two less
         * than or equal to 1 << (31 - width of array entry) to ensure
         * lack of wraparound of index calculations, but defined to a
         * value a bit less than this to help users trap runaway
         * programs before saturating systems.
         */
        static final int MAXIMUM_QUEUE_CAPACITY = 1 << 26; // 64M

        volatile long totalSteals; // cumulative number of steals
        int seed;                  // for random scanning; initialize nonzero
        volatile int eventCount;   // encoded inactivation count; < 0 if inactive
        int nextWait;              // encoded record of next event waiter
        int rescans;               // remaining scans until block
        int nsteals;               // top-level task executions since last idle
        final int mode;            // lifo, fifo, or shared
        int poolIndex;             // index of this queue in pool (or 0)
        int stealHint;             // index of most recent known stealer
        volatile int runState;     // 1: locked, -1: terminate; else 0
        volatile int base;         // index of next slot for poll
        int top;                   // index of next slot for push
        ForkJoinTask<?>[] array;   // the elements (initially unallocated)
        final ForkJoinPool pool;   // the containing pool (may be null)
        final ForkJoinWorkerThread owner; // owning thread or null if shared
        volatile Thread parker;    // == owner during call to park; else null
        volatile ForkJoinTask<?> currentJoin;  // task being joined in awaitJoin
        ForkJoinTask<?> currentSteal; // current non-local task being executed
        // Heuristic padding to ameliorate unfortunate memory placements
        Object p00, p01, p02, p03, p04, p05, p06, p07;
        Object p08, p09, p0a, p0b, p0c, p0d, p0e;

        WorkQueue(ForkJoinPool pool, ForkJoinWorkerThread owner, int mode) {
            this.mode = mode;
            this.pool = pool;
            this.owner = owner;
            // Place indices in the center of array (that is not yet allocated)
            base = top = INITIAL_QUEUE_CAPACITY >>> 1;
        }

        /**
         * Returns the approximate number of tasks in the queue.
         */
        final int queueSize() {
            int n = base - top;       // non-owner callers must read base first
            return (n >= 0) ? 0 : -n; // ignore transient negative
        }

        /**
         * Provides a more accurate estimate of whether this queue has
         * any tasks than does queueSize, by checking whether a
         * near-empty queue has at least one unclaimed task.
         */
        final boolean isEmpty() {
            ForkJoinTask<?>[] a; int m, s;
            int n = base - (s = top);
            return (n >= 0 ||
                    (n == -1 &&
                     ((a = array) == null ||
                      (m = a.length - 1) < 0 ||
                      U.getObjectVolatile
                      (a, ((m & (s - 1)) << ASHIFT) + ABASE) == null)));
        }

        /**
         * Pushes a task. Call only by owner in unshared queues.
         *
         * @param task the task. Caller must ensure non-null.
         * @throw RejectedExecutionException if array cannot be resized
         */
        final void push(ForkJoinTask<?> task) {
            ForkJoinTask<?>[] a; ForkJoinPool p;
            int s = top, m, n;
            if ((a = array) != null) {    // ignore if queue removed
                U.putOrderedObject
                    (a, (((m = a.length - 1) & s) << ASHIFT) + ABASE, task);
                if ((n = (top = s + 1) - base) <= 2) {
                    if ((p = pool) != null)
                        p.signalWork();
                }
                else if (n >= m)
                    growArray(true);
            }
        }

        /**
         * Pushes a task if lock is free and array is either big
         * enough or can be resized to be big enough.
         *
         * @param task the task. Caller must ensure non-null.
         * @return true if submitted
         */
        final boolean trySharedPush(ForkJoinTask<?> task) {
            boolean submitted = false;
            if (runState == 0 && U.compareAndSwapInt(this, RUNSTATE, 0, 1)) {
                ForkJoinTask<?>[] a = array;
                int s = top;
                try {
                    if ((a != null && a.length > s + 1 - base) ||
                        (a = growArray(false)) != null) { // must presize
                        int j = (((a.length - 1) & s) << ASHIFT) + ABASE;
                        U.putObject(a, (long)j, task);    // don't need "ordered"
                        top = s + 1;
                        submitted = true;
                    }
                } finally {
                    runState = 0;                         // unlock
                }
            }
            return submitted;
        }

        /**
         * Takes next task, if one exists, in LIFO order.  Call only
         * by owner in unshared queues. (We do not have a shared
         * version of this method because it is never needed.)
         */
        final ForkJoinTask<?> pop() {
            ForkJoinTask<?>[] a; ForkJoinTask<?> t; int m;
            if ((a = array) != null && (m = a.length - 1) >= 0) {
                for (int s; (s = top - 1) - base >= 0;) {
                    long j = ((m & s) << ASHIFT) + ABASE;
                    if ((t = (ForkJoinTask<?>)U.getObject(a, j)) == null)
                        break;
                    if (U.compareAndSwapObject(a, j, t, null)) {
                        top = s;
                        return t;
                    }
                }
            }
            return null;
        }

        /**
         * Takes a task in FIFO order if b is base of queue and a task
         * can be claimed without contention. Specialized versions
         * appear in ForkJoinPool methods scan and tryHelpStealer.
         */
        final ForkJoinTask<?> pollAt(int b) {
            ForkJoinTask<?> t; ForkJoinTask<?>[] a;
            if ((a = array) != null) {
                int j = (((a.length - 1) & b) << ASHIFT) + ABASE;
                if ((t = (ForkJoinTask<?>)U.getObjectVolatile(a, j)) != null &&
                    base == b &&
                    U.compareAndSwapObject(a, j, t, null)) {
                    base = b + 1;
                    return t;
                }
            }
            return null;
        }

        /**
         * Takes next task, if one exists, in FIFO order.
         */
        final ForkJoinTask<?> poll() {
            ForkJoinTask<?>[] a; int b; ForkJoinTask<?> t;
            while ((b = base) - top < 0 && (a = array) != null) {
                int j = (((a.length - 1) & b) << ASHIFT) + ABASE;
                t = (ForkJoinTask<?>)U.getObjectVolatile(a, j);
                if (t != null) {
                    if (base == b &&
                        U.compareAndSwapObject(a, j, t, null)) {
                        base = b + 1;
                        return t;
                    }
                }
                else if (base == b) {
                    if (b + 1 == top)
                        break;
                    Thread.yield(); // wait for lagging update
                }
            }
            return null;
        }

        /**
         * Takes next task, if one exists, in order specified by mode.
         */
        final ForkJoinTask<?> nextLocalTask() {
            return mode == 0 ? pop() : poll();
        }

        /**
         * Returns next task, if one exists, in order specified by mode.
         */
        final ForkJoinTask<?> peek() {
            ForkJoinTask<?>[] a = array; int m;
            if (a == null || (m = a.length - 1) < 0)
                return null;
            int i = mode == 0 ? top - 1 : base;
            int j = ((i & m) << ASHIFT) + ABASE;
            return (ForkJoinTask<?>)U.getObjectVolatile(a, j);
        }

        /**
         * Pops the given task only if it is at the current top.
         */
        final boolean tryUnpush(ForkJoinTask<?> t) {
            ForkJoinTask<?>[] a; int s;
            if ((a = array) != null && (s = top) != base &&
                U.compareAndSwapObject
                (a, (((a.length - 1) & --s) << ASHIFT) + ABASE, t, null)) {
                top = s;
                return true;
            }
            return false;
        }

        /**
         * Polls the given task only if it is at the current base.
         */
        final boolean pollFor(ForkJoinTask<?> task) {
            ForkJoinTask<?>[] a; int b;
            if ((b = base) - top < 0 && (a = array) != null) {
                int j = (((a.length - 1) & b) << ASHIFT) + ABASE;
                if (U.getObjectVolatile(a, j) == task && base == b &&
                    U.compareAndSwapObject(a, j, task, null)) {
                    base = b + 1;
                    return true;
                }
            }
            return false;
        }

        /**
         * Initializes or doubles the capacity of array. Call either
         * by owner or with lock held -- it is OK for base, but not
         * top, to move while resizings are in progress.
         *
         * @param rejectOnFailure if true, throw exception if capacity
         * exceeded (relayed ultimately to user); else return null.
         */
        final ForkJoinTask<?>[] growArray(boolean rejectOnFailure) {
            ForkJoinTask<?>[] oldA = array;
            int size = oldA != null ? oldA.length << 1 : INITIAL_QUEUE_CAPACITY;
            if (size <= MAXIMUM_QUEUE_CAPACITY) {
                int oldMask, t, b;
                ForkJoinTask<?>[] a = array = new ForkJoinTask<?>[size];
                if (oldA != null && (oldMask = oldA.length - 1) >= 0 &&
                    (t = top) - (b = base) > 0) {
                    int mask = size - 1;
                    do {
                        ForkJoinTask<?> x;
                        int oldj = ((b & oldMask) << ASHIFT) + ABASE;
                        int j    = ((b &    mask) << ASHIFT) + ABASE;
                        x = (ForkJoinTask<?>)U.getObjectVolatile(oldA, oldj);
                        if (x != null &&
                            U.compareAndSwapObject(oldA, oldj, x, null))
                            U.putObjectVolatile(a, j, x);
                    } while (++b != t);
                }
                return a;
            }
            else if (!rejectOnFailure)
                return null;
            else
                throw new RejectedExecutionException("Queue capacity exceeded");
        }

        /**
         * Removes and cancels all known tasks, ignoring any exceptions.
         */
        final void cancelAll() {
            ForkJoinTask.cancelIgnoringExceptions(currentJoin);
            ForkJoinTask.cancelIgnoringExceptions(currentSteal);
            for (ForkJoinTask<?> t; (t = poll()) != null; )
                ForkJoinTask.cancelIgnoringExceptions(t);
        }

        /**
         * Computes next value for random probes.  Scans don't require
         * a very high quality generator, but also not a crummy one.
         * Marsaglia xor-shift is cheap and works well enough.  Note:
         * This is manually inlined in its usages in ForkJoinPool to
         * avoid writes inside busy scan loops.
         */
        final int nextSeed() {
            int r = seed;
            r ^= r << 13;
            r ^= r >>> 17;
            return seed = r ^= r << 5;
        }

        // Execution methods

        /**
         * Pops and runs tasks until empty.
         */
        private void popAndExecAll() {
            // A bit faster than repeated pop calls
            ForkJoinTask<?>[] a; int m, s; long j; ForkJoinTask<?> t;
            while ((a = array) != null && (m = a.length - 1) >= 0 &&
                   (s = top - 1) - base >= 0 &&
                   (t = ((ForkJoinTask<?>)
                         U.getObject(a, j = ((m & s) << ASHIFT) + ABASE)))
                   != null) {
                if (U.compareAndSwapObject(a, j, t, null)) {
                    top = s;
                    t.doExec();
                }
            }
        }

        /**
         * Polls and runs tasks until empty.
         */
        private void pollAndExecAll() {
            for (ForkJoinTask<?> t; (t = poll()) != null;)
                t.doExec();
        }

        /**
         * If present, removes from queue and executes the given task, or
         * any other cancelled task. Returns (true) immediately on any CAS
         * or consistency check failure so caller can retry.
         *
         * @return 0 if no progress can be made, else positive
         * (this unusual convention simplifies use with tryHelpStealer.)
         */
        final int tryRemoveAndExec(ForkJoinTask<?> task) {
            int stat = 1;
            boolean removed = false, empty = true;
            ForkJoinTask<?>[] a; int m, s, b, n;
            if ((a = array) != null && (m = a.length - 1) >= 0 &&
                (n = (s = top) - (b = base)) > 0) {
                for (ForkJoinTask<?> t;;) {           // traverse from s to b
                    int j = ((--s & m) << ASHIFT) + ABASE;
                    t = (ForkJoinTask<?>)U.getObjectVolatile(a, j);
                    if (t == null)                    // inconsistent length
                        break;
                    else if (t == task) {
                        if (s + 1 == top) {           // pop
                            if (!U.compareAndSwapObject(a, j, task, null))
                                break;
                            top = s;
                            removed = true;
                        }
                        else if (base == b)           // replace with proxy
                            removed = U.compareAndSwapObject(a, j, task,
                                                             new EmptyTask());
                        break;
                    }
                    else if (t.status >= 0)
                        empty = false;
                    else if (s + 1 == top) {          // pop and throw away
                        if (U.compareAndSwapObject(a, j, t, null))
                            top = s;
                        break;
                    }
                    if (--n == 0) {
                        if (!empty && base == b)
                            stat = 0;
                        break;
                    }
                }
            }
            if (removed)
                task.doExec();
            return stat;
        }

        /**
         * Executes a top-level task and any local tasks remaining
         * after execution.
         */
        final void runTask(ForkJoinTask<?> t) {
            if (t != null) {
                currentSteal = t;
                t.doExec();
                if (top != base) {       // process remaining local tasks
                    if (mode == 0)
                        popAndExecAll();
                    else
                        pollAndExecAll();
                }
                ++nsteals;
                currentSteal = null;
            }
        }

        /**
         * Executes a non-top-level (stolen) task.
         */
        final void runSubtask(ForkJoinTask<?> t) {
            if (t != null) {
                ForkJoinTask<?> ps = currentSteal;
                currentSteal = t;
                t.doExec();
                currentSteal = ps;
            }
        }

        /**
         * Returns true if owned and not known to be blocked.
         */
        final boolean isApparentlyUnblocked() {
            Thread wt; Thread.State s;
            return (eventCount >= 0 &&
                    (wt = owner) != null &&
                    (s = wt.getState()) != Thread.State.BLOCKED &&
                    s != Thread.State.WAITING &&
                    s != Thread.State.TIMED_WAITING);
        }

        /**
         * If this owned and is not already interrupted, try to
         * interrupt and/or unpark, ignoring exceptions.
         */
        final void interruptOwner() {
            Thread wt, p;
            if ((wt = owner) != null && !wt.isInterrupted()) {
                try {
                    wt.interrupt();
                } catch (SecurityException ignore) {
                }
            }
            if ((p = parker) != null)
                U.unpark(p);
        }

        // Unsafe mechanics
        private static final sun.misc.Unsafe U;
        private static final long RUNSTATE;
        private static final int ABASE;
        private static final int ASHIFT;
        static {
            int s;
            try {
                U = getUnsafe();
                Class<?> k = WorkQueue.class;
                Class<?> ak = ForkJoinTask[].class;
                RUNSTATE = U.objectFieldOffset
                    (k.getDeclaredField("runState"));
                ABASE = U.arrayBaseOffset(ak);
                s = U.arrayIndexScale(ak);
            } catch (Exception e) {
                throw new Error(e);
            }
            if ((s & (s-1)) != 0)
                throw new Error("data type scale not a power of two");
            ASHIFT = 31 - Integer.numberOfLeadingZeros(s);
        }
    }
    /**
     * Per-thread records for threads that submit to pools. Currently
     * holds only pseudo-random seed / index that is used to choose
     * submission queues in method doSubmit. In the future, this may
     * also incorporate a means to implement different task rejection
     * and resubmission policies.
     *
     * Seeds for submitters and workers/workQueues work in basically
     * the same way but are initialized and updated using slightly
     * different mechanics. Both are initialized using the same
     * approach as in class ThreadLocal, where successive values are
     * unlikely to collide with previous values. This is done during
     * registration for workers, but requires a separate AtomicInteger
     * for submitters. Seeds are then randomly modified upon
     * collisions using xorshifts, which requires a non-zero seed.
     */
    static final class Submitter {
        int seed;
        Submitter() {
            int s = nextSubmitterSeed.getAndAdd(SEED_INCREMENT);
            seed = (s == 0) ? 1 : s; // ensure non-zero
        }
    }

    /** ThreadLocal class for Submitters */
    static final class ThreadSubmitter extends ThreadLocal<Submitter> {
        public Submitter initialValue() { return new Submitter(); }
    }

    // static fields (initialized in static initializer below)

    /**
     * Creates a new ForkJoinWorkerThread. This factory is used unless
     * overridden in ForkJoinPool constructors.
     */
    public static final ForkJoinWorkerThreadFactory
        defaultForkJoinWorkerThreadFactory;

    /**
     * Generator for assigning sequence numbers as pool names.
     */
    private static final AtomicInteger poolNumberGenerator;

    /**
     * Generator for initial hashes/seeds for submitters. Accessed by
     * Submitter class constructor.
     */
    static final AtomicInteger nextSubmitterSeed;

    /**
     * Permission required for callers of methods that may start or
     * kill threads.
     */
    private static final RuntimePermission modifyThreadPermission;

    /**
     * Per-thread submission bookeeping. Shared across all pools
     * to reduce ThreadLocal pollution and because random motion
     * to avoid contention in one pool is likely to hold for others.
     */
    private static final ThreadSubmitter submitters;

    // static constants

    /**
     * The wakeup interval (in nanoseconds) for a worker waiting for a
     * task when the pool is quiescent to instead try to shrink the
     * number of workers.  The exact value does not matter too
     * much. It must be short enough to release resources during
     * sustained periods of idleness, but not so short that threads
     * are continually re-created.
     */
    private static final long SHRINK_RATE =
        4L * 1000L * 1000L * 1000L; // 4 seconds

    /**
     * The timeout value for attempted shrinkage, includes
     * some slop to cope with system timer imprecision.
     */
    private static final long SHRINK_TIMEOUT = SHRINK_RATE - (SHRINK_RATE / 10);

    /**
     * The maximum stolen->joining link depth allowed in method
     * tryHelpStealer.  Must be a power of two. This value also
     * controls the maximum number of times to try to help join a task
     * without any apparent progress or change in pool state before
     * giving up and blocking (see awaitJoin).  Depths for legitimate
     * chains are unbounded, but we use a fixed constant to avoid
     * (otherwise unchecked) cycles and to bound staleness of
     * traversal parameters at the expense of sometimes blocking when
     * we could be helping.
     */
    private static final int MAX_HELP = 64;

    /**
     * Secondary time-based bound (in nanosecs) for helping attempts
     * before trying compensated blocking in awaitJoin. Used in
     * conjunction with MAX_HELP to reduce variance due to different
     * polling rates associated with different helping options. The
     * value should roughly approximate the time required to create
     * and/or activate a worker thread.
     */
    private static final long COMPENSATION_DELAY = 1L << 18; // ~0.25 millisec

    /**
     * Increment for seed generators. See class ThreadLocal for
     * explanation.
     */
    private static final int SEED_INCREMENT = 0x61c88647;

    /**
     * Bits and masks for control variables
     *
     * Field ctl is a long packed with:
     * AC: Number of active running workers minus target parallelism (16 bits)
     * TC: Number of total workers minus target parallelism (16 bits)
     * ST: true if pool is terminating (1 bit)
     * EC: the wait count of top waiting thread (15 bits)
     * ID: poolIndex of top of Treiber stack of waiters (16 bits)
     *
     * When convenient, we can extract the upper 32 bits of counts and
     * the lower 32 bits of queue state, u = (int)(ctl >>> 32) and e =
     * (int)ctl.  The ec field is never accessed alone, but always
     * together with id and st. The offsets of counts by the target
     * parallelism and the positionings of fields makes it possible to
     * perform the most common checks via sign tests of fields: When
     * ac is negative, there are not enough active workers, when tc is
     * negative, there are not enough total workers, and when e is
     * negative, the pool is terminating.  To deal with these possibly
     * negative fields, we use casts in and out of "short" and/or
     * signed shifts to maintain signedness.
     *
     * When a thread is queued (inactivated), its eventCount field is
     * set negative, which is the only way to tell if a worker is
     * prevented from executing tasks, even though it must continue to
     * scan for them to avoid queuing races. Note however that
     * eventCount updates lag releases so usage requires care.
     *
     * Field runState is an int packed with:
     * SHUTDOWN: true if shutdown is enabled (1 bit)
     * SEQ:  a sequence number updated upon (de)registering workers (30 bits)
     * INIT: set true after workQueues array construction (1 bit)
     *
     * The sequence number enables simple consistency checks:
     * Staleness of read-only operations on the workQueues array can
     * be checked by comparing runState before vs after the reads.
     */

    // bit positions/shifts for fields
    private static final int  AC_SHIFT   = 48;
    private static final int  TC_SHIFT   = 32;
    private static final int  ST_SHIFT   = 31;
    private static final int  EC_SHIFT   = 16;

    // bounds
    private static final int  SMASK      = 0xffff;  // short bits
    private static final int  MAX_CAP    = 0x7fff;  // max #workers - 1
    private static final int  SQMASK     = 0xfffe;  // even short bits
    private static final int  SHORT_SIGN = 1 << 15;
    private static final int  INT_SIGN   = 1 << 31;

    // masks
    private static final long STOP_BIT   = 0x0001L << ST_SHIFT;
    private static final long AC_MASK    = ((long)SMASK) << AC_SHIFT;
    private static final long TC_MASK    = ((long)SMASK) << TC_SHIFT;

    // units for incrementing and decrementing
    private static final long TC_UNIT    = 1L << TC_SHIFT;
    private static final long AC_UNIT    = 1L << AC_SHIFT;

    // masks and units for dealing with u = (int)(ctl >>> 32)
    private static final int  UAC_SHIFT  = AC_SHIFT - 32;
    private static final int  UTC_SHIFT  = TC_SHIFT - 32;
    private static final int  UAC_MASK   = SMASK << UAC_SHIFT;
    private static final int  UTC_MASK   = SMASK << UTC_SHIFT;
    private static final int  UAC_UNIT   = 1 << UAC_SHIFT;
    private static final int  UTC_UNIT   = 1 << UTC_SHIFT;

    // masks and units for dealing with e = (int)ctl
    private static final int E_MASK      = 0x7fffffff; // no STOP_BIT
    private static final int E_SEQ       = 1 << EC_SHIFT;

    // runState bits
    private static final int SHUTDOWN    = 1 << 31;

    // access mode for WorkQueue
    static final int LIFO_QUEUE          =  0;
    static final int FIFO_QUEUE          =  1;
    static final int SHARED_QUEUE        = -1;

    // Instance fields

    /*
     * Field layout order in this class tends to matter more than one
     * would like. Runtime layout order is only loosely related to
     * declaration order and may differ across JVMs, but the following
     * empirically works OK on current JVMs.
     */

    volatile long ctl;                         // main pool control
    final int parallelism;                     // parallelism level
    final int localMode;                       // per-worker scheduling mode
    final int submitMask;                      // submit queue index bound
    int nextSeed;                              // for initializing worker seeds
    volatile int runState;                     // shutdown status and seq
    WorkQueue[] workQueues;                    // main registry
    final Mutex lock;                          // for registration
    final Condition termination;               // for awaitTermination
    final ForkJoinWorkerThreadFactory factory; // factory for new workers
    final Thread.UncaughtExceptionHandler ueh; // per-worker UEH
    final AtomicLong stealCount;               // collect counts when terminated
    final AtomicInteger nextWorkerNumber;      // to create worker name string
    final String workerNamePrefix;             // to create worker name string

    //  Creating, registering, and deregistering workers

    /**
     * Tries to create and start a worker
     */
    private void addWorker() {
        Throwable ex = null;
        ForkJoinWorkerThread wt = null;
        try {
            if ((wt = factory.newThread(this)) != null) {
                wt.start();
                return;
            }
        } catch (Throwable e) {
            ex = e;
        }
        deregisterWorker(wt, ex); // adjust counts etc on failure
    }

    /**
     * Callback from ForkJoinWorkerThread constructor to assign a
     * public name. This must be separate from registerWorker because
     * it is called during the "super" constructor call in
     * ForkJoinWorkerThread.
     */
    final String nextWorkerName() {
        return workerNamePrefix.concat
            (Integer.toString(nextWorkerNumber.addAndGet(1)));
    }

    /**
     * Callback from ForkJoinWorkerThread constructor to establish its
     * poolIndex and record its WorkQueue. To avoid scanning bias due
     * to packing entries in front of the workQueues array, we treat
     * the array as a simple power-of-two hash table using per-thread
     * seed as hash, expanding as needed.
     *
     * @param w the worker's queue
     */

    final void registerWorker(WorkQueue w) {
        Mutex lock = this.lock;
        lock.lock();
        try {
            WorkQueue[] ws = workQueues;
            if (w != null && ws != null) {          // skip on shutdown/failure
                int rs, n =  ws.length, m = n - 1;
                int s = nextSeed += SEED_INCREMENT; // rarely-colliding sequence
                w.seed = (s == 0) ? 1 : s;          // ensure non-zero seed
                int r = (s << 1) | 1;               // use odd-numbered indices
                if (ws[r &= m] != null) {           // collision
                    int probes = 0;                 // step by approx half size
                    int step = (n <= 4) ? 2 : ((n >>> 1) & SQMASK) + 2;
                    while (ws[r = (r + step) & m] != null) {
                        if (++probes >= n) {
                            workQueues = ws = Arrays.copyOf(ws, n <<= 1);
                            m = n - 1;
                            probes = 0;
                        }
                    }
                }
                w.eventCount = w.poolIndex = r;     // establish before recording
                ws[r] = w;                          // also update seq
                runState = ((rs = runState) & SHUTDOWN) | ((rs + 2) & ~SHUTDOWN);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Final callback from terminating worker, as well as upon failure
     * to construct or start a worker in addWorker.  Removes record of
     * worker from array, and adjusts counts. If pool is shutting
     * down, tries to complete termination.
     *
     * @param wt the worker thread or null if addWorker failed
     * @param ex the exception causing failure, or null if none
     */
    final void deregisterWorker(ForkJoinWorkerThread wt, Throwable ex) {
        Mutex lock = this.lock;
        WorkQueue w = null;
        if (wt != null && (w = wt.workQueue) != null) {
            w.runState = -1;                // ensure runState is set
            stealCount.getAndAdd(w.totalSteals + w.nsteals);
            int idx = w.poolIndex;
            lock.lock();
            try {                           // remove record from array
                WorkQueue[] ws = workQueues;
                if (ws != null && idx >= 0 && idx < ws.length && ws[idx] == w)
                    ws[idx] = null;
            } finally {
                lock.unlock();
            }
        }

        long c;                             // adjust ctl counts
        do {} while (!U.compareAndSwapLong
                     (this, CTL, c = ctl, (((c - AC_UNIT) & AC_MASK) |
                                           ((c - TC_UNIT) & TC_MASK) |
                                           (c & ~(AC_MASK|TC_MASK)))));

        if (!tryTerminate(false, false) && w != null) {
            w.cancelAll();                  // cancel remaining tasks
            if (w.array != null)            // suppress signal if never ran
                signalWork();               // wake up or create replacement
            if (ex == null)                 // help clean refs on way out
                ForkJoinTask.helpExpungeStaleExceptions();
        }

        if (ex != null)                     // rethrow
            U.throwException(ex);
    }


    // Submissions

    /**
     * Unless shutting down, adds the given task to a submission queue
     * at submitter's current queue index (modulo submission
     * range). If no queue exists at the index, one is created.  If
     * the queue is busy, another index is randomly chosen. The
     * submitMask bounds the effective number of queues to the
     * (nearest power of two for) parallelism level.
     *
     * @param task the task. Caller must ensure non-null.
     */
    private void doSubmit(ForkJoinTask<?> task) {
        Submitter s = submitters.get();
        for (int r = s.seed, m = submitMask;;) {
            WorkQueue[] ws; WorkQueue q;
            int k = r & m & SQMASK;          // use only even indices
            if (runState < 0 || (ws = workQueues) == null || ws.length <= k)
                throw new RejectedExecutionException(); // shutting down
            else if ((q = ws[k]) == null) {  // create new queue
                WorkQueue nq = new WorkQueue(this, null, SHARED_QUEUE);
                Mutex lock = this.lock;      // construct outside lock
                lock.lock();
                try {                        // recheck under lock
                    int rs = runState;       // to update seq
                    if (ws == workQueues && ws[k] == null) {
                        ws[k] = nq;
                        runState = ((rs & SHUTDOWN) | ((rs + 2) & ~SHUTDOWN));
                    }
                } finally {
                    lock.unlock();
                }
            }
            else if (q.trySharedPush(task)) {
                signalWork();
                return;
            }
            else if (m > 1) {                // move to a different index
                r ^= r << 13;                // same xorshift as WorkQueues
                r ^= r >>> 17;
                s.seed = r ^= r << 5;
            }
            else
                Thread.yield();              // yield if no alternatives
        }
    }

    // Maintaining ctl counts

    /**
     * Increments active count; mainly called upon return from blocking.
     */
    final void incrementActiveCount() {
        long c;
        do {} while (!U.compareAndSwapLong(this, CTL, c = ctl, c + AC_UNIT));
    }

    /**
     * Tries to activate or create a worker if too few are active.
     */
    final void signalWork() {
        long c; int u;
        while ((u = (int)((c = ctl) >>> 32)) < 0) {     // too few active
            WorkQueue[] ws = workQueues; int e, i; WorkQueue w; Thread p;
            if ((e = (int)c) > 0) {                     // at least one waiting
                if (ws != null && (i = e & SMASK) < ws.length &&
                    (w = ws[i]) != null && w.eventCount == (e | INT_SIGN)) {
                    long nc = (((long)(w.nextWait & E_MASK)) |
                               ((long)(u + UAC_UNIT) << 32));
                    if (U.compareAndSwapLong(this, CTL, c, nc)) {
                        w.eventCount = (e + E_SEQ) & E_MASK;
                        if ((p = w.parker) != null)
                            U.unpark(p);                // activate and release
                        break;
                    }
                }
                else
                    break;
            }
            else if (e == 0 && (u & SHORT_SIGN) != 0) { // too few total
                long nc = (long)(((u + UTC_UNIT) & UTC_MASK) |
                                 ((u + UAC_UNIT) & UAC_MASK)) << 32;
                if (U.compareAndSwapLong(this, CTL, c, nc)) {
                    addWorker();
                    break;
                }
            }
            else
                break;
        }
    }

    // Scanning for tasks

    /**
     * Top-level runloop for workers, called by ForkJoinWorkerThread.run.
     */
    final void runWorker(WorkQueue w) {
        w.growArray(false);         // initialize queue array in this thread
        do { w.runTask(scan(w)); } while (w.runState >= 0);
    }

    /**
     * Scans for and, if found, returns one task, else possibly
     * inactivates the worker. This method operates on single reads of
     * volatile state and is designed to be re-invoked continuously,
     * in part because it returns upon detecting inconsistencies,
     * contention, or state changes that indicate possible success on
     * re-invocation.
     *
     * The scan searches for tasks across a random permutation of
     * queues (starting at a random index and stepping by a random
     * relative prime, checking each at least once).  The scan
     * terminates upon either finding a non-empty queue, or completing
     * the sweep. If the worker is not inactivated, it takes and
     * returns a task from this queue.  On failure to find a task, we
     * take one of the following actions, after which the caller will
     * retry calling this method unless terminated.
     *
     * * If pool is terminating, terminate the worker.
     *
     * * If not a complete sweep, try to release a waiting worker.  If
     * the scan terminated because the worker is inactivated, then the
     * released worker will often be the calling worker, and it can
     * succeed obtaining a task on the next call. Or maybe it is
     * another worker, but with same net effect. Releasing in other
     * cases as well ensures that we have enough workers running.
     *
     * * If not already enqueued, try to inactivate and enqueue the
     * worker on wait queue. Or, if inactivating has caused the pool
     * to be quiescent, relay to idleAwaitWork to check for
     * termination and possibly shrink pool.
     *
     * * If already inactive, and the caller has run a task since the
     * last empty scan, return (to allow rescan) unless others are
     * also inactivated.  Field WorkQueue.rescans counts down on each
     * scan to ensure eventual inactivation and blocking.
     *
     * * If already enqueued and none of the above apply, park
     * awaiting signal,
     *
     * @param w the worker (via its WorkQueue)
     * @return a task or null of none found
     */
    private final ForkJoinTask<?> scan(WorkQueue w) {
        WorkQueue[] ws;                       // first update random seed
        int r = w.seed; r ^= r << 13; r ^= r >>> 17; w.seed = r ^= r << 5;
        int rs = runState, m;                 // volatile read order matters
        if ((ws = workQueues) != null && (m = ws.length - 1) > 0) {
            int ec = w.eventCount;            // ec is negative if inactive
            int step = (r >>> 16) | 1;        // relative prime
            for (int j = (m + 1) << 2; ; r += step) {
                WorkQueue q; ForkJoinTask<?> t; ForkJoinTask<?>[] a; int b;
                if ((q = ws[r & m]) != null && (b = q.base) - q.top < 0 &&
                    (a = q.array) != null) {  // probably nonempty
                    int i = (((a.length - 1) & b) << ASHIFT) + ABASE;
                    t = (ForkJoinTask<?>)U.getObjectVolatile(a, i);
                    if (q.base == b && ec >= 0 && t != null &&
                        U.compareAndSwapObject(a, i, t, null)) {
                        if (q.top - (q.base = b + 1) > 1)
                            signalWork();    // help pushes signal
                        return t;
                    }
                    else if (ec < 0 || j <= m) {
                        rs = 0;               // mark scan as imcomplete
                        break;                // caller can retry after release
                    }
                }
                if (--j < 0)
                    break;
            }

            long c = ctl; int e = (int)c, a = (int)(c >> AC_SHIFT), nr, ns;
            if (e < 0)                        // decode ctl on empty scan
                w.runState = -1;              // pool is terminating
            else if (rs == 0 || rs != runState) { // incomplete scan
                WorkQueue v; Thread p;        // try to release a waiter
                if (e > 0 && a < 0 && w.eventCount == ec &&
                    (v = ws[e & m]) != null && v.eventCount == (e | INT_SIGN)) {
                    long nc = ((long)(v.nextWait & E_MASK) |
                               ((c + AC_UNIT) & (AC_MASK|TC_MASK)));
                    if (ctl == c && U.compareAndSwapLong(this, CTL, c, nc)) {
                        v.eventCount = (e + E_SEQ) & E_MASK;
                        if ((p = v.parker) != null)
                            U.unpark(p);
                    }
                }
            }
            else if (ec >= 0) {               // try to enqueue/inactivate
                long nc = (long)ec | ((c - AC_UNIT) & (AC_MASK|TC_MASK));
                w.nextWait = e;
                w.eventCount = ec | INT_SIGN; // mark as inactive
                if (ctl != c || !U.compareAndSwapLong(this, CTL, c, nc))
                    w.eventCount = ec;        // unmark on CAS failure
                else {
                    if ((ns = w.nsteals) != 0) {
                        w.nsteals = 0;        // set rescans if ran task
                        w.rescans = (a > 0) ? 0 : a + parallelism;
                        w.totalSteals += ns;
                    }
                    if (a == 1 - parallelism) // quiescent
                        idleAwaitWork(w, nc, c);
                }
            }
            else if (w.eventCount < 0) {      // already queued
                if ((nr = w.rescans) > 0) {   // continue rescanning
                    int ac = a + parallelism;
                    if (((w.rescans = (ac < nr) ? ac : nr - 1) & 3) == 0)
                        Thread.yield();       // yield before block
                }
                else {
                    Thread.interrupted();     // clear status
                    Thread wt = Thread.currentThread();
                    U.putObject(wt, PARKBLOCKER, this);
                    w.parker = wt;            // emulate LockSupport.park
                    if (w.eventCount < 0)     // recheck
                        U.park(false, 0L);
                    w.parker = null;
                    U.putObject(wt, PARKBLOCKER, null);
                }
            }
        }
        return null;
    }

    /**
     * If inactivating worker w has caused the pool to become
     * quiescent, checks for pool termination, and, so long as this is
     * not the only worker, waits for event for up to SHRINK_RATE
     * nanosecs.  On timeout, if ctl has not changed, terminates the
     * worker, which will in turn wake up another worker to possibly
     * repeat this process.
     *
     * @param w the calling worker
     * @param currentCtl the ctl value triggering possible quiescence
     * @param prevCtl the ctl value to restore if thread is terminated
     */
    private void idleAwaitWork(WorkQueue w, long currentCtl, long prevCtl) {
        if (w.eventCount < 0 && !tryTerminate(false, false) &&
            (int)prevCtl != 0 && !hasQueuedSubmissions() && ctl == currentCtl) {
            Thread wt = Thread.currentThread();
            Thread.yield();            // yield before block
            while (ctl == currentCtl) {
                long startTime = System.nanoTime();
                Thread.interrupted();  // timed variant of version in scan()
                U.putObject(wt, PARKBLOCKER, this);
                w.parker = wt;
                if (ctl == currentCtl)
                    U.park(false, SHRINK_RATE);
                w.parker = null;
                U.putObject(wt, PARKBLOCKER, null);
                if (ctl != currentCtl)
                    break;
                if (System.nanoTime() - startTime >= SHRINK_TIMEOUT &&
                    U.compareAndSwapLong(this, CTL, currentCtl, prevCtl)) {
                    w.eventCount = (w.eventCount + E_SEQ) | E_MASK;
                    w.runState = -1;   // shrink
                    break;
                }
            }
        }
    }

    /**
     * Tries to locate and execute tasks for a stealer of the given
     * task, or in turn one of its stealers, Traces currentSteal ->
     * currentJoin links looking for a thread working on a descendant
     * of the given task and with a non-empty queue to steal back and
     * execute tasks from. The first call to this method upon a
     * waiting join will often entail scanning/search, (which is OK
     * because the joiner has nothing better to do), but this method
     * leaves hints in workers to speed up subsequent calls. The
     * implementation is very branchy to cope with potential
     * inconsistencies or loops encountering chains that are stale,
     * unknown, or so long that they are likely cyclic.
     *
     * @param joiner the joining worker
     * @param task the task to join
     * @return 0 if no progress can be made, negative if task
     * known complete, else positive
     */
    private int tryHelpStealer(WorkQueue joiner, ForkJoinTask<?> task) {
        int stat = 0, steps = 0;                    // bound to avoid cycles
        if (joiner != null && task != null) {       // hoist null checks
            restart: for (;;) {
                ForkJoinTask<?> subtask = task;     // current target
                for (WorkQueue j = joiner, v;;) {   // v is stealer of subtask
                    WorkQueue[] ws; int m, s, h;
                    if ((s = task.status) < 0) {
                        stat = s;
                        break restart;
                    }
                    if ((ws = workQueues) == null || (m = ws.length - 1) <= 0)
                        break restart;              // shutting down
                    if ((v = ws[h = (j.stealHint | 1) & m]) == null ||
                        v.currentSteal != subtask) {
                        for (int origin = h;;) {    // find stealer
                            if (((h = (h + 2) & m) & 15) == 1 &&
                                (subtask.status < 0 || j.currentJoin != subtask))
                                continue restart;   // occasional staleness check
                            if ((v = ws[h]) != null &&
                                v.currentSteal == subtask) {
                                j.stealHint = h;    // save hint
                                break;
                            }
                            if (h == origin)
                                break restart;      // cannot find stealer
                        }
                    }
                    for (;;) { // help stealer or descend to its stealer
                        ForkJoinTask[] a;  int b;
                        if (subtask.status < 0)     // surround probes with
                            continue restart;       //   consistency checks
                        if ((b = v.base) - v.top < 0 && (a = v.array) != null) {
                            int i = (((a.length - 1) & b) << ASHIFT) + ABASE;
                            ForkJoinTask<?> t =
                                (ForkJoinTask<?>)U.getObjectVolatile(a, i);
                            if (subtask.status < 0 || j.currentJoin != subtask ||
                                v.currentSteal != subtask)
                                continue restart;   // stale
                            stat = 1;               // apparent progress
                            if (t != null && v.base == b &&
                                U.compareAndSwapObject(a, i, t, null)) {
                                v.base = b + 1;     // help stealer
                                joiner.runSubtask(t);
                            }
                            else if (v.base == b && ++steps == MAX_HELP)
                                break restart;      // v apparently stalled
                        }
                        else {                      // empty -- try to descend
                            ForkJoinTask<?> next = v.currentJoin;
                            if (subtask.status < 0 || j.currentJoin != subtask ||
                                v.currentSteal != subtask)
                                continue restart;   // stale
                            else if (next == null || ++steps == MAX_HELP)
                                break restart;      // dead-end or maybe cyclic
                            else {
                                subtask = next;
                                j = v;
                                break;
                            }
                        }
                    }
                }
            }
        }
        return stat;
    }

    /**
     * If task is at base of some steal queue, steals and executes it.
     *
     * @param joiner the joining worker
     * @param task the task
     */
    private void tryPollForAndExec(WorkQueue joiner, ForkJoinTask<?> task) {
        WorkQueue[] ws;
        if ((ws = workQueues) != null) {
            for (int j = 1; j < ws.length && task.status >= 0; j += 2) {
                WorkQueue q = ws[j];
                if (q != null && q.pollFor(task)) {
                    joiner.runSubtask(task);
                    break;
                }
            }
        }
    }

    /**
     * Tries to decrement active count (sometimes implicitly) and
     * possibly release or create a compensating worker in preparation
     * for blocking. Fails on contention or termination. Otherwise,
     * adds a new thread if no idle workers are available and either
     * pool would become completely starved or: (at least half
     * starved, and fewer than 50% spares exist, and there is at least
     * one task apparently available). Even though the availability
     * check requires a full scan, it is worthwhile in reducing false
     * alarms.
     *
     * @param task if non-null, a task being waited for
     * @param blocker if non-null, a blocker being waited for
     * @return true if the caller can block, else should recheck and retry
     */
    final boolean tryCompensate(ForkJoinTask<?> task, ManagedBlocker blocker) {
        int pc = parallelism, e;
        long c = ctl;
        WorkQueue[] ws = workQueues;
        if ((e = (int)c) >= 0 && ws != null) {
            int u, a, ac, hc;
            int tc = (short)((u = (int)(c >>> 32)) >>> UTC_SHIFT) + pc;
            boolean replace = false;
            if ((a = u >> UAC_SHIFT) <= 0) {
                if ((ac = a + pc) <= 1)
                    replace = true;
                else if ((e > 0 || (task != null &&
                                    ac <= (hc = pc >>> 1) && tc < pc + hc))) {
                    WorkQueue w;
                    for (int j = 0; j < ws.length; ++j) {
                        if ((w = ws[j]) != null && !w.isEmpty()) {
                            replace = true;
                            break;   // in compensation range and tasks available
                        }
                    }
                }
            }
            if ((task == null || task.status >= 0) && // recheck need to block
                (blocker == null || !blocker.isReleasable()) && ctl == c) {
                if (!replace) {          // no compensation
                    long nc = ((c - AC_UNIT) & AC_MASK) | (c & ~AC_MASK);
                    if (U.compareAndSwapLong(this, CTL, c, nc))
                        return true;
                }
                else if (e != 0) {       // release an idle worker
                    WorkQueue w; Thread p; int i;
                    if ((i = e & SMASK) < ws.length && (w = ws[i]) != null) {
                        long nc = ((long)(w.nextWait & E_MASK) |
                                   (c & (AC_MASK|TC_MASK)));
                        if (w.eventCount == (e | INT_SIGN) &&
                            U.compareAndSwapLong(this, CTL, c, nc)) {
                            w.eventCount = (e + E_SEQ) & E_MASK;
                            if ((p = w.parker) != null)
                                U.unpark(p);
                            return true;
                        }
                    }
                }
                else if (tc < MAX_CAP) { // create replacement
                    long nc = ((c + TC_UNIT) & TC_MASK) | (c & ~TC_MASK);
                    if (U.compareAndSwapLong(this, CTL, c, nc)) {
                        addWorker();
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /**
     * Helps and/or blocks until the given task is done.
     *
     * @param joiner the joining worker
     * @param task the task
     * @return task status on exit
     */
    final int awaitJoin(WorkQueue joiner, ForkJoinTask<?> task) {
        int s;
        if ((s = task.status) >= 0) {
            ForkJoinTask<?> prevJoin = joiner.currentJoin;
            joiner.currentJoin = task;
            long startTime = 0L;
            for (int k = 0;;) {
                if ((s = (joiner.isEmpty() ?           // try to help
                          tryHelpStealer(joiner, task) :
                          joiner.tryRemoveAndExec(task))) == 0 &&
                    (s = task.status) >= 0) {
                    if (k == 0) {
                        startTime = System.nanoTime();
                        tryPollForAndExec(joiner, task); // check uncommon case
                    }
                    else if ((k & (MAX_HELP - 1)) == 0 &&
                             System.nanoTime() - startTime >=
                             COMPENSATION_DELAY &&
                             tryCompensate(task, null)) {
                        if (task.trySetSignal()) {
                            synchronized (task) {
                                if (task.status >= 0) {
                                    try {                // see ForkJoinTask
                                        task.wait();     //  for explanation
                                    } catch (InterruptedException ie) {
                                    }
                                }
                                else
                                    task.notifyAll();
                            }
                        }
                        long c;                          // re-activate
                        do {} while (!U.compareAndSwapLong
                                     (this, CTL, c = ctl, c + AC_UNIT));
                    }
                }
                if (s < 0 || (s = task.status) < 0) {
                    joiner.currentJoin = prevJoin;
                    break;
                }
                else if ((k++ & (MAX_HELP - 1)) == MAX_HELP >>> 1)
                    Thread.yield();                     // for politeness
            }
        }
        return s;
    }

    /**
     * Stripped-down variant of awaitJoin used by timed joins. Tries
     * to help join only while there is continuous progress. (Caller
     * will then enter a timed wait.)
     *
     * @param joiner the joining worker
     * @param task the task
     * @return task status on exit
     */
    final int helpJoinOnce(WorkQueue joiner, ForkJoinTask<?> task) {
        int s;
        while ((s = task.status) >= 0 &&
               (joiner.isEmpty() ?
                tryHelpStealer(joiner, task) :
                joiner.tryRemoveAndExec(task)) != 0)
            ;
        return s;
    }

    /**
     * Returns a (probably) non-empty steal queue, if one is found
     * during a random, then cyclic scan, else null.  This method must
     * be retried by caller if, by the time it tries to use the queue,
     * it is empty.
     */
    private WorkQueue findNonEmptyStealQueue(WorkQueue w) {
        // Similar to loop in scan(), but ignoring submissions
        int r = w.seed; r ^= r << 13; r ^= r >>> 17; w.seed = r ^= r << 5;
        int step = (r >>> 16) | 1;
        for (WorkQueue[] ws;;) {
            int rs = runState, m;
            if ((ws = workQueues) == null || (m = ws.length - 1) < 1)
                return null;
            for (int j = (m + 1) << 2; ; r += step) {
                WorkQueue q = ws[((r << 1) | 1) & m];
                if (q != null && !q.isEmpty())
                    return q;
                else if (--j < 0) {
                    if (runState == rs)
                        return null;
                    break;
                }
            }
        }
    }


    /**
     * Runs tasks until {@code isQuiescent()}. We piggyback on
     * active count ctl maintenance, but rather than blocking
     * when tasks cannot be found, we rescan until all others cannot
     * find tasks either.
     */
    final void helpQuiescePool(WorkQueue w) {
        for (boolean active = true;;) {
            ForkJoinTask<?> localTask; // exhaust local queue
            while ((localTask = w.nextLocalTask()) != null)
                localTask.doExec();
            WorkQueue q = findNonEmptyStealQueue(w);
            if (q != null) {
                ForkJoinTask<?> t; int b;
                if (!active) {      // re-establish active count
                    long c;
                    active = true;
                    do {} while (!U.compareAndSwapLong
                                 (this, CTL, c = ctl, c + AC_UNIT));
                }
                if ((b = q.base) - q.top < 0 && (t = q.pollAt(b)) != null)
                    w.runSubtask(t);
            }
            else {
                long c;
                if (active) {       // decrement active count without queuing
                    active = false;
                    do {} while (!U.compareAndSwapLong
                                 (this, CTL, c = ctl, c -= AC_UNIT));
                }
                else
                    c = ctl;        // re-increment on exit
                if ((int)(c >> AC_SHIFT) + parallelism == 0) {
                    do {} while (!U.compareAndSwapLong
                                 (this, CTL, c = ctl, c + AC_UNIT));
                    break;
                }
            }
        }
    }

    /**
     * Gets and removes a local or stolen task for the given worker.
     *
     * @return a task, if available
     */
    final ForkJoinTask<?> nextTaskFor(WorkQueue w) {
        for (ForkJoinTask<?> t;;) {
            WorkQueue q; int b;
            if ((t = w.nextLocalTask()) != null)
                return t;
            if ((q = findNonEmptyStealQueue(w)) == null)
                return null;
            if ((b = q.base) - q.top < 0 && (t = q.pollAt(b)) != null)
                return t;
        }
    }

    /**
     * Returns the approximate (non-atomic) number of idle threads per
     * active thread to offset steal queue size for method
     * ForkJoinTask.getSurplusQueuedTaskCount().
     */
    final int idlePerActive() {
        // Approximate at powers of two for small values, saturate past 4
        int p = parallelism;
        int a = p + (int)(ctl >> AC_SHIFT);
        return (a > (p >>>= 1) ? 0 :
                a > (p >>>= 1) ? 1 :
                a > (p >>>= 1) ? 2 :
                a > (p >>>= 1) ? 4 :
                8);
    }

    //  Termination

    /**
     * Possibly initiates and/or completes termination.  The caller
     * triggering termination runs three passes through workQueues:
     * (0) Setting termination status, followed by wakeups of queued
     * workers; (1) cancelling all tasks; (2) interrupting lagging
     * threads (likely in external tasks, but possibly also blocked in
     * joins).  Each pass repeats previous steps because of potential
     * lagging thread creation.
     *
     * @param now if true, unconditionally terminate, else only
     * if no work and no active workers
     * @param enable if true, enable shutdown when next possible
     * @return true if now terminating or terminated
     */
    private boolean tryTerminate(boolean now, boolean enable) {
        Mutex lock = this.lock;
        for (long c;;) {
            if (((c = ctl) & STOP_BIT) != 0) {      // already terminating
                if ((short)(c >>> TC_SHIFT) == -parallelism) {
                    lock.lock();                    // don't need try/finally
                    termination.signalAll();        // signal when 0 workers
                    lock.unlock();
                }
                return true;
            }
            if (runState >= 0) {                    // not yet enabled
                if (!enable)
                    return false;
                lock.lock();
                runState |= SHUTDOWN;
                lock.unlock();
            }
            if (!now) {                             // check if idle & no tasks
                if ((int)(c >> AC_SHIFT) != -parallelism ||
                    hasQueuedSubmissions())
                    return false;
                // Check for unqueued inactive workers. One pass suffices.
                WorkQueue[] ws = workQueues; WorkQueue w;
                if (ws != null) {
                    for (int i = 1; i < ws.length; i += 2) {
                        if ((w = ws[i]) != null && w.eventCount >= 0)
                            return false;
                    }
                }
            }
            if (U.compareAndSwapLong(this, CTL, c, c | STOP_BIT)) {
                for (int pass = 0; pass < 3; ++pass) {
                    WorkQueue[] ws = workQueues;
                    if (ws != null) {
                        WorkQueue w;
                        int n = ws.length;
                        for (int i = 0; i < n; ++i) {
                            if ((w = ws[i]) != null) {
                                w.runState = -1;
                                if (pass > 0) {
                                    w.cancelAll();
                                    if (pass > 1)
                                        w.interruptOwner();
                                }
                            }
                        }
                        // Wake up workers parked on event queue
                        int i, e; long cc; Thread p;
                        while ((e = (int)(cc = ctl) & E_MASK) != 0 &&
                               (i = e & SMASK) < n &&
                               (w = ws[i]) != null) {
                            long nc = ((long)(w.nextWait & E_MASK) |
                                       ((cc + AC_UNIT) & AC_MASK) |
                                       (cc & (TC_MASK|STOP_BIT)));
                            if (w.eventCount == (e | INT_SIGN) &&
                                U.compareAndSwapLong(this, CTL, cc, nc)) {
                                w.eventCount = (e + E_SEQ) & E_MASK;
                                w.runState = -1;
                                if ((p = w.parker) != null)
                                    U.unpark(p);
                            }
                        }
                    }
                }
            }
        }
    }

    // Exported methods

    // Constructors

    /**
     * Creates a {@code ForkJoinPool} with parallelism equal to {@link
     * java.lang.Runtime#availableProcessors}, using the {@linkplain
     * #defaultForkJoinWorkerThreadFactory default thread factory},
     * no UncaughtExceptionHandler, and non-async LIFO processing mode.
     *
     * @throws SecurityException if a security manager exists and
     *         the caller is not permitted to modify threads
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool() {
        this(Runtime.getRuntime().availableProcessors(),
             defaultForkJoinWorkerThreadFactory, null, false);
    }

    /**
     * Creates a {@code ForkJoinPool} with the indicated parallelism
     * level, the {@linkplain
     * #defaultForkJoinWorkerThreadFactory default thread factory},
     * no UncaughtExceptionHandler, and non-async LIFO processing mode.
     *
     * @param parallelism the parallelism level
     * @throws IllegalArgumentException if parallelism less than or
     *         equal to zero, or greater than implementation limit
     * @throws SecurityException if a security manager exists and
     *         the caller is not permitted to modify threads
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool(int parallelism) {
        this(parallelism, defaultForkJoinWorkerThreadFactory, null, false);
    }

    /**
     * Creates a {@code ForkJoinPool} with the given parameters.
     *
     * @param parallelism the parallelism level. For default value,
     * use {@link java.lang.Runtime#availableProcessors}.
     * @param factory the factory for creating new threads. For default value,
     * use {@link #defaultForkJoinWorkerThreadFactory}.
     * @param handler the handler for internal worker threads that
     * terminate due to unrecoverable errors encountered while executing
     * tasks. For default value, use {@code null}.
     * @param asyncMode if true,
     * establishes local first-in-first-out scheduling mode for forked
     * tasks that are never joined. This mode may be more appropriate
     * than default locally stack-based mode in applications in which
     * worker threads only process event-style asynchronous tasks.
     * For default value, use {@code false}.
     * @throws IllegalArgumentException if parallelism less than or
     *         equal to zero, or greater than implementation limit
     * @throws NullPointerException if the factory is null
     * @throws SecurityException if a security manager exists and
     *         the caller is not permitted to modify threads
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool(int parallelism,
                        ForkJoinWorkerThreadFactory factory,
                        Thread.UncaughtExceptionHandler handler,
                        boolean asyncMode) {
        checkPermission();
        if (factory == null)
            throw new NullPointerException();
        if (parallelism <= 0 || parallelism > MAX_CAP)
            throw new IllegalArgumentException();
        this.parallelism = parallelism;
        this.factory = factory;
        this.ueh = handler;
        this.localMode = asyncMode ? FIFO_QUEUE : LIFO_QUEUE;
        long np = (long)(-parallelism); // offset ctl counts
        this.ctl = ((np << AC_SHIFT) & AC_MASK) | ((np << TC_SHIFT) & TC_MASK);
        // Use nearest power 2 for workQueues size. See Hackers Delight sec 3.2.
        int n = parallelism - 1;
        n |= n >>> 1; n |= n >>> 2; n |= n >>> 4; n |= n >>> 8; n |= n >>> 16;
        int size = (n + 1) << 1;        // #slots = 2*#workers
        this.submitMask = size - 1;     // room for max # of submit queues
        this.workQueues = new WorkQueue[size];
        this.termination = (this.lock = new Mutex()).newCondition();
        this.stealCount = new AtomicLong();
        this.nextWorkerNumber = new AtomicInteger();
        int pn = poolNumberGenerator.incrementAndGet();
        StringBuilder sb = new StringBuilder("ForkJoinPool-");
        sb.append(Integer.toString(pn));
        sb.append("-worker-");
        this.workerNamePrefix = sb.toString();
        lock.lock();
        this.runState = 1;              // set init flag
        lock.unlock();
    }

    // Execution methods

    /**
     * Performs the given task, returning its result upon completion.
     * If the computation encounters an unchecked Exception or Error,
     * it is rethrown as the outcome of this invocation.  Rethrown
     * exceptions behave in the same way as regular exceptions, but,
     * when possible, contain stack traces (as displayed for example
     * using {@code ex.printStackTrace()}) of both the current thread
     * as well as the thread actually encountering the exception;
     * minimally only the latter.
     *
     * @param task the task
     * @return the task's result
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> T invoke(ForkJoinTask<T> task) {
        if (task == null)
            throw new NullPointerException();
        doSubmit(task);
        return task.join();
    }

    /**
     * Arranges for (asynchronous) execution of the given task.
     *
     * @param task the task
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public void execute(ForkJoinTask<?> task) {
        if (task == null)
            throw new NullPointerException();
        doSubmit(task);
    }

    // AbstractExecutorService methods

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public void execute(Runnable task) {
        if (task == null)
            throw new NullPointerException();
        ForkJoinTask<?> job;
        if (task instanceof ForkJoinTask<?>) // avoid re-wrap
            job = (ForkJoinTask<?>) task;
        else
            job = new ForkJoinTask.AdaptedRunnableAction(task);
        doSubmit(job);
    }

    /**
     * Submits a ForkJoinTask for execution.
     *
     * @param task the task to submit
     * @return the task
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
        if (task == null)
            throw new NullPointerException();
        doSubmit(task);
        return task;
    }

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Callable<T> task) {
        ForkJoinTask<T> job = new ForkJoinTask.AdaptedCallable<T>(task);
        doSubmit(job);
        return job;
    }

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Runnable task, T result) {
        ForkJoinTask<T> job = new ForkJoinTask.AdaptedRunnable<T>(task, result);
        doSubmit(job);
        return job;
    }

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public ForkJoinTask<?> submit(Runnable task) {
        if (task == null)
            throw new NullPointerException();
        ForkJoinTask<?> job;
        if (task instanceof ForkJoinTask<?>) // avoid re-wrap
            job = (ForkJoinTask<?>) task;
        else
            job = new ForkJoinTask.AdaptedRunnableAction(task);
        doSubmit(job);
        return job;
    }

    /**
     * @throws NullPointerException       {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        // In previous versions of this class, this method constructed
        // a task to run ForkJoinTask.invokeAll, but now external
        // invocation of multiple tasks is at least as efficient.
        List<ForkJoinTask<T>> fs = new ArrayList<ForkJoinTask<T>>(tasks.size());
        // Workaround needed because method wasn't declared with
        // wildcards in return type but should have been.
        @SuppressWarnings({"unchecked", "rawtypes"})
            List<Future<T>> futures = (List<Future<T>>) (List) fs;

        boolean done = false;
        try {
            for (Callable<T> t : tasks) {
                ForkJoinTask<T> f = new ForkJoinTask.AdaptedCallable<T>(t);
                doSubmit(f);
                fs.add(f);
            }
            for (ForkJoinTask<T> f : fs)
                f.quietlyJoin();
            done = true;
            return futures;
        } finally {
            if (!done)
                for (ForkJoinTask<T> f : fs)
                    f.cancel(false);
        }
    }

    /**
     * Returns the factory used for constructing new workers.
     *
     * @return the factory used for constructing new workers
     */
    public ForkJoinWorkerThreadFactory getFactory() {
        return factory;
    }

    /**
     * Returns the handler for internal worker threads that terminate
     * due to unrecoverable errors encountered while executing tasks.
     *
     * @return the handler, or {@code null} if none
     */
    public Thread.UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return ueh;
    }

    /**
     * Returns the targeted parallelism level of this pool.
     *
     * @return the targeted parallelism level of this pool
     */
    public int getParallelism() {
        return parallelism;
    }

    /**
     * Returns the number of worker threads that have started but not
     * yet terminated.  The result returned by this method may differ
     * from {@link #getParallelism} when threads are created to
     * maintain parallelism when others are cooperatively blocked.
     *
     * @return the number of worker threads
     */
    public int getPoolSize() {
        return parallelism + (short)(ctl >>> TC_SHIFT);
    }

    /**
     * Returns {@code true} if this pool uses local first-in-first-out
     * scheduling mode for forked tasks that are never joined.
     *
     * @return {@code true} if this pool uses async mode
     */
    public boolean getAsyncMode() {
        return localMode != 0;
    }

    /**
     * Returns an estimate of the number of worker threads that are
     * not blocked waiting to join tasks or for other managed
     * synchronization. This method may overestimate the
     * number of running threads.
     *
     * @return the number of worker threads
     */
    public int getRunningThreadCount() {
        int rc = 0;
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 1; i < ws.length; i += 2) {
                if ((w = ws[i]) != null && w.isApparentlyUnblocked())
                    ++rc;
            }
        }
        return rc;
    }

    /**
     * Returns an estimate of the number of threads that are currently
     * stealing or executing tasks. This method may overestimate the
     * number of active threads.
     *
     * @return the number of active threads
     */
    public int getActiveThreadCount() {
        int r = parallelism + (int)(ctl >> AC_SHIFT);
        return (r <= 0) ? 0 : r; // suppress momentarily negative values
    }

    /**
     * Returns {@code true} if all worker threads are currently idle.
     * An idle worker is one that cannot obtain a task to execute
     * because none are available to steal from other threads, and
     * there are no pending submissions to the pool. This method is
     * conservative; it might not return {@code true} immediately upon
     * idleness of all threads, but will eventually become true if
     * threads remain inactive.
     *
     * @return {@code true} if all threads are currently idle
     */
    public boolean isQuiescent() {
        return (int)(ctl >> AC_SHIFT) + parallelism == 0;
    }

    /**
     * Returns an estimate of the total number of tasks stolen from
     * one thread's work queue by another. The reported value
     * underestimates the actual total number of steals when the pool
     * is not quiescent. This value may be useful for monitoring and
     * tuning fork/join programs: in general, steal counts should be
     * high enough to keep threads busy, but low enough to avoid
     * overhead and contention across threads.
     *
     * @return the number of steals
     */
    public long getStealCount() {
        long count = stealCount.get();
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 1; i < ws.length; i += 2) {
                if ((w = ws[i]) != null)
                    count += w.totalSteals;
            }
        }
        return count;
    }

    /**
     * Returns an estimate of the total number of tasks currently held
     * in queues by worker threads (but not including tasks submitted
     * to the pool that have not begun executing). This value is only
     * an approximation, obtained by iterating across all threads in
     * the pool. This method may be useful for tuning task
     * granularities.
     *
     * @return the number of queued tasks
     */
    public long getQueuedTaskCount() {
        long count = 0;
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 1; i < ws.length; i += 2) {
                if ((w = ws[i]) != null)
                    count += w.queueSize();
            }
        }
        return count;
    }

    /**
     * Returns an estimate of the number of tasks submitted to this
     * pool that have not yet begun executing.  This method may take
     * time proportional to the number of submissions.
     *
     * @return the number of queued submissions
     */
    public int getQueuedSubmissionCount() {
        int count = 0;
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 0; i < ws.length; i += 2) {
                if ((w = ws[i]) != null)
                    count += w.queueSize();
            }
        }
        return count;
    }

    /**
     * Returns {@code true} if there are any tasks submitted to this
     * pool that have not yet begun executing.
     *
     * @return {@code true} if there are any queued submissions
     */
    public boolean hasQueuedSubmissions() {
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 0; i < ws.length; i += 2) {
                if ((w = ws[i]) != null && !w.isEmpty())
                    return true;
            }
        }
        return false;
    }

    /**
     * Removes and returns the next unexecuted submission if one is
     * available.  This method may be useful in extensions to this
     * class that re-assign work in systems with multiple pools.
     *
     * @return the next submission, or {@code null} if none
     */
    protected ForkJoinTask<?> pollSubmission() {
        WorkQueue[] ws; WorkQueue w; ForkJoinTask<?> t;
        if ((ws = workQueues) != null) {
            for (int i = 0; i < ws.length; i += 2) {
                if ((w = ws[i]) != null && (t = w.poll()) != null)
                    return t;
            }
        }
        return null;
    }

    /**
     * Removes all available unexecuted submitted and forked tasks
     * from scheduling queues and adds them to the given collection,
     * without altering their execution status. These may include
     * artificially generated or wrapped tasks. This method is
     * designed to be invoked only when the pool is known to be
     * quiescent. Invocations at other times may not remove all
     * tasks. A failure encountered while attempting to add elements
     * to collection {@code c} may result in elements being in
     * neither, either or both collections when the associated
     * exception is thrown.  The behavior of this operation is
     * undefined if the specified collection is modified while the
     * operation is in progress.
     *
     * @param c the collection to transfer elements into
     * @return the number of elements transferred
     */
    protected int drainTasksTo(Collection<? super ForkJoinTask<?>> c) {
        int count = 0;
        WorkQueue[] ws; WorkQueue w; ForkJoinTask<?> t;
        if ((ws = workQueues) != null) {
            for (int i = 0; i < ws.length; ++i) {
                if ((w = ws[i]) != null) {
                    while ((t = w.poll()) != null) {
                        c.add(t);
                        ++count;
                    }
                }
            }
        }
        return count;
    }

    /**
     * Returns a string identifying this pool, as well as its state,
     * including indications of run state, parallelism level, and
     * worker and task counts.
     *
     * @return a string identifying this pool, as well as its state
     */
    public String toString() {
        // Use a single pass through workQueues to collect counts
        long qt = 0L, qs = 0L; int rc = 0;
        long st = stealCount.get();
        long c = ctl;
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 0; i < ws.length; ++i) {
                if ((w = ws[i]) != null) {
                    int size = w.queueSize();
                    if ((i & 1) == 0)
                        qs += size;
                    else {
                        qt += size;
                        st += w.totalSteals;
                        if (w.isApparentlyUnblocked())
                            ++rc;
                    }
                }
            }
        }
        int pc = parallelism;
        int tc = pc + (short)(c >>> TC_SHIFT);
        int ac = pc + (int)(c >> AC_SHIFT);
        if (ac < 0) // ignore transient negative
            ac = 0;
        String level;
        if ((c & STOP_BIT) != 0)
            level = (tc == 0) ? "Terminated" : "Terminating";
        else
            level = runState < 0 ? "Shutting down" : "Running";
        return super.toString() +
            "[" + level +
            ", parallelism = " + pc +
            ", size = " + tc +
            ", active = " + ac +
            ", running = " + rc +
            ", steals = " + st +
            ", tasks = " + qt +
            ", submissions = " + qs +
            "]";
    }

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     * Tasks that are in the process of being submitted concurrently
     * during the course of this method may or may not be rejected.
     *
     * @throws SecurityException if a security manager exists and
     *         the caller is not permitted to modify threads
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public void shutdown() {
        checkPermission();
        tryTerminate(false, true);
    }

    /**
     * Attempts to cancel and/or stop all tasks, and reject all
     * subsequently submitted tasks.  Tasks that are in the process of
     * being submitted or executed concurrently during the course of
     * this method may or may not be rejected. This method cancels
     * both existing and unexecuted tasks, in order to permit
     * termination in the presence of task dependencies. So the method
     * always returns an empty list (unlike the case for some other
     * Executors).
     *
     * @return an empty list
     * @throws SecurityException if a security manager exists and
     *         the caller is not permitted to modify threads
     *         because it does not hold {@link
     *         java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public List<Runnable> shutdownNow() {
        checkPermission();
        tryTerminate(true, true);
        return Collections.emptyList();
    }

    /**
     * Returns {@code true} if all tasks have completed following shut down.
     *
     * @return {@code true} if all tasks have completed following shut down
     */
    public boolean isTerminated() {
        long c = ctl;
        return ((c & STOP_BIT) != 0L &&
                (short)(c >>> TC_SHIFT) == -parallelism);
    }

    /**
     * Returns {@code true} if the process of termination has
     * commenced but not yet completed.  This method may be useful for
     * debugging. A return of {@code true} reported a sufficient
     * period after shutdown may indicate that submitted tasks have
     * ignored or suppressed interruption, or are waiting for IO,
     * causing this executor not to properly terminate. (See the
     * advisory notes for class {@link ForkJoinTask} stating that
     * tasks should not normally entail blocking operations.  But if
     * they do, they must abort them on interrupt.)
     *
     * @return {@code true} if terminating but not yet terminated
     */
    public boolean isTerminating() {
        long c = ctl;
        return ((c & STOP_BIT) != 0L &&
                (short)(c >>> TC_SHIFT) != -parallelism);
    }

    /**
     * Returns {@code true} if this pool has been shut down.
     *
     * @return {@code true} if this pool has been shut down
     */
    public boolean isShutdown() {
        return runState < 0;
    }

    /**
     * Blocks until all tasks have completed execution after a shutdown
     * request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if this executor terminated and
     *         {@code false} if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
        long nanos = unit.toNanos(timeout);
        final Mutex lock = this.lock;
        lock.lock();
        try {
            for (;;) {
                if (isTerminated())
                    return true;
                if (nanos <= 0)
                    return false;
                nanos = termination.awaitNanos(nanos);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Interface for extending managed parallelism for tasks running
     * in {@link ForkJoinPool}s.
     *
     * <p>A {@code ManagedBlocker} provides two methods.  Method
     * {@code isReleasable} must return {@code true} if blocking is
     * not necessary. Method {@code block} blocks the current thread
     * if necessary (perhaps internally invoking {@code isReleasable}
     * before actually blocking). These actions are performed by any
     * thread invoking {@link ForkJoinPool#managedBlock}.  The
     * unusual methods in this API accommodate synchronizers that may,
     * but don't usually, block for long periods. Similarly, they
     * allow more efficient internal handling of cases in which
     * additional workers may be, but usually are not, needed to
     * ensure sufficient parallelism.  Toward this end,
     * implementations of method {@code isReleasable} must be amenable
     * to repeated invocation.
     *
     * <p>For example, here is a ManagedBlocker based on a
     * ReentrantLock:
     *  <pre> {@code
     * class ManagedLocker implements ManagedBlocker {
     *   final ReentrantLock lock;
     *   boolean hasLock = false;
     *   ManagedLocker(ReentrantLock lock) { this.lock = lock; }
     *   public boolean block() {
     *     if (!hasLock)
     *       lock.lock();
     *     return true;
     *   }
     *   public boolean isReleasable() {
     *     return hasLock || (hasLock = lock.tryLock());
     *   }
     * }}</pre>
     *
     * <p>Here is a class that possibly blocks waiting for an
     * item on a given queue:
     *  <pre> {@code
     * class QueueTaker<E> implements ManagedBlocker {
     *   final BlockingQueue<E> queue;
     *   volatile E item = null;
     *   QueueTaker(BlockingQueue<E> q) { this.queue = q; }
     *   public boolean block() throws InterruptedException {
     *     if (item == null)
     *       item = queue.take();
     *     return true;
     *   }
     *   public boolean isReleasable() {
     *     return item != null || (item = queue.poll()) != null;
     *   }
     *   public E getItem() { // call after pool.managedBlock completes
     *     return item;
     *   }
     * }}</pre>
     */
    public static interface ManagedBlocker {
        /**
         * Possibly blocks the current thread, for example waiting for
         * a lock or condition.
         *
         * @return {@code true} if no additional blocking is necessary
         * (i.e., if isReleasable would return true)
         * @throws InterruptedException if interrupted while waiting
         * (the method is not required to do so, but is allowed to)
         */
        boolean block() throws InterruptedException;

        /**
         * Returns {@code true} if blocking is unnecessary.
         */
        boolean isReleasable();
    }

    /**
     * Blocks in accord with the given blocker.  If the current thread
     * is a {@link ForkJoinWorkerThread}, this method possibly
     * arranges for a spare thread to be activated if necessary to
     * ensure sufficient parallelism while the current thread is blocked.
     *
     * <p>If the caller is not a {@link ForkJoinTask}, this method is
     * behaviorally equivalent to
     *  <pre> {@code
     * while (!blocker.isReleasable())
     *   if (blocker.block())
     *     return;
     * }</pre>
     *
     * If the caller is a {@code ForkJoinTask}, then the pool may
     * first be expanded to ensure parallelism, and later adjusted.
     *
     * @param blocker the blocker
     * @throws InterruptedException if blocker.block did so
     */
    public static void managedBlock(ManagedBlocker blocker)
        throws InterruptedException {
        Thread t = Thread.currentThread();
        ForkJoinPool p = ((t instanceof ForkJoinWorkerThread) ?
                          ((ForkJoinWorkerThread)t).pool : null);
        while (!blocker.isReleasable()) {
            if (p == null || p.tryCompensate(null, blocker)) {
                try {
                    do {} while (!blocker.isReleasable() && !blocker.block());
                } finally {
                    if (p != null)
                        p.incrementActiveCount();
                }
                break;
            }
        }
    }

    // AbstractExecutorService overrides.  These rely on undocumented
    // fact that ForkJoinTask.adapt returns ForkJoinTasks that also
    // implement RunnableFuture.

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return new ForkJoinTask.AdaptedRunnable<T>(runnable, value);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return new ForkJoinTask.AdaptedCallable<T>(callable);
    }

    // Unsafe mechanics
    private static final sun.misc.Unsafe U;
    private static final long CTL;
    private static final long PARKBLOCKER;
    private static final int ABASE;
    private static final int ASHIFT;

    static {
        poolNumberGenerator = new AtomicInteger();
        nextSubmitterSeed = new AtomicInteger(0x55555555);
        modifyThreadPermission = new RuntimePermission("modifyThread");
        defaultForkJoinWorkerThreadFactory =
            new DefaultForkJoinWorkerThreadFactory();
        submitters = new ThreadSubmitter();
        int s;
        try {
            U = getUnsafe();
            Class<?> k = ForkJoinPool.class;
            Class<?> ak = ForkJoinTask[].class;
            CTL = U.objectFieldOffset
                (k.getDeclaredField("ctl"));
            Class<?> tk = Thread.class;
            PARKBLOCKER = U.objectFieldOffset
                (tk.getDeclaredField("parkBlocker"));
            ABASE = U.arrayBaseOffset(ak);
            s = U.arrayIndexScale(ak);
        } catch (Exception e) {
            throw new Error(e);
        }
        if ((s & (s-1)) != 0)
            throw new Error("data type scale not a power of two");
        ASHIFT = 31 - Integer.numberOfLeadingZeros(s);
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
