// Rev 1.64 from http://gee.cs.oswego.edu/cgi-bin/viewcvs.cgi/jsr166/src/jsr166e/ForkJoinPool.java?view=co

/*
 * Written by Doug Lea with assistance from members of JCP JSR-166
 * Expert Group and released to the public domain, as explained at
 * http://creativecommons.org/publicdomain/zero/1.0/
 */

package jsr166e;

import java.lang.Thread.UncaughtExceptionHandler;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RunnableFuture;
import java.util.concurrent.TimeUnit;

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
 * <p>A static {@link #commonPool()} is available and appropriate for
 * most applications. The common pool is used by any ForkJoinTask that
 * is not explicitly submitted to a specified pool. Using the common
 * pool normally reduces resource usage (its threads are slowly
 * reclaimed during periods of non-use, and reinstated upon subsequent
 * use).
 *
 * <p>For applications that require separate or custom pools, a {@code
 * ForkJoinPool} may be constructed with a given target parallelism
 * level; by default, equal to the number of available processors. The
 * pool attempts to maintain enough active (or available) threads by
 * dynamically adding, suspending, or resuming internal worker
 * threads, even if some tasks are stalled waiting to join others.
 * However, no such adjustments are guaranteed in the face of blocked
 * I/O or other unmanaged synchronization. The nested {@link
 * ManagedBlocker} interface enables extension of the kinds of
 * synchronization accommodated.
 *
 * <p>In addition to execution and lifecycle control methods, this
 * class provides status check methods (for example
 * {@link #getStealCount}) that are intended to aid in developing,
 * tuning, and monitoring fork/join applications. Also, method
 * {@link #toString} returns indications of pool state in a
 * convenient form for informal monitoring.
 *
 * <p>As is the case with other ExecutorServices, there are three
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
 * <caption>Summary of task execution methods</caption>
 *  <tr>
 *    <td></td>
 *    <td ALIGN=CENTER> <b>Call from non-fork/join clients</b></td>
 *    <td ALIGN=CENTER> <b>Call from within fork/join computations</b></td>
 *  </tr>
 *  <tr>
 *    <td> <b>Arrange async execution</b></td>
 *    <td> {@link #execute(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#fork}</td>
 *  </tr>
 *  <tr>
 *    <td> <b>Await and obtain result</b></td>
 *    <td> {@link #invoke(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#invoke}</td>
 *  </tr>
 *  <tr>
 *    <td> <b>Arrange exec and obtain Future</b></td>
 *    <td> {@link #submit(ForkJoinTask)}</td>
 *    <td> {@link ForkJoinTask#fork} (ForkJoinTasks <em>are</em> Futures)</td>
 *  </tr>
 * </table>
 *
 * <p>The common pool is by default constructed with default
 * parameters, but these may be controlled by setting three
 * {@linkplain System#getProperty system properties}:
 * <ul>
 * <li>{@code java.util.concurrent.ForkJoinPool.common.parallelism}
 * - the parallelism level, a non-negative integer
 * <li>{@code java.util.concurrent.ForkJoinPool.common.threadFactory}
 * - the class name of a {@link ForkJoinWorkerThreadFactory}
 * <li>{@code java.util.concurrent.ForkJoinPool.common.exceptionHandler}
 * - the class name of a {@link UncaughtExceptionHandler}
 * </ul>
 * The system class loader is used to load these classes.
 * Upon any error in establishing these settings, default parameters
 * are used. It is possible to disable or limit the use of threads in
 * the common pool by setting the parallelism property to zero, and/or
 * using a factory that may return {@code null}.
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
     * See also "Correct and Efficient Work-Stealing for Weak Memory
     * Models" by Le, Pop, Cohen, and Nardelli, PPoPP 2013
     * (http://www.di.ens.fr/~zappa/readings/ppopp13.pdf) for an
     * analysis of memory ordering (atomic, volatile etc) issues.  The
     * main differences ultimately stem from GC requirements that we
     * null out taken slots as soon as we can, to maintain as small a
     * footprint as possible even in programs generating huge numbers
     * of tasks. To accomplish this, we shift the CAS arbitrating pop
     * vs poll (steal) from being on the indices ("base" and "top") to
     * the slots themselves.  So, both a successful pop and poll
     * mainly entail a CAS of a slot from non-null to null.  Because
     * we rely on CASes of references, we do not need tag bits on base
     * or top.  They are simple ints as used in any circular
     * array-based queue (see for example ArrayDeque).  Updates to the
     * indices must still be ordered in a way that guarantees that top
     * == base means the queue is empty, but otherwise may err on the
     * side of possibly making the queue appear nonempty when a push,
     * pop, or poll have not fully committed. Note that this means
     * that the poll operation, considered individually, is not
     * wait-free. One thief cannot successfully continue until another
     * in-progress one (or, if previously empty, a push) completes.
     * However, in the aggregate, we ensure at least probabilistic
     * non-blockingness.  If an attempted steal fails, a thief always
     * chooses a different random victim target to try next. So, in
     * order for one thief to progress, it suffices for any
     * in-progress poll or new push on any empty queue to
     * complete. (This is why we normally use method pollAt and its
     * variants that try once at the apparent base index, else
     * consider alternative actions, rather than method poll.)
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
     * processing). Instead, we randomly associate submission queues
     * with submitting threads, using a form of hashing.  The
     * Submitter probe value serves as a hash code for
     * choosing existing queues, and may be randomly repositioned upon
     * contention with other submitters.  In essence, submitters act
     * like workers except that they are restricted to executing local
     * tasks that they submitted (or in the case of CountedCompleters,
     * others with the same root task).  However, because most
     * shared/external queue operations are more expensive than
     * internal, and because, at steady state, external submitters
     * will compete for CPU with workers, ForkJoinTask.join and
     * related methods disable them from repeatedly helping to process
     * tasks if all workers are active.  Insertion of tasks in shared
     * mode requires a lock (mainly to protect in the case of
     * resizing) but we use only a simple spinlock (using bits in
     * field qlock), because submitters encountering a busy queue move
     * on to try or create other queues -- they block only when
     * creating and registering new queues.
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
     * Field "plock" is a form of sequence lock with a saturating
     * shutdown bit (similarly for per-queue "qlocks"), mainly
     * protecting updates to the workQueues array, as well as to
     * enable shutdown.  When used as a lock, it is normally only very
     * briefly held, so is nearly always available after at most a
     * brief spin, but we use a monitor-based backup strategy to
     * block when needed.
     *
     * Recording WorkQueues.  WorkQueues are recorded in the
     * "workQueues" array that is created upon first use and expanded
     * if necessary.  Updates to the array while recording new workers
     * and unrecording terminated ones are protected from each other
     * by a lock but the array is otherwise concurrently readable, and
     * accessed directly.  To simplify index-based operations, the
     * array size is always a power of two, and all readers must
     * tolerate null slots. Worker queues are at odd indices. Shared
     * (submission) queues are at even indices, up to a maximum of 64
     * slots, to limit growth even if array needs to expand to add
     * more workers. Grouping them together in this way simplifies and
     * speeds up task scanning.
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
     * to the wait queue.  Because enqueued workers may actually be
     * rescanning rather than waiting, we set and clear the "parker"
     * field of WorkQueues to reduce unnecessary calls to unpark.
     * (This requires a secondary recheck to avoid missed signals.)
     * Note the unusual conventions about Thread.interrupts
     * surrounding parking and other blocking: Because interrupts are
     * used solely to alert threads to check termination, which is
     * checked anyway upon blocking, we clear status (using
     * Thread.interrupted) before any call to park, so that park does
     * not immediately return due to status being set via some other
     * unrelated call to interrupt in user code.
     *
     * Signalling.  We create or wake up workers only when there
     * appears to be at least one task they might be able to find and
     * execute.  When a submission is added or another worker adds a
     * task to a queue that has fewer than two tasks, they signal
     * waiting workers (or trigger creation of new ones if fewer than
     * the given parallelism level -- signalWork).  These primary
     * signals are buttressed by others whenever other threads remove
     * a task from a queue and notice that there are other tasks there
     * as well.  So in general, pools will be over-signalled. On most
     * platforms, signalling (unpark) overhead time is noticeably
     * long, and the time between signalling a thread and it actually
     * making progress can be very noticeably long, so it is worth
     * offloading these delays from critical paths as much as
     * possible. Additionally, workers spin-down gradually, by staying
     * alive so long as they see the ctl state changing.  Similar
     * stability-sensing techniques are also used before blocking in
     * awaitJoin and helpComplete.
     *
     * Trimming workers. To release resources after periods of lack of
     * use, a worker starting to wait when the pool is quiescent will
     * time out and terminate if the pool has remained quiescent for a
     * given period -- a short period if there are more threads than
     * parallelism, longer as the number of threads decreases. This
     * will slowly propagate, eventually terminating all workers after
     * periods of non-use.
     *
     * Shutdown and Termination. A call to shutdownNow atomically sets
     * a plock bit and then (non-atomically) sets each worker's
     * qlock status, cancels all unprocessed tasks, and wakes up
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
     * A third form (implemented in tryRemoveAndExec) amounts to
     * helping a hypothetical compensator: If we can readily tell that
     * a possible action of a compensator is to steal and execute the
     * task being joined, the joining thread can do so directly,
     * without the need for a compensation thread (although at the
     * expense of larger run-time stacks, but the tradeoff is
     * typically worthwhile).
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
     * stale/wrong) of where to locate them.  It is only a hint
     * because a worker might have had multiple steals and the hint
     * records only one of them (usually the most current).  Hinting
     * isolates cost to when it is needed, rather than adding to
     * per-task overhead.  (2) It is "shallow", ignoring nesting and
     * potentially cyclic mutual steals.  (3) It is intentionally
     * racy: field currentJoin is updated only while actively joining,
     * which means that we miss links in the chain during long-lived
     * tasks, GC stalls etc (which is OK since blocking in such cases
     * is usually a good idea).  (4) We bound the number of attempts
     * to find work (see MAX_HELP) and fall back to suspending the
     * worker and if necessary replacing it with another.
     *
     * Helping actions for CountedCompleters are much simpler: Method
     * helpComplete can take and execute any task with the same root
     * as the task being waited on. However, this still entails some
     * traversal of completer chains, so is less efficient than using
     * CountedCompleters without explicit joins.
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
     * methods tryCompensate and awaitJoin.
     *
     * Common Pool
     * ===========
     *
     * The static common pool always exists after static
     * initialization.  Since it (or any other created pool) need
     * never be used, we minimize initial construction overhead and
     * footprint to the setup of about a dozen fields, with no nested
     * allocation. Most bootstrapping occurs within method
     * fullExternalPush during the first submission to the pool.
     *
     * When external threads submit to the common pool, they can
     * perform subtask processing (see externalHelpJoin and related
     * methods).  This caller-helps policy makes it sensible to set
     * common pool parallelism level to one (or more) less than the
     * total number of available cores, or even zero for pure
     * caller-runs.  We do not need to record whether external
     * submissions are to the common pool -- if not, externalHelpJoin
     * returns quickly (at the most helping to signal some common pool
     * workers). These submitters would otherwise be blocked waiting
     * for completion, so the extra effort (with liberally sprinkled
     * task status checks) in inapplicable cases amounts to an odd
     * form of limited spin-wait before blocking in ForkJoinTask.join.
     *
     * Style notes
     * ===========
     *
     * There is a lot of representation-level coupling among classes
     * ForkJoinPool, ForkJoinWorkerThread, and ForkJoinTask.  The
     * fields of WorkQueue maintain data structures managed by
     * ForkJoinPool, so are directly accessed.  There is little point
     * trying to reduce this, since any associated future changes in
     * representations will need to be accompanied by algorithmic
     * changes anyway. Several methods intrinsically sprawl because
     * they must accumulate sets of consistent reads of volatiles held
     * in local variables.  Methods signalWork() and scan() are the
     * main bottlenecks, so are especially heavily
     * micro-optimized/mangled.  There are lots of inline assignments
     * (of form "while ((local = field) != 0)") which are usually the
     * simplest way to ensure the required read orderings (which are
     * sometimes critical). This leads to a "C"-like style of listing
     * declarations of these locals at the heads of methods or blocks.
     * There are several occurrences of the unusual "do {} while
     * (!cas...)"  which is the simplest way to force an update of a
     * CAS'ed variable. There are also other coding oddities (including
     * several unnecessary-looking hoisted null checks) that help
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
         * @return the new worker thread
         * @throws NullPointerException if the pool is null
         */
        public ForkJoinWorkerThread newThread(ForkJoinPool pool);
    }

    /**
     * Default ForkJoinWorkerThreadFactory implementation; creates a
     * new ForkJoinWorkerThread.
     */
    static final class DefaultForkJoinWorkerThreadFactory
        implements ForkJoinWorkerThreadFactory {
        public final ForkJoinWorkerThread newThread(ForkJoinPool pool) {
            return new ForkJoinWorkerThread(pool);
        }
    }

    /**
     * Class for artificial tasks that are used to replace the target
     * of local joins if they are removed from an interior queue slot
     * in WorkQueue.tryRemoveAndExec. We don't need the proxy to
     * actually do anything beyond having a unique identity.
     */
    static final class EmptyTask extends ForkJoinTask<Void> {
        private static final long serialVersionUID = -7721805057305804111L;
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
     * for push, or under lock for external/shared push, and accessed
     * by other threads only after reading (volatile) base.  Both top
     * and base are allowed to wrap around on overflow, but (top -
     * base) (or more commonly -(base - top) to force volatile read of
     * base before top) still estimates size. The lock ("qlock") is
     * forced to -1 on termination, causing all further lock attempts
     * to fail. (Note: we don't need CAS for termination state because
     * upon pool shutdown, all shared-queues will stop being used
     * anyway.)  Nearly all lock bodies are set up so that exceptions
     * within lock bodies are "impossible" (modulo JVM errors that
     * would cause failure anyway.)
     *
     * The array slots are read and written using the emulation of
     * volatiles/atomics provided by Unsafe. Insertions must in
     * general use putOrderedObject as a form of releasing store to
     * ensure that all writes to the task object are ordered before
     * its publication in the queue.  All removals entail a CAS to
     * null.  The array is always a power of two. To ensure safety of
     * Unsafe array operations, all accesses perform explicit null
     * checks and implicit bounds checks via power-of-two masking.
     *
     * In addition to basic queuing support, this class contains
     * fields described elsewhere to control execution. It turns out
     * to work better memory-layout-wise to include them in this class
     * rather than a separate class.
     *
     * Performance on most platforms is very sensitive to placement of
     * instances of both WorkQueues and their arrays -- we absolutely
     * do not want multiple WorkQueue instances or multiple queue
     * arrays sharing cache lines. (It would be best for queue objects
     * and their arrays to share, but there is nothing available to
     * help arrange that). The @Contended annotation alerts JVMs to
     * try to keep instances apart.
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

        // Heuristic padding to ameliorate unfortunate memory placements
        volatile long pad00, pad01, pad02, pad03, pad04, pad05, pad06;

        volatile int eventCount;   // encoded inactivation count; < 0 if inactive
        int nextWait;              // encoded record of next event waiter
        int nsteals;               // number of steals
        int hint;                  // steal index hint
        short poolIndex;           // index of this queue in pool
        final short mode;          // 0: lifo, > 0: fifo, < 0: shared
        volatile int qlock;        // 1: locked, -1: terminate; else 0
        volatile int base;         // index of next slot for poll
        int top;                   // index of next slot for push
        ForkJoinTask<?>[] array;   // the elements (initially unallocated)
        final ForkJoinPool pool;   // the containing pool (may be null)
        final ForkJoinWorkerThread owner; // owning thread or null if shared
        volatile Thread parker;    // == owner during call to park; else null
        volatile ForkJoinTask<?> currentJoin;  // task being joined in awaitJoin
        ForkJoinTask<?> currentSteal; // current non-local task being executed

        volatile Object pad10, pad11, pad12, pad13, pad14, pad15, pad16, pad17;
        volatile Object pad18, pad19, pad1a, pad1b, pad1c, pad1d;

        WorkQueue(ForkJoinPool pool, ForkJoinWorkerThread owner, int mode,
                  int seed) {
            this.pool = pool;
            this.owner = owner;
            this.mode = (short)mode;
            this.hint = seed; // store initial seed for runWorker
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
                      U.getObject
                      (a, (long)((m & (s - 1)) << ASHIFT) + ABASE) == null)));
        }

        /**
         * Pushes a task. Call only by owner in unshared queues.  (The
         * shared-queue version is embedded in method externalPush.)
         *
         * @param task the task. Caller must ensure non-null.
         * @throws RejectedExecutionException if array cannot be resized
         */
        final void push(ForkJoinTask<?> task) {
            ForkJoinTask<?>[] a; ForkJoinPool p;
            int s = top, n;
            if ((a = array) != null) {    // ignore if queue removed
                int m = a.length - 1;
                U.putOrderedObject(a, ((m & s) << ASHIFT) + ABASE, task);
                if ((n = (top = s + 1) - base) <= 2)
                    (p = pool).signalWork(p.workQueues, this);
                else if (n >= m)
                    growArray();
            }
        }

        /**
         * Initializes or doubles the capacity of array. Call either
         * by owner or with lock held -- it is OK for base, but not
         * top, to move while resizings are in progress.
         */
        final ForkJoinTask<?>[] growArray() {
            ForkJoinTask<?>[] oldA = array;
            int size = oldA != null ? oldA.length << 1 : INITIAL_QUEUE_CAPACITY;
            if (size > MAXIMUM_QUEUE_CAPACITY)
                throw new RejectedExecutionException("Queue capacity exceeded");
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

        /**
         * Takes next task, if one exists, in LIFO order.  Call only
         * by owner in unshared queues.
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
                    base == b && U.compareAndSwapObject(a, j, t, null)) {
                    U.putOrderedInt(this, QBASE, b + 1);
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
                    if (U.compareAndSwapObject(a, j, t, null)) {
                        U.putOrderedInt(this, QBASE, b + 1);
                        return t;
                    }
                }
                else if (base == b) {
                    if (b + 1 == top)
                        break;
                    Thread.yield(); // wait for lagging update (very rare)
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
         * (A shared version is available only via FJP.tryExternalUnpush)
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
         * Removes and cancels all known tasks, ignoring any exceptions.
         */
        final void cancelAll() {
            ForkJoinTask.cancelIgnoringExceptions(currentJoin);
            ForkJoinTask.cancelIgnoringExceptions(currentSteal);
            for (ForkJoinTask<?> t; (t = poll()) != null; )
                ForkJoinTask.cancelIgnoringExceptions(t);
        }

        // Specialized execution methods

        /**
         * Polls and runs tasks until empty.
         */
        final void pollAndExecAll() {
            for (ForkJoinTask<?> t; (t = poll()) != null;)
                t.doExec();
        }

        /**
         * Executes a top-level task and any local tasks remaining
         * after execution.
         */
        final void runTask(ForkJoinTask<?> task) {
            if ((currentSteal = task) != null) {
                task.doExec();
                ForkJoinTask<?>[] a = array;
                int md = mode;
                ++nsteals;
                currentSteal = null;
                if (md != 0)
                    pollAndExecAll();
                else if (a != null) {
                    int s, m = a.length - 1;
                    while ((s = top - 1) - base >= 0) {
                        long i = ((m & s) << ASHIFT) + ABASE;
                        ForkJoinTask<?> t = (ForkJoinTask<?>)U.getObject(a, i);
                        if (t == null)
                            break;
                        if (U.compareAndSwapObject(a, i, t, null)) {
                            top = s;
                            t.doExec();
                        }
                    }
                }
            }
        }

        /**
         * If present, removes from queue and executes the given task,
         * or any other cancelled task. Returns (true) on any CAS
         * or consistency check failure so caller can retry.
         *
         * @return false if no progress can be made, else true
         */
        final boolean tryRemoveAndExec(ForkJoinTask<?> task) {
            boolean stat;
            ForkJoinTask<?>[] a; int m, s, b, n;
            if (task != null && (a = array) != null && (m = a.length - 1) >= 0 &&
                (n = (s = top) - (b = base)) > 0) {
                boolean removed = false, empty = true;
                stat = true;
                for (ForkJoinTask<?> t;;) {           // traverse from s to b
                    long j = ((--s & m) << ASHIFT) + ABASE;
                    t = (ForkJoinTask<?>)U.getObject(a, j);
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
                            stat = false;
                        break;
                    }
                }
                if (removed)
                    task.doExec();
            }
            else
                stat = false;
            return stat;
        }

        /**
         * Tries to poll for and execute the given task or any other
         * task in its CountedCompleter computation.
         */
        final boolean pollAndExecCC(CountedCompleter<?> root) {
            ForkJoinTask<?>[] a; int b; Object o; CountedCompleter<?> t, r;
            if ((b = base) - top < 0 && (a = array) != null) {
                long j = (((a.length - 1) & b) << ASHIFT) + ABASE;
                if ((o = U.getObjectVolatile(a, j)) == null)
                    return true; // retry
                if (o instanceof CountedCompleter) {
                    for (t = (CountedCompleter<?>)o, r = t;;) {
                        if (r == root) {
                            if (base == b &&
                                U.compareAndSwapObject(a, j, t, null)) {
                                U.putOrderedInt(this, QBASE, b + 1);
                                t.doExec();
                            }
                            return true;
                        }
                        else if ((r = r.completer) == null)
                            break; // not part of root computation
                    }
                }
            }
            return false;
        }

        /**
         * Tries to pop and execute the given task or any other task
         * in its CountedCompleter computation.
         */
        final boolean externalPopAndExecCC(CountedCompleter<?> root) {
            ForkJoinTask<?>[] a; int s; Object o; CountedCompleter<?> t, r;
            if (base - (s = top) < 0 && (a = array) != null) {
                long j = (((a.length - 1) & (s - 1)) << ASHIFT) + ABASE;
                if ((o = U.getObject(a, j)) instanceof CountedCompleter) {
                    for (t = (CountedCompleter<?>)o, r = t;;) {
                        if (r == root) {
                            if (U.compareAndSwapInt(this, QLOCK, 0, 1)) {
                                if (top == s && array == a &&
                                    U.compareAndSwapObject(a, j, t, null)) {
                                    top = s - 1;
                                    qlock = 0;
                                    t.doExec();
                                }
                                else
                                    qlock = 0;
                            }
                            return true;
                        }
                        else if ((r = r.completer) == null)
                            break;
                    }
                }
            }
            return false;
        }

        /**
         * Internal version
         */
        final boolean internalPopAndExecCC(CountedCompleter<?> root) {
            ForkJoinTask<?>[] a; int s; Object o; CountedCompleter<?> t, r;
            if (base - (s = top) < 0 && (a = array) != null) {
                long j = (((a.length - 1) & (s - 1)) << ASHIFT) + ABASE;
                if ((o = U.getObject(a, j)) instanceof CountedCompleter) {
                    for (t = (CountedCompleter<?>)o, r = t;;) {
                        if (r == root) {
                            if (U.compareAndSwapObject(a, j, t, null)) {
                                top = s - 1;
                                t.doExec();
                            }
                            return true;
                        }
                        else if ((r = r.completer) == null)
                            break;
                    }
                }
            }
            return false;
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

        // Unsafe mechanics
        private static final sun.misc.Unsafe U;
        private static final long QBASE;
        private static final long QLOCK;
        private static final int ABASE;
        private static final int ASHIFT;
        static {
            try {
                U = getUnsafe();
                Class<?> k = WorkQueue.class;
                Class<?> ak = ForkJoinTask[].class;
                QBASE = U.objectFieldOffset
                    (k.getDeclaredField("base"));
                QLOCK = U.objectFieldOffset
                    (k.getDeclaredField("qlock"));
                ABASE = U.arrayBaseOffset(ak);
                int scale = U.arrayIndexScale(ak);
                if ((scale & (scale - 1)) != 0)
                    throw new Error("data type scale not a power of two");
                ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
            } catch (Exception e) {
                throw new Error(e);
            }
        }
    }

    // static fields (initialized in static initializer below)

    /**
     * Per-thread submission bookkeeping. Shared across all pools
     * to reduce ThreadLocal pollution and because random motion
     * to avoid contention in one pool is likely to hold for others.
     * Lazily initialized on first submission (but null-checked
     * in other contexts to avoid unnecessary initialization).
     */
    static final ThreadLocal<Submitter> submitters;

    /**
     * Creates a new ForkJoinWorkerThread. This factory is used unless
     * overridden in ForkJoinPool constructors.
     */
    public static final ForkJoinWorkerThreadFactory
        defaultForkJoinWorkerThreadFactory;

    /**
     * Permission required for callers of methods that may start or
     * kill threads.
     */
    private static final RuntimePermission modifyThreadPermission;

    /**
     * Common (static) pool. Non-null for public use unless a static
     * construction exception, but internal usages null-check on use
     * to paranoically avoid potential initialization circularities
     * as well as to simplify generated code.
     */
    static final ForkJoinPool common;

    /**
     * Common pool parallelism. To allow simpler use and management
     * when common pool threads are disabled, we allow the underlying
     * common.parallelism field to be zero, but in that case still report
     * parallelism as 1 to reflect resulting caller-runs mechanics.
     */
    static final int commonParallelism;

    /**
     * Sequence number for creating workerNamePrefix.
     */
    private static int poolNumberSequence;

    /**
     * Returns the next sequence number. We don't expect this to
     * ever contend, so use simple builtin sync.
     */
    private static final synchronized int nextPoolId() {
        return ++poolNumberSequence;
    }

    // static constants

    /**
     * Initial timeout value (in nanoseconds) for the thread
     * triggering quiescence to park waiting for new work. On timeout,
     * the thread will instead try to shrink the number of
     * workers. The value should be large enough to avoid overly
     * aggressive shrinkage during most transient stalls (long GCs
     * etc).
     */
    private static final long IDLE_TIMEOUT      = 2000L * 1000L * 1000L; // 2sec

    /**
     * Timeout value when there are more threads than parallelism level
     */
    private static final long FAST_IDLE_TIMEOUT =  200L * 1000L * 1000L;

    /**
     * Tolerance for idle timeouts, to cope with timer undershoots
     */
    private static final long TIMEOUT_SLOP = 2000000L;

    /**
     * The maximum stolen->joining link depth allowed in method
     * tryHelpStealer.  Must be a power of two.  Depths for legitimate
     * chains are unbounded, but we use a fixed constant to avoid
     * (otherwise unchecked) cycles and to bound staleness of
     * traversal parameters at the expense of sometimes blocking when
     * we could be helping.
     */
    private static final int MAX_HELP = 64;

    /**
     * Increment for seed generators. See class ThreadLocal for
     * explanation.
     */
    private static final int SEED_INCREMENT = 0x61c88647;

    /*
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
     * Field plock is an int packed with:
     * SHUTDOWN: true if shutdown is enabled (1 bit)
     * SEQ:  a sequence lock, with PL_LOCK bit set if locked (30 bits)
     * SIGNAL: set when threads may be waiting on the lock (1 bit)
     *
     * The sequence number enables simple consistency checks:
     * Staleness of read-only operations on the workQueues array can
     * be checked by comparing plock before vs after the reads.
     */

    // bit positions/shifts for fields
    private static final int  AC_SHIFT   = 48;
    private static final int  TC_SHIFT   = 32;
    private static final int  ST_SHIFT   = 31;
    private static final int  EC_SHIFT   = 16;

    // bounds
    private static final int  SMASK      = 0xffff;  // short bits
    private static final int  MAX_CAP    = 0x7fff;  // max #workers - 1
    private static final int  EVENMASK   = 0xfffe;  // even short bits
    private static final int  SQMASK     = 0x007e;  // max 64 (even) slots
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

    // plock bits
    private static final int SHUTDOWN    = 1 << 31;
    private static final int PL_LOCK     = 2;
    private static final int PL_SIGNAL   = 1;
    private static final int PL_SPINS    = 1 << 8;

    // access mode for WorkQueue
    static final int LIFO_QUEUE          =  0;
    static final int FIFO_QUEUE          =  1;
    static final int SHARED_QUEUE        = -1;

    // Heuristic padding to ameliorate unfortunate memory placements
    volatile long pad00, pad01, pad02, pad03, pad04, pad05, pad06;

    // Instance fields
    volatile long stealCount;                  // collects worker counts
    volatile long ctl;                         // main pool control
    volatile int plock;                        // shutdown status and seqLock
    volatile int indexSeed;                    // worker/submitter index seed
    final short parallelism;                   // parallelism level
    final short mode;                          // LIFO/FIFO
    WorkQueue[] workQueues;                    // main registry
    final ForkJoinWorkerThreadFactory factory;
    final UncaughtExceptionHandler ueh;        // per-worker UEH
    final String workerNamePrefix;             // to create worker name string

    volatile Object pad10, pad11, pad12, pad13, pad14, pad15, pad16, pad17;
    volatile Object pad18, pad19, pad1a, pad1b;

    /**
     * Acquires the plock lock to protect worker array and related
     * updates. This method is called only if an initial CAS on plock
     * fails. This acts as a spinlock for normal cases, but falls back
     * to builtin monitor to block when (rarely) needed. This would be
     * a terrible idea for a highly contended lock, but works fine as
     * a more conservative alternative to a pure spinlock.
     */
    private int acquirePlock() {
        int spins = PL_SPINS, ps, nps;
        for (;;) {
            if (((ps = plock) & PL_LOCK) == 0 &&
                U.compareAndSwapInt(this, PLOCK, ps, nps = ps + PL_LOCK))
                return nps;
            else if (spins >= 0) {
                if (ThreadLocalRandom.current().nextInt() >= 0)
                    --spins;
            }
            else if (U.compareAndSwapInt(this, PLOCK, ps, ps | PL_SIGNAL)) {
                synchronized (this) {
                    if ((plock & PL_SIGNAL) != 0) {
                        try {
                            wait();
                        } catch (InterruptedException ie) {
                            try {
                                Thread.currentThread().interrupt();
                            } catch (SecurityException ignore) {
                            }
                        }
                    }
                    else
                        notifyAll();
                }
            }
        }
    }

    /**
     * Unlocks and signals any thread waiting for plock. Called only
     * when CAS of seq value for unlock fails.
     */
    private void releasePlock(int ps) {
        plock = ps;
        synchronized (this) { notifyAll(); }
    }

    /**
     * Tries to create and start one worker if fewer than target
     * parallelism level exist. Adjusts counts etc on failure.
     */
    private void tryAddWorker() {
        long c; int u, e;
        while ((u = (int)((c = ctl) >>> 32)) < 0 &&
               (u & SHORT_SIGN) != 0 && (e = (int)c) >= 0) {
            long nc = ((long)(((u + UTC_UNIT) & UTC_MASK) |
                              ((u + UAC_UNIT) & UAC_MASK)) << 32) | (long)e;
            if (U.compareAndSwapLong(this, CTL, c, nc)) {
                ForkJoinWorkerThreadFactory fac;
                Throwable ex = null;
                ForkJoinWorkerThread wt = null;
                try {
                    if ((fac = factory) != null &&
                        (wt = fac.newThread(this)) != null) {
                        wt.start();
                        break;
                    }
                } catch (Throwable rex) {
                    ex = rex;
                }
                deregisterWorker(wt, ex);
                break;
            }
        }
    }

    //  Registering and deregistering workers

    /**
     * Callback from ForkJoinWorkerThread to establish and record its
     * WorkQueue. To avoid scanning bias due to packing entries in
     * front of the workQueues array, we treat the array as a simple
     * power-of-two hash table using per-thread seed as hash,
     * expanding as needed.
     *
     * @param wt the worker thread
     * @return the worker's queue
     */
    final WorkQueue registerWorker(ForkJoinWorkerThread wt) {
        UncaughtExceptionHandler handler; WorkQueue[] ws; int s, ps;
        wt.setDaemon(true);
        if ((handler = ueh) != null)
            wt.setUncaughtExceptionHandler(handler);
        do {} while (!U.compareAndSwapInt(this, INDEXSEED, s = indexSeed,
                                          s += SEED_INCREMENT) ||
                     s == 0); // skip 0
        WorkQueue w = new WorkQueue(this, wt, mode, s);
        if (((ps = plock) & PL_LOCK) != 0 ||
            !U.compareAndSwapInt(this, PLOCK, ps, ps += PL_LOCK))
            ps = acquirePlock();
        int nps = (ps & SHUTDOWN) | ((ps + PL_LOCK) & ~SHUTDOWN);
        try {
            if ((ws = workQueues) != null) {    // skip if shutting down
                int n = ws.length, m = n - 1;
                int r = (s << 1) | 1;           // use odd-numbered indices
                if (ws[r &= m] != null) {       // collision
                    int probes = 0;             // step by approx half size
                    int step = (n <= 4) ? 2 : ((n >>> 1) & EVENMASK) + 2;
                    while (ws[r = (r + step) & m] != null) {
                        if (++probes >= n) {
                            workQueues = ws = Arrays.copyOf(ws, n <<= 1);
                            m = n - 1;
                            probes = 0;
                        }
                    }
                }
                w.poolIndex = (short)r;
                w.eventCount = r; // volatile write orders
                ws[r] = w;
            }
        } finally {
            if (!U.compareAndSwapInt(this, PLOCK, ps, nps))
                releasePlock(nps);
        }
        wt.setName(workerNamePrefix.concat(Integer.toString(w.poolIndex >>> 1)));
        return w;
    }

    /**
     * Final callback from terminating worker, as well as upon failure
     * to construct or start a worker.  Removes record of worker from
     * array, and adjusts counts. If pool is shutting down, tries to
     * complete termination.
     *
     * @param wt the worker thread, or null if construction failed
     * @param ex the exception causing failure, or null if none
     */
    final void deregisterWorker(ForkJoinWorkerThread wt, Throwable ex) {
        WorkQueue w = null;
        if (wt != null && (w = wt.workQueue) != null) {
            int ps; long sc;
            w.qlock = -1;                // ensure set
            do {} while (!U.compareAndSwapLong(this, STEALCOUNT,
                                               sc = stealCount,
                                               sc + w.nsteals));
            if (((ps = plock) & PL_LOCK) != 0 ||
                !U.compareAndSwapInt(this, PLOCK, ps, ps += PL_LOCK))
                ps = acquirePlock();
            int nps = (ps & SHUTDOWN) | ((ps + PL_LOCK) & ~SHUTDOWN);
            try {
                int idx = w.poolIndex;
                WorkQueue[] ws = workQueues;
                if (ws != null && idx >= 0 && idx < ws.length && ws[idx] == w)
                    ws[idx] = null;
            } finally {
                if (!U.compareAndSwapInt(this, PLOCK, ps, nps))
                    releasePlock(nps);
            }
        }

        long c;                          // adjust ctl counts
        do {} while (!U.compareAndSwapLong
                     (this, CTL, c = ctl, (((c - AC_UNIT) & AC_MASK) |
                                           ((c - TC_UNIT) & TC_MASK) |
                                           (c & ~(AC_MASK|TC_MASK)))));

        if (!tryTerminate(false, false) && w != null && w.array != null) {
            w.cancelAll();               // cancel remaining tasks
            WorkQueue[] ws; WorkQueue v; Thread p; int u, i, e;
            while ((u = (int)((c = ctl) >>> 32)) < 0 && (e = (int)c) >= 0) {
                if (e > 0) {             // activate or create replacement
                    if ((ws = workQueues) == null ||
                        (i = e & SMASK) >= ws.length ||
                        (v = ws[i]) == null)
                        break;
                    long nc = (((long)(v.nextWait & E_MASK)) |
                               ((long)(u + UAC_UNIT) << 32));
                    if (v.eventCount != (e | INT_SIGN))
                        break;
                    if (U.compareAndSwapLong(this, CTL, c, nc)) {
                        v.eventCount = (e + E_SEQ) & E_MASK;
                        if ((p = v.parker) != null)
                            U.unpark(p);
                        break;
                    }
                }
                else {
                    if ((short)u < 0)
                        tryAddWorker();
                    break;
                }
            }
        }
        if (ex == null)                     // help clean refs on way out
            ForkJoinTask.helpExpungeStaleExceptions();
        else                                // rethrow
            ForkJoinTask.rethrow(ex);
    }

    // Submissions

    /**
     * Per-thread records for threads that submit to pools. Currently
     * holds only pseudo-random seed / index that is used to choose
     * submission queues in method externalPush. In the future, this may
     * also incorporate a means to implement different task rejection
     * and resubmission policies.
     *
     * Seeds for submitters and workers/workQueues work in basically
     * the same way but are initialized and updated using slightly
     * different mechanics. Both are initialized using the same
     * approach as in class ThreadLocal, where successive values are
     * unlikely to collide with previous values. Seeds are then
     * randomly modified upon collisions using xorshifts, which
     * requires a non-zero seed.
     */
    static final class Submitter {
        int seed;
        Submitter(int s) { seed = s; }
    }

    /**
     * Unless shutting down, adds the given task to a submission queue
     * at submitter's current queue index (modulo submission
     * range). Only the most common path is directly handled in this
     * method. All others are relayed to fullExternalPush.
     *
     * @param task the task. Caller must ensure non-null.
     */
    final void externalPush(ForkJoinTask<?> task) {
        Submitter z = submitters.get();
        WorkQueue q; int r, m, s, n, am; ForkJoinTask<?>[] a;
        int ps = plock;
        WorkQueue[] ws = workQueues;
        if (z != null && ps > 0 && ws != null && (m = (ws.length - 1)) >= 0 &&
            (q = ws[m & (r = z.seed) & SQMASK]) != null && r != 0 &&
            U.compareAndSwapInt(q, QLOCK, 0, 1)) { // lock
            if ((a = q.array) != null &&
                (am = a.length - 1) > (n = (s = q.top) - q.base)) {
                int j = ((am & s) << ASHIFT) + ABASE;
                U.putOrderedObject(a, j, task);
                q.top = s + 1;                     // push on to deque
                q.qlock = 0;
                if (n <= 1)
                    signalWork(ws, q);
                return;
            }
            q.qlock = 0;
        }
        fullExternalPush(task);
    }

    /**
     * Full version of externalPush. This method is called, among
     * other times, upon the first submission of the first task to the
     * pool, so must perform secondary initialization.  It also
     * detects first submission by an external thread by looking up
     * its ThreadLocal, and creates a new shared queue if the one at
     * index if empty or contended. The plock lock body must be
     * exception-free (so no try/finally) so we optimistically
     * allocate new queues outside the lock and throw them away if
     * (very rarely) not needed.
     *
     * Secondary initialization occurs when plock is zero, to create
     * workQueue array and set plock to a valid value.  This lock body
     * must also be exception-free. Because the plock seq value can
     * eventually wrap around zero, this method harmlessly fails to
     * reinitialize if workQueues exists, while still advancing plock.
     */
    private void fullExternalPush(ForkJoinTask<?> task) {
        int r = 0; // random index seed
        for (Submitter z = submitters.get();;) {
            WorkQueue[] ws; WorkQueue q; int ps, m, k;
            if (z == null) {
                if (U.compareAndSwapInt(this, INDEXSEED, r = indexSeed,
                                        r += SEED_INCREMENT) && r != 0)
                    submitters.set(z = new Submitter(r));
            }
            else if (r == 0) {                  // move to a different index
                r = z.seed;
                r ^= r << 13;                   // same xorshift as WorkQueues
                r ^= r >>> 17;
                z.seed = r ^= (r << 5);
            }
            if ((ps = plock) < 0)
                throw new RejectedExecutionException();
            else if (ps == 0 || (ws = workQueues) == null ||
                     (m = ws.length - 1) < 0) { // initialize workQueues
                int p = parallelism;            // find power of two table size
                int n = (p > 1) ? p - 1 : 1;    // ensure at least 2 slots
                n |= n >>> 1; n |= n >>> 2;  n |= n >>> 4;
                n |= n >>> 8; n |= n >>> 16; n = (n + 1) << 1;
                WorkQueue[] nws = ((ws = workQueues) == null || ws.length == 0 ?
                                   new WorkQueue[n] : null);
                if (((ps = plock) & PL_LOCK) != 0 ||
                    !U.compareAndSwapInt(this, PLOCK, ps, ps += PL_LOCK))
                    ps = acquirePlock();
                if (((ws = workQueues) == null || ws.length == 0) && nws != null)
                    workQueues = nws;
                int nps = (ps & SHUTDOWN) | ((ps + PL_LOCK) & ~SHUTDOWN);
                if (!U.compareAndSwapInt(this, PLOCK, ps, nps))
                    releasePlock(nps);
            }
            else if ((q = ws[k = r & m & SQMASK]) != null) {
                if (q.qlock == 0 && U.compareAndSwapInt(q, QLOCK, 0, 1)) {
                    ForkJoinTask<?>[] a = q.array;
                    int s = q.top;
                    boolean submitted = false;
                    try {                      // locked version of push
                        if ((a != null && a.length > s + 1 - q.base) ||
                            (a = q.growArray()) != null) {   // must presize
                            int j = (((a.length - 1) & s) << ASHIFT) + ABASE;
                            U.putOrderedObject(a, j, task);
                            q.top = s + 1;
                            submitted = true;
                        }
                    } finally {
                        q.qlock = 0;  // unlock
                    }
                    if (submitted) {
                        signalWork(ws, q);
                        return;
                    }
                }
                r = 0; // move on failure
            }
            else if (((ps = plock) & PL_LOCK) == 0) { // create new queue
                q = new WorkQueue(this, null, SHARED_QUEUE, r);
                q.poolIndex = (short)k;
                if (((ps = plock) & PL_LOCK) != 0 ||
                    !U.compareAndSwapInt(this, PLOCK, ps, ps += PL_LOCK))
                    ps = acquirePlock();
                if ((ws = workQueues) != null && k < ws.length && ws[k] == null)
                    ws[k] = q;
                int nps = (ps & SHUTDOWN) | ((ps + PL_LOCK) & ~SHUTDOWN);
                if (!U.compareAndSwapInt(this, PLOCK, ps, nps))
                    releasePlock(nps);
            }
            else
                r = 0;
        }
    }

    // Maintaining ctl counts

    /**
     * Increments active count; mainly called upon return from blocking.
     */
    final void incrementActiveCount() {
        long c;
        do {} while (!U.compareAndSwapLong
                     (this, CTL, c = ctl, ((c & ~AC_MASK) |
                                           ((c & AC_MASK) + AC_UNIT))));
    }

    /**
     * Tries to create or activate a worker if too few are active.
     *
     * @param ws the worker array to use to find signallees
     * @param q if non-null, the queue holding tasks to be processed
     */
    final void signalWork(WorkQueue[] ws, WorkQueue q) {
        for (;;) {
            long c; int e, u, i; WorkQueue w; Thread p;
            if ((u = (int)((c = ctl) >>> 32)) >= 0)
                break;
            if ((e = (int)c) <= 0) {
                if ((short)u < 0)
                    tryAddWorker();
                break;
            }
            if (ws == null || ws.length <= (i = e & SMASK) ||
                (w = ws[i]) == null)
                break;
            long nc = (((long)(w.nextWait & E_MASK)) |
                       ((long)(u + UAC_UNIT)) << 32);
            int ne = (e + E_SEQ) & E_MASK;
            if (w.eventCount == (e | INT_SIGN) &&
                U.compareAndSwapLong(this, CTL, c, nc)) {
                w.eventCount = ne;
                if ((p = w.parker) != null)
                    U.unpark(p);
                break;
            }
            if (q != null && q.base >= q.top)
                break;
        }
    }

    // Scanning for tasks

    /**
     * Top-level runloop for workers, called by ForkJoinWorkerThread.run.
     */
    final void runWorker(WorkQueue w) {
        w.growArray(); // allocate queue
        for (int r = w.hint; scan(w, r) == 0; ) {
            r ^= r << 13; r ^= r >>> 17; r ^= r << 5; // xorshift
        }
    }

    /**
     * Scans for and, if found, runs one task, else possibly
     * inactivates the worker. This method operates on single reads of
     * volatile state and is designed to be re-invoked continuously,
     * in part because it returns upon detecting inconsistencies,
     * contention, or state changes that indicate possible success on
     * re-invocation.
     *
     * The scan searches for tasks across queues starting at a random
     * index, checking each at least twice.  The scan terminates upon
     * either finding a non-empty queue, or completing the sweep. If
     * the worker is not inactivated, it takes and runs a task from
     * this queue. Otherwise, if not activated, it tries to activate
     * itself or some other worker by signalling. On failure to find a
     * task, returns (for retry) if pool state may have changed during
     * an empty scan, or tries to inactivate if active, else possibly
     * blocks or terminates via method awaitWork.
     *
     * @param w the worker (via its WorkQueue)
     * @param r a random seed
     * @return worker qlock status if would have waited, else 0
     */
    private final int scan(WorkQueue w, int r) {
        WorkQueue[] ws; int m;
        long c = ctl;                            // for consistency check
        if ((ws = workQueues) != null && (m = ws.length - 1) >= 0 && w != null) {
            for (int j = m + m + 1, ec = w.eventCount;;) {
                WorkQueue q; int b, e; ForkJoinTask<?>[] a; ForkJoinTask<?> t;
                if ((q = ws[(r - j) & m]) != null &&
                    (b = q.base) - q.top < 0 && (a = q.array) != null) {
                    long i = (((a.length - 1) & b) << ASHIFT) + ABASE;
                    if ((t = ((ForkJoinTask<?>)
                              U.getObjectVolatile(a, i))) != null) {
                        if (ec < 0)
                            helpRelease(c, ws, w, q, b);
                        else if (q.base == b &&
                                 U.compareAndSwapObject(a, i, t, null)) {
                            U.putOrderedInt(q, QBASE, b + 1);
                            if ((b + 1) - q.top < 0)
                                signalWork(ws, q);
                            w.runTask(t);
                        }
                    }
                    break;
                }
                else if (--j < 0) {
                    if ((ec | (e = (int)c)) < 0) // inactive or terminating
                        return awaitWork(w, c, ec);
                    else if (ctl == c) {         // try to inactivate and enqueue
                        long nc = (long)ec | ((c - AC_UNIT) & (AC_MASK|TC_MASK));
                        w.nextWait = e;
                        w.eventCount = ec | INT_SIGN;
                        if (!U.compareAndSwapLong(this, CTL, c, nc))
                            w.eventCount = ec;   // back out
                    }
                    break;
                }
            }
        }
        return 0;
    }

    /**
     * A continuation of scan(), possibly blocking or terminating
     * worker w. Returns without blocking if pool state has apparently
     * changed since last invocation.  Also, if inactivating w has
     * caused the pool to become quiescent, checks for pool
     * termination, and, so long as this is not the only worker, waits
     * for event for up to a given duration.  On timeout, if ctl has
     * not changed, terminates the worker, which will in turn wake up
     * another worker to possibly repeat this process.
     *
     * @param w the calling worker
     * @param c the ctl value on entry to scan
     * @param ec the worker's eventCount on entry to scan
     */
    private final int awaitWork(WorkQueue w, long c, int ec) {
        int stat, ns; long parkTime, deadline;
        if ((stat = w.qlock) >= 0 && w.eventCount == ec && ctl == c &&
            !Thread.interrupted()) {
            int e = (int)c;
            int u = (int)(c >>> 32);
            int d = (u >> UAC_SHIFT) + parallelism; // active count

            if (e < 0 || (d <= 0 && tryTerminate(false, false)))
                stat = w.qlock = -1;          // pool is terminating
            else if ((ns = w.nsteals) != 0) { // collect steals and retry
                long sc;
                w.nsteals = 0;
                do {} while (!U.compareAndSwapLong(this, STEALCOUNT,
                                                   sc = stealCount, sc + ns));
            }
            else {
                long pc = ((d > 0 || ec != (e | INT_SIGN)) ? 0L :
                           ((long)(w.nextWait & E_MASK)) | // ctl to restore
                           ((long)(u + UAC_UNIT)) << 32);
                if (pc != 0L) {               // timed wait if last waiter
                    int dc = -(short)(c >>> TC_SHIFT);
                    parkTime = (dc < 0 ? FAST_IDLE_TIMEOUT:
                                (dc + 1) * IDLE_TIMEOUT);
                    deadline = System.nanoTime() + parkTime - TIMEOUT_SLOP;
                }
                else
                    parkTime = deadline = 0L;
                if (w.eventCount == ec && ctl == c) {
                    Thread wt = Thread.currentThread();
                    U.putObject(wt, PARKBLOCKER, this);
                    w.parker = wt;            // emulate LockSupport.park
                    if (w.eventCount == ec && ctl == c)
                        U.park(false, parkTime);  // must recheck before park
                    w.parker = null;
                    U.putObject(wt, PARKBLOCKER, null);
                    if (parkTime != 0L && ctl == c &&
                        deadline - System.nanoTime() <= 0L &&
                        U.compareAndSwapLong(this, CTL, c, pc))
                        stat = w.qlock = -1;  // shrink pool
                }
            }
        }
        return stat;
    }

    /**
     * Possibly releases (signals) a worker. Called only from scan()
     * when a worker with apparently inactive status finds a non-empty
     * queue. This requires revalidating all of the associated state
     * from caller.
     */
    private final void helpRelease(long c, WorkQueue[] ws, WorkQueue w,
                                   WorkQueue q, int b) {
        WorkQueue v; int e, i; Thread p;
        if (w != null && w.eventCount < 0 && (e = (int)c) > 0 &&
            ws != null && ws.length > (i = e & SMASK) &&
            (v = ws[i]) != null && ctl == c) {
            long nc = (((long)(v.nextWait & E_MASK)) |
                       ((long)((int)(c >>> 32) + UAC_UNIT)) << 32);
            int ne = (e + E_SEQ) & E_MASK;
            if (q != null && q.base == b && w.eventCount < 0 &&
                v.eventCount == (e | INT_SIGN) &&
                U.compareAndSwapLong(this, CTL, c, nc)) {
                v.eventCount = ne;
                if ((p = v.parker) != null)
                    U.unpark(p);
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
        if (task != null && joiner != null &&
            joiner.base - joiner.top >= 0) {        // hoist checks
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
                    if ((v = ws[h = (j.hint | 1) & m]) == null ||
                        v.currentSteal != subtask) {
                        for (int origin = h;;) {    // find stealer
                            if (((h = (h + 2) & m) & 15) == 1 &&
                                (subtask.status < 0 || j.currentJoin != subtask))
                                continue restart;   // occasional staleness check
                            if ((v = ws[h]) != null &&
                                v.currentSteal == subtask) {
                                j.hint = h;        // save hint
                                break;
                            }
                            if (h == origin)
                                break restart;      // cannot find stealer
                        }
                    }
                    for (;;) { // help stealer or descend to its stealer
                        ForkJoinTask[] a; int b;
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
                            if (v.base == b) {
                                if (t == null)
                                    break restart;
                                if (U.compareAndSwapObject(a, i, t, null)) {
                                    U.putOrderedInt(v, QBASE, b + 1);
                                    ForkJoinTask<?> ps = joiner.currentSteal;
                                    int jt = joiner.top;
                                    do {
                                        joiner.currentSteal = t;
                                        t.doExec(); // clear local tasks too
                                    } while (task.status >= 0 &&
                                             joiner.top != jt &&
                                             (t = joiner.pop()) != null);
                                    joiner.currentSteal = ps;
                                    break restart;
                                }
                            }
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
     * Analog of tryHelpStealer for CountedCompleters. Tries to steal
     * and run tasks within the target's computation.
     *
     * @param task the task to join
     */
    private int helpComplete(WorkQueue joiner, CountedCompleter<?> task) {
        WorkQueue[] ws; int m;
        int s = 0;
        if ((ws = workQueues) != null && (m = ws.length - 1) >= 0 &&
            joiner != null && task != null) {
            int j = joiner.poolIndex;
            int scans = m + m + 1;
            long c = 0L;              // for stability check
            for (int k = scans; ; j += 2) {
                WorkQueue q;
                if ((s = task.status) < 0)
                    break;
                else if (joiner.internalPopAndExecCC(task))
                    k = scans;
                else if ((s = task.status) < 0)
                    break;
                else if ((q = ws[j & m]) != null && q.pollAndExecCC(task))
                    k = scans;
                else if (--k < 0) {
                    if (c == (c = ctl))
                        break;
                    k = scans;
                }
            }
        }
        return s;
    }

    /**
     * Tries to decrement active count (sometimes implicitly) and
     * possibly release or create a compensating worker in preparation
     * for blocking. Fails on contention or termination. Otherwise,
     * adds a new thread if no idle workers are available and pool
     * may become starved.
     *
     * @param c the assumed ctl value
     */
    final boolean tryCompensate(long c) {
        WorkQueue[] ws = workQueues;
        int pc = parallelism, e = (int)c, m, tc;
        if (ws != null && (m = ws.length - 1) >= 0 && e >= 0 && ctl == c) {
            WorkQueue w = ws[e & m];
            if (e != 0 && w != null) {
                Thread p;
                long nc = ((long)(w.nextWait & E_MASK) |
                           (c & (AC_MASK|TC_MASK)));
                int ne = (e + E_SEQ) & E_MASK;
                if (w.eventCount == (e | INT_SIGN) &&
                    U.compareAndSwapLong(this, CTL, c, nc)) {
                    w.eventCount = ne;
                    if ((p = w.parker) != null)
                        U.unpark(p);
                    return true;   // replace with idle worker
                }
            }
            else if ((tc = (short)(c >>> TC_SHIFT)) >= 0 &&
                     (int)(c >> AC_SHIFT) + pc > 1) {
                long nc = ((c - AC_UNIT) & AC_MASK) | (c & ~AC_MASK);
                if (U.compareAndSwapLong(this, CTL, c, nc))
                    return true;   // no compensation
            }
            else if (tc + pc < MAX_CAP) {
                long nc = ((c + TC_UNIT) & TC_MASK) | (c & ~TC_MASK);
                if (U.compareAndSwapLong(this, CTL, c, nc)) {
                    ForkJoinWorkerThreadFactory fac;
                    Throwable ex = null;
                    ForkJoinWorkerThread wt = null;
                    try {
                        if ((fac = factory) != null &&
                            (wt = fac.newThread(this)) != null) {
                            wt.start();
                            return true;
                        }
                    } catch (Throwable rex) {
                        ex = rex;
                    }
                    deregisterWorker(wt, ex); // clean up and return false
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
        int s = 0;
        if (task != null && (s = task.status) >= 0 && joiner != null) {
            ForkJoinTask<?> prevJoin = joiner.currentJoin;
            joiner.currentJoin = task;
            do {} while (joiner.tryRemoveAndExec(task) && // process local tasks
                         (s = task.status) >= 0);
            if (s >= 0 && (task instanceof CountedCompleter))
                s = helpComplete(joiner, (CountedCompleter<?>)task);
            long cc = 0;        // for stability checks
            while (s >= 0 && (s = task.status) >= 0) {
                if ((s = tryHelpStealer(joiner, task)) == 0 &&
                    (s = task.status) >= 0) {
                    if (!tryCompensate(cc))
                        cc = ctl;
                    else {
                        if (task.trySetSignal() && (s = task.status) >= 0) {
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
                        long c; // reactivate
                        do {} while (!U.compareAndSwapLong
                                     (this, CTL, c = ctl,
                                      ((c & ~AC_MASK) |
                                       ((c & AC_MASK) + AC_UNIT))));
                    }
                }
            }
            joiner.currentJoin = prevJoin;
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
     */
    final void helpJoinOnce(WorkQueue joiner, ForkJoinTask<?> task) {
        int s;
        if (joiner != null && task != null && (s = task.status) >= 0) {
            ForkJoinTask<?> prevJoin = joiner.currentJoin;
            joiner.currentJoin = task;
            do {} while (joiner.tryRemoveAndExec(task) && // process local tasks
                         (s = task.status) >= 0);
            if (s >= 0) {
                if (task instanceof CountedCompleter)
                    helpComplete(joiner, (CountedCompleter<?>)task);
                do {} while (task.status >= 0 &&
                             tryHelpStealer(joiner, task) > 0);
            }
            joiner.currentJoin = prevJoin;
        }
    }

    /**
     * Returns a (probably) non-empty steal queue, if one is found
     * during a scan, else null.  This method must be retried by
     * caller if, by the time it tries to use the queue, it is empty.
     */
    private WorkQueue findNonEmptyStealQueue() {
        int r = ThreadLocalRandom.current().nextInt();
        for (;;) {
            int ps = plock, m; WorkQueue[] ws; WorkQueue q;
            if ((ws = workQueues) != null && (m = ws.length - 1) >= 0) {
                for (int j = (m + 1) << 2; j >= 0; --j) {
                    if ((q = ws[(((r - j) << 1) | 1) & m]) != null &&
                        q.base - q.top < 0)
                        return q;
                }
            }
            if (plock == ps)
                return null;
        }
    }

    /**
     * Runs tasks until {@code isQuiescent()}. We piggyback on
     * active count ctl maintenance, but rather than blocking
     * when tasks cannot be found, we rescan until all others cannot
     * find tasks either.
     */
    final void helpQuiescePool(WorkQueue w) {
        ForkJoinTask<?> ps = w.currentSteal;
        for (boolean active = true;;) {
            long c; WorkQueue q; ForkJoinTask<?> t; int b;
            while ((t = w.nextLocalTask()) != null)
                t.doExec();
            if ((q = findNonEmptyStealQueue()) != null) {
                if (!active) {      // re-establish active count
                    active = true;
                    do {} while (!U.compareAndSwapLong
                                 (this, CTL, c = ctl,
                                  ((c & ~AC_MASK) |
                                   ((c & AC_MASK) + AC_UNIT))));
                }
                if ((b = q.base) - q.top < 0 && (t = q.pollAt(b)) != null) {
                    (w.currentSteal = t).doExec();
                    w.currentSteal = ps;
                }
            }
            else if (active) {      // decrement active count without queuing
                long nc = ((c = ctl) & ~AC_MASK) | ((c & AC_MASK) - AC_UNIT);
                if ((int)(nc >> AC_SHIFT) + parallelism == 0)
                    break;          // bypass decrement-then-increment
                if (U.compareAndSwapLong(this, CTL, c, nc))
                    active = false;
            }
            else if ((int)((c = ctl) >> AC_SHIFT) + parallelism <= 0 &&
                     U.compareAndSwapLong
                     (this, CTL, c, ((c & ~AC_MASK) |
                                     ((c & AC_MASK) + AC_UNIT))))
                break;
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
            if ((q = findNonEmptyStealQueue()) == null)
                return null;
            if ((b = q.base) - q.top < 0 && (t = q.pollAt(b)) != null)
                return t;
        }
    }

    /**
     * Returns a cheap heuristic guide for task partitioning when
     * programmers, frameworks, tools, or languages have little or no
     * idea about task granularity.  In essence by offering this
     * method, we ask users only about tradeoffs in overhead vs
     * expected throughput and its variance, rather than how finely to
     * partition tasks.
     *
     * In a steady state strict (tree-structured) computation, each
     * thread makes available for stealing enough tasks for other
     * threads to remain active. Inductively, if all threads play by
     * the same rules, each thread should make available only a
     * constant number of tasks.
     *
     * The minimum useful constant is just 1. But using a value of 1
     * would require immediate replenishment upon each steal to
     * maintain enough tasks, which is infeasible.  Further,
     * partitionings/granularities of offered tasks should minimize
     * steal rates, which in general means that threads nearer the top
     * of computation tree should generate more than those nearer the
     * bottom. In perfect steady state, each thread is at
     * approximately the same level of computation tree. However,
     * producing extra tasks amortizes the uncertainty of progress and
     * diffusion assumptions.
     *
     * So, users will want to use values larger (but not much larger)
     * than 1 to both smooth over transient shortages and hedge
     * against uneven progress; as traded off against the cost of
     * extra task overhead. We leave the user to pick a threshold
     * value to compare with the results of this call to guide
     * decisions, but recommend values such as 3.
     *
     * When all threads are active, it is on average OK to estimate
     * surplus strictly locally. In steady-state, if one thread is
     * maintaining say 2 surplus tasks, then so are others. So we can
     * just use estimated queue length.  However, this strategy alone
     * leads to serious mis-estimates in some non-steady-state
     * conditions (ramp-up, ramp-down, other stalls). We can detect
     * many of these by further considering the number of "idle"
     * threads, that are known to have zero queued tasks, so
     * compensate by a factor of (#idle/#active) threads.
     *
     * Note: The approximation of #busy workers as #active workers is
     * not very good under current signalling scheme, and should be
     * improved.
     */
    static int getSurplusQueuedTaskCount() {
        Thread t; ForkJoinWorkerThread wt; ForkJoinPool pool; WorkQueue q;
        if (((t = Thread.currentThread()) instanceof ForkJoinWorkerThread)) {
            int p = (pool = (wt = (ForkJoinWorkerThread)t).pool).parallelism;
            int n = (q = wt.workQueue).top - q.base;
            int a = (int)(pool.ctl >> AC_SHIFT) + p;
            return n - (a > (p >>>= 1) ? 0 :
                        a > (p >>>= 1) ? 1 :
                        a > (p >>>= 1) ? 2 :
                        a > (p >>>= 1) ? 4 :
                        8);
        }
        return 0;
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
        int ps;
        if (this == common)                        // cannot shut down
            return false;
        if ((ps = plock) >= 0) {                   // enable by setting plock
            if (!enable)
                return false;
            if ((ps & PL_LOCK) != 0 ||
                !U.compareAndSwapInt(this, PLOCK, ps, ps += PL_LOCK))
                ps = acquirePlock();
            int nps = ((ps + PL_LOCK) & ~SHUTDOWN) | SHUTDOWN;
            if (!U.compareAndSwapInt(this, PLOCK, ps, nps))
                releasePlock(nps);
        }
        for (long c;;) {
            if (((c = ctl) & STOP_BIT) != 0) {     // already terminating
                if ((short)(c >>> TC_SHIFT) + parallelism <= 0) {
                    synchronized (this) {
                        notifyAll();               // signal when 0 workers
                    }
                }
                return true;
            }
            if (!now) {                            // check if idle & no tasks
                WorkQueue[] ws; WorkQueue w;
                if ((int)(c >> AC_SHIFT) + parallelism > 0)
                    return false;
                if ((ws = workQueues) != null) {
                    for (int i = 0; i < ws.length; ++i) {
                        if ((w = ws[i]) != null &&
                            (!w.isEmpty() ||
                             ((i & 1) != 0 && w.eventCount >= 0))) {
                            signalWork(ws, w);
                            return false;
                        }
                    }
                }
            }
            if (U.compareAndSwapLong(this, CTL, c, c | STOP_BIT)) {
                for (int pass = 0; pass < 3; ++pass) {
                    WorkQueue[] ws; WorkQueue w; Thread wt;
                    if ((ws = workQueues) != null) {
                        int n = ws.length;
                        for (int i = 0; i < n; ++i) {
                            if ((w = ws[i]) != null) {
                                w.qlock = -1;
                                if (pass > 0) {
                                    w.cancelAll();
                                    if (pass > 1 && (wt = w.owner) != null) {
                                        if (!wt.isInterrupted()) {
                                            try {
                                                wt.interrupt();
                                            } catch (Throwable ignore) {
                                            }
                                        }
                                        U.unpark(wt);
                                    }
                                }
                            }
                        }
                        // Wake up workers parked on event queue
                        int i, e; long cc; Thread p;
                        while ((e = (int)(cc = ctl) & E_MASK) != 0 &&
                               (i = e & SMASK) < n && i >= 0 &&
                               (w = ws[i]) != null) {
                            long nc = ((long)(w.nextWait & E_MASK) |
                                       ((cc + AC_UNIT) & AC_MASK) |
                                       (cc & (TC_MASK|STOP_BIT)));
                            if (w.eventCount == (e | INT_SIGN) &&
                                U.compareAndSwapLong(this, CTL, cc, nc)) {
                                w.eventCount = (e + E_SEQ) & E_MASK;
                                w.qlock = -1;
                                if ((p = w.parker) != null)
                                    U.unpark(p);
                            }
                        }
                    }
                }
            }
        }
    }

    // external operations on common pool

    /**
     * Returns common pool queue for a thread that has submitted at
     * least one task.
     */
    static WorkQueue commonSubmitterQueue() {
        Submitter z; ForkJoinPool p; WorkQueue[] ws; int m, r;
        return ((z = submitters.get()) != null &&
                (p = common) != null &&
                (ws = p.workQueues) != null &&
                (m = ws.length - 1) >= 0) ?
            ws[m & z.seed & SQMASK] : null;
    }

    /**
     * Tries to pop the given task from submitter's queue in common pool.
     */
    final boolean tryExternalUnpush(ForkJoinTask<?> task) {
        WorkQueue joiner; ForkJoinTask<?>[] a; int m, s;
        Submitter z = submitters.get();
        WorkQueue[] ws = workQueues;
        boolean popped = false;
        if (z != null && ws != null && (m = ws.length - 1) >= 0 &&
            (joiner = ws[z.seed & m & SQMASK]) != null &&
            joiner.base != (s = joiner.top) &&
            (a = joiner.array) != null) {
            long j = (((a.length - 1) & (s - 1)) << ASHIFT) + ABASE;
            if (U.getObject(a, j) == task &&
                U.compareAndSwapInt(joiner, QLOCK, 0, 1)) {
                if (joiner.top == s && joiner.array == a &&
                    U.compareAndSwapObject(a, j, task, null)) {
                    joiner.top = s - 1;
                    popped = true;
                }
                joiner.qlock = 0;
            }
        }
        return popped;
    }

    final int externalHelpComplete(CountedCompleter<?> task) {
        WorkQueue joiner; int m, j;
        Submitter z = submitters.get();
        WorkQueue[] ws = workQueues;
        int s = 0;
        if (z != null && ws != null && (m = ws.length - 1) >= 0 &&
            (joiner = ws[(j = z.seed) & m & SQMASK]) != null && task != null) {
            int scans = m + m + 1;
            long c = 0L;             // for stability check
            j |= 1;                  // poll odd queues
            for (int k = scans; ; j += 2) {
                WorkQueue q;
                if ((s = task.status) < 0)
                    break;
                else if (joiner.externalPopAndExecCC(task))
                    k = scans;
                else if ((s = task.status) < 0)
                    break;
                else if ((q = ws[j & m]) != null && q.pollAndExecCC(task))
                    k = scans;
                else if (--k < 0) {
                    if (c == (c = ctl))
                        break;
                    k = scans;
                }
            }
        }
        return s;
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
        this(Math.min(MAX_CAP, Runtime.getRuntime().availableProcessors()),
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
                        UncaughtExceptionHandler handler,
                        boolean asyncMode) {
        this(checkParallelism(parallelism),
             checkFactory(factory),
             handler,
             (asyncMode ? FIFO_QUEUE : LIFO_QUEUE),
             "ForkJoinPool-" + nextPoolId() + "-worker-");
        checkPermission();
    }

    private static int checkParallelism(int parallelism) {
        if (parallelism <= 0 || parallelism > MAX_CAP)
            throw new IllegalArgumentException();
        return parallelism;
    }

    private static ForkJoinWorkerThreadFactory checkFactory
        (ForkJoinWorkerThreadFactory factory) {
        if (factory == null)
            throw new NullPointerException();
        return factory;
    }

    /**
     * Creates a {@code ForkJoinPool} with the given parameters, without
     * any security checks or parameter validation.  Invoked directly by
     * makeCommonPool.
     */
    private ForkJoinPool(int parallelism,
                         ForkJoinWorkerThreadFactory factory,
                         UncaughtExceptionHandler handler,
                         int mode,
                         String workerNamePrefix) {
        this.workerNamePrefix = workerNamePrefix;
        this.factory = factory;
        this.ueh = handler;
        this.mode = (short)mode;
        this.parallelism = (short)parallelism;
        long np = (long)(-parallelism); // offset ctl counts
        this.ctl = ((np << AC_SHIFT) & AC_MASK) | ((np << TC_SHIFT) & TC_MASK);
    }

    /**
     * Returns the common pool instance. This pool is statically
     * constructed; its run state is unaffected by attempts to {@link
     * #shutdown} or {@link #shutdownNow}. However this pool and any
     * ongoing processing are automatically terminated upon program
     * {@link System#exit}.  Any program that relies on asynchronous
     * task processing to complete before program termination should
     * invoke {@code commonPool().}{@link #awaitQuiescence awaitQuiescence},
     * before exit.
     *
     * @return the common pool instance
     * @since 1.8
     */
    public static ForkJoinPool commonPool() {
        // assert common != null : "static init error";
        return common;
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
     * @param <T> the type of the task's result
     * @return the task's result
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> T invoke(ForkJoinTask<T> task) {
        if (task == null)
            throw new NullPointerException();
        externalPush(task);
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
        externalPush(task);
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
            job = new ForkJoinTask.RunnableExecuteAction(task);
        externalPush(job);
    }

    /**
     * Submits a ForkJoinTask for execution.
     *
     * @param task the task to submit
     * @param <T> the type of the task's result
     * @return the task
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
        if (task == null)
            throw new NullPointerException();
        externalPush(task);
        return task;
    }

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Callable<T> task) {
        ForkJoinTask<T> job = new ForkJoinTask.AdaptedCallable<T>(task);
        externalPush(job);
        return job;
    }

    /**
     * @throws NullPointerException if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *         scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Runnable task, T result) {
        ForkJoinTask<T> job = new ForkJoinTask.AdaptedRunnable<T>(task, result);
        externalPush(job);
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
        externalPush(job);
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
        ArrayList<Future<T>> futures = new ArrayList<Future<T>>(tasks.size());

        boolean done = false;
        try {
            for (Callable<T> t : tasks) {
                ForkJoinTask<T> f = new ForkJoinTask.AdaptedCallable<T>(t);
                futures.add(f);
                externalPush(f);
            }
            for (int i = 0, size = futures.size(); i < size; i++)
                ((ForkJoinTask<?>)futures.get(i)).quietlyJoin();
            done = true;
            return futures;
        } finally {
            if (!done)
                for (int i = 0, size = futures.size(); i < size; i++)
                    futures.get(i).cancel(false);
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
    public UncaughtExceptionHandler getUncaughtExceptionHandler() {
        return ueh;
    }

    /**
     * Returns the targeted parallelism level of this pool.
     *
     * @return the targeted parallelism level of this pool
     */
    public int getParallelism() {
        int par;
        return ((par = parallelism) > 0) ? par : 1;
    }

    /**
     * Returns the targeted parallelism level of the common pool.
     *
     * @return the targeted parallelism level of the common pool
     * @since 1.8
     */
    public static int getCommonPoolParallelism() {
        return commonParallelism;
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
        return mode == FIFO_QUEUE;
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
        return parallelism + (int)(ctl >> AC_SHIFT) <= 0;
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
        long count = stealCount;
        WorkQueue[] ws; WorkQueue w;
        if ((ws = workQueues) != null) {
            for (int i = 1; i < ws.length; i += 2) {
                if ((w = ws[i]) != null)
                    count += w.nsteals;
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
        long st = stealCount;
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
                        st += w.nsteals;
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
            level = plock < 0 ? "Shutting down" : "Running";
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
     * Possibly initiates an orderly shutdown in which previously
     * submitted tasks are executed, but no new tasks will be
     * accepted. Invocation has no effect on execution state if this
     * is the {@link #commonPool()}, and no additional effect if
     * already shut down.  Tasks that are in the process of being
     * submitted concurrently during the course of this method may or
     * may not be rejected.
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
     * Possibly attempts to cancel and/or stop all tasks, and reject
     * all subsequently submitted tasks.  Invocation has no effect on
     * execution state if this is the {@link #commonPool()}, and no
     * additional effect if already shut down. Otherwise, tasks that
     * are in the process of being submitted or executed concurrently
     * during the course of this method may or may not be
     * rejected. This method cancels both existing and unexecuted
     * tasks, in order to permit termination in the presence of task
     * dependencies. So the method always returns an empty list
     * (unlike the case for some other Executors).
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
                (short)(c >>> TC_SHIFT) + parallelism <= 0);
    }

    /**
     * Returns {@code true} if the process of termination has
     * commenced but not yet completed.  This method may be useful for
     * debugging. A return of {@code true} reported a sufficient
     * period after shutdown may indicate that submitted tasks have
     * ignored or suppressed interruption, or are waiting for I/O,
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
                (short)(c >>> TC_SHIFT) + parallelism > 0);
    }

    /**
     * Returns {@code true} if this pool has been shut down.
     *
     * @return {@code true} if this pool has been shut down
     */
    public boolean isShutdown() {
        return plock < 0;
    }

    /**
     * Blocks until all tasks have completed execution after a
     * shutdown request, or the timeout occurs, or the current thread
     * is interrupted, whichever happens first. Because the {@link
     * #commonPool()} never terminates until program shutdown, when
     * applied to the common pool, this method is equivalent to {@link
     * #awaitQuiescence(long, TimeUnit)} but always returns {@code false}.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if this executor terminated and
     *         {@code false} if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(long timeout, TimeUnit unit)
        throws InterruptedException {
        if (Thread.interrupted())
            throw new InterruptedException();
        if (this == common) {
            awaitQuiescence(timeout, unit);
            return false;
        }
        long nanos = unit.toNanos(timeout);
        if (isTerminated())
            return true;
        if (nanos <= 0L)
            return false;
        long deadline = System.nanoTime() + nanos;
        synchronized (this) {
            for (;;) {
                if (isTerminated())
                    return true;
                if (nanos <= 0L)
                    return false;
                long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
                wait(millis > 0L ? millis : 1L);
                nanos = deadline - System.nanoTime();
            }
        }
    }

    /**
     * If called by a ForkJoinTask operating in this pool, equivalent
     * in effect to {@link ForkJoinTask#helpQuiesce}. Otherwise,
     * waits and/or attempts to assist performing tasks until this
     * pool {@link #isQuiescent} or the indicated timeout elapses.
     *
     * @param timeout the maximum time to wait
     * @param unit the time unit of the timeout argument
     * @return {@code true} if quiescent; {@code false} if the
     * timeout elapsed.
     */
    public boolean awaitQuiescence(long timeout, TimeUnit unit) {
        long nanos = unit.toNanos(timeout);
        ForkJoinWorkerThread wt;
        Thread thread = Thread.currentThread();
        if ((thread instanceof ForkJoinWorkerThread) &&
            (wt = (ForkJoinWorkerThread)thread).pool == this) {
            helpQuiescePool(wt.workQueue);
            return true;
        }
        long startTime = System.nanoTime();
        WorkQueue[] ws;
        int r = 0, m;
        boolean found = true;
        while (!isQuiescent() && (ws = workQueues) != null &&
               (m = ws.length - 1) >= 0) {
            if (!found) {
                if ((System.nanoTime() - startTime) > nanos)
                    return false;
                Thread.yield(); // cannot block
            }
            found = false;
            for (int j = (m + 1) << 2; j >= 0; --j) {
                ForkJoinTask<?> t; WorkQueue q; int b;
                if ((q = ws[r++ & m]) != null && (b = q.base) - q.top < 0) {
                    found = true;
                    if ((t = q.pollAt(b)) != null)
                        t.doExec();
                    break;
                }
            }
        }
        return true;
    }

    /**
     * Waits and/or attempts to assist performing tasks indefinitely
     * until the {@link #commonPool()} {@link #isQuiescent}.
     */
    static void quiesceCommonPool() {
        common.awaitQuiescence(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
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
     * thread invoking {@link ForkJoinPool#managedBlock(ManagedBlocker)}.
     * The unusual methods in this API accommodate synchronizers that
     * may, but don't usually, block for long periods. Similarly, they
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
         * @return {@code true} if blocking is unnecessary
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
        if (t instanceof ForkJoinWorkerThread) {
            ForkJoinPool p = ((ForkJoinWorkerThread)t).pool;
            while (!blocker.isReleasable()) {
                if (p.tryCompensate(p.ctl)) {
                    try {
                        do {} while (!blocker.isReleasable() &&
                                     !blocker.block());
                    } finally {
                        p.incrementActiveCount();
                    }
                    break;
                }
            }
        }
        else {
            do {} while (!blocker.isReleasable() &&
                         !blocker.block());
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
    private static final long STEALCOUNT;
    private static final long PLOCK;
    private static final long INDEXSEED;
    private static final long QBASE;
    private static final long QLOCK;

    static {
        // initialize field offsets for CAS etc
        try {
            U = getUnsafe();
            Class<?> k = ForkJoinPool.class;
            CTL = U.objectFieldOffset
                (k.getDeclaredField("ctl"));
            STEALCOUNT = U.objectFieldOffset
                (k.getDeclaredField("stealCount"));
            PLOCK = U.objectFieldOffset
                (k.getDeclaredField("plock"));
            INDEXSEED = U.objectFieldOffset
                (k.getDeclaredField("indexSeed"));
            Class<?> tk = Thread.class;
            PARKBLOCKER = U.objectFieldOffset
                (tk.getDeclaredField("parkBlocker"));
            Class<?> wk = WorkQueue.class;
            QBASE = U.objectFieldOffset
                (wk.getDeclaredField("base"));
            QLOCK = U.objectFieldOffset
                (wk.getDeclaredField("qlock"));
            Class<?> ak = ForkJoinTask[].class;
            ABASE = U.arrayBaseOffset(ak);
            int scale = U.arrayIndexScale(ak);
            if ((scale & (scale - 1)) != 0)
                throw new Error("data type scale not a power of two");
            ASHIFT = 31 - Integer.numberOfLeadingZeros(scale);
        } catch (Exception e) {
            throw new Error(e);
        }

        submitters = new ThreadLocal<Submitter>();
        defaultForkJoinWorkerThreadFactory =
            new DefaultForkJoinWorkerThreadFactory();
        modifyThreadPermission = new RuntimePermission("modifyThread");

        common = java.security.AccessController.doPrivileged
            (new java.security.PrivilegedAction<ForkJoinPool>() {
                public ForkJoinPool run() { return makeCommonPool(); }});
        int par = common.parallelism; // report 1 even if threads disabled
        commonParallelism = par > 0 ? par : 1;
    }

    /**
     * Creates and returns the common pool, respecting user settings
     * specified via system properties.
     */
    private static ForkJoinPool makeCommonPool() {
        int parallelism = -1;
        ForkJoinWorkerThreadFactory factory
            = defaultForkJoinWorkerThreadFactory;
        UncaughtExceptionHandler handler = null;
        try {  // ignore exceptions in accessing/parsing properties
            String pp = System.getProperty
                ("java.util.concurrent.ForkJoinPool.common.parallelism");
            String fp = System.getProperty
                ("java.util.concurrent.ForkJoinPool.common.threadFactory");
            String hp = System.getProperty
                ("java.util.concurrent.ForkJoinPool.common.exceptionHandler");
            if (pp != null)
                parallelism = Integer.parseInt(pp);
            if (fp != null)
                factory = ((ForkJoinWorkerThreadFactory)ClassLoader.
                           getSystemClassLoader().loadClass(fp).newInstance());
            if (hp != null)
                handler = ((UncaughtExceptionHandler)ClassLoader.
                           getSystemClassLoader().loadClass(hp).newInstance());
        } catch (Exception ignore) {
        }

        if (parallelism < 0 && // default 1 less than #cores
            (parallelism = Runtime.getRuntime().availableProcessors() - 1) < 0)
            parallelism = 0;
        if (parallelism > MAX_CAP)
            parallelism = MAX_CAP;
        return new ForkJoinPool(parallelism, factory, handler, LIFO_QUEUE,
                                "ForkJoinPool.commonPool-worker-");
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

