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

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

/**
 * An {@link ExecutorService} for running {@link ForkJoinTask}s.
 * A {@code ForkJoinPool} provides the entry point for submissions
 * from non-{@code ForkJoinTask}s, as well as management and
 * monitoring operations.
 *
 * <p>A {@code ForkJoinPool} differs from other kinds of {@link
 * ExecutorService} mainly by virtue of employing
 * <em>work-stealing</em>: all threads in the pool attempt to find and
 * execute subtasks created by other active tasks (eventually blocking
 * waiting for work if none exist). This enables efficient processing
 * when most tasks spawn other subtasks (as do most {@code
 * ForkJoinTask}s). A {@code ForkJoinPool} may also be used for mixed
 * execution of some plain {@code Runnable}- or {@code Callable}-
 * based activities along with {@code ForkJoinTask}s. When setting
 * {@linkplain #setAsyncMode async mode}, a {@code ForkJoinPool} may
 * also be appropriate for use with fine-grained tasks of any form
 * that are never joined. Otherwise, other {@code ExecutorService}
 * implementations are typically more appropriate choices.
 *
 * <p>A {@code ForkJoinPool} is constructed with a given target
 * parallelism level; by default, equal to the number of available
 * processors. Unless configured otherwise via {@link
 * #setMaintainsParallelism}, the pool attempts to maintain this
 * number of active (or available) threads by dynamically adding,
 * suspending, or resuming internal worker threads, even if some tasks
 * are stalled waiting to join others. However, no such adjustments
 * are performed in the face of blocked IO or other unmanaged
 * synchronization. The nested {@link ManagedBlocker} interface
 * enables extension of the kinds of synchronization accommodated.
 * The target parallelism level may also be changed dynamically
 * ({@link #setParallelism}). The total number of threads may be
 * limited using method {@link #setMaximumPoolSize}, in which case it
 * may become possible for the activities of a pool to stall due to
 * the lack of available threads to process new tasks. When the pool
 * is executing tasks, these and other configuration setting methods
 * may only gradually affect actual pool sizes. It is normally best
 * practice to invoke these methods only when the pool is known to be
 * quiescent.
 *
 * <p>In addition to execution and lifecycle control methods, this
 * class provides status check methods (for example
 * {@link #getStealCount}) that are intended to aid in developing,
 * tuning, and monitoring fork/join applications. Also, method
 * {@link #toString} returns indications of pool state in a
 * convenient form for informal monitoring.
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
 * <pre>
 * static final ForkJoinPool mainPool = new ForkJoinPool();
 * ...
 * public void sort(long[] array) {
 *   mainPool.invoke(new SortTask(array, 0, array.length));
 * }
 * </pre>
 *
 * <p><b>Implementation notes</b>: This implementation restricts the
 * maximum number of running threads to 32767. Attempts to create
 * pools with greater than the maximum number result in
 * {@code IllegalArgumentException}.
 *
 * <p>This implementation rejects submitted tasks (that is, by throwing
 * {@link RejectedExecutionException}) only when the pool is shut down.
 *
 * @author Doug Lea
 * @since 1.7
 */
public class ForkJoinPool extends AbstractExecutorService {

    /*
     * Implementation Overview
     *
     * This class provides the central bookkeeping and control for a
     * set of worker threads: Submissions from non-FJ threads enter
     * into a submission queue. Workers take these tasks and typically
     * split them into subtasks that may be stolen by other workers.
     * The main work-stealing mechanics implemented in class
     * ForkJoinWorkerThread give first priority to processing tasks
     * from their own queues (LIFO or FIFO, depending on mode), then
     * to randomized FIFO steals of tasks in other worker queues, and
     * lastly to new submissions. These mechanics do not consider
     * affinities, loads, cache localities, etc, so rarely provide the
     * best possible performance on a given machine, but portably
     * provide good throughput by averaging over these factors.
     * (Further, even if we did try to use such information, we do not
     * usually have a basis for exploiting it. For example, some sets
     * of tasks profit from cache affinities, but others are harmed by
     * cache pollution effects.)
     *
     * The main throughput advantages of work-stealing stem from
     * decentralized control -- workers mostly steal tasks from each
     * other. We do not want to negate this by creating bottlenecks
     * implementing the management responsibilities of this class. So
     * we use a collection of techniques that avoid, reduce, or cope
     * well with contention. These entail several instances of
     * bit-packing into CASable fields to maintain only the minimally
     * required atomicity. To enable such packing, we restrict maximum
     * parallelism to (1<<15)-1 (enabling twice this to fit into a 16
     * bit field), which is far in excess of normal operating range.
     * Even though updates to some of these bookkeeping fields do
     * sometimes contend with each other, they don't normally
     * cache-contend with updates to others enough to warrant memory
     * padding or isolation. So they are all held as fields of
     * ForkJoinPool objects.  The main capabilities are as follows:
     *
     * 1. Creating and removing workers. Workers are recorded in the
     * "workers" array. This is an array as opposed to some other data
     * structure to support index-based random steals by workers.
     * Updates to the array recording new workers and unrecording
     * terminated ones are protected from each other by a lock
     * (workerLock) but the array is otherwise concurrently readable,
     * and accessed directly by workers. To simplify index-based
     * operations, the array size is always a power of two, and all
     * readers must tolerate null slots. Currently, all but the first
     * worker thread creation is on-demand, triggered by task
     * submissions, replacement of terminated workers, and/or
     * compensation for blocked workers. However, all other support
     * code is set up to work with other policies.
     *
     * 2. Bookkeeping for dynamically adding and removing workers. We
     * maintain a given level of parallelism (or, if
     * maintainsParallelism is false, at least avoid starvation). When
     * some workers are known to be blocked (on joins or via
     * ManagedBlocker), we may create or resume others to take their
     * place until they unblock (see below). Implementing this
     * requires counts of the number of "running" threads (i.e., those
     * that are neither blocked nor artifically suspended) as well as
     * the total number.  These two values are packed into one field,
     * "workerCounts" because we need accurate snapshots when deciding
     * to create, resume or suspend.  To support these decisions,
     * updates must be prospective (not retrospective).  For example,
     * the running count is decremented before blocking by a thread
     * about to block, but incremented by the thread about to unblock
     * it. (In a few cases, these prospective updates may need to be
     * rolled back, for example when deciding to create a new worker
     * but the thread factory fails or returns null. In these cases,
     * we are no worse off wrt other decisions than we would be
     * otherwise.)  Updates to the workerCounts field sometimes
     * transiently encounter a fair amount of contention when join
     * dependencies are such that many threads block or unblock at
     * about the same time. We alleviate this by sometimes bundling
     * updates (for example blocking one thread on join and resuming a
     * spare cancel each other out), and in most other cases
     * performing an alternative action (like releasing waiters and
     * finding spares; see below) as a more productive form of
     * backoff.
     *
     * 3. Maintaining global run state. The run state of the pool
     * consists of a runLevel (SHUTDOWN, TERMINATING, etc) similar to
     * those in other Executor implementations, as well as a count of
     * "active" workers -- those that are, or soon will be, or
     * recently were executing tasks. The runLevel and active count
     * are packed together in order to correctly trigger shutdown and
     * termination. Without care, active counts can be subject to very
     * high contention.  We substantially reduce this contention by
     * relaxing update rules.  A worker must claim active status
     * prospectively, by activating if it sees that a submitted or
     * stealable task exists (it may find after activating that the
     * task no longer exists). It stays active while processing this
     * task (if it exists) and any other local subtasks it produces,
     * until it cannot find any other tasks. It then tries
     * inactivating (see method preStep), but upon update contention
     * instead scans for more tasks, later retrying inactivation if it
     * doesn't find any.
     *
     * 4. Managing idle workers waiting for tasks. We cannot let
     * workers spin indefinitely scanning for tasks when none are
     * available. On the other hand, we must quickly prod them into
     * action when new tasks are submitted or generated.  We
     * park/unpark these idle workers using an event-count scheme.
     * Field eventCount is incremented upon events that may enable
     * workers that previously could not find a task to now find one:
     * Submission of a new task to the pool, or another worker pushing
     * a task onto a previously empty queue.  (We also use this
     * mechanism for termination and reconfiguration actions that
     * require wakeups of idle workers).  Each worker maintains its
     * last known event count, and blocks when a scan for work did not
     * find a task AND its lastEventCount matches the current
     * eventCount. Waiting idle workers are recorded in a variant of
     * Treiber stack headed by field eventWaiters which, when nonzero,
     * encodes the thread index and count awaited for by the worker
     * thread most recently calling eventSync. This thread in turn has
     * a record (field nextEventWaiter) for the next waiting worker.
     * In addition to allowing simpler decisions about need for
     * wakeup, the event count bits in eventWaiters serve the role of
     * tags to avoid ABA errors in Treiber stacks.  To reduce delays
     * in task diffusion, workers not otherwise occupied may invoke
     * method releaseWaiters, that removes and signals (unparks)
     * workers not waiting on current count. To minimize task
     * production stalls associate with signalling, any worker pushing
     * a task on an empty queue invokes the weaker method signalWork,
     * that only releases idle workers until it detects interference
     * by other threads trying to release, and lets them take
     * over. The net effect is a tree-like diffusion of signals, where
     * released threads and possibly others) help with unparks.  To
     * further reduce contention effects a bit, failed CASes to
     * increment field eventCount are tolerated without retries.
     * Conceptually they are merged into the same event, which is OK
     * when their only purpose is to enable workers to scan for work.
     *
     * 5. Managing suspension of extra workers. When a worker is about
     * to block waiting for a join (or via ManagedBlockers), we may
     * create a new thread to maintain parallelism level, or at least
     * avoid starvation (see below). Usually, extra threads are needed
     * for only very short periods, yet join dependencies are such
     * that we sometimes need them in bursts. Rather than create new
     * threads each time this happens, we suspend no-longer-needed
     * extra ones as "spares". For most purposes, we don't distinguish
     * "extra" spare threads from normal "core" threads: On each call
     * to preStep (the only point at which we can do this) a worker
     * checks to see if there are now too many running workers, and if
     * so, suspends itself.  Methods preJoin and doBlock look for
     * suspended threads to resume before considering creating a new
     * replacement. We don't need a special data structure to maintain
     * spares; simply scanning the workers array looking for
     * worker.isSuspended() is fine because the calling thread is
     * otherwise not doing anything useful anyway; we are at least as
     * happy if after locating a spare, the caller doesn't actually
     * block because the join is ready before we try to adjust and
     * compensate.  Note that this is intrinsically racy.  One thread
     * may become a spare at about the same time as another is
     * needlessly being created. We counteract this and related slop
     * in part by requiring resumed spares to immediately recheck (in
     * preStep) to see whether they they should re-suspend. The only
     * effective difference between "extra" and "core" threads is that
     * we allow the "extra" ones to time out and die if they are not
     * resumed within a keep-alive interval of a few seconds. This is
     * implemented mainly within ForkJoinWorkerThread, but requires
     * some coordination (isTrimmed() -- meaning killed while
     * suspended) to correctly maintain pool counts.
     *
     * 6. Deciding when to create new workers. The main dynamic
     * control in this class is deciding when to create extra threads,
     * in methods preJoin and doBlock. We always need to create one
     * when the number of running threads becomes zero. But because
     * blocked joins are typically dependent, we don't necessarily
     * need or want one-to-one replacement. Using a one-to-one
     * compensation rule often leads to enough useless overhead
     * creating, suspending, resuming, and/or killing threads to
     * signficantly degrade throughput.  We use a rule reflecting the
     * idea that, the more spare threads you already have, the more
     * evidence you need to create another one; where "evidence" is
     * expressed as the current deficit -- target minus running
     * threads. To reduce flickering and drift around target values,
     * the relation is quadratic: adding a spare if (dc*dc)>=(sc*pc)
     * (where dc is deficit, sc is number of spare threads and pc is
     * target parallelism.)  This effectively reduces churn at the
     * price of systematically undershooting target parallelism when
     * many threads are blocked.  However, biasing toward undeshooting
     * partially compensates for the above mechanics to suspend extra
     * threads, that normally lead to overshoot because we can only
     * suspend workers in-between top-level actions. It also better
     * copes with the fact that some of the methods in this class tend
     * to never become compiled (but are interpreted), so some
     * components of the entire set of controls might execute many
     * times faster than others. And similarly for cases where the
     * apparent lack of work is just due to GC stalls and other
     * transient system activity.
     *
     * 7. Maintaining other configuration parameters and monitoring
     * statistics. Updates to fields controlling parallelism level,
     * max size, etc can only meaningfully take effect for individual
     * threads upon their next top-level actions; i.e., between
     * stealing/running tasks/submission, which are separated by calls
     * to preStep.  Memory ordering for these (assumed infrequent)
     * reconfiguration calls is ensured by using reads and writes to
     * volatile field workerCounts (that must be read in preStep anyway)
     * as "fences" -- user-level reads are preceded by reads of
     * workCounts, and writes are followed by no-op CAS to
     * workerCounts. The values reported by other management and
     * monitoring methods are either computed on demand, or are kept
     * in fields that are only updated when threads are otherwise
     * idle.
     *
     * Beware that there is a lot of representation-level coupling
     * among classes ForkJoinPool, ForkJoinWorkerThread, and
     * ForkJoinTask.  For example, direct access to "workers" array by
     * workers, and direct access to ForkJoinTask.status by both
     * ForkJoinPool and ForkJoinWorkerThread.  There is little point
     * trying to reduce this, since any associated future changes in
     * representations will need to be accompanied by algorithmic
     * changes anyway.
     *
     * Style notes: There are lots of inline assignments (of form
     * "while ((local = field) != 0)") which are usually the simplest
     * way to ensure read orderings. Also several occurrences of the
     * unusual "do {} while(!cas...)" which is the simplest way to
     * force an update of a CAS'ed variable. There are also a few
     * other coding oddities that help some methods perform reasonably
     * even when interpreted (not compiled).
     *
     * The order of declarations in this file is: (1) statics (2)
     * fields (along with constants used when unpacking some of them)
     * (3) internal control methods (4) callbacks and other support
     * for ForkJoinTask and ForkJoinWorkerThread classes, (5) exported
     * methods (plus a few little helpers).
     */

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
     * Creates a new ForkJoinWorkerThread. This factory is used unless
     * overridden in ForkJoinPool constructors.
     */
    public static final ForkJoinWorkerThreadFactory
            defaultForkJoinWorkerThreadFactory =
            new DefaultForkJoinWorkerThreadFactory();

    /**
     * Permission required for callers of methods that may start or
     * kill threads.
     */
    private static final RuntimePermission modifyThreadPermission =
            new RuntimePermission("modifyThread");

    /**
     * If there is a security manager, makes sure caller has
     * permission to modify threads.
     */
    private static void checkPermission() {
        SecurityManager security = System.getSecurityManager();
        if (security != null)
            security.checkPermission(modifyThreadPermission);
    }

    /**
     * Generator for assigning sequence numbers as pool names.
     */
    private static final AtomicInteger poolNumberGenerator =
            new AtomicInteger();

    /**
     * Absolute bound for parallelism level. Twice this number must
     * fit into a 16bit field to enable word-packing for some counts.
     */
    private static final int MAX_THREADS = 0x7fff;

    /**
     * Array holding all worker threads in the pool.  Array size must
     * be a power of two.  Updates and replacements are protected by
     * workerLock, but the array is always kept in a consistent enough
     * state to be randomly accessed without locking by workers
     * performing work-stealing, as well as other traversal-based
     * methods in this class. All readers must tolerate that some
     * array slots may be null.
     */
    volatile ForkJoinWorkerThread[] workers;

    /**
     * Queue for external submissions.
     */
    private final LinkedTransferQueue<ForkJoinTask<?>> submissionQueue;

    /**
     * Lock protecting updates to workers array.
     */
    private final ReentrantLock workerLock;

    /**
     * Latch released upon termination.
     */
    private final CountDownLatch terminationLatch;

    /**
     * Creation factory for worker threads.
     */
    private final ForkJoinWorkerThreadFactory factory;

    /**
     * Sum of per-thread steal counts, updated only when threads are
     * idle or terminating.
     */
    private volatile long stealCount;

    /**
     * Encoded record of top of treiber stack of threads waiting for
     * events. The top 32 bits contain the count being waited for. The
     * bottom word contains one plus the pool index of waiting worker
     * thread.
     */
    private volatile long eventWaiters;

    private static final int EVENT_COUNT_SHIFT = 32;
    private static final long WAITER_INDEX_MASK = (1L << EVENT_COUNT_SHIFT) - 1L;

    /**
     * A counter for events that may wake up worker threads:
     * - Submission of a new task to the pool
     * - A worker pushing a task on an empty queue
     * - termination and reconfiguration
     */
    private volatile int eventCount;

    /**
     * Lifecycle control. The low word contains the number of workers
     * that are (probably) executing tasks. This value is atomically
     * incremented before a worker gets a task to run, and decremented
     * when worker has no tasks and cannot find any.  Bits 16-18
     * contain runLevel value. When all are zero, the pool is
     * running. Level transitions are monotonic (running -> shutdown
     * -> terminating -> terminated) so each transition adds a bit.
     * These are bundled together to ensure consistent read for
     * termination checks (i.e., that runLevel is at least SHUTDOWN
     * and active threads is zero).
     */
    private volatile int runState;

    // Note: The order among run level values matters.
    private static final int RUNLEVEL_SHIFT = 16;
    private static final int SHUTDOWN = 1 << RUNLEVEL_SHIFT;
    private static final int TERMINATING = 1 << (RUNLEVEL_SHIFT + 1);
    private static final int TERMINATED = 1 << (RUNLEVEL_SHIFT + 2);
    private static final int ACTIVE_COUNT_MASK = (1 << RUNLEVEL_SHIFT) - 1;
    private static final int ONE_ACTIVE = 1; // active update delta

    /**
     * Holds number of total (i.e., created and not yet terminated)
     * and running (i.e., not blocked on joins or other managed sync)
     * threads, packed together to ensure consistent snapshot when
     * making decisions about creating and suspending spare
     * threads. Updated only by CAS. Note that adding a new worker
     * requires incrementing both counts, since workers start off in
     * running state.  This field is also used for memory-fencing
     * configuration parameters.
     */
    private volatile int workerCounts;

    private static final int TOTAL_COUNT_SHIFT = 16;
    private static final int RUNNING_COUNT_MASK = (1 << TOTAL_COUNT_SHIFT) - 1;
    private static final int ONE_RUNNING = 1;
    private static final int ONE_TOTAL = 1 << TOTAL_COUNT_SHIFT;

    /*
     * Fields parallelism. maxPoolSize, locallyFifo,
     * maintainsParallelism, and ueh are non-volatile, but external
     * reads/writes use workerCount fences to ensure visability.
     */

    /**
     * The target parallelism level.
     */
    private int parallelism;

    /**
     * The maximum allowed pool size.
     */
    private int maxPoolSize;

    /**
     * True if use local fifo, not default lifo, for local polling
     * Replicated by ForkJoinWorkerThreads
     */
    private boolean locallyFifo;

    /**
     * Controls whether to add spares to maintain parallelism
     */
    private boolean maintainsParallelism;

    /**
     * The uncaught exception handler used when any worker
     * abruptly terminates
     */
    private Thread.UncaughtExceptionHandler ueh;

    /**
     * Pool number, just for assigning useful names to worker threads
     */
    private final int poolNumber;

    // utilities for updating fields

    /**
     * Adds delta to running count.  Used mainly by ForkJoinTask.
     *
     * @param delta the number to add
     */
    final void updateRunningCount(int delta) {
        int wc;
        do {
        } while (!UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                wc = workerCounts,
                wc + delta));
    }

    /**
     * Write fence for user modifications of pool parameters
     * (parallelism. etc).  Note that it doesn't matter if CAS fails.
     */
    private void workerCountWriteFence() {
        int wc;
        UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                wc = workerCounts, wc);
    }

    /**
     * Read fence for external reads of pool parameters
     * (parallelism. maxPoolSize, etc).
     */
    private void workerCountReadFence() {
        int ignore = workerCounts;
    }

    /**
     * Tries incrementing active count; fails on contention.
     * Called by workers before executing tasks.
     *
     * @return true on success
     */
    final boolean tryIncrementActiveCount() {
        int c;
        return UNSAFE.compareAndSwapInt(this, runStateOffset,
                c = runState, c + ONE_ACTIVE);
    }

    /**
     * Tries decrementing active count; fails on contention.
     * Called when workers cannot find tasks to run.
     */
    final boolean tryDecrementActiveCount() {
        int c;
        return UNSAFE.compareAndSwapInt(this, runStateOffset,
                c = runState, c - ONE_ACTIVE);
    }

    /**
     * Advances to at least the given level. Returns true if not
     * already in at least the given level.
     */
    private boolean advanceRunLevel(int level) {
        for (; ;) {
            int s = runState;
            if ((s & level) != 0)
                return false;
            if (UNSAFE.compareAndSwapInt(this, runStateOffset, s, s | level))
                return true;
        }
    }

    // workers array maintenance

    /**
     * Records and returns a workers array index for new worker.
     */
    private int recordWorker(ForkJoinWorkerThread w) {
        // Try using slot totalCount-1. If not available, scan and/or resize
        int k = (workerCounts >>> TOTAL_COUNT_SHIFT) - 1;
        final ReentrantLock lock = this.workerLock;
        lock.lock();
        try {
            ForkJoinWorkerThread[] ws = workers;
            int len = ws.length;
            if (k < 0 || k >= len || ws[k] != null) {
                for (k = 0; k < len && ws[k] != null; ++k)
                    ;
                if (k == len)
                    ws = Arrays.copyOf(ws, len << 1);
            }
            ws[k] = w;
            workers = ws; // volatile array write ensures slot visibility
        } finally {
            lock.unlock();
        }
        return k;
    }

    /**
     * Nulls out record of worker in workers array
     */
    private void forgetWorker(ForkJoinWorkerThread w) {
        int idx = w.poolIndex;
        // Locking helps method recordWorker avoid unecessary expansion
        final ReentrantLock lock = this.workerLock;
        lock.lock();
        try {
            ForkJoinWorkerThread[] ws = workers;
            if (idx >= 0 && idx < ws.length && ws[idx] == w) // verify
                ws[idx] = null;
        } finally {
            lock.unlock();
        }
    }

    // adding and removing workers

    /**
     * Tries to create and add new worker. Assumes that worker counts
     * are already updated to accommodate the worker, so adjusts on
     * failure.
     *
     * @return new worker or null if creation failed
     */
    private ForkJoinWorkerThread addWorker() {
        ForkJoinWorkerThread w = null;
        try {
            w = factory.newThread(this);
        } finally { // Adjust on either null or exceptional factory return
            if (w == null) {
                onWorkerCreationFailure();
                return null;
            }
        }
        w.start(recordWorker(w), locallyFifo, ueh);
        return w;
    }

    /**
     * Adjusts counts upon failure to create worker
     */
    private void onWorkerCreationFailure() {
        int c;
        do {
        } while (!UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                c = workerCounts,
                c - (ONE_RUNNING | ONE_TOTAL)));
        tryTerminate(false); // in case of failure during shutdown
    }

    /**
     * Create enough total workers to establish target parallelism,
     * giving up if terminating or addWorker fails
     */
    private void ensureEnoughTotalWorkers() {
        int wc;
        while (runState < TERMINATING &&
                ((wc = workerCounts) >>> TOTAL_COUNT_SHIFT) < parallelism) {
            if ((UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                    wc, wc + (ONE_RUNNING | ONE_TOTAL)) &&
                    addWorker() == null))
                break;
        }
    }

    /**
     * Final callback from terminating worker.  Removes record of
     * worker from array, and adjusts counts. If pool is shutting
     * down, tries to complete terminatation, else possibly replaces
     * the worker.
     *
     * @param w the worker
     */
    final void workerTerminated(ForkJoinWorkerThread w) {
        if (w.active) { // force inactive
            w.active = false;
            do {
            } while (!tryDecrementActiveCount());
        }
        forgetWorker(w);

        // decrement total count, and if was running, running count
        int unit = w.isTrimmed() ? ONE_TOTAL : (ONE_RUNNING | ONE_TOTAL);
        int wc;
        do {
        } while (!UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                wc = workerCounts, wc - unit));

        accumulateStealCount(w); // collect final count
        if (!tryTerminate(false))
            ensureEnoughTotalWorkers();
    }

    // Waiting for and signalling events

    /**
     * Ensures eventCount on exit is different (mod 2^32) than on
     * entry.  CAS failures are OK -- any change in count suffices.
     */
    private void advanceEventCount() {
        int c;
        UNSAFE.compareAndSwapInt(this, eventCountOffset, c = eventCount, c + 1);
    }

    /**
     * Releases workers blocked on a count not equal to current count.
     */
    final void releaseWaiters() {
        long top;
        int id;
        while ((id = (int) ((top = eventWaiters) & WAITER_INDEX_MASK)) > 0 &&
                (int) (top >>> EVENT_COUNT_SHIFT) != eventCount) {
            ForkJoinWorkerThread[] ws = workers;
            ForkJoinWorkerThread w;
            if (ws.length >= id && (w = ws[id - 1]) != null &&
                    UNSAFE.compareAndSwapLong(this, eventWaitersOffset,
                            top, w.nextWaiter))
                LockSupport.unpark(w);
        }
    }

    /**
     * Advances eventCount and releases waiters until interference by
     * other releasing threads is detected.
     */
    final void signalWork() {
        int ec;
        UNSAFE.compareAndSwapInt(this, eventCountOffset, ec = eventCount, ec + 1);
        outer:
        for (; ;) {
            long top = eventWaiters;
            ec = eventCount;
            for (; ;) {
                ForkJoinWorkerThread[] ws;
                ForkJoinWorkerThread w;
                int id = (int) (top & WAITER_INDEX_MASK);
                if (id <= 0 || (int) (top >>> EVENT_COUNT_SHIFT) == ec)
                    return;
                if ((ws = workers).length < id || (w = ws[id - 1]) == null ||
                        !UNSAFE.compareAndSwapLong(this, eventWaitersOffset,
                                top, top = w.nextWaiter))
                    continue outer;      // possibly stale; reread
                LockSupport.unpark(w);
                if (top != eventWaiters) // let someone else take over
                    return;
            }
        }
    }

    /**
     * If worker is inactive, blocks until terminating or event count
     * advances from last value held by worker; in any case helps
     * release others.
     *
     * @param w the calling worker thread
     */
    private void eventSync(ForkJoinWorkerThread w) {
        if (!w.active) {
            int prev = w.lastEventCount;
            long nextTop = (((long) prev << EVENT_COUNT_SHIFT) |
                    ((long) (w.poolIndex + 1)));
            long top;
            while ((runState < SHUTDOWN || !tryTerminate(false)) &&
                    (((int) (top = eventWaiters) & WAITER_INDEX_MASK) == 0 ||
                            (int) (top >>> EVENT_COUNT_SHIFT) == prev) &&
                    eventCount == prev) {
                if (UNSAFE.compareAndSwapLong(this, eventWaitersOffset,
                        w.nextWaiter = top, nextTop)) {
                    accumulateStealCount(w); // transfer steals while idle
                    Thread.interrupted();    // clear/ignore interrupt
                    while (eventCount == prev)
                        w.doPark();
                    break;
                }
            }
            w.lastEventCount = eventCount;
        }
        releaseWaiters();
    }

    /**
     * Callback from workers invoked upon each top-level action (i.e.,
     * stealing a task or taking a submission and running
     * it). Performs one or both of the following:
     *
     * * If the worker cannot find work, updates its active status to
     * inactive and updates activeCount unless there is contention, in
     * which case it may try again (either in this or a subsequent
     * call).  Additionally, awaits the next task event and/or helps
     * wake up other releasable waiters.
     *
     * * If there are too many running threads, suspends this worker
     * (first forcing inactivation if necessary).  If it is not
     * resumed before a keepAlive elapses, the worker may be "trimmed"
     * -- killed while suspended within suspendAsSpare. Otherwise,
     * upon resume it rechecks to make sure that it is still needed.
     *
     * @param w      the worker
     * @param worked false if the worker scanned for work but didn't
     *               find any (in which case it may block waiting for work).
     */
    final void preStep(ForkJoinWorkerThread w, boolean worked) {
        boolean active = w.active;
        boolean inactivate = !worked & active;
        for (; ;) {
            if (inactivate) {
                int c = runState;
                if (UNSAFE.compareAndSwapInt(this, runStateOffset,
                        c, c - ONE_ACTIVE))
                    inactivate = active = w.active = false;
            }
            int wc = workerCounts;
            if ((wc & RUNNING_COUNT_MASK) <= parallelism) {
                if (!worked)
                    eventSync(w);
                return;
            }
            if (!(inactivate |= active) &&  // must inactivate to suspend
                    UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                            wc, wc - ONE_RUNNING) &&
                    !w.suspendAsSpare())        // false if trimmed
                return;
        }
    }

    /**
     * Adjusts counts and creates or resumes compensating threads for
     * a worker about to block on task joinMe, returning early if
     * joinMe becomes ready. First tries resuming an existing spare
     * (which usually also avoids any count adjustment), but must then
     * decrement running count to determine whether a new thread is
     * needed. See above for fuller explanation.
     */
    final void preJoin(ForkJoinTask<?> joinMe) {
        boolean dec = false;       // true when running count decremented
        for (; ;) {
            releaseWaiters();      // help other threads progress

            if (joinMe.status < 0) // surround spare search with done checks
                return;
            ForkJoinWorkerThread spare = null;
            for (ForkJoinWorkerThread w : workers) {
                if (w != null && w.isSuspended()) {
                    spare = w;
                    break;
                }
            }
            if (joinMe.status < 0)
                return;

            if (spare != null && spare.tryUnsuspend()) {
                if (dec || joinMe.requestSignal() < 0) {
                    int c;
                    do {
                    } while (!UNSAFE.compareAndSwapInt(this,
                            workerCountsOffset,
                            c = workerCounts,
                            c + ONE_RUNNING));
                } // else no net count change
                LockSupport.unpark(spare);
                return;
            }

            int wc = workerCounts; // decrement running count
            if (!dec && (wc & RUNNING_COUNT_MASK) != 0 &&
                    (dec = UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                            wc, wc -= ONE_RUNNING)) &&
                    joinMe.requestSignal() < 0) { // cannot block
                int c;                        // back out
                do {
                } while (!UNSAFE.compareAndSwapInt(this,
                        workerCountsOffset,
                        c = workerCounts,
                        c + ONE_RUNNING));
                return;
            }

            if (dec) {
                int tc = wc >>> TOTAL_COUNT_SHIFT;
                int pc = parallelism;
                int dc = pc - (wc & RUNNING_COUNT_MASK); // deficit count
                if ((dc < pc && (dc <= 0 || (dc * dc < (tc - pc) * pc) ||
                        !maintainsParallelism)) ||
                        tc >= maxPoolSize) // cannot add
                    return;
                if (spare == null &&
                        UNSAFE.compareAndSwapInt(this, workerCountsOffset, wc,
                                wc + (ONE_RUNNING | ONE_TOTAL))) {
                    addWorker();
                    return;
                }
            }
        }
    }

    /**
     * Same idea as preJoin but with too many differing details to
     * integrate: There are no task-based signal counts, and only one
     * way to do the actual blocking. So for simplicity it is directly
     * incorporated into this method.
     */
    final void doBlock(ManagedBlocker blocker, boolean maintainPar)
            throws InterruptedException {
        maintainPar &= maintainsParallelism; // override
        boolean dec = false;
        boolean done = false;
        for (; ;) {
            releaseWaiters();
            if (done = blocker.isReleasable())
                break;
            ForkJoinWorkerThread spare = null;
            for (ForkJoinWorkerThread w : workers) {
                if (w != null && w.isSuspended()) {
                    spare = w;
                    break;
                }
            }
            if (done = blocker.isReleasable())
                break;
            if (spare != null && spare.tryUnsuspend()) {
                if (dec) {
                    int c;
                    do {
                    } while (!UNSAFE.compareAndSwapInt(this,
                            workerCountsOffset,
                            c = workerCounts,
                            c + ONE_RUNNING));
                }
                LockSupport.unpark(spare);
                break;
            }
            int wc = workerCounts;
            if (!dec && (wc & RUNNING_COUNT_MASK) != 0)
                dec = UNSAFE.compareAndSwapInt(this, workerCountsOffset,
                        wc, wc -= ONE_RUNNING);
            if (dec) {
                int tc = wc >>> TOTAL_COUNT_SHIFT;
                int pc = parallelism;
                int dc = pc - (wc & RUNNING_COUNT_MASK);
                if ((dc < pc && (dc <= 0 || (dc * dc < (tc - pc) * pc) ||
                        !maintainPar)) ||
                        tc >= maxPoolSize)
                    break;
                if (spare == null &&
                        UNSAFE.compareAndSwapInt(this, workerCountsOffset, wc,
                                wc + (ONE_RUNNING | ONE_TOTAL))) {
                    addWorker();
                    break;
                }
            }
        }

        try {
            if (!done)
                do {
                } while (!blocker.isReleasable() && !blocker.block());
        } finally {
            if (dec) {
                int c;
                do {
                } while (!UNSAFE.compareAndSwapInt(this,
                        workerCountsOffset,
                        c = workerCounts,
                        c + ONE_RUNNING));
            }
        }
    }

    /**
     * Possibly initiates and/or completes termination.
     *
     * @param now if true, unconditionally terminate, else only
     *            if shutdown and empty queue and no active workers
     * @return true if now terminating or terminated
     */
    private boolean tryTerminate(boolean now) {
        if (now)
            advanceRunLevel(SHUTDOWN); // ensure at least SHUTDOWN
        else if (runState < SHUTDOWN ||
                !submissionQueue.isEmpty() ||
                (runState & ACTIVE_COUNT_MASK) != 0)
            return false;

        if (advanceRunLevel(TERMINATING))
            startTerminating();

        // Finish now if all threads terminated; else in some subsequent call
        if ((workerCounts >>> TOTAL_COUNT_SHIFT) == 0) {
            advanceRunLevel(TERMINATED);
            terminationLatch.countDown();
        }
        return true;
    }

    /**
     * Actions on transition to TERMINATING
     */
    private void startTerminating() {
        // Clear out and cancel submissions, ignoring exceptions
        ForkJoinTask<?> task;
        while ((task = submissionQueue.poll()) != null) {
            try {
                task.cancel(false);
            } catch (Throwable ignore) {
            }
        }
        // Propagate run level
        for (ForkJoinWorkerThread w : workers) {
            if (w != null)
                w.shutdown();    // also resumes suspended workers
        }
        // Ensure no straggling local tasks
        for (ForkJoinWorkerThread w : workers) {
            if (w != null)
                w.cancelTasks();
        }
        // Wake up idle workers
        advanceEventCount();
        releaseWaiters();
        // Unstick pending joins
        for (ForkJoinWorkerThread w : workers) {
            if (w != null && !w.isTerminated()) {
                try {
                    w.interrupt();
                } catch (SecurityException ignore) {
                }
            }
        }
    }

    // misc support for ForkJoinWorkerThread

    /**
     * Returns pool number
     */
    final int getPoolNumber() {
        return poolNumber;
    }

    /**
     * Accumulates steal count from a worker, clearing
     * the worker's value
     */
    final void accumulateStealCount(ForkJoinWorkerThread w) {
        int sc = w.stealCount;
        if (sc != 0) {
            long c;
            w.stealCount = 0;
            do {
            } while (!UNSAFE.compareAndSwapLong(this, stealCountOffset,
                    c = stealCount, c + sc));
        }
    }

    /**
     * Returns the approximate (non-atomic) number of idle threads per
     * active thread.
     */
    final int idlePerActive() {
        int ac = runState;    // no mask -- artifically boosts during shutdown
        int pc = parallelism; // use targeted parallelism, not rc
        // Use exact results for small values, saturate past 4
        return pc <= ac ? 0 : pc >>> 1 <= ac ? 1 : pc >>> 2 <= ac ? 3 : pc >>> 3;
    }

    /**
     * Returns the approximate (non-atomic) difference between running
     * and active counts.
     */
    final int inactiveCount() {
        return (workerCounts & RUNNING_COUNT_MASK) -
                (runState & ACTIVE_COUNT_MASK);
    }

    // Public and protected methods

    // Constructors

    /**
     * Creates a {@code ForkJoinPool} with parallelism equal to {@link
     * java.lang.Runtime#availableProcessors}, and using the {@linkplain
     * #defaultForkJoinWorkerThreadFactory default thread factory}.
     *
     * @throws SecurityException if a security manager exists and
     *                           the caller is not permitted to modify threads
     *                           because it does not hold {@link
     *                           java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool() {
        this(Runtime.getRuntime().availableProcessors(),
                defaultForkJoinWorkerThreadFactory);
    }

    /**
     * Creates a {@code ForkJoinPool} with the indicated parallelism
     * level and using the {@linkplain
     * #defaultForkJoinWorkerThreadFactory default thread factory}.
     *
     * @param parallelism the parallelism level
     * @throws IllegalArgumentException if parallelism less than or
     *                                  equal to zero, or greater than implementation limit
     * @throws SecurityException        if a security manager exists and
     *                                  the caller is not permitted to modify threads
     *                                  because it does not hold {@link
     *                                  java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool(int parallelism) {
        this(parallelism, defaultForkJoinWorkerThreadFactory);
    }

    /**
     * Creates a {@code ForkJoinPool} with parallelism equal to {@link
     * java.lang.Runtime#availableProcessors}, and using the given
     * thread factory.
     *
     * @param factory the factory for creating new threads
     * @throws NullPointerException if the factory is null
     * @throws SecurityException    if a security manager exists and
     *                              the caller is not permitted to modify threads
     *                              because it does not hold {@link
     *                              java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool(ForkJoinWorkerThreadFactory factory) {
        this(Runtime.getRuntime().availableProcessors(), factory);
    }

    /**
     * Creates a {@code ForkJoinPool} with the given parallelism and
     * thread factory.
     *
     * @param parallelism the parallelism level
     * @param factory     the factory for creating new threads
     * @throws IllegalArgumentException if parallelism less than or
     *                                  equal to zero, or greater than implementation limit
     * @throws NullPointerException     if the factory is null
     * @throws SecurityException        if a security manager exists and
     *                                  the caller is not permitted to modify threads
     *                                  because it does not hold {@link
     *                                  java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public ForkJoinPool(int parallelism, ForkJoinWorkerThreadFactory factory) {
        checkPermission();
        if (factory == null)
            throw new NullPointerException();
        if (parallelism <= 0 || parallelism > MAX_THREADS)
            throw new IllegalArgumentException();
        this.poolNumber = poolNumberGenerator.incrementAndGet();
        int arraySize = initialArraySizeFor(parallelism);
        this.parallelism = parallelism;
        this.factory = factory;
        this.maxPoolSize = MAX_THREADS;
        this.maintainsParallelism = true;
        this.workers = new ForkJoinWorkerThread[arraySize];
        this.submissionQueue = new LinkedTransferQueue<ForkJoinTask<?>>();
        this.workerLock = new ReentrantLock();
        this.terminationLatch = new CountDownLatch(1);
        // Start first worker; remaining workers added upon first submission
        workerCounts = ONE_RUNNING | ONE_TOTAL;
        addWorker();
    }

    /**
     * Returns initial power of two size for workers array.
     *
     * @param pc the initial parallelism level
     */
    private static int initialArraySizeFor(int pc) {
        // See Hackers Delight, sec 3.2. We know MAX_THREADS < (1 >>> 16)
        int size = pc < MAX_THREADS ? pc + 1 : MAX_THREADS;
        size |= size >>> 1;
        size |= size >>> 2;
        size |= size >>> 4;
        size |= size >>> 8;
        return size + 1;
    }

    // Execution methods

    /**
     * Common code for execute, invoke and submit
     */
    private <T> void doSubmit(ForkJoinTask<T> task) {
        if (task == null)
            throw new NullPointerException();
        if (runState >= SHUTDOWN)
            throw new RejectedExecutionException();
        submissionQueue.offer(task);
        advanceEventCount();
        releaseWaiters();
        if ((workerCounts >>> TOTAL_COUNT_SHIFT) < parallelism)
            ensureEnoughTotalWorkers();
    }

    /**
     * Performs the given task, returning its result upon completion.
     *
     * @param task the task
     * @return the task's result
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public <T> T invoke(ForkJoinTask<T> task) {
        doSubmit(task);
        return task.join();
    }

    /**
     * Arranges for (asynchronous) execution of the given task.
     *
     * @param task the task
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public void execute(ForkJoinTask<?> task) {
        doSubmit(task);
    }

    // AbstractExecutorService methods

    /**
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public void execute(Runnable task) {
        ForkJoinTask<?> job;
        if (task instanceof ForkJoinTask<?>) // avoid re-wrap
            job = (ForkJoinTask<?>) task;
        else
            job = ForkJoinTask.adapt(task, null);
        doSubmit(job);
    }

    /**
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Callable<T> task) {
        ForkJoinTask<T> job = ForkJoinTask.adapt(task);
        doSubmit(job);
        return job;
    }

    /**
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(Runnable task, T result) {
        ForkJoinTask<T> job = ForkJoinTask.adapt(task, result);
        doSubmit(job);
        return job;
    }

    /**
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public ForkJoinTask<?> submit(Runnable task) {
        ForkJoinTask<?> job;
        if (task instanceof ForkJoinTask<?>) // avoid re-wrap
            job = (ForkJoinTask<?>) task;
        else
            job = ForkJoinTask.adapt(task, null);
        doSubmit(job);
        return job;
    }

    /**
     * Submits a ForkJoinTask for execution.
     *
     * @param task the task to submit
     * @return the task
     * @throws NullPointerException       if the task is null
     * @throws RejectedExecutionException if the task cannot be
     *                                    scheduled for execution
     */
    public <T> ForkJoinTask<T> submit(ForkJoinTask<T> task) {
        doSubmit(task);
        return task;
    }

    /**
     * @throws NullPointerException       {@inheritDoc}
     * @throws RejectedExecutionException {@inheritDoc}
     */
    public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) {
        ArrayList<ForkJoinTask<T>> forkJoinTasks =
                new ArrayList<ForkJoinTask<T>>(tasks.size());
        for (Callable<T> task : tasks)
            forkJoinTasks.add(ForkJoinTask.adapt(task));
        invoke(new InvokeAll<T>(forkJoinTasks));

        @SuppressWarnings({"unchecked", "rawtypes"})
        List<Future<T>> futures = (List<Future<T>>) (List) forkJoinTasks;
        return futures;
    }

    static final class InvokeAll<T> extends RecursiveAction {
        final ArrayList<ForkJoinTask<T>> tasks;

        InvokeAll(ArrayList<ForkJoinTask<T>> tasks) {
            this.tasks = tasks;
        }

        public void compute() {
            try {
                invokeAll(tasks);
            }
            catch (Exception ignore) {
            }
        }

        private static final long serialVersionUID = -7914297376763021607L;
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
        workerCountReadFence();
        return ueh;
    }

    /**
     * Sets the handler for internal worker threads that terminate due
     * to unrecoverable errors encountered while executing tasks.
     * Unless set, the current default or ThreadGroup handler is used
     * as handler.
     *
     * @param h the new handler
     * @return the old handler, or {@code null} if none
     * @throws SecurityException if a security manager exists and
     *                           the caller is not permitted to modify threads
     *                           because it does not hold {@link
     *                           java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public Thread.UncaughtExceptionHandler
    setUncaughtExceptionHandler(Thread.UncaughtExceptionHandler h) {
        checkPermission();
        workerCountReadFence();
        Thread.UncaughtExceptionHandler old = ueh;
        if (h != old) {
            ueh = h;
            workerCountWriteFence();
            for (ForkJoinWorkerThread w : workers) {
                if (w != null)
                    w.setUncaughtExceptionHandler(h);
            }
        }
        return old;
    }

    /**
     * Sets the target parallelism level of this pool.
     *
     * @param parallelism the target parallelism
     * @throws IllegalArgumentException if parallelism less than or
     *                                  equal to zero or greater than maximum size bounds
     * @throws SecurityException        if a security manager exists and
     *                                  the caller is not permitted to modify threads
     *                                  because it does not hold {@link
     *                                  java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public void setParallelism(int parallelism) {
        checkPermission();
        if (parallelism <= 0 || parallelism > maxPoolSize)
            throw new IllegalArgumentException();
        workerCountReadFence();
        int pc = this.parallelism;
        if (pc != parallelism) {
            this.parallelism = parallelism;
            workerCountWriteFence();
            // Release spares. If too many, some will die after re-suspend
            for (ForkJoinWorkerThread w : workers) {
                if (w != null && w.tryUnsuspend()) {
                    updateRunningCount(1);
                    LockSupport.unpark(w);
                }
            }
            ensureEnoughTotalWorkers();
            advanceEventCount();
            releaseWaiters(); // force config recheck by existing workers
        }
    }

    /**
     * Returns the targeted parallelism level of this pool.
     *
     * @return the targeted parallelism level of this pool
     */
    public int getParallelism() {
        //        workerCountReadFence(); // inlined below
        int ignore = workerCounts;
        return parallelism;
    }

    /**
     * Returns the number of worker threads that have started but not
     * yet terminated.  This result returned by this method may differ
     * from {@link #getParallelism} when threads are created to
     * maintain parallelism when others are cooperatively blocked.
     *
     * @return the number of worker threads
     */
    public int getPoolSize() {
        return workerCounts >>> TOTAL_COUNT_SHIFT;
    }

    /**
     * Returns the maximum number of threads allowed to exist in the
     * pool. Unless set using {@link #setMaximumPoolSize}, the
     * maximum is an implementation-defined value designed only to
     * prevent runaway growth.
     *
     * @return the maximum
     */
    public int getMaximumPoolSize() {
        workerCountReadFence();
        return maxPoolSize;
    }

    /**
     * Sets the maximum number of threads allowed to exist in the
     * pool. The given value should normally be greater than or equal
     * to the {@link #getParallelism parallelism} level. Setting this
     * value has no effect on current pool size. It controls
     * construction of new threads.
     *
     * @throws IllegalArgumentException if negative or greater than
     *                                  internal implementation limit
     */
    public void setMaximumPoolSize(int newMax) {
        if (newMax < 0 || newMax > MAX_THREADS)
            throw new IllegalArgumentException();
        maxPoolSize = newMax;
        workerCountWriteFence();
    }

    /**
     * Returns {@code true} if this pool dynamically maintains its
     * target parallelism level. If false, new threads are added only
     * to avoid possible starvation.  This setting is by default true.
     *
     * @return {@code true} if maintains parallelism
     */
    public boolean getMaintainsParallelism() {
        workerCountReadFence();
        return maintainsParallelism;
    }

    /**
     * Sets whether this pool dynamically maintains its target
     * parallelism level. If false, new threads are added only to
     * avoid possible starvation.
     *
     * @param enable {@code true} to maintain parallelism
     */
    public void setMaintainsParallelism(boolean enable) {
        maintainsParallelism = enable;
        workerCountWriteFence();
    }

    /**
     * Establishes local first-in-first-out scheduling mode for forked
     * tasks that are never joined. This mode may be more appropriate
     * than default locally stack-based mode in applications in which
     * worker threads only process asynchronous tasks.  This method is
     * designed to be invoked only when the pool is quiescent, and
     * typically only before any tasks are submitted. The effects of
     * invocations at other times may be unpredictable.
     *
     * @param async if {@code true}, use locally FIFO scheduling
     * @return the previous mode
     * @see #getAsyncMode
     */
    public boolean setAsyncMode(boolean async) {
        workerCountReadFence();
        boolean oldMode = locallyFifo;
        if (oldMode != async) {
            locallyFifo = async;
            workerCountWriteFence();
            for (ForkJoinWorkerThread w : workers) {
                if (w != null)
                    w.setAsyncMode(async);
            }
        }
        return oldMode;
    }

    /**
     * Returns {@code true} if this pool uses local first-in-first-out
     * scheduling mode for forked tasks that are never joined.
     *
     * @return {@code true} if this pool uses async mode
     * @see #setAsyncMode
     */
    public boolean getAsyncMode() {
        workerCountReadFence();
        return locallyFifo;
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
        return workerCounts & RUNNING_COUNT_MASK;
    }

    /**
     * Returns an estimate of the number of threads that are currently
     * stealing or executing tasks. This method may overestimate the
     * number of active threads.
     *
     * @return the number of active threads
     */
    public int getActiveThreadCount() {
        return runState & ACTIVE_COUNT_MASK;
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
        return (runState & ACTIVE_COUNT_MASK) == 0;
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
        return stealCount;
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
        for (ForkJoinWorkerThread w : workers) {
            if (w != null)
                count += w.getQueueSize();
        }
        return count;
    }

    /**
     * Returns an estimate of the number of tasks submitted to this
     * pool that have not yet begun executing.  This method takes time
     * proportional to the number of submissions.
     *
     * @return the number of queued submissions
     */
    public int getQueuedSubmissionCount() {
        return submissionQueue.size();
    }

    /**
     * Returns {@code true} if there are any tasks submitted to this
     * pool that have not yet begun executing.
     *
     * @return {@code true} if there are any queued submissions
     */
    public boolean hasQueuedSubmissions() {
        return !submissionQueue.isEmpty();
    }

    /**
     * Removes and returns the next unexecuted submission if one is
     * available.  This method may be useful in extensions to this
     * class that re-assign work in systems with multiple pools.
     *
     * @return the next submission, or {@code null} if none
     */
    protected ForkJoinTask<?> pollSubmission() {
        return submissionQueue.poll();
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
        int n = submissionQueue.drainTo(c);
        for (ForkJoinWorkerThread w : workers) {
            if (w != null)
                n += w.drainTasksTo(c);
        }
        return n;
    }

    /**
     * Returns a string identifying this pool, as well as its state,
     * including indications of run state, parallelism level, and
     * worker and task counts.
     *
     * @return a string identifying this pool, as well as its state
     */
    public String toString() {
        long st = getStealCount();
        long qt = getQueuedTaskCount();
        long qs = getQueuedSubmissionCount();
        int wc = workerCounts;
        int tc = wc >>> TOTAL_COUNT_SHIFT;
        int rc = wc & RUNNING_COUNT_MASK;
        int pc = parallelism;
        int rs = runState;
        int ac = rs & ACTIVE_COUNT_MASK;
        return super.toString() +
                "[" + runLevelToString(rs) +
                ", parallelism = " + pc +
                ", size = " + tc +
                ", active = " + ac +
                ", running = " + rc +
                ", steals = " + st +
                ", tasks = " + qt +
                ", submissions = " + qs +
                "]";
    }

    private static String runLevelToString(int s) {
        return ((s & TERMINATED) != 0 ? "Terminated" :
                ((s & TERMINATING) != 0 ? "Terminating" :
                        ((s & SHUTDOWN) != 0 ? "Shutting down" :
                                "Running")));
    }

    /**
     * Initiates an orderly shutdown in which previously submitted
     * tasks are executed, but no new tasks will be accepted.
     * Invocation has no additional effect if already shut down.
     * Tasks that are in the process of being submitted concurrently
     * during the course of this method may or may not be rejected.
     *
     * @throws SecurityException if a security manager exists and
     *                           the caller is not permitted to modify threads
     *                           because it does not hold {@link
     *                           java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public void shutdown() {
        checkPermission();
        advanceRunLevel(SHUTDOWN);
        tryTerminate(false);
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
     *                           the caller is not permitted to modify threads
     *                           because it does not hold {@link
     *                           java.lang.RuntimePermission}{@code ("modifyThread")}
     */
    public List<Runnable> shutdownNow() {
        checkPermission();
        tryTerminate(true);
        return Collections.emptyList();
    }

    /**
     * Returns {@code true} if all tasks have completed following shut down.
     *
     * @return {@code true} if all tasks have completed following shut down
     */
    public boolean isTerminated() {
        return runState >= TERMINATED;
    }

    /**
     * Returns {@code true} if the process of termination has
     * commenced but not yet completed.  This method may be useful for
     * debugging. A return of {@code true} reported a sufficient
     * period after shutdown may indicate that submitted tasks have
     * ignored or suppressed interruption, causing this executor not
     * to properly terminate.
     *
     * @return {@code true} if terminating but not yet terminated
     */
    public boolean isTerminating() {
        return (runState & (TERMINATING | TERMINATED)) == TERMINATING;
    }

    /**
     * Returns {@code true} if this pool has been shut down.
     *
     * @return {@code true} if this pool has been shut down
     */
    public boolean isShutdown() {
        return runState >= SHUTDOWN;
    }

    /**
     * Blocks until all tasks have completed execution after a shutdown
     * request, or the timeout occurs, or the current thread is
     * interrupted, whichever happens first.
     *
     * @param timeout the maximum time to wait
     * @param unit    the time unit of the timeout argument
     * @return {@code true} if this executor terminated and
     *         {@code false} if the timeout elapsed before termination
     * @throws InterruptedException if interrupted while waiting
     */
    public boolean awaitTermination(long timeout, TimeUnit unit)
            throws InterruptedException {
        return terminationLatch.await(timeout, unit);
    }

    /**
     * Interface for extending managed parallelism for tasks running
     * in {@link ForkJoinPool}s.
     *
     * <p>A {@code ManagedBlocker} provides two methods.
     * Method {@code isReleasable} must return {@code true} if
     * blocking is not necessary. Method {@code block} blocks the
     * current thread if necessary (perhaps internally invoking
     * {@code isReleasable} before actually blocking).
     *
     * <p>For example, here is a ManagedBlocker based on a
     * ReentrantLock:
     * <pre> {@code
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
     */
    public static interface ManagedBlocker {
        /**
         * Possibly blocks the current thread, for example waiting for
         * a lock or condition.
         *
         * @return {@code true} if no additional blocking is necessary
         *         (i.e., if isReleasable would return true)
         * @throws InterruptedException if interrupted while waiting
         *                              (the method is not required to do so, but is allowed to)
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
     * ensure parallelism while the current thread is blocked.
     *
     * <p>If {@code maintainParallelism} is {@code true} and the pool
     * supports it ({@link #getMaintainsParallelism}), this method
     * attempts to maintain the pool's nominal parallelism. Otherwise
     * it activates a thread only if necessary to avoid complete
     * starvation. This option may be preferable when blockages use
     * timeouts, or are almost always brief.
     *
     * <p>If the caller is not a {@link ForkJoinTask}, this method is
     * behaviorally equivalent to
     * <pre> {@code
     * while (!blocker.isReleasable())
     *   if (blocker.block())
     *     return;
     * }</pre>
     *
     * If the caller is a {@code ForkJoinTask}, then the pool may
     * first be expanded to ensure parallelism, and later adjusted.
     *
     * @param blocker             the blocker
     * @param maintainParallelism if {@code true} and supported by
     *                            this pool, attempt to maintain the pool's nominal parallelism;
     *                            otherwise activate a thread only if necessary to avoid
     *                            complete starvation.
     * @throws InterruptedException if blocker.block did so
     */
    public static void managedBlock(ManagedBlocker blocker,
                                    boolean maintainParallelism)
            throws InterruptedException {
        Thread t = Thread.currentThread();
        if (t instanceof ForkJoinWorkerThread)
            ((ForkJoinWorkerThread) t).pool.
                    doBlock(blocker, maintainParallelism);
        else
            awaitBlocker(blocker);
    }

    /**
     * Performs Non-FJ blocking
     */
    private static void awaitBlocker(ManagedBlocker blocker)
            throws InterruptedException {
        do {
        } while (!blocker.isReleasable() && !blocker.block());
    }

    // AbstractExecutorService overrides.  These rely on undocumented
    // fact that ForkJoinTask.adapt returns ForkJoinTasks that also
    // implement RunnableFuture.

    protected <T> RunnableFuture<T> newTaskFor(Runnable runnable, T value) {
        return (RunnableFuture<T>) ForkJoinTask.adapt(runnable, value);
    }

    protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
        return (RunnableFuture<T>) ForkJoinTask.adapt(callable);
    }

    // Unsafe mechanics

    private static final sun.misc.Unsafe UNSAFE = getUnsafe();
    private static final long workerCountsOffset =
            objectFieldOffset("workerCounts", ForkJoinPool.class);
    private static final long runStateOffset =
            objectFieldOffset("runState", ForkJoinPool.class);
    private static final long eventCountOffset =
            objectFieldOffset("eventCount", ForkJoinPool.class);
    private static final long eventWaitersOffset =
            objectFieldOffset("eventWaiters", ForkJoinPool.class);
    private static final long stealCountOffset =
            objectFieldOffset("stealCount", ForkJoinPool.class);


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
