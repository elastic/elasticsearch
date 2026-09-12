/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Runs leaf fork and subquery plans, bounding how many of them execute at once. Non-blocking and query-wide; callbacks are invoked
 * outside this runner's lock.
 * <p>
 * <b>Only leaf plans may be admitted here.</b> A sub plan that itself contains nested merges consumes the output of the leaves
 * below it, so admitting one would let it hold a permit while it waits for leaves that are queued behind that very permit. With
 * the default {@code branch_parallel_degree} of 2 this deadlocks immediately: two nested merge plans take both permits, park on
 * their nested exchange sources, and the leaves that would feed them can never start. Merge plans are therefore dispatched
 * outside this runner, and only the leaves they bottom out in are counted.
 *
 * <h2>How the queue fills when a query has more than one {@code MergeExec}</h2>
 *
 * There is one runner per query, but one {@link MergeLevelExecutor} per {@code MergeExec} level, and <em>every</em> one of them
 * submits into this same queue. Each executor independently opens a window of {@code branch_parallel_degree} sub plans without
 * knowing what the other levels are doing, so arrivals routinely outrun {@link #maxRunning} - which is the reason this class needs a
 * queue at all rather than just a counter.
 * <p>
 * Take a topmost merge with two branches - a plain leaf {@code a}, and a subquery {@code n} that unions two leaves {@code c} and
 * {@code d} - with {@code branch_parallel_degree = 2}. {@code ComputeService} builds one executor over {@code [a, n]} and calls
 * {@code execute(2)}:
 * <ul>
 * <li>{@code a} is a leaf, so it is submitted here. {@code running} is 0, below the cap, so it goes straight to the executor.</li>
 * <li>{@code n} contains a {@code MergeExec}, so it never reaches this queue. It is dispatched directly instead, which builds a
 *     <em>second</em> executor over {@code [c, d]}: a merge sub plan does not become a task, it becomes another submitter.</li>
 * <li>That second executor also calls {@code execute(2)}. {@code c} is submitted and admitted ({@code running} 1 -> 2), then
 *     {@code d} is submitted, finds {@code running == maxRunning}, and waits in {@code pending}.</li>
 * </ul>
 * {@code d} is admitted by {@link #takeReadyLocked} as soon as <em>any</em> running leaf finishes, {@code a} or {@code c} alike: the
 * queue is FIFO over the whole query rather than per level, so a deeper level's leaf can start before a shallower one's. Queue depth
 * is therefore bounded by the number of concurrently open merge levels times {@code branch_parallel_degree}, less the cap - not by how
 * many sub plans the query has in total. Deeper nesting just adds more submitters: each level that is itself a merge expands into
 * another executor rather than a task.
 * <p>
 * A single-merge query queues too, though only ever one task and only briefly: a finishing leaf starts the next one before it releases
 * its own permit, so the newcomer is queued and then admitted moments later when the permit drops. See
 * {@code MergeLevelExecutor.SubPlan.complete}.
 *
 * <h2>The {@code Locked} suffix</h2>
 *
 * {@link #takeReadyLocked} and {@link #drainLocked} do not take the runner's monitor themselves. They read and write
 * {@code pending}, {@code running}, {@code finished} and {@code failure}, which every public method mutates under
 * {@code synchronized (this)}. The suffix means <em>the caller already holds that monitor</em> — a convention, not a
 * lock on the queued task. Calling either helper outside a {@code synchronized} block would race. Callbacks
 * ({@link Task#skip}, {@link Task#fail}, {@link #dispatch}) always run after the lock is released, so a task cannot
 * re-enter the runner while it still holds the monitor.
 */
final class SubPlanTaskRunner {

    /**
     * A unit of work admitted by the runner: one leaf fork or subquery plan.
     * <p>
     * A task is handed at least one of the three methods below, and must reach a terminal state - resolving its listener and closing
     * its exchange sink - on whichever one it gets. Normally it is handed exactly one, but an implementation must tolerate
     * {@link #fail} arriving after {@link #execute} has already completed: if {@code execute} resolves its completion listener and
     * then throws, the runner reports that failure to a task that has already finished. {@code MergeLevelExecutor.SubPlan} guards
     * against this with a compare-and-set.
     * <p>
     * None of the three methods may throw. They are the only way a sub plan reaches a terminal state, so an escaping throwable
     * leaves that plan's listener unresolved and the query waiting on a branch that will never report.
     * <p>
     * The backstop {@link #dispatch} installs - see {@code AbstractRunnable#onFailure} there - only narrows that, it does not close
     * it. It covers an {@link Exception} raised by a callback the runner invokes on the executor, and nothing else. An
     * {@link Error} is not covered, because {@code AbstractRunnable#run} catches {@code Exception} rather than {@code Throwable}, as
     * do {@code AbstractThrottledTaskRunner} and {@code RefCountingListener}; an {@code AssertionError} out of
     * {@code MergeLevelExecutor.SubPlan.skip}'s {@code assert exchangeSink == null} under {@code -ea}, for instance, escapes to the
     * thread pool's uncaught handler with the branch listener unresolved, and the query then hangs until it times out rather than
     * failing.
     * <p>
     * {@link #finish()}, {@link #fail(Exception)} and a failing {@link #taskFinished} hand the same callbacks to a whole
     * drained batch at once, off the executor. There a throw cannot strand the tasks behind it - each call is isolated, see
     * {@code terminateAll} - but the first throwable is still rethrown to the caller once the batch is done.
     * <p>
     * Keeping this contract narrow is what lets the runner stay independent of ES query execution - it is a scheduling algorithm and
     * nothing more, which is also what makes it unit-testable without a {@code ComputeService}, an exchange service or a thread pool.
     */
    interface Task {

        /**
         * Runs the plan. The task must resolve {@code completion} exactly once, when the plan finishes, to release its permit - a
         * task that never resolves it stalls every plan queued behind it. Called only after the runner has admitted this task, on
         * the runner's executor.
         */
        void execute(ActionListener<Void> completion);

        /**
         * Abandons the plan without running it, because the query already has all the results it needs
         * ({@link SubPlanTaskRunner#finish()}). Report success so the coordinator's exchange source can finish; do not open a
         * sink if none was opened yet.
         */
        void skip();

        /**
         * Abandons the plan because the query has failed ({@link SubPlanTaskRunner#fail(Exception)} or a sibling leaf failed).
         * Close any sink that was opened and report {@code failure} on the plan's listener.
         */
        void fail(Exception failure);
    }

    private final int maxRunning;
    private final Executor executor;
    private final Deque<Task> pending = new ArrayDeque<>();
    private int running;
    private boolean finished;
    private Exception failure;

    /**
     * @param maxRunning cap on concurrently executing leaves, from the {@code branch_parallel_degree} pragma. Must be at least 1.
     * @param executor where admitted tasks run. Typically the search thread pool; tests pass {@code Runnable::run} to stay on the
     *                 calling thread.
     */
    SubPlanTaskRunner(int maxRunning, Executor executor) {
        if (maxRunning < 1) {
            throw new IllegalArgumentException("maxRunning must be positive");
        }
        this.maxRunning = maxRunning;
        this.executor = executor;
    }

    /**
     * Offers one leaf plan to the runner. Exactly one of three things happens, always after this method has dropped the monitor:
     * <ul>
     * <li>the query has already {@link #fail(Exception) failed} — {@link Task#fail} with that exception;</li>
     * <li>the query has already {@link #finish() finished} — {@link Task#skip};</li>
     * <li>otherwise the task is appended to {@code pending}, and if a permit is free {@link #takeReadyLocked} admits it and
     *     {@link #dispatch} starts it.</li>
     * </ul>
     * Example with {@code maxRunning = 2}: the first two {@code submit} calls dispatch immediately ({@code running} 0→1, 1→2). A
     * third finds {@code running == maxRunning}, stays in {@code pending}, and is admitted later from {@link #taskFinished}.
     */
    void submit(Task task) {
        Exception rejection;
        boolean skip;
        Task ready = null;
        synchronized (this) {
            rejection = failure;
            skip = rejection == null && finished;
            if (rejection == null && skip == false) {
                pending.addLast(task);
                ready = takeReadyLocked();
            }
        }
        if (rejection != null) {
            task.fail(rejection);
        } else if (skip) {
            task.skip();
        } else if (ready != null) {
            dispatch(ready);
        }
    }

    /**
     * Marks the query as having all the rows it needs, typically after a coordinator {@code LIMIT} is satisfied. Queued leaves are
     * drained and {@link Task#skip skipped}; later {@link #submit} calls skip too. In-flight leaves keep running until they complete.
     * No-op if already finished or if {@link #fail(Exception)} won the race.
     * <p>
     * Merge sub plans never sit in {@code pending}, so they are not skipped here. {@code MergeLevelExecutor.tryExecuteNextSubPlan}
     * consults {@link #finished()} and resolves an unstarted merge as empty success instead of expanding it.
     * <p>
     * Example: {@code maxRunning = 1}, leaves {@code a, b, c} submitted. {@code a} is running, {@code b} and {@code c} are queued.
     * {@code finish()} skips {@code b} and {@code c}. When {@code a} later completes, {@link #takeReadyLocked} sees {@code finished}
     * and admits nothing.
     */
    void finish() {
        List<Task> skipped;
        synchronized (this) {
            if (finished || failure != null) {
                return;
            }
            finished = true;
            skipped = drainLocked();
        }
        terminateAll(skipped, Task::skip);
    }

    /**
     * Marks the query failed and rejects everything still queued. Later {@link #submit} calls {@link Task#fail} with the same
     * exception. In-flight leaves are not failed here; they complete (or fail) through their own {@code execute} path, and
     * {@link #taskFinished} then refuses to admit anything further.
     * <p>
     * The first failure wins: a second {@code fail} is ignored so callbacks see a single exception. Example: leaf {@code a} is
     * running, {@code b} and {@code c} are queued. {@code fail(e)} rejects {@code b} and {@code c} immediately. When {@code a}
     * later completes, {@code pending} is empty and {@code failure != null}, so nothing else is dispatched.
     */
    void fail(Exception e) {
        List<Task> rejected;
        synchronized (this) {
            if (failure != null) {
                return;
            }
            failure = e;
            rejected = drainLocked();
        }
        terminateAll(rejected, task -> task.fail(e));
    }

    /**
     * Hands every drained task its terminal callback, even if one of them throws. {@link Task#skip} and {@link Task#fail} are the
     * only way a sub plan reports, and here they are being handed to a whole batch at once - from {@link #finish()},
     * {@link #fail(Exception)} or a failing {@link #taskFinished} - so letting the first throw abort the loop would strand every
     * task behind it - their listeners never resolve and the query hangs. Each call is therefore isolated and the first throwable
     * is rethrown afterwards, once the batch is done, so the diagnostic still surfaces.
     * <p>
     * {@link Exception} and {@link AssertionError} are isolated and nothing else: those are what a contract-violating callback
     * raises, {@code assert exchangeSink == null} in {@code MergeLevelExecutor.SubPlan.skip} being the example to keep in mind.
     * A fatal {@link Error} - out of memory, stack overflow - propagates immediately instead, because calling more callbacks
     * after one is not a service to anybody.
     */
    private static void terminateAll(List<Task> tasks, Consumer<Task> terminal) {
        Throwable firstFailure = null;
        for (Task task : tasks) {
            try {
                terminal.accept(task);
            } catch (Exception | AssertionError t) {
                firstFailure = ExceptionsHelper.useOrSuppress(firstFailure, t);
            }
        }
        if (firstFailure instanceof RuntimeException runtime) {
            throw runtime;
        }
        if (firstFailure instanceof AssertionError assertion) {
            throw assertion;
        }
        // The callbacks are void and declare no checked exceptions, so there is nothing else firstFailure can be.
        assert firstFailure == null : firstFailure;
    }

    /**
     * The exception recorded by {@link #fail(Exception)} or by a leaf that failed while running, or {@code null}.
     * {@code MergeLevelExecutor.tryExecuteNextSubPlan} reads this before starting a merge, which never enters the queue
     * and so would otherwise miss a failure that only drained {@code pending}.
     */
    synchronized Exception failure() {
        return failure;
    }

    /**
     * Whether {@link #finish()} has been called because the query already has all the rows it needs.
     * {@code MergeLevelExecutor} consults this for merge sub plans, which never enter the queue and so
     * are not skipped by {@link #finish()} itself.
     */
    synchronized boolean finished() {
        return finished;
    }

    /**
     * Releases one permit after a dispatched task has resolved its completion listener, then either rejects the rest of the
     * queue (if that task failed) or admits the next pending leaf.
     * <p>
     * Example with {@code maxRunning = 2} and queued {@code c}: {@code a} completes successfully, {@code running} 2→1,
     * {@link #takeReadyLocked} admits {@code c}, {@link #dispatch} starts it. If instead {@code a} failed, {@code failure} is
     * set, the queue is drained, and {@code c} is {@link Task#fail failed} rather than started.
     */
    private void taskFinished(Exception taskFailure) {
        List<Task> rejected = List.of();
        Task ready;
        synchronized (this) {
            running--;
            assert running >= 0;
            if (taskFailure != null && failure == null) {
                failure = taskFailure;
                rejected = drainLocked();
            }
            ready = takeReadyLocked();
        }
        if (taskFailure != null) {
            terminateAll(rejected, task -> task.fail(taskFailure));
        }
        if (ready != null) {
            dispatch(ready);
        }
    }

    /**
     * Admits the next pending task, or returns {@code null} if there is nothing to admit. At most one task is ever admissible:
     * {@code pending} only grows while {@code running == maxRunning}, because {@link #submit} consumes its own task straight away
     * whenever a slot is free, and {@link #taskFinished} frees exactly one slot and drains it immediately.
     * <p>
     * Named {@code Locked} because this method does <em>not</em> take the runner's monitor: it assumes the caller already holds
     * {@code synchronized (this)}. {@link #submit} and {@link #taskFinished} call it from inside their synchronized blocks, then
     * {@link #dispatch} the result only after dropping the lock. The suffix is not about a task being stuck in the queue.
     */
    private Task takeReadyLocked() {
        if (finished || failure != null || running >= maxRunning || pending.isEmpty()) {
            return null;
        }
        running++;
        Task task = pending.removeFirst();
        assert running >= maxRunning || pending.isEmpty()
            : "a second task was admissible: running=" + running + " maxRunning=" + maxRunning + " pending=" + pending.size();
        return task;
    }

    /**
     * Snapshot-and-clear of {@code pending}. Named {@code Locked} for the same reason as {@link #takeReadyLocked}: the caller
     * already holds {@code synchronized (this)}. {@link #finish()} and {@link #fail(Exception)} drain under the lock, then invoke
     * {@link Task#skip} / {@link Task#fail} on the snapshot after releasing it, so those callbacks can {@link #submit} without
     * deadlocking on the same monitor.
     */
    private List<Task> drainLocked() {
        List<Task> drained = new ArrayList<>(pending);
        pending.clear();
        return drained;
    }

    /**
     * Hands {@code task} to {@link #executor}. On the executor thread, {@link #terminateIfNeeded} re-checks finished/failed
     * (the query may have ended while this runnable sat in the executor queue), then {@link Task#execute} runs with a completion
     * listener that always comes back to {@link #taskFinished} to release the permit.
     *
     * <h4>Rejection fails the query</h4>
     *
     * {@link #executor} is the bounded search pool, so a submission can be rejected - the queue is full, or the pool is shutting
     * down. That fails the whole query, through {@link #rejectDispatch}: this leaf cannot run, and a query missing a branch cannot
     * produce a correct answer, so there is nothing to do but report the rejection. Failing one branch and carrying on would be a
     * correctness bug, not a kindness. This matches {@code AbstractThrottledTaskRunner}, which also treats a rejection as a terminal
     * per-task failure.
     * <p>
     * It is worth knowing that this is a failure point sub plans did not have before this runner existed: they were started inline,
     * on whichever thread {@code ComputeService.execute} was called on, and never touched a queue. The hop is not optional for a
     * leaf - a dispatch also happens from {@link #taskFinished}, that is, from a driver or exchange completion thread, and a leaf
     * goes on to {@code ComputeService.executePlan}, which plans against that sub plan's real {@code SearchExecutionContext}s:
     * Lucene query rewriting, weight construction, {@code SearchStats} lookups. That work does not belong on a driver thread. So a
     * multi-branch query does shed load slightly sooner than a single-branch one under search-pool pressure, and that is the
     * accepted trade-off rather than an oversight.
     * <p>
     * Merge sub plans are exempt from both the permit and the hop; see {@code MergeLevelExecutor.executeSubPlanWithNestedSubPlans}
     * for why that is safe.
     *
     * <h4>Where the permit stands when something throws</h4>
     *
     * Every point in the runnable below that can throw is already past this task's permit release: {@link #terminateIfNeeded}
     * decrements under the lock before it calls back, and on the other path {@code completion} enters {@link #taskFinished}, which
     * decrements as its first statement. {@code onFailure} therefore only has to record the query-wide failure, never to release a
     * permit. That backstop is what keeps a {@link Task#skip} or {@link Task#fail} that throws an {@link Exception} from stranding
     * the query: without it the exception escapes to the thread pool's uncaught handler with this branch's listener still
     * unresolved. It does not extend to an {@link Error} - see the {@link Task} javadoc for what that leaves open.
     */
    private void dispatch(Task task) {
        // Marks this dispatch as settled: the permit is no longer the caller's to give back. Set by whichever of the three
        // routes below gets there first - onRejection, doRun starting, or a throw out of executor.execute - because more than
        // one of them can fire for a single dispatch and each would otherwise call taskFinished for the same permit.
        // EsThreadPoolExecutor.execute invokes onRejection inside try { ... } finally { onAfter(); } and does not swallow a
        // throw from it; a direct executor runs doRun inline, so a throw from there reaches the catch as well.
        var settled = new AtomicBoolean();
        var runnable = new AbstractRunnable() {
            @Override
            protected void doRun() {
                // From here the permit belongs to the paths below - terminateIfNeeded releases it, or completion does - so the
                // catch around executor.execute must not treat a throw escaping this runnable as a task that never ran.
                settled.set(true);
                // Re-check on the executor thread: the query may have failed or finished while this task sat in the queue.
                if (terminateIfNeeded(task)) {
                    return;
                }
                ActionListener<Void> completion = ActionListener.notifyOnce(
                    ActionListener.wrap(ignored -> taskFinished(null), SubPlanTaskRunner.this::taskFinished)
                );
                try {
                    task.execute(completion);
                } catch (Exception e) {
                    // Recorded unconditionally, and first. If the task resolved its completion listener before throwing, both
                    // calls below are no-ops - notifyOnce has consumed the listener, and MergeLevelExecutor.SubPlan's
                    // compare-and-set has closed the task - so without this the exception would vanish with no failure recorded
                    // anywhere and the query reporting success. When the task had not completed, this is a redundant first-wins
                    // call ahead of the one taskFinished makes.
                    fail(e);
                    completion.onFailure(e);
                    task.fail(e);
                }
            }

            @Override
            public void onRejection(Exception e) {
                // The permit was taken by takeReadyLocked and nothing has run yet, so this path still owns it.
                if (settled.compareAndSet(false, true)) {
                    rejectDispatch(task, e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                // Only reachable from a Task callback in doRun, all of which run after the permit has been released - see the
                // javadoc above. Record the failure so the query terminates instead of waiting on a branch that will never
                // report; the duplicate fail is absorbed by the compare-and-set in MergeLevelExecutor.SubPlan.
                // AbstractRunnable#run catches Exception, not Throwable, so an Error from those callbacks does not arrive here.
                fail(e);
                task.fail(e);
            }
        };
        try {
            executor.execute(runnable);
        } catch (Exception e) {
            // EsThreadPoolExecutor routes a rejected AbstractRunnable to onRejection above and does not rethrow, but a plain
            // Executor - Runnable::run in the tests, say - still throws the rejection here.
            if (settled.compareAndSet(false, true)) {
                rejectDispatch(task, e);
            } else {
                // Already settled, so the permit has been accounted for and the failure recorded by whichever route got here
                // first. This is a second exception on its way out - typically a Task callback breaking its no-throw contract
                // while the first was being reported - and it is not this method's to absorb.
                throw e;
            }
        }
    }

    /**
     * Terminal handling for a task that never reached the executor: releases its permit, records {@code e} as the query's failure -
     * which drains and fails everything still queued - and fails the task itself. Shared by the two ways a rejection surfaces, an
     * {@code onRejection} callback and a throw out of {@link Executor#execute}.
     * <p>
     * {@link #taskFinished} may rethrow after isolating a contract-violating {@link Task#fail} on the drained batch. This leaf
     * is not in that batch, so it is failed in {@code finally} and cannot be skipped by that rethrow.
     */
    private void rejectDispatch(Task task, Exception e) {
        try {
            taskFinished(e);
        } finally {
            task.fail(e);
        }
    }

    /**
     * Last check before {@link Task#execute}: the query may have {@link #fail(Exception) failed} or {@link #finish() finished}
     * after this task was admitted and dispatched, while it waited in the executor queue. If so, the permit is released here
     * ({@code running--}) and the task is failed or skipped instead of started.
     * <p>
     * Example: {@code maxRunning = 3}, all three leaves selected and handed to a deferred executor. The first throws in
     * {@code execute}, {@link #taskFinished} records {@code failure} and drains {@code pending} (empty — the other two already
     * hold permits). When those two run, this method sees {@code failure != null}, decrements {@code running}, and
     * {@link Task#fail}s them without starting the plan.
     *
     * @return {@code true} if the task was terminated and must not be executed
     */
    private boolean terminateIfNeeded(Task task) {
        Exception rejection;
        boolean skip;
        synchronized (this) {
            rejection = failure;
            skip = rejection == null && finished;
            if (rejection == null && skip == false) {
                return false;
            }
            running--;
            assert running >= 0;
        }
        if (rejection != null) {
            task.fail(rejection);
        } else {
            task.skip();
        }
        return true;
    }
}
