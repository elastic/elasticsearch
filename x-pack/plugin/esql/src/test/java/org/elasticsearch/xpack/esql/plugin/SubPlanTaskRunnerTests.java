/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plugin;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Scheduling tests for {@link SubPlanTaskRunner}: a real runner driven by stand-in {@link SubPlanTaskRunner.Task}s.
 * <p>
 * The runner's contract is deliberately narrow — admit at most {@code branch_parallel_degree} tasks at once, and hand every
 * task exactly one of {@code execute}, {@code skip} or {@code fail} — so these tests need no {@code ComputeService},
 * exchange service or thread pool. What they do need to reproduce is the awkward part of the runner's environment: leaves
 * arrive from several {@link MergeLevelExecutor}s at different nesting levels, none of which knows what the others are
 * doing, so tasks routinely arrive after the query has already failed or finished, and after a permit was handed out but
 * before the task reached the executor. Each test below pins down one of those orderings.
 */
public class SubPlanTaskRunnerTests extends ESTestCase {

    /**
     * The cap is query-wide and enforced with a queue, not just a counter. Four leaves are submitted at a degree of 2, as
     * two nesting levels each opening their own window of 2 would do, and only the first two may start; the other two wait.
     * <p>
     * Each completion then admits exactly one queued leaf, in submission order, so the queue is FIFO across the whole
     * query rather than per level.
     */
    public void testLimitsConcurrencyAcrossAllSubmittedLeaves() {
        var harness = Harness.inline(2);
        harness.submit(0, 1, 2, 3);

        assertThat(harness.started, contains(0, 1));
        assertThat(harness.maxRunning.get(), equalTo(2));

        harness.complete(0);
        assertThat(harness.started, contains(0, 1, 2));
        harness.complete(1);
        assertThat(harness.started, contains(0, 1, 2, 3));
        harness.complete(2);
        harness.complete(3);

        assertThat(harness.running.get(), equalTo(0));
        assertThat(harness.rejected, empty());
    }

    /**
     * A running leaf failing takes the queue down with it, and the exception is remembered for leaves that have not been
     * submitted yet.
     * <p>
     * The latecomer matters: an executor at another nesting level may still be walking its own branches when the query
     * fails, and every branch it submits from then on must be failed with the original exception. Dropping it or starting
     * it would leave that branch's listener unresolved and the query hanging on a sub plan that can never finish.
     */
    public void testFailureRejectsQueuedAndSubsequentLeaves() {
        var harness = Harness.inline(1);
        harness.submit(0, 1, 2);
        var failure = new IllegalStateException("test failure");

        harness.fail(0, failure);
        harness.submit(3);

        assertThat(harness.started, contains(0));
        assertThat(harness.rejected, contains(1, 2, 3));
        assertThat(harness.rejectionFailures.get(1), equalTo(failure));
        assertThat(harness.rejectionFailures.get(2), equalTo(failure));
        assertThat(harness.rejectionFailures.get(3), equalTo(failure));
    }

    /**
     * {@link SubPlanTaskRunner#finish()} — the query already has all the rows it needs, typically from a coordinator
     * {@code LIMIT} — retires queued leaves through {@code skip} rather than {@code fail}. The distinction is not
     * cosmetic: {@code skip} reports empty success, which is what lets the coordinator's exchange source finish, whereas
     * {@code fail} would surface an error for a query that actually succeeded.
     * <p>
     * The already-running leaf is left alone and still completes normally at the end. {@code finished()} stays observable
     * afterwards because merge sub plans never enter the queue and so cannot be skipped here; {@link MergeLevelExecutor}
     * has to ask.
     */
    public void testFinishSkipsQueuedAndSubsequentLeaves() {
        var harness = Harness.inline(1);
        harness.submit(0, 1, 2);

        harness.runner.finish();
        harness.submit(3);

        assertThat(harness.runner.finished(), equalTo(true));
        assertThat(harness.started, contains(0));
        assertThat(harness.skipped, contains(1, 2, 3));
        assertThat(harness.rejected, empty());
        harness.complete(0);
    }

    /**
     * A task that resolves its completion listener twice must not release two permits. {@code Task#execute} promises to
     * resolve it exactly once, but the counter it decrements is shared by the whole query, so the runner wraps the
     * listener in {@code notifyOnce} rather than trusting the task: a double release would inflate the cap for the rest of
     * the query and, once {@code running} went negative, trip the assertion in {@code taskFinished}.
     * <p>
     * Resolving the first leaf's listener twice must therefore admit one more leaf, not two.
     */
    public void testCompletionReleasesPermitOnlyOnce() {
        var harness = Harness.inline(1);
        harness.submit(0, 1, 2);

        ActionListener<Void> first = harness.completions.get(0);
        harness.running.decrementAndGet();
        first.onResponse(null);
        first.onResponse(null);

        assertThat(harness.started, contains(0, 1));
        harness.complete(1);
        assertThat(harness.started, contains(0, 1, 2));
        harness.complete(2);
    }

    /**
     * A leaf whose {@code execute} throws instead of returning is treated as the query failing. The runner has to do three
     * things on that one thread: release the permit, hand the exception back to the leaf that threw — it may already have
     * opened an exchange sink that needs closing — and record the exception so later submissions are rejected with it.
     * <p>
     * That the throwing leaf is failed and not skipped is the assertion worth keeping: skipping means "empty success", so
     * treating a startup failure that way would drop a branch's rows and report the query as fine.
     */
    public void testSynchronousStartupFailureRejectsCurrentAndQueuedLeaves() {
        var failure = new IllegalStateException("startup failure");
        var harness = Harness.inline(1);
        harness.failOnExecute(failure);

        harness.submit(0, 1);

        assertThat(harness.skipped, empty());
        assertThat(harness.rejected, contains(0, 1));
        assertThat(harness.rejectionFailures.get(0), equalTo(failure));
        assertThat(harness.rejectionFailures.get(1), equalTo(failure));
        assertThat(harness.runner.failure(), equalTo(failure));
    }

    /**
     * A leaf holds its permit from the moment it is admitted, but its plan only starts later, once the executor gets round
     * to the runnable. Anything the query does in that window has to be honoured before the plan starts, which is what
     * {@code terminateIfNeeded} is for: a leaf admitted before the failure but not yet started must be failed on the
     * executor thread and give its permit back, rather than starting work for a query that is already over.
     */
    public void testSynchronousFailureDoesNotStartAlreadySelectedLeaves() {
        var failure = new IllegalStateException("startup failure");
        // A deferred executor lets all three leaves be selected before any of them runs, so the failure of the first
        // lands while the other two are already sitting in the executor queue holding permits.
        var harness = Harness.deferred(3);
        harness.failOnExecuteOf(0, failure);

        harness.submit(0, 1, 2);
        harness.runDispatched();

        assertThat(harness.started, contains(0));
        assertThat(harness.skipped, empty());
        assertThat(harness.rejected, containsInAnyOrder(0, 1, 2));
    }

    /**
     * The search pool is bounded, so a submission can be rejected — the queue is full, or the node is shutting down. A leaf that
     * never reaches the executor cannot produce its rows, and a query missing a branch cannot produce a correct answer, so the
     * rejection has to fail the whole query rather than quietly leaving a branch out.
     * <p>
     * {@code EsThreadPoolExecutor} reports this by calling {@code onRejection} on the {@link AbstractRunnable} instead of throwing,
     * which is the path exercised here: the permit the runner had already taken must come back, the rejected leaf must be failed
     * (not skipped — skipping is empty success), and the exception must be recorded so the leaves behind it are failed with it.
     */
    public void testRejectedDispatchFailsTheQuery() {
        var rejection = new EsRejectedExecutionException("queue full");
        var harness = Harness.rejectingViaCallback(2, rejection);

        harness.submit(0, 1, 2);

        assertThat(harness.started, empty());
        assertThat(harness.skipped, empty());
        assertThat(harness.rejected, contains(0, 1, 2));
        assertThat(harness.rejectionFailures.get(0), equalTo(rejection));
        assertThat(harness.rejectionFailures.get(2), equalTo(rejection));
        assertThat(harness.runner.failure(), equalTo(rejection));
    }

    /**
     * The same rejection, arriving the other way. Only {@code EsThreadPoolExecutor} turns a rejection into an {@code onRejection}
     * callback; a plain {@link Executor} throws it out of {@code execute} instead, which is what the executors in these tests do and
     * what any non-Elasticsearch executor would do. Both routes must end in the same place, or a rejection would escape into
     * {@code submit}'s caller with the permit still held.
     */
    public void testRejectionThrownByTheExecutorFailsTheQuery() {
        var rejection = new EsRejectedExecutionException("queue full");
        var harness = Harness.rejectingByThrowing(2, rejection);

        harness.submit(0, 1);

        assertThat(harness.started, empty());
        assertThat(harness.rejected, contains(0, 1));
        assertThat(harness.runner.failure(), equalTo(rejection));
    }

    /**
     * A rejection can surface twice for one dispatch. {@code EsThreadPoolExecutor.execute} calls {@code onRejection} inside
     * {@code try { ... } finally { onAfter(); }} and does not swallow a throw from it, so a {@code Task#fail} that breaks its
     * no-throw contract sends that exception on out of {@code execute} and into the catch that handles a plain executor's
     * rejection. Only the first route may release the permit: releasing twice for one task takes {@code running} negative — the
     * assertion in {@code taskFinished} — and, with assertions off, lets one more leaf run than the cap allows.
     */
    public void testThrowFromFailOnRejectionReleasesThePermitOnce() {
        var rejection = new EsRejectedExecutionException("queue full");
        var harness = Harness.rejectingViaCallback(1, rejection);
        var thrownByFail = new IllegalStateException("fail failure");
        harness.failOnFailOf(0, thrownByFail);

        // The contract-violating throw is not absorbed: onRejection has already settled the dispatch, so the catch around
        // executor.execute rethrows rather than releasing the permit a second time.
        assertThat(expectThrows(IllegalStateException.class, () -> harness.submit(0)), equalTo(thrownByFail));

        assertThat(harness.rejected, contains(0));
        assertThat(harness.runner.failure(), equalTo(rejection));
        // The permit came back exactly once, so the next leaf is admitted normally rather than finding a negative count.
        harness.submit(1);
        assertThat(harness.rejected, contains(0, 1));
    }

    /**
     * {@code Task#skip} and {@code Task#fail} must not throw, but if one does the runner cannot let the exception escape to the
     * thread pool's uncaught handler: those callbacks are the only way a sub plan reaches a terminal state, so the branch's listener
     * would stay unresolved and the query would wait on it until a timeout. The failure has to be recorded instead, so the query
     * ends with that exception.
     * <p>
     * The throw happens where it is most likely to: {@code terminateIfNeeded} skipping a leaf that was admitted before
     * {@code finish()} and only reached the executor afterwards. The permit is already back by then — it is released under the lock,
     * before the callback — which is why the backstop only records the failure and does not touch the counter.
     */
    public void testThrowFromSkipOnTheExecutorFailsTheQuery() {
        var failure = new IllegalStateException("skip failure");
        // A deferred executor lets both leaves be admitted before either runs, so finish() lands while they sit in the queue.
        var harness = Harness.deferred(2);
        harness.failOnSkipOf(0, failure);

        harness.submit(0, 1);
        harness.runner.finish();
        harness.runDispatched();

        assertThat(harness.started, empty());
        assertThat(harness.skipped, contains(0));
        assertThat(harness.runner.failure(), equalTo(failure));
        // The leaf that threw is failed too, and the one behind it is failed rather than skipped: the query is over.
        assertThat(harness.rejected, contains(0, 1));

        harness.submit(2);
        assertThat(harness.rejected, hasItem(2));
    }

    /**
     * A leaf that resolves its completion listener and only then throws. Both terminal calls in the catch are no-ops by that point -
     * {@code notifyOnce} has consumed the listener and {@code SubPlan}'s compare-and-set has closed the task - so unless the runner
     * records the exception itself it disappears: no failure, no log, and the query goes on to report success.
     */
    public void testThrowAfterCompletionIsStillRecordedAsTheQueryFailure() {
        var failure = new IllegalStateException("thrown while unwinding");
        var harness = Harness.inline(1);
        harness.completeThenFailOnExecuteOf(0, failure);

        harness.submit(0);

        assertThat(harness.started, contains(0));
        assertThat(harness.runner.failure(), equalTo(failure));
        // And the recorded failure is what later leaves are rejected with, rather than them running for a doomed query.
        harness.submit(1);
        assertThat(harness.rejected, hasItem(1));
        assertThat(harness.rejectionFailures.get(1), equalTo(failure));
    }

    /**
     * A direct executor runs the task inline, so a throw from {@code doRun} - here {@code Task#fail} breaking its no-throw contract
     * while the runner reports a startup failure - comes back out of {@code executor.execute}. The permit was already released on
     * the way through, so the catch must not release it again: doing so takes {@code running} negative, tripping the assertion in
     * {@code taskFinished}, or with assertions off lets more than {@code maxRunning} leaves run at once.
     */
    public void testThrowAfterDoRunStartedDoesNotReleaseThePermitTwice() {
        var harness = Harness.inline(1);
        harness.failOnExecuteOf(0, new IllegalStateException("startup failure"));
        var thrownByFail = new IllegalStateException("fail failure");
        harness.failOnFailOf(0, thrownByFail);

        assertThat(expectThrows(IllegalStateException.class, () -> harness.submit(0)), equalTo(thrownByFail));

        // The permit came back exactly once, so the runner is still in a state where it can admit and retire another leaf. (Leaf 0
        // appears in `rejected` more than once: unlike MergeLevelExecutor.SubPlan, the stand-in has no compare-and-set to absorb
        // the repeat, which is what makes the runner hand it a second fail here at all.)
        harness.submit(1);
        assertThat(harness.rejected, hasItem(1));
    }

    /**
     * {@link SubPlanTaskRunner#finish()} retires the whole queue in one pass. A task whose {@code skip} throws must not take its
     * siblings with it: they are the only reference each of their sub plans has, so a drain that stops half way leaves those
     * listeners unresolved and hangs the query. The throw still surfaces, after the batch rather than instead of it.
     */
    public void testThrowFromSkipStillRetiresTheRestOfTheQueue() {
        var harness = Harness.deferred(1);
        harness.submit(0, 1, 2, 3);
        var thrownBySkip = new IllegalStateException("skip failure");
        harness.failOnSkipOf(1, thrownBySkip);

        assertThat(expectThrows(IllegalStateException.class, harness.runner::finish), equalTo(thrownBySkip));

        // 0 holds the only permit and was never queued; 1 threw, and 2 and 3 behind it were retired all the same.
        assertThat(harness.skipped, contains(1, 2, 3));
    }

    /**
     * A running leaf failing retires the queue through {@code taskFinished}, not {@link SubPlanTaskRunner#fail(Exception)}.
     * A task whose {@code fail} throws must not take its siblings with it: they are the only reference each of their sub
     * plans has, so a drain that stops half way leaves those listeners unresolved and hangs the query.
     */
    public void testThrowFromFailInTaskFinishedStillRetiresTheRestOfTheQueue() {
        var harness = Harness.deferred(1);
        harness.submit(0, 1, 2, 3);
        harness.runDispatched();
        var thrownByFail = new IllegalStateException("fail failure");
        harness.failOnFailOf(1, thrownByFail);

        try {
            harness.fail(0, new IllegalStateException("leaf failure"));
        } catch (RuntimeException | AssertionError ignored) {
            // terminateAll rethrows after the batch. The completion listener is an ActionListener, which turns an
            // unexpected callback throw into AssertionError under -ea and rethrows the original without -ea.
        }

        // 0 is the running leaf and is not in the drained batch; 1 threw, and 2 and 3 were failed all the same.
        assertThat(harness.rejected, contains(1, 2, 3));
    }

    /**
     * {@code rejectDispatch} fails the leaf that never reached the executor after {@code taskFinished} has drained the
     * queue. If a queued {@code fail} throws, {@code terminateAll} rethrows once the batch is done; that must not skip
     * the rejected leaf itself.
     */
    public void testThrowFromFailInTaskFinishedStillFailsRejectedDispatch() {
        var harness = Harness.deferred(1);
        harness.submit(0, 1, 2, 3);
        var thrownByFail = new IllegalStateException("fail failure");
        harness.failOnFailOf(1, thrownByFail);
        var rejection = new EsRejectedExecutionException("queue full");

        assertThat(expectThrows(IllegalStateException.class, () -> harness.rejectDispatched(rejection)), equalTo(thrownByFail));

        // 1 threw; 2 and 3 were failed all the same; 0 is the rejected dispatch, failed in finally.
        assertThat(harness.rejected, contains(1, 2, 3, 0));
        assertThat(harness.rejectionFailures.get(0), equalTo(rejection));
    }

    /**
     * Drives a real {@link SubPlanTaskRunner} with stand-in {@link SubPlanTaskRunner.Task}s, recording which
     * of the three callbacks each task received and holding on to the completion listeners so a test can finish tasks when it
     * chooses.
     */
    private static final class Harness {
        private final AtomicInteger running = new AtomicInteger();
        private final AtomicInteger maxRunning = new AtomicInteger();
        private final List<Integer> started = new ArrayList<>();
        private final List<Integer> skipped = new ArrayList<>();
        private final List<Integer> rejected = new ArrayList<>();
        private final Map<Integer, ActionListener<Void>> completions = new HashMap<>();
        private final Map<Integer, Exception> rejectionFailures = new HashMap<>();
        private final Map<Integer, RuntimeException> executeFailures = new HashMap<>();
        private final Map<Integer, RuntimeException> skipFailures = new HashMap<>();
        private final Map<Integer, RuntimeException> failFailures = new HashMap<>();
        private final Map<Integer, RuntimeException> postCompletionFailures = new HashMap<>();
        private final List<Runnable> dispatched = new ArrayList<>();
        private final SubPlanTaskRunner runner;
        private RuntimeException executeFailureForAll;

        /** Runs each leaf on the calling thread, the way a thread pool with a free slot would. */
        private static Harness inline(int degree) {
            return new Harness(degree, true);
        }

        /** Queues each leaf instead of running it, so a test controls exactly when it starts. */
        private static Harness deferred(int degree) {
            return new Harness(degree, false);
        }

        /** Rejects every leaf the way {@code EsThreadPoolExecutor} does for an {@link AbstractRunnable}: a callback, no throw. */
        private static Harness rejectingViaCallback(int degree, Exception rejection) {
            return new Harness(degree, (Executor) runnable -> ((AbstractRunnable) runnable).onRejection(rejection));
        }

        /** Rejects every leaf the way a plain {@link Executor} does, by throwing out of {@code execute}. */
        private static Harness rejectingByThrowing(int degree, RuntimeException rejection) {
            return new Harness(degree, (Executor) runnable -> { throw rejection; });
        }

        private Harness(int degree, boolean inline) {
            this.runner = new SubPlanTaskRunner(degree, inline ? Runnable::run : dispatched::add);
        }

        private Harness(int degree, Executor executor) {
            this.runner = new SubPlanTaskRunner(degree, executor);
        }

        private void failOnExecute(RuntimeException failure) {
            this.executeFailureForAll = failure;
        }

        private void failOnExecuteOf(int task, RuntimeException failure) {
            executeFailures.put(task, failure);
        }

        private void failOnSkipOf(int task, RuntimeException failure) {
            skipFailures.put(task, failure);
        }

        private void failOnFailOf(int task, RuntimeException failure) {
            failFailures.put(task, failure);
        }

        /** A leaf that finishes normally and then throws on the way out, the way a synchronous executePlan could. */
        private void completeThenFailOnExecuteOf(int task, RuntimeException failure) {
            postCompletionFailures.put(task, failure);
        }

        private void submit(Integer... tasks) {
            for (Integer task : tasks) {
                runner.submit(new FakeSubPlan(task));
            }
        }

        private void runDispatched() {
            List<Runnable> toRun = new ArrayList<>(dispatched);
            dispatched.clear();
            toRun.forEach(Runnable::run);
        }

        /** Rejects the next queued dispatch the way {@code EsThreadPoolExecutor} reports a full search pool. */
        private void rejectDispatched(Exception rejection) {
            assert dispatched.isEmpty() == false;
            ((AbstractRunnable) dispatched.removeFirst()).onRejection(rejection);
        }

        private void complete(int task) {
            running.decrementAndGet();
            completions.get(task).onResponse(null);
        }

        private void fail(int task, Exception failure) {
            running.decrementAndGet();
            completions.get(task).onFailure(failure);
        }

        private final class FakeSubPlan implements SubPlanTaskRunner.Task {
            private final int id;

            private FakeSubPlan(int id) {
                this.id = id;
            }

            @Override
            public void execute(ActionListener<Void> completion) {
                started.add(id);
                maxRunning.accumulateAndGet(running.incrementAndGet(), Math::max);
                completions.put(id, completion);
                RuntimeException failure = executeFailures.getOrDefault(id, executeFailureForAll);
                if (failure != null) {
                    throw failure;
                }
                RuntimeException afterCompletion = postCompletionFailures.get(id);
                if (afterCompletion != null) {
                    running.decrementAndGet();
                    completion.onResponse(null);
                    throw afterCompletion;
                }
            }

            @Override
            public void skip() {
                skipped.add(id);
                RuntimeException failure = skipFailures.get(id);
                if (failure != null) {
                    throw failure;
                }
            }

            @Override
            public void fail(Exception failure) {
                rejected.add(id);
                rejectionFailures.put(id, failure);
                RuntimeException thrown = failFailures.get(id);
                if (thrown != null) {
                    throw thrown;
                }
            }
        }
    }
}
