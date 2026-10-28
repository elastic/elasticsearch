/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.scheduling;

import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class QuerySchedulerTests extends ESTestCase {

    private final AtomicLong clock = new AtomicLong();
    private final AtomicInteger clockReads = new AtomicInteger();
    private final QueryScheduler scheduler = new QueryScheduler(() -> {
        clockReads.incrementAndGet();
        return clock.get();
    });
    private final ManualPool pool = new ManualPool();

    /**
     * A query alone waits until its first task starts. Its other tasks queue behind its own running task, which is not
     * waiting.
     */
    public void testOnlyTheTimeWithNothingRunningIsWaiting() {
        Waits waits = new Waits();
        try (var executor = executor("q", pool, waits)) {
            executor.execute(() -> {
                clock.set(30);
                pool.runNext();
                clock.set(40);
            });
            executor.execute(() -> clock.set(35));
        }
        clock.set(10);
        pool.runNext();
        assertThat(waits.total(), equalTo(10L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A query waits again once its running tasks complete while others are still queued. A shard accumulates the
     * waiting time once, however many of its tasks are queued.
     */
    public void testWaitsAgainWhenNothingRuns() {
        Waits waits = new Waits();
        try (var executor = executor("q", pool, waits)) {
            for (int i = 0; i < 3; i++) {
                executor.execute(() -> clock.addAndGet(10));
            }
        }
        pool.runNext();
        assertThat(clock.get(), equalTo(10L));
        assertThat(waits.total(), equalTo(0L));
        clock.set(20);
        pool.runNext();
        assertThat(waits.values, contains(10L));
        pool.runNext();
        assertThat(waits.values, contains(10L));
    }

    /**
     * More tasks queued through an executor that already has tasks queued while the query waits add no shard, so the
     * waiting time is taken once, when the first task starts.
     */
    public void testQueuingMoreTasksOfAWaitingExecutorTakesTheWaitingTimeOnce() {
        Waits waits = new Waits();
        try (var executor = executor("q", pool, waits)) {
            executor.execute(() -> {});
            clock.set(10);
            executor.execute(() -> {});
            clock.set(20);
            executor.execute(() -> {});
        }
        clock.set(30);
        pool.runNext();
        assertThat(waits.values, contains(30L));
    }

    /**
     * Each shard accumulates the time it had tasks queued while the query waited, whatever the other shards had queued. A
     * shard is charged when one of its tasks leaves the queue.
     */
    public void testEachShardAccumulatesTheTimeItHadTasksQueued() {
        Waits first = new Waits();
        Waits second = new Waits();
        var firstExecutor = executor("q", pool, first);
        var secondExecutor = executor("q", pool, second);
        firstExecutor.execute(() -> {});
        clock.set(10);
        secondExecutor.execute(() -> {});
        firstExecutor.close();
        secondExecutor.close();
        clock.set(20);
        pool.runNext();
        assertThat(first.total(), equalTo(20L));
        assertThat(second.values, empty());
        pool.runNext();
        assertThat(first.total(), equalTo(20L));
        assertThat(second.total(), equalTo(10L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A shard of the query's queued tasks that has no work left, like a shard whose slices ES|QL drivers already read,
     * does not accumulate waiting time.
     */
    public void testShardWithoutWorkDoesNotAccumulate() {
        Waits done = new Waits();
        Waits pending = new Waits();
        try (var executor = executor("q", pool, done, pending)) {
            executor.execute(() -> {});
        }
        clock.set(10);
        done.hasWork = false;
        pool.runNext();
        assertThat(done.values, empty());
        assertThat(pending.values, contains(10L));
    }

    /**
     * A shard counts while any executor of the query that works on it still has work for it, even if another executor
     * queued earlier is done with it.
     */
    public void testShardCountsWhileAnyExecutorHasWorkForIt() {
        Waits shard = new Waits();
        try (
            var done = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, () -> false)));
            var pending = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, () -> true)))
        ) {
            done.execute(() -> {});
            pending.execute(() -> {});
        }
        clock.set(10);
        pool.runNext();
        assertThat(shard.values, empty());
        pool.runNext();
        assertThat(shard.values, contains(10L));
    }

    /**
     * A shard that tasks of several executors of the query work on accumulates the waiting time once.
     */
    public void testShardOfSeveralExecutorsAccumulatesOnce() {
        Waits shared = new Waits();
        Waits other = new Waits();
        try (var dataExecutor = executor("q", pool, shared, other); var fetchExecutor = executor("q", pool, shared)) {
            dataExecutor.execute(() -> {});
            fetchExecutor.execute(() -> {});
        }
        clock.set(10);
        pool.runNext();
        assertThat(shared.values, contains(10L));
        assertThat(other.values, contains(10L));
    }

    /**
     * A task submitted to another executor through an executor is one of its tasks, like an ES|QL driver resuming on
     * another queue.
     */
    public void testTaskSubmittedViaAnotherExecutor() {
        Waits waits = new Waits();
        ManualPool other = new ManualPool();
        try (var executor = executor("q", pool, waits)) {
            executor.execute(() -> {});
            clock.set(10);
            executor.execute(() -> {}, other);
        }
        assertThat(pool.tasks.size(), equalTo(1));
        assertThat(other.tasks.size(), equalTo(1));
        clock.set(20);
        other.runNext();
        assertThat(waits.values, contains(20L));
        pool.runNext();
        assertThat(waits.values, contains(20L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Queries wait independently: one running does not stop another from waiting.
     */
    public void testQueriesWaitIndependently() {
        Waits a = new Waits();
        Waits b = new Waits();
        try (var executorA = executor("a", pool, a); var executorB = executor("b", pool, b)) {
            executorA.execute(() -> {
                clock.set(25);
                pool.runNext();
            });
            executorB.execute(() -> {});
        }
        clock.set(5);
        pool.runNext();
        assertThat(a.values, contains(5L));
        assertThat(b.values, contains(25L));
    }

    /**
     * A rejected task's shard keeps the time it waited. The task keeps the rejection behavior of the task it wraps, and
     * leaves nothing tracked.
     */
    public void testRejectedTask() {
        Waits waits = new Waits();
        AtomicInteger rejections = new AtomicInteger();
        AtomicInteger afters = new AtomicInteger();
        Executor rejecting = new Executor() {
            @Override
            public void execute(Runnable command) {
                clock.addAndGet(10);
                // like EsThreadPoolExecutor, which hands the rejection to an AbstractRunnable instead of throwing
                AbstractRunnable abstractRunnable = (AbstractRunnable) command;
                try {
                    abstractRunnable.onRejection(new EsRejectedExecutionException("rejected"));
                } finally {
                    abstractRunnable.onAfter();
                }
            }
        };
        try (var executor = executor("q", rejecting, waits)) {
            executor.execute(new AbstractRunnable() {
                @Override
                public void onFailure(Exception e) {
                    fail("rejection must not be reported as a failure");
                }

                @Override
                public void onRejection(Exception e) {
                    rejections.incrementAndGet();
                }

                @Override
                public void onAfter() {
                    afters.incrementAndGet();
                }

                @Override
                protected void doRun() {
                    fail("must not run");
                }
            });
        }
        assertThat(rejections.get(), equalTo(1));
        assertThat(afters.get(), equalTo(1));
        assertThat(waits.values, contains(10L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A task the pool rejects by throwing leaves nothing tracked, and the exception reaches the caller.
     */
    public void testPoolThrows() {
        Executor throwing = command -> { throw new EsRejectedExecutionException("rejected"); };
        try (var executor = executor("q", throwing)) {
            expectThrows(EsRejectedExecutionException.class, () -> executor.execute(() -> fail("must not run")));
        }
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A pool that queues the task and then throws still runs it later: the task was completed when the pool threw, so
     * running it changes nothing.
     */
    public void testPoolThrowsAfterQueuingTheTask() {
        Waits waits = new Waits();
        Executor throwing = command -> {
            pool.execute(command);
            throw new EsRejectedExecutionException("rejected");
        };
        AtomicInteger ran = new AtomicInteger();
        try (var executor = executor("q", throwing, waits)) {
            expectThrows(EsRejectedExecutionException.class, () -> executor.execute(ran::incrementAndGet));
        }
        assertThat(scheduler.queries(), equalTo(0));
        clock.set(10);
        pool.runNext();
        assertThat(ran.get(), equalTo(1));
        assertThat(waits.values, empty());
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * The clock is read only when the query starts or stops waiting, or when an executor starts queuing while the query
     * waits. A task leaving the queue charges only the shards of its executor, once for all the waiting since they were last
     * charged, here one executor per shard as for DSL phases, so the cost of a task does not grow with the tasks queued.
     */
    public void testClockReadsAndChargesPerTask() {
        int shardCount = between(1, 10);
        List<Waits> shards = new ArrayList<>();
        List<QueryScheduler.QueryExecutor> executors = new ArrayList<>();
        for (int i = 0; i < shardCount; i++) {
            Waits shard = new Waits();
            shards.add(shard);
            executors.add(executor("q", pool, shard));
        }
        // the first task runs the others, which start and complete while it runs
        executors.get(0).execute(() -> {
            while (pool.tasks.isEmpty() == false) {
                pool.runNext();
            }
        });
        assertThat(clockReads.get(), equalTo(1));
        for (int i = 1; i < shardCount; i++) {
            clock.incrementAndGet();
            executors.get(i).execute(() -> {});
            assertThat(clockReads.get(), equalTo(i + 1));
        }
        // more tasks of executors that already have one queued change nothing
        executors.get(0).execute(() -> {});
        assertThat(clockReads.get(), equalTo(shardCount));
        executors.forEach(QueryScheduler.QueryExecutor::close);
        clock.incrementAndGet();
        pool.runNext();
        assertThat(clockReads.get(), equalTo(shardCount + 1));
        for (int i = 0; i < shardCount; i++) {
            assertThat(shards.get(i).values, contains((long) (shardCount - i)));
        }
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Charging the waiting while tasks are queued charges their shards up to now, and a task leaving the queue then
     * charges only the rest, so a shard is charged the same in total, once even when several executors queue it.
     */
    public void testChargeWaitingWhileQueued() {
        Waits first = new Waits();
        Waits second = new Waits();
        var alone = executor("q", pool, first);
        var shared = executor("q", pool, first, second);
        alone.execute(() -> {});
        clock.set(10);
        scheduler.chargeWaiting();
        assertThat(first.values, contains(10L));
        shared.execute(() -> {});
        clock.set(25);
        scheduler.chargeWaiting();
        scheduler.chargeWaiting();
        assertThat(first.values, contains(10L, 15L));
        assertThat(second.values, contains(15L));
        alone.close();
        shared.close();
        clock.set(30);
        pool.runNext();
        assertThat(first.values, contains(10L, 15L, 5L));
        clock.set(40);
        scheduler.chargeWaiting();
        assertThat(first.values, contains(10L, 15L, 5L, 10L));
        assertThat(second.values, contains(15L, 15L));
        pool.runNext();
        assertThat(first.total(), equalTo(40L));
        assertThat(second.total(), equalTo(30L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Random tasks of executors sharing shards are queued, started in any order, run while others start, and complete. Each
     * shard accumulates in total the time the query waited while the shard had tasks queued, once however many executors
     * queued it, as a reference that follows every step computes. After charging the waiting while tasks are queued, each
     * shard has accumulated all of it.
     */
    public void testAccumulatesTheWaitingOfAReference() {
        List<Waits> shards = new ArrayList<>();
        for (int i = between(1, 5); i > 0; i--) {
            shards.add(new Waits());
        }
        List<QueryScheduler.QueryExecutor> executors = new ArrayList<>();
        List<List<Waits>> executorShards = new ArrayList<>();
        for (int i = between(1, 6); i > 0; i--) {
            List<Waits> some = randomNonEmptySubsetOf(shards);
            executorShards.add(some);
            executors.add(executor("q", pool, some.toArray(Waits[]::new)));
        }
        Reference reference = new Reference(executors.size(), shards);
        List<Runnable> queued = new ArrayList<>();
        int[] steps = { between(10, 200) };
        Runnable[] step = new Runnable[1];
        step[0] = () -> {
            while (steps[0]-- > 0) {
                switch (between(0, 4)) {
                    case 0 -> {
                        int e = between(0, executors.size() - 1);
                        reference.queued[e]++;
                        executors.get(e).execute(() -> {
                            reference.queued[e]--;
                            reference.running++;
                            if (randomBoolean()) {
                                step[0].run();
                            }
                            reference.running--;
                        });
                        queued.add(pool.tasks.pollLast());
                    }
                    case 1 -> {
                        if (queued.isEmpty() == false) {
                            queued.remove(between(0, queued.size() - 1)).run();
                        }
                    }
                    case 2 -> reference.advance(between(0, 10), executorShards);
                    case 3 -> {
                        return;
                    }
                    case 4 -> {
                        scheduler.chargeWaiting();
                        for (Waits shard : shards) {
                            assertThat(shard.total(), equalTo(reference.waited.get(shard)));
                        }
                    }
                    default -> throw new AssertionError();
                }
            }
        };
        while (steps[0] > 0) {
            step[0].run();
        }
        while (queued.isEmpty() == false) {
            reference.advance(between(0, 10), executorShards);
            queued.remove(between(0, queued.size() - 1)).run();
        }
        executors.forEach(QueryScheduler.QueryExecutor::close);
        for (Waits shard : shards) {
            assertThat(shard.total(), equalTo(reference.waited.get(shard)));
        }
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Follows the tasks of one query and accumulates its waiting to each shard as time passes.
     */
    private class Reference {
        final int[] queued;
        int running;
        final Map<Waits, Long> waited = new IdentityHashMap<>();

        Reference(int executors, List<Waits> shards) {
            queued = new int[executors];
            shards.forEach(shard -> waited.put(shard, 0L));
        }

        void advance(long nanos, List<List<Waits>> executorShards) {
            boolean anyQueued = false;
            Set<Waits> queuedShards = Collections.newSetFromMap(new IdentityHashMap<>());
            for (int e = 0; e < queued.length; e++) {
                if (queued[e] > 0) {
                    anyQueued = true;
                    queuedShards.addAll(executorShards.get(e));
                }
            }
            if (running == 0 && anyQueued) {
                queuedShards.forEach(shard -> waited.merge(shard, nanos, Long::sum));
            }
            clock.addAndGet(nanos);
        }
    }

    /**
     * A wrapped AbstractRunnable runs, fails, completes and forces its execution as it would unwrapped.
     */
    public void testAbstractRunnableBehavior() {
        List<String> calls = new ArrayList<>();
        boolean force = randomBoolean();
        try (var executor = executor("q", pool)) {
            executor.execute(new AbstractRunnable() {
                @Override
                public boolean isForceExecution() {
                    return force;
                }

                @Override
                public void onFailure(Exception e) {
                    calls.add("failure:" + e.getMessage());
                }

                @Override
                public void onAfter() {
                    calls.add("after");
                }

                @Override
                protected void doRun() {
                    calls.add("run");
                    throw new IllegalStateException("boom");
                }
            });
        }
        assertThat(((AbstractRunnable) pool.tasks.peek()).isForceExecution(), equalTo(force));
        pool.runNext();
        assertThat(calls, contains("run", "failure:boom", "after"));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A query is tracked while it has tasks queued or running, and is forgotten after. An executor of the query with no
     * task does not track it, and tracks it again when it submits one.
     */
    public void testQueryIsForgottenWhenIdle() {
        Waits waits = new Waits();
        var executor = executor("q", pool, waits);
        assertThat(scheduler.queries(), equalTo(0));
        executor.execute(() -> {});
        assertThat(scheduler.queries(), equalTo(1));
        pool.runNext();
        assertThat(scheduler.queries(), equalTo(0));

        // the same executor after its query was forgotten: the query waits from this task on, not from the last one
        clock.set(10);
        executor.execute(() -> {});
        executor.close();
        assertThat(scheduler.queries(), equalTo(1));
        clock.set(15);
        pool.runNext();
        assertThat(waits.values, contains(5L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Tasks submitted and run from many threads leave nothing tracked.
     */
    public void testConcurrentQueries() throws Exception {
        QueryScheduler concurrent = new QueryScheduler(System::nanoTime);
        AtomicLong waited = new AtomicLong();
        AtomicInteger ran = new AtomicInteger();
        int queries = between(2, 8);
        int tasksPerQuery = between(1, 20);
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            Thread[] submitters = new Thread[queries];
            for (int q = 0; q < queries; q++) {
                String queryId = "q" + q;
                submitters[q] = new Thread(() -> {
                    try (
                        var executor = concurrent.newExecutor(
                            new TaskId("coordinator", queryId.hashCode()),
                            threadPool.generic(),
                            List.of(new QueryScheduler.Shard(queryId, (w, now) -> waited.addAndGet(w), () -> true))
                        )
                    ) {
                        for (int t = 0; t < tasksPerQuery; t++) {
                            executor.execute(ran::incrementAndGet);
                        }
                    }
                });
                submitters[q].start();
            }
            for (Thread submitter : submitters) {
                submitter.join();
            }
            assertBusy(() -> {
                assertThat(ran.get(), equalTo(queries * tasksPerQuery));
                assertThat(concurrent.queries(), equalTo(0));
            });
        } finally {
            terminate(threadPool);
        }
    }

    private QueryScheduler.QueryExecutor executor(String query, Executor pool, Waits... shards) {
        List<QueryScheduler.Shard> searched = new ArrayList<>();
        for (Waits shard : shards) {
            searched.add(new QueryScheduler.Shard(shard, shard, shard::hasWork));
        }
        return scheduler.newExecutor(queryId(query), pool, searched);
    }

    private static TaskId queryId(String query) {
        return new TaskId("coordinator", query.hashCode());
    }

    private static class Waits implements QueryScheduler.SearchWaitingAccumulator {
        final List<Long> values = new ArrayList<>();
        volatile boolean hasWork = true;

        boolean hasWork() {
            return hasWork;
        }

        @Override
        public void accumulateSearchWaiting(long waitedInNanos, long nowInNanos) {
            values.add(waitedInNanos);
        }

        long total() {
            return values.stream().mapToLong(Long::longValue).sum();
        }
    }

    /**
     * Runs its tasks only when the test asks, in submission order. A task can run the next one from inside, so both run
     * at once as they would on two threads.
     */
    private static class ManualPool implements Executor {
        final Deque<Runnable> tasks = new ArrayDeque<>();

        @Override
        public void execute(Runnable command) {
            tasks.add(command);
        }

        void runNext() {
            tasks.poll().run();
        }
    }
}
