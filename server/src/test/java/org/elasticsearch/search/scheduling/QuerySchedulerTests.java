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
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
        var executor = executor("q", pool, waits);
        executor.execute(() -> {
            clock.set(30);
            pool.runNext();
            clock.set(40);
        });
        executor.execute(() -> clock.set(35));
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
        var executor = executor("q", pool, waits);
        for (int i = 0; i < 3; i++) {
            executor.execute(() -> clock.addAndGet(10));
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
        var executor = executor("q", pool, waits);
        executor.execute(() -> {});
        clock.set(10);
        executor.execute(() -> {});
        clock.set(20);
        executor.execute(() -> {});
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
        var executor = executor("q", pool, done, pending);
        executor.execute(() -> {});
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
        var done = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, () -> false)));
        var pending = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, () -> true)));
        done.execute(() -> {});
        pending.execute(() -> {});
        clock.set(10);
        pool.runNext();
        assertThat(shard.values, empty());
        pool.runNext();
        assertThat(shard.values, contains(10L));
    }

    /**
     * A shard is charged from when an executor of the query with work for it queues it, not from when another executor
     * without work for it did.
     */
    public void testShardWithoutWorkInOneExecutorIsChargedFromWhenAnotherHasWork() {
        Waits shared = new Waits();
        Waits other = new Waits();
        var withoutWork = scheduler.newExecutor(
            queryId("q"),
            pool,
            List.of(new QueryScheduler.Shard(shared, shared, () -> false), new QueryScheduler.Shard(other, other, other::hasWork))
        );
        withoutWork.execute(() -> {});
        clock.set(10);
        var withWork = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shared, shared, shared::hasWork)));
        withWork.execute(() -> {});
        clock.set(20);
        pool.runNext();
        pool.runNext();
        assertThat(other.total(), equalTo(20L));
        assertThat(shared.total(), equalTo(10L));
    }

    /**
     * A shared shard an executor ran out of work for is charged only from when another executor with work for it queues it.
     */
    public void testSharedShardOutOfWorkIsChargedFromWhenAnotherHasWork() {
        Waits shard = new Waits();
        Waits other = new Waits();
        AtomicBoolean firstHasWork = new AtomicBoolean(true);
        var first = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, firstHasWork::get)));
        var second = executor("q", pool, other);
        first.execute(() -> {});
        second.execute(() -> {});
        clock.set(10);
        firstHasWork.set(false);
        clock.set(20);
        var third = executor("q", pool, shard);
        third.execute(() -> {});
        clock.set(30);
        while (pool.tasks.isEmpty() == false) {
            pool.runNext();
        }
        assertThat(shard.total(), equalTo(10L));
        assertThat(other.total(), equalTo(30L));
    }

    /**
     * A shared shard that gets work again after its executor ran out of it, which the engines do not do, is charged again from
     * where the executor last charged, rather than failing its tasks.
     */
    public void testSharedShardGettingWorkAgainIsChargedAgain() {
        Waits shard = new Waits();
        Waits other = new Waits();
        AtomicBoolean firstHasWork = new AtomicBoolean(true);
        var first = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, firstHasWork::get)));
        var second = executor("q", pool, other);
        first.execute(() -> {});
        first.execute(() -> {});
        second.execute(() -> {});
        clock.set(10);
        firstHasWork.set(false);
        pool.runNext();
        assertThat(shard.total(), equalTo(0L));
        clock.set(20);
        firstHasWork.set(true);
        clock.set(30);
        while (pool.tasks.isEmpty() == false) {
            pool.runNext();
        }
        assertThat(shard.total(), equalTo(20L));
        assertThat(other.total(), equalTo(30L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A shared shard an executor runs out of work for while it still has tasks queued is charged, once another executor
     * with work for it queues it, only from then, even if the first executor did not charge it in between.
     */
    public void testSharedShardOutOfWorkBeforeAChargeIsChargedFromWhenAnotherHasWork() {
        Waits shard = new Waits();
        AtomicBoolean firstHasWork = new AtomicBoolean(true);
        var first = scheduler.newExecutor(queryId("q"), pool, List.of(new QueryScheduler.Shard(shard, shard, firstHasWork::get)));
        var other = executor("q", pool);
        first.execute(() -> firstHasWork.set(false));
        first.execute(() -> {});
        other.execute(() -> {});
        clock.set(10);
        pool.runNext();
        assertThat(shard.total(), equalTo(10L));
        clock.set(20);
        var third = executor("q", pool, shard);
        third.execute(() -> {});
        clock.set(30);
        while (pool.tasks.isEmpty() == false) {
            pool.runNext();
        }
        assertThat(shard.total(), equalTo(20L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * A shard that tasks of several executors of the query work on accumulates the waiting time once.
     */
    public void testShardOfSeveralExecutorsAccumulatesOnce() {
        Waits shared = new Waits();
        Waits other = new Waits();
        var dataExecutor = executor("q", pool, shared, other);
        var fetchExecutor = executor("q", pool, shared);
        dataExecutor.execute(() -> {});
        fetchExecutor.execute(() -> {});
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
        var executor = executor("q", pool, waits);
        executor.execute(() -> {});
        clock.set(10);
        executor.execute(() -> {}, other);
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
        var executorA = executor("a", pool, a);
        var executorB = executor("b", pool, b);
        executorA.execute(() -> {
            clock.set(25);
            pool.runNext();
        });
        executorB.execute(() -> {});
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
        var executor = executor("q", rejecting, waits);
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
        var executor = executor("q", throwing);
        expectThrows(EsRejectedExecutionException.class, () -> executor.execute(() -> fail("must not run")));
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
        var executor = executor("q", throwing, waits);
        expectThrows(EsRejectedExecutionException.class, () -> executor.execute(ran::incrementAndGet));
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
        clock.incrementAndGet();
        pool.runNext();
        assertThat(clockReads.get(), equalTo(shardCount + 1));
        for (int i = 0; i < shardCount; i++) {
            assertThat(shards.get(i).values, contains((long) (shardCount - i)));
        }
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Random tasks of a few queries go through executors sharing shards: they queue, start in any order, run while others
     * start, get rejected, and complete. Executors join late, and a task can run out of work for shards of its executor,
     * like an ES|QL driver reading the last slice of a shard. A reference follows each step: while a query has tasks queued
     * and none running, each shard that an executor with tasks queued still has work for accumulates the time, once. The
     * shards never get ahead of it, and match it at the end. A round takes microseconds, so many run.
     */
    public void testAccumulatesTheWaitingOfAReference() {
        for (int round = 0; round < 1000; round++) {
            accumulatesTheWaitingOfAReference();
        }
    }

    private void accumulatesTheWaitingOfAReference() {
        List<Waits> shards = new ArrayList<>();
        for (int i = between(1, 5); i > 0; i--) {
            shards.add(new Waits());
        }
        Reference reference = new Reference(between(1, 3), shards);
        List<Runnable> queued = new ArrayList<>();
        int[] steps = { between(10, 300) };
        Runnable[] step = new Runnable[1];
        step[0] = () -> {
            while (steps[0]-- > 0) {
                switch (between(0, 6)) {
                    case 0 -> reference.newExecutor(randomNonEmptySubsetOf(shards), queued::add);
                    case 1 -> {
                        if (reference.executors.isEmpty() == false) {
                            reference.submit(randomFrom(reference.executors), () -> {
                                if (randomBoolean()) {
                                    step[0].run();
                                }
                            }, queued);
                        }
                    }
                    case 2 -> {
                        if (queued.isEmpty() == false) {
                            queued.remove(between(0, queued.size() - 1)).run();
                        }
                    }
                    case 3 -> {
                        if (queued.isEmpty() == false) {
                            reject(queued.remove(between(0, queued.size() - 1)));
                        }
                    }
                    case 4 -> reference.advance(between(0, 10));
                    case 5 -> {
                        return;
                    }
                    case 6 -> reference.assertAccumulatedAtMost();
                    default -> throw new AssertionError();
                }
            }
        };
        while (steps[0] > 0) {
            step[0].run();
        }
        while (queued.isEmpty() == false) {
            reference.advance(between(0, 10));
            Runnable task = queued.remove(between(0, queued.size() - 1));
            if (rarely()) {
                reject(task);
            } else {
                task.run();
            }
        }
        reference.assertAccumulated();
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Rejects a queued task like {@code EsThreadPoolExecutor} does.
     */
    private static void reject(Runnable task) {
        AbstractRunnable abstractRunnable = (AbstractRunnable) task;
        try {
            abstractRunnable.onRejection(new EsRejectedExecutionException("rejected"));
        } finally {
            abstractRunnable.onAfter();
        }
    }

    /**
     * Follows the tasks of a few queries and accumulates their waiting to each shard as time passes.
     */
    private class Reference {
        final int[] running;
        final List<ReferenceExecutor> executors = new ArrayList<>();
        final Map<Waits, Long> waited = new IdentityHashMap<>();

        Reference(int queries, List<Waits> shards) {
            running = new int[queries];
            shards.forEach(shard -> waited.put(shard, 0L));
        }

        void newExecutor(List<Waits> shards, Executor delegate) {
            int query = between(0, running.length - 1);
            List<AtomicBoolean> hasWork = new ArrayList<>();
            List<QueryScheduler.Shard> searched = new ArrayList<>();
            for (Waits shard : shards) {
                AtomicBoolean shardHasWork = new AtomicBoolean(rarely() == false);
                hasWork.add(shardHasWork);
                searched.add(new QueryScheduler.Shard(shard, shard, shardHasWork::get));
            }
            var executor = scheduler.newExecutor(queryId("q" + query), delegate, searched);
            executors.add(new ReferenceExecutor(query, shards, hasWork, executor));
        }

        /**
         * Submits a task that runs {@code body} and may run out of work for shards of its executor, before or after. The task
         * goes to the executor's pool, to another pool like a resumed driver, or to a pool that throws.
         */
        void submit(ReferenceExecutor executor, Runnable body, List<Runnable> queued) {
            AbstractRunnable task = task(executor, body);
            executor.queued++;
            switch (between(0, 9)) {
                case 0 -> {
                    expectThrows(EsRejectedExecutionException.class, () -> executor.executor.execute(task, r -> {
                        throw new EsRejectedExecutionException("rejected");
                    }));
                    executor.queued--;
                }
                case 1, 2, 3 -> executor.executor.execute(task, queued::add);
                default -> executor.executor.execute(task);
            }
        }

        private AbstractRunnable task(ReferenceExecutor executor, Runnable body) {
            return new AbstractRunnable() {
                @Override
                protected void doRun() {
                    executor.queued--;
                    running[executor.query]++;
                    try {
                        outOfWork();
                        body.run();
                        outOfWork();
                    } finally {
                        running[executor.query]--;
                    }
                }

                private void outOfWork() {
                    if (randomBoolean()) {
                        randomFrom(executor.hasWork).set(false);
                    }
                }

                @Override
                public void onRejection(Exception e) {
                    executor.queued--;
                }

                @Override
                public void onFailure(Exception e) {
                    throw new AssertionError(e);
                }
            };
        }

        void advance(long nanos) {
            for (int query = 0; query < running.length; query++) {
                boolean anyQueued = false;
                Set<Waits> queuedShards = Collections.newSetFromMap(new IdentityHashMap<>());
                for (ReferenceExecutor executor : executors) {
                    if (executor.query == query && executor.queued > 0) {
                        anyQueued = true;
                        for (int i = 0; i < executor.shards.size(); i++) {
                            if (executor.hasWork.get(i).get()) {
                                queuedShards.add(executor.shards.get(i));
                            }
                        }
                    }
                }
                if (running[query] == 0 && anyQueued) {
                    queuedShards.forEach(shard -> waited.merge(shard, nanos, Long::sum));
                }
            }
            clock.addAndGet(nanos);
        }

        void assertAccumulated() {
            waited.forEach((shard, nanos) -> assertThat(shard.total(), equalTo(nanos)));
        }

        /**
         * While tasks are queued, a shard is charged only up to when a task of its executor last left the queue.
         */
        void assertAccumulatedAtMost() {
            waited.forEach((shard, nanos) -> assertThat(shard.total(), lessThanOrEqualTo(nanos)));
        }
    }

    /**
     * An executor of one query as the reference follows it: its tasks queued, and whether it has work for each of its shards.
     */
    private static class ReferenceExecutor {
        final int query;
        final List<Waits> shards;
        final List<AtomicBoolean> hasWork;
        final QueryScheduler.QueryExecutor executor;
        int queued;

        ReferenceExecutor(int query, List<Waits> shards, List<AtomicBoolean> hasWork, QueryScheduler.QueryExecutor executor) {
            this.query = query;
            this.shards = shards;
            this.hasWork = hasWork;
            this.executor = executor;
        }
    }

    /**
     * A wrapped AbstractRunnable runs, fails, completes and forces its execution as it would unwrapped.
     */
    public void testAbstractRunnableBehavior() {
        List<String> calls = new ArrayList<>();
        boolean force = randomBoolean();
        var executor = executor("q", pool);
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
        assertThat(scheduler.queries(), equalTo(1));
        clock.set(15);
        pool.runNext();
        assertThat(waits.values, contains(5L));
        assertThat(scheduler.queries(), equalTo(0));
    }

    /**
     * Tasks of a few queries submitted from many threads, through executors the threads share or their own, all over the
     * same shards of each query. Nothing is left tracked, and no shard is charged more than the time it all took.
     */
    public void testConcurrentQueries() throws Exception {
        QueryScheduler concurrent = new QueryScheduler(System::nanoTime);
        int queries = between(1, 4);
        int submittersPerQuery = between(2, 6);
        int tasksPerSubmitter = between(1, 100);
        AtomicInteger ran = new AtomicInteger();
        Map<String, AtomicLong> waited = new ConcurrentHashMap<>();
        TestThreadPool threadPool = new TestThreadPool(getTestName());
        try {
            Executor search = threadPool.executor(ThreadPool.Names.SEARCH);
            List<Thread> threads = new ArrayList<>();
            CyclicBarrier start = new CyclicBarrier(queries * submittersPerQuery + 1);
            for (int q = 0; q < queries; q++) {
                TaskId queryId = new TaskId("coordinator", q);
                List<QueryScheduler.Shard> shards = new ArrayList<>();
                for (int s = between(1, 3); s > 0; s--) {
                    String shard = "q" + q + "s" + s;
                    AtomicLong shardWaited = waited.computeIfAbsent(shard, k -> new AtomicLong());
                    shards.add(new QueryScheduler.Shard(shard, (w, now) -> shardWaited.addAndGet(w), () -> true));
                }
                var sharedExecutor = concurrent.newExecutor(queryId, search, shards);
                for (int t = 0; t < submittersPerQuery; t++) {
                    threads.add(new Thread(() -> {
                        safeAwait(start);
                        for (int i = 0; i < tasksPerSubmitter; i++) {
                            var executor = randomBoolean()
                                ? sharedExecutor
                                : concurrent.newExecutor(queryId, search, randomNonEmptySubsetOf(shards));
                            executor.execute(ran::incrementAndGet);
                        }
                    }));
                }
            }
            threads.forEach(Thread::start);
            long startNanos = System.nanoTime();
            safeAwait(start);
            for (Thread thread : threads) {
                thread.join();
            }
            assertBusy(() -> {
                assertThat(ran.get(), equalTo(queries * submittersPerQuery * tasksPerSubmitter));
                assertThat(concurrent.queries(), equalTo(0));
            });
            long tookNanos = System.nanoTime() - startNanos;
            waited.values().forEach(w -> assertThat(w.get(), lessThanOrEqualTo(tookNanos)));
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
