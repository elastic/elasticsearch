/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.repositories.azure.executors;

import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ScalingExecutorBuilder;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.reactivestreams.Subscription;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class TrampolineSchedulerTests extends ESTestCase {

    private static final String POOL = "reactor";
    private static final String EVENT_LOOP = "event_loop";

    private ThreadPool threadPool;
    private TrampolineScheduler scheduler;

    @Before
    public void createScheduler() {
        threadPool = new TestThreadPool(
            getTestName(),
            new ScalingExecutorBuilder(POOL, 1, 8, TimeValue.timeValueSeconds(30), false),
            new ScalingExecutorBuilder(EVENT_LOOP, 1, 2, TimeValue.timeValueSeconds(30), false)
        );
        scheduler = new TrampolineScheduler(threadPool, POOL);
    }

    @After
    public void terminateThreadPool() {
        scheduler.dispose();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testWorkerRunsTasksOneAtATimeInSubmissionOrder() throws Exception {
        final Scheduler.Worker worker = scheduler.createWorker();
        final int submitters = 8;
        final int tasksPerSubmitter = 500;
        final AtomicBoolean running = new AtomicBoolean();
        final AtomicInteger overlaps = new AtomicInteger();
        final AtomicInteger offPoolRuns = new AtomicInteger();
        final ConcurrentHashMap<Integer, List<Integer>> orderBySubmitter = new ConcurrentHashMap<>();
        final CountDownLatch done = new CountDownLatch(submitters * tasksPerSubmitter);

        runInParallel(submitters, submitter -> {
            for (int i = 0; i < tasksPerSubmitter; i++) {
                final int task = i;
                worker.schedule(() -> {
                    if (running.compareAndSet(false, true) == false) {
                        overlaps.incrementAndGet();
                    }
                    if (Thread.currentThread().getName().contains("[" + POOL + "]") == false) {
                        offPoolRuns.incrementAndGet();
                    }
                    orderBySubmitter.computeIfAbsent(submitter, k -> new ArrayList<>()).add(task);
                    Thread.yield();
                    running.set(false);
                    done.countDown();
                });
            }
        });

        assertTrue(done.await(30, TimeUnit.SECONDS));
        assertThat("tasks of one worker ran concurrently", overlaps.get(), equalTo(0));
        assertThat("tasks ran outside the thread pool", offPoolRuns.get(), equalTo(0));
        for (int submitter = 0; submitter < submitters; submitter++) {
            final List<Integer> expected = new ArrayList<>();
            for (int i = 0; i < tasksPerSubmitter; i++) {
                expected.add(i);
            }
            assertThat("tasks of submitter " + submitter + " ran out of order", orderBySubmitter.get(submitter), equalTo(expected));
        }
    }

    public void testDelayedTaskRunsAfterTheDelayAndNotConcurrentlyWithRunningTask() throws Exception {
        final Scheduler.Worker worker = scheduler.createWorker();
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean blockerRunning = new AtomicBoolean();
        final AtomicBoolean delayedSawBlocker = new AtomicBoolean();
        final CountDownLatch delayedDone = new CountDownLatch(1);

        worker.schedule(() -> {
            blockerRunning.set(true);
            blockerStarted.countDown();
            safeAwait(release);
            blockerRunning.set(false);
        });
        assertTrue(blockerStarted.await(10, TimeUnit.SECONDS));

        final long scheduledAt = System.nanoTime();
        final long delayMillis = 20;
        worker.schedule(() -> {
            delayedSawBlocker.set(blockerRunning.get());
            delayedDone.countDown();
        }, delayMillis, TimeUnit.MILLISECONDS);

        // the delay elapses while the blocking task is running: the delayed task must wait for it
        assertFalse(delayedDone.await(delayMillis * 5, TimeUnit.MILLISECONDS));
        release.countDown();
        assertTrue(delayedDone.await(10, TimeUnit.SECONDS));
        assertFalse("the delayed task ran while another task of the worker was running", delayedSawBlocker.get());
        assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - scheduledAt), greaterThanOrEqualTo(delayMillis));
    }

    public void testDisposingATaskBeforeItRunsSkipsIt() throws Exception {
        final Scheduler.Worker worker = scheduler.createWorker();
        final CountDownLatch release = new CountDownLatch(1);
        final List<String> ran = new CopyOnWriteArrayList<>();
        final CountDownLatch lastDone = new CountDownLatch(1);

        worker.schedule(() -> safeAwait(release));
        final Disposable queued = worker.schedule(() -> ran.add("queued"));
        final Disposable delayed = worker.schedule(() -> ran.add("delayed"), 10, TimeUnit.MILLISECONDS);
        worker.schedule(() -> {
            ran.add("last");
            lastDone.countDown();
        });
        queued.dispose();
        delayed.dispose();
        assertTrue(queued.isDisposed());
        assertTrue(delayed.isDisposed());

        release.countDown();
        assertTrue(lastDone.await(10, TimeUnit.SECONDS));
        // give a wrongly surviving delayed task the chance to show up
        safeSleep(50);
        assertThat(ran, contains("last"));
    }

    public void testDisposingTheWorkerDropsPendingTasksAndRejectsNewOnes() throws Exception {
        final Scheduler.Worker worker = scheduler.createWorker();
        final CountDownLatch blockerStarted = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final CountDownLatch blockerDone = new CountDownLatch(1);
        final List<String> ran = new CopyOnWriteArrayList<>();

        worker.schedule(() -> {
            blockerStarted.countDown();
            safeAwait(release);
            blockerDone.countDown();
        });
        assertTrue(blockerStarted.await(10, TimeUnit.SECONDS));
        final List<Disposable> pending = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int task = i;
            pending.add(worker.schedule(() -> ran.add("queued-" + task)));
        }
        pending.add(worker.schedule(() -> ran.add("delayed"), 10, TimeUnit.MILLISECONDS));
        pending.add(worker.schedulePeriodically(() -> ran.add("periodic"), 10, 10, TimeUnit.MILLISECONDS));

        worker.dispose();
        assertTrue(worker.isDisposed());
        for (Disposable disposable : pending) {
            assertTrue(disposable.isDisposed());
        }
        expectThrows(RejectedExecutionException.class, () -> worker.schedule(() -> ran.add("after dispose")));
        expectThrows(RejectedExecutionException.class, () -> worker.schedule(() -> ran.add("after dispose"), 1, TimeUnit.MILLISECONDS));
        expectThrows(
            RejectedExecutionException.class,
            () -> worker.schedulePeriodically(() -> ran.add("after dispose"), 1, 1, TimeUnit.MILLISECONDS)
        );

        release.countDown();
        assertTrue(blockerDone.await(10, TimeUnit.SECONDS));
        safeSleep(50);
        assertThat(ran, empty());
    }

    public void testPeriodicTaskRunsUntilDisposed() throws Exception {
        final Scheduler.Worker worker = scheduler.createWorker();
        final AtomicInteger ticks = new AtomicInteger();
        final CountDownLatch threeTicks = new CountDownLatch(3);
        final AtomicBoolean running = new AtomicBoolean();
        final AtomicInteger overlaps = new AtomicInteger();

        final Disposable periodic = worker.schedulePeriodically(() -> {
            if (running.compareAndSet(false, true) == false) {
                overlaps.incrementAndGet();
            }
            ticks.incrementAndGet();
            threeTicks.countDown();
            running.set(false);
        }, 0, 5, TimeUnit.MILLISECONDS);
        // an immediate task interleaves with the ticks and must not run concurrently with them
        for (int i = 0; i < 20; i++) {
            worker.schedule(() -> {
                if (running.compareAndSet(false, true) == false) {
                    overlaps.incrementAndGet();
                }
                Thread.yield();
                running.set(false);
            });
        }

        assertTrue(threeTicks.await(10, TimeUnit.SECONDS));
        periodic.dispose();
        assertTrue(periodic.isDisposed());
        safeSleep(50);
        final int ticksAfterDispose = ticks.get();
        safeSleep(50);
        assertThat("periodic task kept running after dispose", ticks.get(), equalTo(ticksAfterDispose));
        assertThat(overlaps.get(), equalTo(0));
    }

    public void testDirectSchedulingRunsOnThePool() throws Exception {
        final CountDownLatch done = new CountDownLatch(2);
        final AtomicInteger offPoolRuns = new AtomicInteger();
        final Runnable task = () -> {
            if (Thread.currentThread().getName().contains("[" + POOL + "]") == false) {
                offPoolRuns.incrementAndGet();
            }
            done.countDown();
        };
        scheduler.schedule(task);
        scheduler.schedule(task, 10, TimeUnit.MILLISECONDS);
        assertTrue(done.await(10, TimeUnit.SECONDS));
        assertThat(offPoolRuns.get(), equalTo(0));
    }

    /**
     * The scenario that motivated this scheduler: a {@code concatMap} over blocking callables, subscribed on the scheduler and consumed
     * like reactor-netty consumes a request body (128 requested up front, then 64 more from the event loop every 64 items). With workers
     * that may run two tasks of one subscription concurrently, {@code concatMap} emits items twice and drops others; with this scheduler
     * the refill requests are serialized behind the running production.
     */
    public void testConcatMapUnderConcurrentDemandDeliversEveryItemOnceInOrder() throws Exception {
        final int items = 2000;
        final int subscriptions = 500;
        final int concurrentSubscriptions = 8;
        final Executor eventLoop = threadPool.executor(EVENT_LOOP);
        final List<String> problems = new ArrayList<>();
        for (int first = 0; first < subscriptions; first += concurrentSubscriptions) {
            final CountDownLatch done = new CountDownLatch(concurrentSubscriptions);
            final List<RecordingSubscriber> subscribers = new ArrayList<>();
            for (int i = 0; i < concurrentSubscriptions; i++) {
                final RecordingSubscriber subscriber = new RecordingSubscriber(first + i, eventLoop, done);
                subscribers.add(subscriber);
                Flux.range(0, items)
                    .map(i2 -> i2)
                    .concatMap(pos -> Mono.fromCallable(() -> pos))
                    .subscribeOn(scheduler)
                    .subscribe(subscriber);
            }
            assertTrue(done.await(60, TimeUnit.SECONDS));
            for (RecordingSubscriber subscriber : subscribers) {
                subscriber.verify(items, problems);
            }
        }
        assertThat(String.join("\n", problems), problems, hasSize(0));
    }

    private static final class RecordingSubscriber extends BaseSubscriber<Integer> {
        private final int id;
        private final Executor eventLoop;
        private final CountDownLatch done;
        private final List<Integer> received = new CopyOnWriteArrayList<>();
        private volatile Throwable error;
        private int sinceRefill;

        RecordingSubscriber(int id, Executor eventLoop, CountDownLatch done) {
            this.id = id;
            this.eventLoop = eventLoop;
            this.done = done;
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            request(128);
        }

        @Override
        protected void hookOnNext(Integer value) {
            received.add(value);
            if (++sinceRefill == 64) {
                sinceRefill = 0;
                eventLoop.execute(() -> request(64));
            }
        }

        @Override
        protected void hookOnComplete() {
            done.countDown();
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
            done.countDown();
        }

        void verify(int items, List<String> problems) {
            if (error != null) {
                problems.add("subscription " + id + " failed: " + error);
                return;
            }
            final List<String> wrong = new ArrayList<>();
            for (int i = 0; i < received.size() && wrong.size() < 3; i++) {
                if (received.get(i) != i) {
                    wrong.add("index " + i + " carries " + received.get(i));
                }
            }
            if (received.size() != items || wrong.isEmpty() == false) {
                problems.add("subscription " + id + " received " + received.size() + " of " + items + " items; " + wrong);
            }
        }
    }
}
