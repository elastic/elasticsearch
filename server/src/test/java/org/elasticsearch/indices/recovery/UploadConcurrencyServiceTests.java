/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.UploadConcurrencyService.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.UploadConcurrencyService.UPLOAD_CONCURRENCY_MAX_SETTING;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class UploadConcurrencyServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    // what to close when the test is done, as the repositories close their task runners' registrations
    private final List<Releasable> registrations = new ArrayList<>();

    @Before
    public void createThreadPool() {
        // the upload pool is only larger than the snapshot pool on nodes with enough heap, which the tests cannot rely on
        final int snapshotMax = randomIntBetween(1, 4);
        threadPool = newThreadPool(
            getTestName(),
            Settings.builder()
                .put("thread_pool.snapshot.core", 1)
                .put("thread_pool.snapshot.max", snapshotMax)
                .put("thread_pool.snapshot_upload.core", 1)
                .put("thread_pool.snapshot_upload.max", snapshotMax + randomIntBetween(3, 10))
                .build()
        );
    }

    /** A thread pool with the pool for uploads, which the stateless plugin adds to the pools of its nodes. */
    private static ThreadPool newThreadPool(String name, Settings settings) {
        return new TestThreadPool(name, settings, ThreadPool.snapshotUploadExecutorBuilder(settings));
    }

    /** The settings of a node that has upload concurrency, which the stateless plugin registers. */
    private static ClusterSettings newClusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Sets.union(
                ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                Set.of(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING, UPLOAD_CONCURRENCY_MAX_SETTING)
            )
        );
    }

    @After
    public void stopThreadPool() {
        Releasables.close(registrations);
        terminate(threadPool);
    }

    /** A thread pool that remembers the periodic tasks that were scheduled on it. */
    private static class RecordingThreadPool extends TestThreadPool {
        final List<Scheduler.Cancellable> scheduled = new CopyOnWriteArrayList<>();
        final List<TimeValue> intervals = new CopyOnWriteArrayList<>();

        RecordingThreadPool(String name) {
            super(name, ThreadPool.snapshotUploadExecutorBuilder(Settings.EMPTY));
        }

        @Override
        public Scheduler.Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, Executor executor) {
            final Scheduler.Cancellable cancellable = super.scheduleWithFixedDelay(command, interval, executor);
            scheduled.add(cancellable);
            intervals.add(interval);
            return cancellable;
        }
    }

    /** The measurements of a node, which the tests set, and how often they are read. */
    private static class FakeProbes {
        final AtomicReference<CgroupV2Probe.CpuPressure> cpuPressure = new AtomicReference<>(new CgroupV2Probe.CpuPressure(0L));
        final AtomicReference<CgroupV2Probe.CpuThrottling> cpuThrottling = new AtomicReference<>(new CgroupV2Probe.CpuThrottling(0L, 0L));
        final AtomicInteger reads = new AtomicInteger();

        UploadConcurrencyService.Probes probes() {
            return new UploadConcurrencyService.Probes(() -> {
                reads.incrementAndGet();
                return cpuPressure.get();
            }, () -> {
                reads.incrementAndGet();
                return cpuThrottling.get();
            });
        }
    }

    /** A node with the given settings, whose clock the test moves. */
    private class TestNode {
        final ClusterSettings clusterSettings;
        final FakeProbes probes = new FakeProbes();
        final AtomicLong nanoTime = new AtomicLong(randomLong());
        final UploadConcurrencyService service;

        TestNode(Settings settings, ThreadPool pool) {
            clusterSettings = newClusterSettings(settings);
            service = new UploadConcurrencyService(clusterSettings, pool, probes.probes(), nanoTime::get);
        }

        TestNode(Settings settings) {
            this(settings, threadPool);
        }

        /** Runs one tick, which is one interval of the controller. */
        void tick() {
            nanoTime.addAndGet(UploadConcurrencyService.TICK_INTERVAL.nanos());
            service.tick();
        }

        void apply(Settings settings) {
            clusterSettings.applySettings(settings);
        }
    }

    public void testNothingIsReadWhileAdaptiveIsOff() {
        final TestNode node = new TestNode(Settings.EMPTY);
        for (int i = 0; i < 4; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), equalTo(0));

        // on: the measurements are read
        node.apply(adaptive(true));
        for (int i = 0; i < 2; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), greaterThan(0));

        // off again: no more reads
        node.apply(adaptive(false));
        node.tick();
        final int reads = node.probes.reads.get();
        for (int i = 0; i < 4; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), equalTo(reads));
    }

    public void testEachMeasurementIsReadOnceAnInterval() {
        final TestNode node = new TestNode(Settings.EMPTY);
        final AtomicInteger pressureReads = new AtomicInteger();
        final AtomicInteger throttlingReads = new AtomicInteger();
        final var service = new UploadConcurrencyService(node.clusterSettings, threadPool, new UploadConcurrencyService.Probes(() -> {
            pressureReads.incrementAndGet();
            return null;
        }, () -> {
            throttlingReads.incrementAndGet();
            return null;
        }), node.nanoTime::get);
        node.apply(adaptive(true));
        final int ticks = between(3, 10);
        for (int i = 0; i < ticks; i++) {
            node.nanoTime.addAndGet(UploadConcurrencyService.TICK_INTERVAL.nanos());
            service.tick();
        }
        // the startup line reads each once, and so does every interval
        assertThat(pressureReads.get(), equalTo(1 + ticks));
        assertThat(throttlingReads.get(), equalTo(1 + ticks));
    }

    /** A task that holds its slot until the test lets it go. */
    private static class BlockingTask extends AbstractRunnable implements Comparable<BlockingTask> {
        private static final AtomicLong SEQUENCE = new AtomicLong();

        private final long order = SEQUENCE.incrementAndGet();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        @Override
        protected void doRun() {
            started.countDown();
            safeAwait(release);
        }

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError(e);
        }

        @Override
        public int compareTo(BlockingTask other) {
            return Long.compare(order, other.order);
        }
    }

    /** A task runner for a repository that is registered with the node, as the one of a repository is. */
    private PrioritizedThrottledTaskRunner<BlockingTask> newRepositoryRunner(TestNode node) {
        final var runner = new PrioritizedThrottledTaskRunner<BlockingTask>(
            "test",
            threadPool.info(ThreadPool.Names.SNAPSHOT).getMax(),
            node.service.getUploadExecutor(),
            node.service::tryAcquireUploadPermit
        );
        registrations.add(node.service.registerUploadTaskRunner(runner));
        return runner;
    }

    private static List<BlockingTask> enqueueBlockingTasks(PrioritizedThrottledTaskRunner<BlockingTask> runner, int count) {
        final List<BlockingTask> tasks = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            final var task = new BlockingTask();
            tasks.add(task);
            runner.enqueueTask(task);
        }
        return tasks;
    }

    private static void awaitStarted(List<BlockingTask> tasks) {
        tasks.forEach(task -> safeAwait(task.started));
    }

    private static void releaseAll(List<BlockingTask> tasks) {
        tasks.forEach(task -> task.release.countDown());
    }

    private static Settings adaptive(boolean on) {
        return Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), on).build();
    }

    public void testUploadExecutorFollowsTheSwitch() throws Exception {
        final TestNode node = new TestNode(Settings.EMPTY);
        for (boolean on : new boolean[] { false, true, false }) {
            node.apply(adaptive(on));
            final var threadName = new CompletableFuture<String>();
            node.service.getUploadExecutor().execute(() -> threadName.complete(Thread.currentThread().getName()));
            assertThat(threadName.get(), containsString(on ? "[snapshot_upload]" : "[snapshot]"));
        }
    }

    public void testUploadConcurrencyBounds() {
        final TestNode node = new TestNode(Settings.EMPTY);
        // today's concurrency is the SNAPSHOT pool's, and the upload pool never has less
        final int snapshotMax = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        assertThat(node.service.getUploadBudget(), equalTo(snapshotMax));
        assertThat(threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax(), greaterThan(snapshotMax - 1));
    }

    public void testPermitsAreOnlyCountedWhileAdaptiveIsOff() {
        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final List<Releasable> permits = new ArrayList<>();
        // one asker, as the runner of a repository is
        final Runnable retry = () -> {};
        // not consulted: all are granted, however many, and counted
        for (int i = 0; i < floor + 3; i++) {
            permits.add(node.service.tryAcquireUploadPermit(retry));
            assertNotNull(permits.get(i));
        }
        assertThat(node.service.getRunningUploadTasks(), equalTo(floor + 3));

        // switched on while they run: they count against the budget, which starts at today's concurrency, so no more are granted
        node.apply(adaptive(true));
        assertThat(node.service.getUploadBudget(), equalTo(floor));
        assertNull(node.service.tryAcquireUploadPermit(retry));
        Releasables.close(permits.remove(0));
        Releasables.close(permits.remove(0));
        Releasables.close(permits.remove(0));
        assertThat(node.service.getRunningUploadTasks(), equalTo(floor));
        assertNull(node.service.tryAcquireUploadPermit(retry));
        // one more is only granted when one is given back, and then up to the budget
        Releasables.close(permits.remove(0));
        final Releasable granted = node.service.tryAcquireUploadPermit(retry);
        assertNotNull(granted);
        assertNull(node.service.tryAcquireUploadPermit(retry));
        permits.add(granted);

        // switched off again: not consulted, and what runs is still counted
        node.apply(adaptive(false));
        permits.add(node.service.tryAcquireUploadPermit(retry));
        assertNotNull(permits.get(permits.size() - 1));
        assertThat(node.service.getRunningUploadTasks(), equalTo(permits.size()));
        permits.forEach(Releasables::close);
        assertThat(node.service.getRunningUploadTasks(), equalTo(0));
    }

    public void testRepositoryRunnersFollowTheSwitchAndTheCeiling() {
        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int nodeCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        final var runner = newRepositoryRunner(node);
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));

        // on: the budget is what limits the tasks, so a runner may run as many as the node's ceiling
        node.apply(adaptive(true));
        assertThat(runner.getMaxRunningTasks(), equalTo(node.service.getUploadConcurrencyCeiling()));
        // a runner that registers while it is on gets that too
        final var late = newRepositoryRunner(node);
        assertThat(late.getMaxRunningTasks(), equalTo(node.service.getUploadConcurrencyCeiling()));

        // a new setting is picked up at the next interval
        final int max = randomIntBetween(1, nodeCeiling + 5);
        node.apply(
            Settings.builder()
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                .build()
        );
        node.tick();
        assertThat(runner.getMaxRunningTasks(), equalTo(Math.max(floor, Math.min(max, nodeCeiling))));
        assertThat(late.getMaxRunningTasks(), equalTo(runner.getMaxRunningTasks()));

        // off: today's concurrency, whatever the ceiling was
        node.apply(
            Settings.builder()
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), false)
                .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                .build()
        );
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));
        assertThat(late.getMaxRunningTasks(), equalTo(floor));

        // the setting changed while off, and nothing ticks then: on starts from the new value
        final int other = randomIntBetween(1, nodeCeiling + 5);
        node.apply(
            Settings.builder()
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), other)
                .build()
        );
        assertThat(runner.getMaxRunningTasks(), equalTo(Math.max(floor, Math.min(other, nodeCeiling))));
    }

    public void testAnUnregisteredRunnerIsLeftAlone() {
        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var runner = new PrioritizedThrottledTaskRunner<BlockingTask>(
            "test",
            floor,
            node.service.getUploadExecutor(),
            node.service::tryAcquireUploadPermit
        );
        node.service.registerUploadTaskRunner(runner).close();
        node.apply(adaptive(true));
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));
    }

    public void testBudgetGivenBackByOneRepositoryStartsATaskOfAnother() {
        final TestNode node = new TestNode(Settings.EMPTY);
        node.apply(adaptive(true));
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var repositoryA = newRepositoryRunner(node);
        final var repositoryB = newRepositoryRunner(node);

        // one repository uses all of the budget, so a task of the other one waits, in its own queue
        final var runningInA = enqueueBlockingTasks(repositoryA, floor);
        awaitStarted(runningInA);
        final var waitingInB = enqueueBlockingTasks(repositoryB, 1).get(0);
        assertThat(repositoryB.runningTasks(), equalTo(0));
        assertThat(repositoryB.queueSize(), equalTo(1));
        assertThat(repositoryA.queueSize(), equalTo(0));

        // when a task of the first one finishes, the budget goes to the second, which has nothing to do with the finished task
        runningInA.get(0).release.countDown();
        safeAwait(waitingInB.started);
        assertThat(node.service.getRunningUploadTasks(), lessThanOrEqualTo(floor));

        waitingInB.release.countDown();
        releaseAll(runningInA);
    }

    public void testSwitchingAdaptiveOffStartsTheTasksWaitingForBudget() {
        final TestNode node = new TestNode(Settings.EMPTY);
        node.apply(adaptive(true));
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var repositoryA = newRepositoryRunner(node);
        final var repositoryB = newRepositoryRunner(node);
        final var runningInA = enqueueBlockingTasks(repositoryA, floor);
        awaitStarted(runningInA);
        final var waitingInB = enqueueBlockingTasks(repositoryB, 1).get(0);
        assertThat(repositoryB.queueSize(), equalTo(1));

        // nothing finished and the budget did not change, but it is not consulted any more
        node.apply(adaptive(false));
        safeAwait(waitingInB.started);

        waitingInB.release.countDown();
        releaseAll(runningInA);
    }

    public void testBudgetRisesWithTheController() {
        final TestNode node = new TestNode(Settings.EMPTY);
        node.apply(adaptive(true));
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        assertThat(node.service.getUploadConcurrencyCeiling(), greaterThan(floor));
        final var repositoryA = newRepositoryRunner(node);
        final var repositoryB = newRepositoryRunner(node);
        final var runningInA = enqueueBlockingTasks(repositoryA, floor);
        awaitStarted(runningInA);
        final var waitingInB = enqueueBlockingTasks(repositoryB, 1).get(0);
        assertThat(repositoryB.queueSize(), equalTo(1));

        // work is queued and nothing holds the uploads back: the budget goes up by one, which the waiting task takes
        for (int i = 0; i < 2; i++) {
            node.tick();
        }
        assertThat(node.service.getUploadBudget(), equalTo(floor + 1));
        safeAwait(waitingInB.started);
        assertThat(node.service.getRunningUploadTasks(), equalTo(floor + 1));

        waitingInB.release.countDown();
        releaseAll(runningInA);
    }

    /** A task that only counts that it was rejected, as at shutdown. */
    private static class RejectedTask extends AbstractRunnable implements Comparable<RejectedTask> {
        private static final AtomicLong SEQUENCE = new AtomicLong();

        private final long order = SEQUENCE.incrementAndGet();
        private final CountDownLatch rejected;

        RejectedTask(CountDownLatch rejected) {
            this.rejected = rejected;
        }

        @Override
        protected void doRun() {
            throw new AssertionError("the executor rejects everything");
        }

        @Override
        public void onRejection(Exception e) {
            rejected.countDown();
        }

        @Override
        public void onFailure(Exception e) {
            throw new AssertionError(e);
        }

        @Override
        public int compareTo(RejectedTask other) {
            return Long.compare(order, other.order);
        }
    }

    public void testGivingBackBudgetOfRejectedTasksDoesNotNestTheRetriesOfTheWaitingRunners() {
        final TestNode node = new TestNode(Settings.EMPTY);
        node.apply(adaptive(true));
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int tasksPerRunner = 5_000;
        final var rejected = new CountDownLatch(2 * tasksPerRunner);
        // an executor that has shut down
        final Executor rejecting = command -> ((AbstractRunnable) command).onRejection(new EsRejectedExecutionException("shut down"));
        final var runners = new ArrayList<PrioritizedThrottledTaskRunner<RejectedTask>>();
        for (int i = 0; i < 2; i++) {
            final var runner = new PrioritizedThrottledTaskRunner<RejectedTask>(
                "test",
                floor,
                rejecting,
                node.service::tryAcquireUploadPermit
            );
            registrations.add(node.service.registerUploadTaskRunner(runner));
            runners.add(runner);
        }

        // the budget is used up, so that the tasks of both runners queue, and then given back to them task by task
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < floor; i++) {
            held.add(node.service.tryAcquireUploadPermit(() -> {}));
        }
        for (var runner : runners) {
            for (int i = 0; i < tasksPerRunner; i++) {
                runner.enqueueTask(new RejectedTask(rejected));
            }
        }
        assertThat(runners.get(0).queueSize() + runners.get(1).queueSize(), equalTo(2 * tasksPerRunner));
        held.forEach(Releasable::close);

        // each rejected task gives its budget back, which is handed on without nesting: this does not overflow the stack
        safeAwait(rejected);
        assertThat(runners.get(0).queueSize() + runners.get(1).queueSize(), equalTo(0));
    }

    public void testUploadErrorsAreOnlyCountedWhileAdaptiveIsOn() {
        final TestNode node = new TestNode(Settings.EMPTY);
        node.service.onUploadReadError();
        node.service.onUploadWriteError();
        assertThat(node.service.getUploadReadErrors(), equalTo(0L));
        assertThat(node.service.getUploadWriteErrors(), equalTo(0L));

        node.apply(adaptive(true));
        node.service.onUploadReadError();
        node.service.onUploadWriteError();
        node.service.onUploadWriteError();
        assertThat(node.service.getUploadReadErrors(), equalTo(1L));
        assertThat(node.service.getUploadWriteErrors(), equalTo(2L));

        node.apply(adaptive(false));
        node.service.onUploadReadError();
        assertThat(node.service.getUploadReadErrors(), equalTo(1L));
    }

    public void testTickIsOnlyScheduledWhileAdaptiveIsOn() {
        final var recordingThreadPool = new RecordingThreadPool(getTestName() + "-recording");
        try {
            final TestNode node = new TestNode(Settings.EMPTY, recordingThreadPool);
            node.service.start();
            // off: nothing is scheduled
            assertThat(recordingThreadPool.scheduled, hasSize(0));

            node.apply(adaptive(true));
            assertThat(recordingThreadPool.scheduled, hasSize(1));
            assertThat(recordingThreadPool.intervals, equalTo(List.of(TimeValue.timeValueSeconds(5))));
            assertFalse(recordingThreadPool.scheduled.get(0).isCancelled());
            // setting it again does not add a second tick
            node.apply(adaptive(true));
            assertThat(recordingThreadPool.scheduled, hasSize(1));

            // off: cancelled
            node.apply(adaptive(false));
            assertTrue(recordingThreadPool.scheduled.get(0).isCancelled());
            assertThat(recordingThreadPool.scheduled, hasSize(1));

            // on again: a new one, until the node stops
            node.apply(adaptive(true));
            assertThat(recordingThreadPool.scheduled, hasSize(2));
            assertFalse(recordingThreadPool.scheduled.get(1).isCancelled());
            node.service.stop();
            assertTrue(recordingThreadPool.scheduled.get(1).isCancelled());
            node.apply(adaptive(false));
            node.apply(adaptive(true));
            assertThat(recordingThreadPool.scheduled, hasSize(2));
        } finally {
            terminate(recordingThreadPool);
        }
    }

    public void testAdaptiveIsOnBeforeTheNodeStarts() {
        final var recordingThreadPool = new RecordingThreadPool(getTestName() + "-recording");
        try {
            final TestNode node = new TestNode(adaptive(true), recordingThreadPool);
            // nothing is scheduled by a node that has not started
            assertThat(recordingThreadPool.scheduled, hasSize(0));
            node.service.start();
            assertThat(recordingThreadPool.scheduled, hasSize(1));
        } finally {
            terminate(recordingThreadPool);
        }
    }

    public void testSwitchingAdaptiveOnAndOffWhileTicking() throws Exception {
        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var runner = newRepositoryRunner(node);
        final int rounds = 500;
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread ticker = new Thread(() -> {
            safeAwait(barrier);
            for (int i = 0; i < rounds; i++) {
                node.tick();
            }
        });
        ticker.start();
        safeAwait(barrier);
        for (int i = 0; i < rounds; i++) {
            node.apply(adaptive(i % 2 == 0));
        }
        ticker.join();
        node.apply(adaptive(false));
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));
        assertThat(node.service.getUploadBudget(), equalTo(floor));
    }

    private static void assertStatusLogged(TestNode node, boolean logged) {
        assertLogged(node, "status line", Level.DEBUG, "snapshot upload concurrency; *", logged);
    }

    private static void assertLogged(TestNode node, String name, Level level, String message, boolean logged) {
        assertTickLogs(node, expectation(name, level, message, logged));
    }

    private static MockLog.LoggingExpectation expectation(String name, Level level, String message, boolean logged) {
        final String logger = UploadConcurrencyService.class.getCanonicalName();
        return logged
            ? new MockLog.SeenEventExpectation(name, logger, level, message)
            : new MockLog.UnseenEventExpectation(name, logger, level, message);
    }

    private static void assertTickLogs(TestNode node, MockLog.LoggingExpectation... expectations) {
        try (var mockLog = MockLog.capture(UploadConcurrencyService.class)) {
            for (var expectation : expectations) {
                mockLog.addExpectation(expectation);
            }
            node.tick();
            mockLog.assertAllExpectationsMatched();
        }
    }

    @TestLogging(value = "org.elasticsearch.indices.recovery.UploadConcurrencyService:DEBUG", reason = "the status is logged at debug")
    public void testStatusIsLoggedWhileUploadsRunOrAreQueuedAndForThirtySecondsAfter() throws Exception {
        final TestNode node = new TestNode(adaptive(true));
        final var runner = newRepositoryRunner(node);
        // nothing has run yet
        assertStatusLogged(node, false);
        assertStatusLogged(node, false);

        // some run and one waits for the budget
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var tasks = enqueueBlockingTasks(runner, floor + 1);
        awaitStarted(tasks.subList(0, floor));
        assertStatusLogged(node, true);

        releaseAll(tasks);
        assertBusy(() -> {
            assertThat(node.service.getRunningUploadTasks(), equalTo(0));
            assertThat(runner.queueSize(), equalTo(0));
        });
        // nothing runs now, but it did less than 30 seconds ago
        for (int seconds = 5; seconds <= 30; seconds += 5) {
            assertStatusLogged(node, true);
        }
        assertStatusLogged(node, false);
    }

    @TestLogging(value = "org.elasticsearch.indices.recovery.UploadConcurrencyService:DEBUG", reason = "the status is logged at debug")
    public void testARaiseIsLoggedAtDebugAndACutAtInfo() throws Exception {
        final TestNode node = new TestNode(adaptive(true));
        final var runner = newRepositoryRunner(node);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var tasks = enqueueBlockingTasks(runner, floor);
        awaitStarted(tasks);
        final String status = "snapshot upload concurrency; *";
        final String change = "changing snapshot upload concurrency from *";

        // the first interval has nothing to compare with, and the status is not at info
        assertLogged(node, "no status at info", Level.INFO, status, false);

        // quiet with work queued: up by one, with the reason, at debug
        final var waiting = enqueueBlockingTasks(runner, 1);
        final String raise = "changing snapshot upload concurrency from [" + floor + "] to [" + (floor + 1) + "] (raise: *";
        assertTickLogs(
            node,
            expectation("raise at debug", Level.DEBUG, raise, true),
            expectation("no raise at info", Level.INFO, change, false)
        );
        awaitStarted(waiting);

        // some pressure, but not enough to cut: held, which is not a change
        node.probes.cpuPressure.set(new CgroupV2Probe.CpuPressure(150_000L));
        assertLogged(node, "hold", Level.INFO, change, false);

        // more pressure: down, at info
        node.probes.cpuPressure.set(new CgroupV2Probe.CpuPressure(1_150_000L));
        final String cut = "changing snapshot upload concurrency from [" + (floor + 1) + "] to * (cut: *";
        assertLogged(node, "cut at info", Level.INFO, cut, true);

        releaseAll(tasks);
        releaseAll(waiting);
    }

    @TestLogging(value = "org.elasticsearch.indices.recovery.UploadConcurrencyService:INFO", reason = "the level a node runs at")
    public void testAtInfoOnlyACutIsLogged() throws Exception {
        final TestNode node = new TestNode(adaptive(true));
        final var runner = newRepositoryRunner(node);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var tasks = enqueueBlockingTasks(runner, floor);
        awaitStarted(tasks);
        node.tick();

        // a raise, a hold and the status are not logged
        final var waiting = enqueueBlockingTasks(runner, 1);
        assertTickLogs(
            node,
            expectation("no raise", Level.INFO, "changing snapshot upload concurrency from *", false),
            expectation("no status", Level.INFO, "snapshot upload concurrency; *", false)
        );
        assertThat(node.service.getUploadBudget(), equalTo(floor + 1));
        awaitStarted(waiting);

        // a cut is
        node.probes.cpuPressure.set(new CgroupV2Probe.CpuPressure(1_000_000L));
        assertLogged(node, "cut", Level.INFO, "changing snapshot upload concurrency from * (cut: *", true);

        releaseAll(tasks);
        releaseAll(waiting);
    }

    @TestLogging(value = "org.elasticsearch.indices.recovery.UploadConcurrencyService:INFO", reason = "the level a node runs at")
    public void testErrorsSinceTheLastStatusAreCountedWhetherOrNotItIsLogged() throws Exception {
        final TestNode node = new TestNode(adaptive(true));
        final var runner = newRepositoryRunner(node);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final var tasks = enqueueBlockingTasks(runner, floor);
        awaitStarted(tasks);
        node.tick();

        // errors in intervals of the active window, while the status is not logged as debug is off
        node.service.onUploadReadError();
        node.service.onUploadReadError();
        node.service.onUploadWriteError();
        node.tick();
        node.tick();

        // an operator turns debug on: the status says what happened since the last one, which is nothing
        final var serviceLogger = LogManager.getLogger(UploadConcurrencyService.class);
        final Level saved = serviceLogger.getLevel();
        Loggers.setLevel(serviceLogger, Level.DEBUG);
        try {
            assertLogged(
                node,
                "no errors",
                Level.DEBUG,
                "snapshot upload concurrency; *upload errors since last log read [0] upload [0]; *",
                true
            );
            node.service.onUploadReadError();
            assertLogged(
                node,
                "the new error",
                Level.DEBUG,
                "snapshot upload concurrency; *upload errors since last log read [1] upload [0]; *",
                true
            );
        } finally {
            Loggers.setLevel(serviceLogger, saved);
            releaseAll(tasks);
        }
    }

    public void testARunnerRegisteredWhileTheCapChangesGetsTheNewCap() throws Exception {
        final TestNode node = new TestNode(adaptive(true));
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();

        // a runner that is slow to be given the cap that registering read, as when a change of the cap happens in between
        final var entered = new CountDownLatch(1);
        final var proceed = new CountDownLatch(1);
        final var slow = new AtomicBoolean(true);
        final var runner = new PrioritizedThrottledTaskRunner<BlockingTask>("test", floor, node.service.getUploadExecutor()) {
            @Override
            public void setMaxRunningTasks(int maxRunningTasks) {
                if (slow.getAndSet(false)) {
                    entered.countDown();
                    safeAwait(proceed);
                }
                super.setMaxRunningTasks(maxRunningTasks);
            }
        };
        final Releasable[] registration = new Releasable[1];
        final Thread registering = new Thread(() -> registration[0] = node.service.registerUploadTaskRunner(runner));
        registering.start();
        safeAwait(entered);

        // the cap is lowered by the tick, which has to wait for the registration if it is to apply the cap to the runner too
        final Thread ticking = new Thread(() -> {
            node.apply(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), floor)
                    .build()
            );
            node.tick();
        });
        ticking.start();
        assertBusy(() -> assertTrue(ticking.getState() == Thread.State.BLOCKED || ticking.getState() == Thread.State.TERMINATED));

        proceed.countDown();
        registering.join();
        ticking.join();
        registrations.add(registration[0]);
        assertThat(node.service.getUploadConcurrencyCeiling(), equalTo(floor));
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));
    }

    public void testUploadConcurrencyMaxSetting() {
        assertThat(UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.EMPTY), equalTo(40));
        expectThrows(
            IllegalArgumentException.class,
            () -> UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.builder().put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), 0).build())
        );

        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int nodeCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        // the ceiling is the lower of the setting and what the node's size allows, never below today's concurrency
        assertThat(node.service.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(40, nodeCeiling))));
        node.apply(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build());
        for (int max : new int[] { 1, floor, nodeCeiling, nodeCeiling + 50, randomIntBetween(1, 200) }) {
            node.apply(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                    .build()
            );
            node.tick();
            assertThat(node.service.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(max, nodeCeiling))));
        }
    }

    public void testDefaultSettingDoesNotClipTheLargestNodes() {
        // a node with the target of a 64GiB node: the pool allows 40, which the default of the setting must not cut
        final ThreadPool xlargeThreadPool = newThreadPool(
            getTestName() + "-xlarge",
            Settings.builder()
                .put("thread_pool.snapshot.core", 1)
                .put("thread_pool.snapshot.max", 10)
                .put("thread_pool.snapshot_upload.core", 1)
                .put("thread_pool.snapshot_upload.max", 40)
                .build()
        );
        try {
            final TestNode node = new TestNode(Settings.EMPTY, xlargeThreadPool);
            node.apply(adaptive(true));
            node.tick();
            assertThat(node.service.getUploadConcurrencyCeiling(), equalTo(40));
        } finally {
            terminate(xlargeThreadPool);
        }
    }

    public void testBudgetBacksOffAndRecoversToTheNodeTarget() {
        // a node with the target of an 8GiB node: today's concurrency is 10 and the pool allows 20
        final ThreadPool largeThreadPool = newThreadPool(
            getTestName() + "-large",
            Settings.builder()
                .put("thread_pool.snapshot.core", 1)
                .put("thread_pool.snapshot.max", 10)
                .put("thread_pool.snapshot_upload.core", 1)
                .put("thread_pool.snapshot_upload.max", 20)
                .build()
        );
        try {
            final TestNode node = new TestNode(Settings.EMPTY, largeThreadPool);
            node.apply(adaptive(true));
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(10));

            // quiet: one more per interval up to the target, and no further
            for (int expected = 11; expected <= 20; expected++) {
                node.tick();
                assertThat(node.service.getUploadBudget(), equalTo(expected));
            }
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(20));

            // cpu pressure over the interval: three quarters, then back up one at a time
            node.probes.cpuPressure.set(new CgroupV2Probe.CpuPressure(1_000_000L));
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(15));
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(16));

            // upload errors: half, and no recovery while the cooldown runs
            node.service.onUploadWriteError();
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(10));
            for (int i = 0; i < UploadConcurrencyController.ERROR_COOLDOWN_INTERVALS; i++) {
                node.tick();
                assertThat(node.service.getUploadBudget(), equalTo(10));
            }
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(11));

            // a probe that cannot be read stops the recovery
            node.probes.cpuThrottling.set(null);
            node.tick();
            assertThat(node.service.getUploadBudget(), equalTo(11));
        } finally {
            terminate(largeThreadPool);
        }
    }

    public void testHeapGuardedNodeNeverGrows() {
        // a node where today's heap guard applies has an upload pool that is the SNAPSHOT pool's size
        final int snapshotMax = randomIntBetween(1, 5);
        final Settings poolSettings = Settings.builder()
            .put("thread_pool.snapshot.core", 1)
            .put("thread_pool.snapshot.max", snapshotMax)
            .put("thread_pool.snapshot_upload.core", 1)
            .put("thread_pool.snapshot_upload.max", snapshotMax)
            .build();
        final ThreadPool guardedThreadPool = newThreadPool(getTestName() + "-guarded", poolSettings);
        try {
            final TestNode node = new TestNode(Settings.EMPTY, guardedThreadPool);
            node.apply(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), randomIntBetween(1, 200))
                    .build()
            );
            node.tick();
            assertThat(node.service.getUploadConcurrencyCeiling(), equalTo(snapshotMax));
            final var runner = new PrioritizedThrottledTaskRunner<BlockingTask>(
                "test",
                snapshotMax,
                node.service.getUploadExecutor(),
                node.service::tryAcquireUploadPermit
            );
            registrations.add(node.service.registerUploadTaskRunner(runner));
            assertThat(runner.getMaxRunningTasks(), equalTo(snapshotMax));
        } finally {
            terminate(guardedThreadPool);
        }
    }

    public void testIntervalSignals() {
        // cpu pressure: stalled microseconds over the interval
        assertThat(
            UploadConcurrencyService.cpuPressure(
                new CgroupV2Probe.CpuPressure(1_000L),
                new CgroupV2Probe.CpuPressure(251_000L),
                5_000_000_000L
            ),
            equalTo(OptionalDouble.of(0.05))
        );
        assertThat(UploadConcurrencyService.cpuPressure(null, new CgroupV2Probe.CpuPressure(1L), 1L), equalTo(OptionalDouble.empty()));
        assertThat(UploadConcurrencyService.cpuPressure(new CgroupV2Probe.CpuPressure(1L), null, 1L), equalTo(OptionalDouble.empty()));
        // a counter that went back is unknown, not negative
        assertThat(
            UploadConcurrencyService.cpuPressure(new CgroupV2Probe.CpuPressure(10L), new CgroupV2Probe.CpuPressure(5L), 1L),
            equalTo(OptionalDouble.empty())
        );

        // throttling: microseconds throttled in the interval
        assertThat(
            UploadConcurrencyService.throttledMicros(new CgroupV2Probe.CpuThrottling(100L, 1L), new CgroupV2Probe.CpuThrottling(160L, 2L)),
            equalTo(OptionalLong.of(60L))
        );
        assertThat(UploadConcurrencyService.throttledMicros(null, new CgroupV2Probe.CpuThrottling(1L, 1L)), equalTo(OptionalLong.empty()));
        assertThat(
            UploadConcurrencyService.throttledMicros(new CgroupV2Probe.CpuThrottling(100L, 1L), new CgroupV2Probe.CpuThrottling(50L, 1L)),
            equalTo(OptionalLong.empty())
        );
    }
}
