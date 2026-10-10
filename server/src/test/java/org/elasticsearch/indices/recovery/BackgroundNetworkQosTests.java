/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.common.util.concurrent.PrioritizedThrottledTaskRunner;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.core.Releasables;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.Scheduler;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.BACKGROUND_QOS_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.MIN_FLOOR_BYTES_PER_SEC;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.UPLOAD_CONCURRENCY_INTERVAL_TICKS;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.UPLOAD_CONCURRENCY_MAX_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.applyOperatorMax;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeFloor;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeRate;
import static org.elasticsearch.indices.recovery.RecoverySettings.INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class BackgroundNetworkQosTests extends ESTestCase {

    private static final long MB = ByteSizeUnit.MB.toBytes(1);
    private static final long SHARE = 1000 * MB;

    private ThreadPool threadPool;
    // what to close when the test is done, as the repositories close their task runners' registrations
    private final List<Releasable> registrations = new ArrayList<>();

    @Before
    public void createThreadPool() {
        // the upload pool is only larger than the snapshot pool on nodes with enough heap, which the tests cannot rely on
        final int snapshotMax = randomIntBetween(1, 4);
        threadPool = new TestThreadPool(
            getTestName(),
            Settings.builder()
                .put("thread_pool.snapshot.core", 1)
                .put("thread_pool.snapshot.max", snapshotMax)
                .put("thread_pool.snapshot_upload.core", 1)
                .put("thread_pool.snapshot_upload.max", snapshotMax + randomIntBetween(3, 10))
                .build()
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

        RecordingThreadPool(String name) {
            super(name);
        }

        @Override
        public Scheduler.Cancellable scheduleWithFixedDelay(Runnable command, TimeValue interval, Executor executor) {
            final Scheduler.Cancellable cancellable = super.scheduleWithFixedDelay(command, interval, executor);
            scheduled.add(cancellable);
            return cancellable;
        }
    }

    /** The measurements of a node, which the tests set, and how often they are read. */
    private static class FakeProbes {
        final AtomicReference<NetworkProbe.NetworkStats> network = new AtomicReference<>(new NetworkProbe.NetworkStats(0L, 0L));
        final AtomicReference<CgroupV2Probe.CpuPressure> cpuPressure = new AtomicReference<>(new CgroupV2Probe.CpuPressure(0L));
        final AtomicReference<CgroupV2Probe.CpuThrottling> cpuThrottling = new AtomicReference<>(new CgroupV2Probe.CpuThrottling(0L, 0L));
        final AtomicInteger reads = new AtomicInteger();
        private long received;
        private long transmitted;

        BackgroundNetworkQos.Probes probes() {
            return new BackgroundNetworkQos.Probes(() -> {
                reads.incrementAndGet();
                return network.get();
            }, () -> {
                reads.incrementAndGet();
                return cpuPressure.get();
            }, () -> {
                reads.incrementAndGet();
                return cpuThrottling.get();
            });
        }

        /** The pod used this much of each direction in the last second. */
        void use(long receiveBytes, long transmitBytes) {
            received += receiveBytes;
            transmitted += transmitBytes;
            publish();
        }

        void publish() {
            network.set(new NetworkProbe.NetworkStats(received, transmitted));
        }
    }

    private static Settings nodeBandwidthSettings() {
        return Settings.builder()
            .put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "1000mb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "2000mb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "2000mb")
            .build();
    }

    /** A node with the given settings, whose clock the test moves. */
    private class TestNode {
        final ClusterSettings clusterSettings;
        final FakeProbes probes = new FakeProbes();
        final AtomicLong nanoTime = new AtomicLong(randomLong());
        final BackgroundNetworkQos qos;
        final RecoverySettings recoverySettings;

        TestNode(Settings settings, boolean stateless, ThreadPool pool) {
            clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            recoverySettings = new RecoverySettings(settings, clusterSettings);
            qos = new BackgroundNetworkQos(clusterSettings, pool, recoverySettings, stateless, probes.probes(), nanoTime::get);
        }

        TestNode(Settings settings) {
            this(settings, true, threadPool);
        }

        void tick(long nanos) {
            nanoTime.addAndGet(nanos);
            qos.tick();
        }

        void tick() {
            tick(TimeUnit.SECONDS.toNanos(1));
        }

        void apply(Settings settings) {
            clusterSettings.applySettings(settings);
        }

        void switchQos(boolean on) {
            apply(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), on).build());
        }
    }

    public void testFloor() {
        assertThat(computeFloor(SHARE), equalTo(400 * MB));
        assertThat(computeFloor(50 * MB), equalTo(40 * MB));
        // capped at the share
        assertThat(computeFloor(30 * MB), equalTo(30 * MB));
        assertThat(computeFloor(0L), equalTo(MIN_FLOOR_BYTES_PER_SEC));
    }

    public void testRateRisesSlowlyWhenForegroundIdle() {
        long rate = computeFloor(SHARE);
        // target is share - headroom = 900MB/s, reached in steps of 100MB/s
        for (long expected = 500; expected <= 900; expected += 100) {
            rate = computeRate(rate, SHARE, rate, rate);
            assertThat(rate, equalTo(expected * MB));
        }
        assertThat(computeRate(rate, SHARE, rate, rate), equalTo(900 * MB));
    }

    public void testRateFallsAtOnceWhenForegroundRises() {
        final long rate = 900 * MB;
        // foreground 300MB/s: target 1000 - 300 - 100 = 600
        assertThat(computeRate(rate, SHARE, 300 * MB + 500 * MB, 500 * MB), equalTo(600 * MB));
        // busy foreground: down to the floor, not below
        assertThat(computeRate(rate, SHARE, 2000 * MB, randomLongBetween(0, 900 * MB)), equalTo(400 * MB));
    }

    public void testRateWithoutProbeIsFloor() {
        assertThat(computeRate(randomLongBetween(0, SHARE), SHARE, -1L, randomLongBetween(0, SHARE)), equalTo(400 * MB));
    }

    public void testRateNeverAboveShare() {
        final long share = 100 * MB;
        // headroom keeps it below the share; a node count lower than the background count means no foreground
        assertThat(computeRate(share, share, 0L, 50 * MB), equalTo(90 * MB));
    }

    public void testOperatorMax() {
        assertThat(applyOperatorMax(900 * MB, 100 * MB), equalTo(100 * MB));
        assertThat(applyOperatorMax(50 * MB, 100 * MB), equalTo(50 * MB));
        // not set, or no limit
        assertThat(applyOperatorMax(900 * MB, -1L), equalTo(900 * MB));
        assertThat(applyOperatorMax(900 * MB, 0L), equalTo(900 * MB));
    }

    public void testCountingRateLimiter() {
        final CountingRateLimiter limiter = new CountingRateLimiter(1_000_000.0);
        assertThat(limiter.getMBPerSec(), equalTo(1_000_000.0));
        limiter.pause(randomLongBetween(1, 1000));
        assertThat(limiter.getPauseNanos(), equalTo(0L));
        limiter.setMBPerSec(42.0);
        assertThat(limiter.getMBPerSec(), equalTo(42.0));
    }

    public void testTickAdjustsLimiters() {
        final TestNode node = new TestNode(nodeBandwidthSettings());
        node.switchQos(true);
        assertTrue(node.qos.isBackgroundQosEnabled());
        // the first tick has nothing to compare with
        node.tick();
        assertLimiters(node.qos, 400, 400);
        // idle foreground: up to 900MB/s in steps of 100MB/s
        for (int expected = 500; expected <= 900; expected += 100) {
            node.tick();
            assertLimiters(node.qos, expected, expected);
        }
        node.tick();
        assertLimiters(node.qos, 900, 900);

        // busy foreground in one direction only: that direction goes straight down to the floor
        node.probes.use(800 * MB, 0L);
        node.tick();
        assertLimiters(node.qos, 400, 900);
        node.probes.use(0L, 800 * MB);
        node.tick();
        assertLimiters(node.qos, 500, 400);

        // probe unavailable: floor
        node.probes.network.set(null);
        node.tick();
        assertLimiters(node.qos, 400, 400);

        // available again and idle: the first tick has nothing to compare to, then the rates rise from the floor
        node.probes.publish();
        node.tick();
        assertLimiters(node.qos, 400, 400);
        node.probes.use(0L, 0L);
        node.tick();
        assertLimiters(node.qos, 500, 500);

        // switched off: back to the floor
        node.switchQos(false);
        assertFalse(node.qos.isBackgroundQosEnabled());
        node.tick();
        assertLimiters(node.qos, 400, 400);
    }

    public void testBackgroundBytesAreCountedOnEveryReadAndAreNotForeground() throws IOException {
        final TestNode node = new TestNode(nodeBandwidthSettings());
        node.switchQos(true);
        // ticks of 1/512 s, so that a test of MB/s only moves KBs
        final long tickNanos = TimeUnit.SECONDS.toNanos(1) / 512;
        final long mbPerTick = MB / 512;
        node.tick(tickNanos);

        // a snapshot uploads 600MB/s, in very many parts that are far below what a limiter looks at, and the interfaces show it too
        readInSmallParts(node.qos, 600 * mbPerTick);
        node.probes.use(600 * mbPerTick, 600 * mbPerTick);
        node.tick(tickNanos);
        // no foreground: the limiters rise from the floor
        assertLimiters(node.qos, 500, 500);

        // 200MB/s of foreground on top of the snapshot's 600MB/s: the budget is 1000 - 200 - 100 = 700, so it rises again. Had the
        // snapshot not been counted, it would be all foreground, the budget 100, below the floor, and the limiters would fall.
        readInSmallParts(node.qos, 600 * mbPerTick);
        node.probes.use(800 * mbPerTick, 800 * mbPerTick);
        node.tick(tickNanos);
        assertLimiters(node.qos, 600, 600);
    }

    /** Reads this many bytes through the counting wrapper in parts of a few bytes, some byte by byte. */
    private static void readInSmallParts(BackgroundNetworkQos qos, long totalBytes) throws IOException {
        long remaining = totalBytes;
        final long before = qos.getCountedUploadBytes();
        while (remaining > 0) {
            final int part = (int) Math.min(remaining, randomIntBetween(1, 300));
            try (InputStream in = qos.countUploadBytes(new ByteArrayInputStream(new byte[part]))) {
                if (randomBoolean()) {
                    for (int i = 0; i < part; i++) {
                        assertThat(in.read(), equalTo(0));
                    }
                    assertThat(in.read(), equalTo(-1));
                } else {
                    final byte[] buffer = new byte[randomIntBetween(1, 64)];
                    int read = 0;
                    int n;
                    while ((n = in.read(buffer, 0, buffer.length)) != -1) {
                        read += n;
                    }
                    assertThat(read, equalTo(part));
                }
            }
            remaining -= part;
        }
        assertThat(qos.getCountedUploadBytes() - before, equalTo(totalBytes));
    }

    public void testBytesReadAgainAfterResetAreCountedAgain() throws IOException {
        final TestNode node = new TestNode(nodeBandwidthSettings());
        try (InputStream in = node.qos.countUploadBytes(new ByteArrayInputStream(new byte[100]))) {
            assertTrue(in.markSupported());
            in.mark(100);
            in.readNBytes(60);
            in.reset();
            in.readNBytes(100);
        }
        assertThat(node.qos.getCountedUploadBytes(), equalTo(160L));
    }

    public void testOperatorMaxBytesPerSecCapsTheRate() {
        // set on the node
        final Settings settings = Settings.builder()
            .put(nodeBandwidthSettings())
            .put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "300mb")
            .build();
        final TestNode node = new TestNode(settings);
        node.switchQos(true);
        for (int i = 0; i < 7; i++) {
            node.tick();
        }
        // the computed rate is 900MB/s, the operator's setting is lower
        assertLimiters(node.qos, 300, 300);

        // raised dynamically
        node.apply(
            Settings.builder()
                .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
                .put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "700mb")
                .build()
        );
        node.tick();
        assertLimiters(node.qos, 700, 700);
        // and unset
        final TestNode unset = new TestNode(nodeBandwidthSettings());
        unset.switchQos(true);
        for (int i = 0; i < 7; i++) {
            unset.tick();
        }
        assertLimiters(unset.qos, 900, 900);
        unset.apply(
            Settings.builder()
                .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
                .put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "200mb")
                .build()
        );
        unset.tick();
        assertLimiters(unset.qos, 200, 200);
        unset.apply(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build());
        unset.tick();
        assertLimiters(unset.qos, 900, 900);
        // zero is no limit
        unset.apply(
            Settings.builder()
                .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
                .put(INDICES_RECOVERY_MAX_BYTES_PER_SEC_SETTING.getKey(), "0")
                .build()
        );
        unset.tick();
        assertLimiters(unset.qos, 900, 900);
    }

    public void testNotEnabledWithoutNodeBandwidthSettings() {
        final TestNode node = new TestNode(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build());
        assertFalse(node.qos.isBackgroundQosEnabled());
    }

    public void testNetworkQosOnlyAppliesOnStatelessNodes() {
        final Settings settings = Settings.builder()
            .put(nodeBandwidthSettings())
            .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
            .build();
        final TestNode stateful = new TestNode(settings, false, threadPool);
        assertFalse(stateful.qos.isBackgroundQosEnabled());
        assertFalse(stateful.qos.isActive());
        for (int i = 0; i < 20; i++) {
            stateful.tick();
        }
        assertThat(stateful.probes.reads.get(), equalTo(0));
        assertTrue(new TestNode(settings, true, threadPool).qos.isBackgroundQosEnabled());
    }

    public void testNothingIsReadOrCountedWhileBothSwitchesAreOff() throws IOException {
        final TestNode node = new TestNode(nodeBandwidthSettings());
        assertFalse(node.qos.isActive());
        for (int i = 0; i < 4 * UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), equalTo(0));

        // on: the measurements are read
        node.switchQos(true);
        node.apply(
            Settings.builder()
                .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                .build()
        );
        for (int i = 0; i < 2 * UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), greaterThan(0));

        // off again: no more reads
        node.apply(
            Settings.builder()
                .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), false)
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), false)
                .build()
        );
        node.tick();
        final int reads = node.probes.reads.get();
        for (int i = 0; i < 4 * UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            node.tick();
        }
        assertThat(node.probes.reads.get(), equalTo(reads));
    }

    public void testOnlyTheMeasurementsOfAnOnSwitchAreRead() {
        // QoS alone reads the network only, adaptive uploads alone the CPU only
        final TestNode qosOnly = new TestNode(nodeBandwidthSettings());
        qosOnly.switchQos(true);
        final AtomicInteger networkReads = new AtomicInteger();
        final AtomicInteger otherReads = new AtomicInteger();
        final BackgroundNetworkQos counting = new BackgroundNetworkQos(
            qosOnly.clusterSettings,
            threadPool,
            qosOnly.recoverySettings,
            true,
            new BackgroundNetworkQos.Probes(() -> {
                networkReads.incrementAndGet();
                return new NetworkProbe.NetworkStats(0L, 0L);
            }, () -> {
                otherReads.incrementAndGet();
                return null;
            }, () -> {
                otherReads.incrementAndGet();
                return null;
            }),
            qosOnly.nanoTime::get
        );
        for (int i = 0; i < 3 * UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            qosOnly.nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            counting.tick();
        }
        assertThat(networkReads.get(), greaterThan(0));
        // only the startup line reads these, once
        assertThat(otherReads.get(), equalTo(2));
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
            node.qos.getUploadExecutor(),
            node.qos::tryAcquireUploadPermit
        );
        registrations.add(node.qos.registerUploadTaskRunner(runner));
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
            node.qos.getUploadExecutor().execute(() -> threadName.complete(Thread.currentThread().getName()));
            assertThat(threadName.get(), containsString(on ? "[snapshot_upload]" : "[snapshot]"));
        }
    }

    public void testUploadConcurrencyBounds() {
        final TestNode node = new TestNode(Settings.EMPTY);
        // today's concurrency is the SNAPSHOT pool's, and the upload pool never has less
        final int snapshotMax = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        assertThat(node.qos.getUploadBudget(), equalTo(snapshotMax));
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
            permits.add(node.qos.tryAcquireUploadPermit(retry));
            assertNotNull(permits.get(i));
        }
        assertThat(node.qos.getRunningUploadTasks(), equalTo(floor + 3));

        // switched on while they run: they count against the budget, which starts at today's concurrency, so no more are granted
        node.apply(adaptive(true));
        assertThat(node.qos.getUploadBudget(), equalTo(floor));
        assertNull(node.qos.tryAcquireUploadPermit(retry));
        Releasables.close(permits.remove(0));
        Releasables.close(permits.remove(0));
        Releasables.close(permits.remove(0));
        assertThat(node.qos.getRunningUploadTasks(), equalTo(floor));
        assertNull(node.qos.tryAcquireUploadPermit(retry));
        // one more is only granted when one is given back, and then up to the budget
        Releasables.close(permits.remove(0));
        final Releasable granted = node.qos.tryAcquireUploadPermit(retry);
        assertNotNull(granted);
        assertNull(node.qos.tryAcquireUploadPermit(retry));
        permits.add(granted);

        // switched off again: not consulted, and what runs is still counted
        node.apply(adaptive(false));
        permits.add(node.qos.tryAcquireUploadPermit(retry));
        assertNotNull(permits.get(permits.size() - 1));
        assertThat(node.qos.getRunningUploadTasks(), equalTo(permits.size()));
        permits.forEach(Releasables::close);
        assertThat(node.qos.getRunningUploadTasks(), equalTo(0));
    }

    public void testRepositoryRunnersFollowTheSwitchAndTheCeiling() {
        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int nodeCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        final var runner = newRepositoryRunner(node);
        assertThat(runner.getMaxRunningTasks(), equalTo(floor));

        // on: the budget is what limits the tasks, so a runner may run as many as the node's ceiling
        node.apply(adaptive(true));
        assertThat(runner.getMaxRunningTasks(), equalTo(node.qos.getUploadConcurrencyCeiling()));
        // a runner that registers while it is on gets that too
        final var late = newRepositoryRunner(node);
        assertThat(late.getMaxRunningTasks(), equalTo(node.qos.getUploadConcurrencyCeiling()));

        // a new setting is picked up at the next interval
        final int max = randomIntBetween(1, nodeCeiling + 5);
        node.apply(
            Settings.builder()
                .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                .build()
        );
        for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            node.tick();
        }
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
            node.qos.getUploadExecutor(),
            node.qos::tryAcquireUploadPermit
        );
        node.qos.registerUploadTaskRunner(runner).close();
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
        assertThat(node.qos.getRunningUploadTasks(), lessThanOrEqualTo(floor));

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
        assertThat(node.qos.getUploadConcurrencyCeiling(), greaterThan(floor));
        final var repositoryA = newRepositoryRunner(node);
        final var repositoryB = newRepositoryRunner(node);
        final var runningInA = enqueueBlockingTasks(repositoryA, floor);
        awaitStarted(runningInA);
        final var waitingInB = enqueueBlockingTasks(repositoryB, 1).get(0);
        assertThat(repositoryB.queueSize(), equalTo(1));

        // work is queued and nothing holds the uploads back: the budget goes up by one, which the waiting task takes
        for (int i = 0; i < 2 * UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            node.tick();
        }
        assertThat(node.qos.getUploadBudget(), equalTo(floor + 1));
        safeAwait(waitingInB.started);
        assertThat(node.qos.getRunningUploadTasks(), equalTo(floor + 1));

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
            final var runner = new PrioritizedThrottledTaskRunner<RejectedTask>("test", floor, rejecting, node.qos::tryAcquireUploadPermit);
            registrations.add(node.qos.registerUploadTaskRunner(runner));
            runners.add(runner);
        }

        // the budget is used up, so that the tasks of both runners queue, and then given back to them task by task
        final List<Releasable> held = new ArrayList<>();
        for (int i = 0; i < floor; i++) {
            held.add(node.qos.tryAcquireUploadPermit(() -> {}));
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
        node.qos.onUploadReadError();
        node.qos.onUploadWriteError();
        assertThat(node.qos.getUploadReadErrors(), equalTo(0L));
        assertThat(node.qos.getUploadWriteErrors(), equalTo(0L));

        node.apply(adaptive(true));
        node.qos.onUploadReadError();
        node.qos.onUploadWriteError();
        node.qos.onUploadWriteError();
        assertThat(node.qos.getUploadReadErrors(), equalTo(1L));
        assertThat(node.qos.getUploadWriteErrors(), equalTo(2L));

        node.apply(adaptive(false));
        node.qos.onUploadReadError();
        assertThat(node.qos.getUploadReadErrors(), equalTo(1L));
    }

    public void testTickIsOnlyScheduledWhileASwitchIsOn() {
        final var recordingThreadPool = new RecordingThreadPool(getTestName() + "-recording");
        try {
            final TestNode node = new TestNode(nodeBandwidthSettings(), true, recordingThreadPool);
            node.qos.start();
            // both off: nothing is scheduled
            assertThat(recordingThreadPool.scheduled, hasSize(0));

            node.switchQos(true);
            assertThat(recordingThreadPool.scheduled, hasSize(1));
            assertFalse(recordingThreadPool.scheduled.get(0).isCancelled());
            // the second switch does not add a second tick
            node.apply(
                Settings.builder()
                    .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true)
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .build()
            );
            assertThat(recordingThreadPool.scheduled, hasSize(1));
            // and one of them being off does not stop it
            node.apply(
                Settings.builder()
                    .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), false)
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .build()
            );
            assertFalse(recordingThreadPool.scheduled.get(0).isCancelled());

            // both off: cancelled
            node.apply(
                Settings.builder()
                    .put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), false)
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), false)
                    .build()
            );
            assertTrue(recordingThreadPool.scheduled.get(0).isCancelled());
            assertThat(recordingThreadPool.scheduled, hasSize(1));

            // on again: a new one, until the node stops
            node.switchQos(true);
            assertThat(recordingThreadPool.scheduled, hasSize(2));
            assertFalse(recordingThreadPool.scheduled.get(1).isCancelled());
            node.qos.stop();
            assertTrue(recordingThreadPool.scheduled.get(1).isCancelled());
            node.switchQos(false);
            node.switchQos(true);
            assertThat(recordingThreadPool.scheduled, hasSize(2));
        } finally {
            terminate(recordingThreadPool);
        }
    }

    public void testSwitchesAreOnBeforeTheNodeStarts() {
        final var recordingThreadPool = new RecordingThreadPool(getTestName() + "-recording");
        try {
            final TestNode node = new TestNode(
                Settings.builder().put(nodeBandwidthSettings()).put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build(),
                true,
                recordingThreadPool
            );
            // nothing is scheduled by a node that has not started
            assertThat(recordingThreadPool.scheduled, hasSize(0));
            node.qos.start();
            assertThat(recordingThreadPool.scheduled, hasSize(1));
        } finally {
            terminate(recordingThreadPool);
        }
    }

    public void testSwitchingQosOffPutsTheLimitersBackToTheirFloor() {
        final TestNode node = new TestNode(nodeBandwidthSettings());
        node.qos.start();
        node.switchQos(true);
        final long floorBytes = computeFloor(SHARE);
        node.tick();
        node.probes.use(0L, 0L);
        for (int i = 0; i < 3; i++) {
            node.probes.use(0L, 0L);
            node.tick();
        }
        assertThat(node.qos.getIngressLimiter().getMBPerSec(), greaterThan(floorBytes / (double) MB + 1));

        // nothing ticks any more once both are off, so the switch does it
        node.switchQos(false);
        assertLimiters(node.qos, floorBytes / (double) MB, floorBytes / (double) MB);
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
        assertThat(node.qos.getUploadBudget(), equalTo(floor));
    }

    public void testUploadConcurrencyMaxSetting() {
        assertThat(UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.EMPTY), equalTo(30));
        expectThrows(
            IllegalArgumentException.class,
            () -> UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.builder().put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), 0).build())
        );

        final TestNode node = new TestNode(Settings.EMPTY);
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int nodeCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        // the ceiling is the lower of the setting and what the node's size allows, never below today's concurrency
        assertThat(node.qos.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(30, nodeCeiling))));
        node.apply(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build());
        for (int max : new int[] { 1, floor, nodeCeiling, nodeCeiling + 50, randomIntBetween(1, 200) }) {
            node.apply(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                    .build()
            );
            for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
                node.tick();
            }
            assertThat(node.qos.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(max, nodeCeiling))));
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
        final ThreadPool guardedThreadPool = new TestThreadPool(getTestName() + "-guarded", poolSettings);
        try {
            final TestNode node = new TestNode(Settings.EMPTY, true, guardedThreadPool);
            node.apply(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), randomIntBetween(1, 200))
                    .build()
            );
            for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
                node.tick();
            }
            assertThat(node.qos.getUploadConcurrencyCeiling(), equalTo(snapshotMax));
            final var runner = new PrioritizedThrottledTaskRunner<BlockingTask>(
                "test",
                snapshotMax,
                node.qos.getUploadExecutor(),
                node.qos::tryAcquireUploadPermit
            );
            registrations.add(node.qos.registerUploadTaskRunner(runner));
            assertThat(runner.getMaxRunningTasks(), equalTo(snapshotMax));
        } finally {
            terminate(guardedThreadPool);
        }
    }

    public void testIntervalSignals() {
        // cpu pressure: stalled microseconds over the interval
        assertThat(
            BackgroundNetworkQos.cpuPressure(
                new CgroupV2Probe.CpuPressure(1_000L),
                new CgroupV2Probe.CpuPressure(251_000L),
                5_000_000_000L
            ),
            equalTo(OptionalDouble.of(0.05))
        );
        assertThat(BackgroundNetworkQos.cpuPressure(null, new CgroupV2Probe.CpuPressure(1L), 1L), equalTo(OptionalDouble.empty()));
        assertThat(BackgroundNetworkQos.cpuPressure(new CgroupV2Probe.CpuPressure(1L), null, 1L), equalTo(OptionalDouble.empty()));
        // a counter that went back is unknown, not negative
        assertThat(
            BackgroundNetworkQos.cpuPressure(new CgroupV2Probe.CpuPressure(10L), new CgroupV2Probe.CpuPressure(5L), 1L),
            equalTo(OptionalDouble.empty())
        );

        // throttling: microseconds throttled in the interval
        assertThat(
            BackgroundNetworkQos.throttledMicros(new CgroupV2Probe.CpuThrottling(100L, 1L), new CgroupV2Probe.CpuThrottling(160L, 2L)),
            equalTo(OptionalLong.of(60L))
        );
        assertThat(BackgroundNetworkQos.throttledMicros(null, new CgroupV2Probe.CpuThrottling(1L, 1L)), equalTo(OptionalLong.empty()));
        assertThat(
            BackgroundNetworkQos.throttledMicros(new CgroupV2Probe.CpuThrottling(100L, 1L), new CgroupV2Probe.CpuThrottling(50L, 1L)),
            equalTo(OptionalLong.empty())
        );
    }

    private static void assertLimiters(BackgroundNetworkQos qos, double netIn, double netOut) {
        assertThat(qos.getIngressLimiter().getMBPerSec(), closeTo(netIn, 0.001));
        assertThat(qos.getEgressLimiter().getMBPerSec(), closeTo(netOut, 0.001));
    }
}
