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
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.monitor.os.CgroupV2Probe;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.BACKGROUND_QOS_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.MIN_FLOOR_BYTES_PER_SEC;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.UPLOAD_CONCURRENCY_INTERVAL_TICKS;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.UPLOAD_CONCURRENCY_MAX_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeFloor;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeRate;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class BackgroundNetworkQosTests extends ESTestCase {

    private static final long MB = ByteSizeUnit.MB.toBytes(1);
    private static final long SHARE = 1000 * MB;

    private ThreadPool threadPool;

    @Before
    public void createThreadPool() {
        threadPool = new TestThreadPool(getTestName());
    }

    @After
    public void stopThreadPool() {
        terminate(threadPool);
    }

    /** The measurements of a node, which the tests set. */
    private static class FakeProbes {
        final AtomicReference<NetworkProbe.NetworkStats> network = new AtomicReference<>(new NetworkProbe.NetworkStats(0L, 0L));
        final AtomicReference<CgroupV2Probe.CpuPressure> cpuPressure = new AtomicReference<>(new CgroupV2Probe.CpuPressure(0L));
        final AtomicReference<CgroupV2Probe.CpuThrottling> cpuThrottling = new AtomicReference<>(new CgroupV2Probe.CpuThrottling(0L, 0L));
        final AtomicReference<BackgroundNetworkQos.QueueLatency> writeQueue = new AtomicReference<>(
            new BackgroundNetworkQos.QueueLatency(0L, 0L)
        );
        private long received;
        private long transmitted;

        BackgroundNetworkQos.Probes probes() {
            return new BackgroundNetworkQos.Probes(network::get, cpuPressure::get, cpuThrottling::get, writeQueue::get);
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

    public void testCountingRateLimiter() {
        final CountingRateLimiter limiter = new CountingRateLimiter(1_000_000.0);
        assertThat(limiter.getMBPerSec(), equalTo(1_000_000.0));
        long total = 0;
        for (int i = 0; i < 5; i++) {
            final long bytes = randomLongBetween(1, 1000);
            limiter.pause(bytes);
            total += bytes;
        }
        assertThat(limiter.getBytes(), equalTo(total));
        limiter.setMBPerSec(42.0);
        assertThat(limiter.getMBPerSec(), equalTo(42.0));
    }

    public void testTickAdjustsLimiters() {
        final Settings settings = nodeBandwidthSettings();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final FakeProbes probes = new FakeProbes();
        final AtomicLong nanoTime = new AtomicLong(randomLong());
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            recoverySettings,
            probes.probes(),
            nanoTime::get
        );
        final Runnable tick = () -> {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            qos.tick();
        };

        assertFalse(qos.isBackgroundQosEnabled());
        tick.run();
        assertLimiters(qos, 400, 400);

        clusterSettings.applySettings(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build());
        assertTrue(qos.isBackgroundQosEnabled());
        // idle foreground: up to 900MB/s in steps of 100MB/s
        for (int expected = 500; expected <= 900; expected += 100) {
            tick.run();
            assertLimiters(qos, expected, expected);
        }
        tick.run();
        assertLimiters(qos, 900, 900);

        // busy foreground in one direction only: that direction goes straight down to the floor
        probes.use(800 * MB, 0L);
        tick.run();
        assertLimiters(qos, 400, 900);
        probes.use(0L, 800 * MB);
        tick.run();
        assertLimiters(qos, 500, 400);

        // probe unavailable: floor
        probes.network.set(null);
        tick.run();
        assertLimiters(qos, 400, 400);

        // available again and idle: the first tick has nothing to compare to, then the rates rise from the floor
        probes.publish();
        tick.run();
        assertLimiters(qos, 400, 400);
        probes.use(0L, 0L);
        tick.run();
        assertLimiters(qos, 500, 500);

        // switched off: back to the floor
        clusterSettings.applySettings(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), false).build());
        assertFalse(qos.isBackgroundQosEnabled());
        tick.run();
        assertLimiters(qos, 400, 400);
    }

    public void testBackgroundBytesAreNotForeground() throws IOException {
        final Settings settings = nodeBandwidthSettings();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final FakeProbes probes = new FakeProbes();
        final AtomicLong nanoTime = new AtomicLong(randomLong());
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(settings, clusterSettings),
            probes.probes(),
            nanoTime::get
        );
        clusterSettings.applySettings(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build());
        // ticks of 1/512 s, so that a test of MB/s only moves KBs and the limiter does not pause for long
        final Runnable tick = () -> {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1) / 512);
            qos.tick();
        };
        final long mbPerTick = MB / 512;
        tick.run();
        // a snapshot uploads 600MB/s through the egress limiter, which the node's interfaces also show
        qos.getEgressLimiter().pause(600 * mbPerTick);
        probes.use(0L, 600 * mbPerTick);
        tick.run();
        // no foreground: the limiter rises from the floor
        assertThat(qos.getEgressLimiter().getMBPerSec(), closeTo(500.0, 0.001));
        // 200MB/s of foreground on top of the snapshot's 600MB/s: the budget is 1000 - 200 - 100 = 700, so it rises again. Had the
        // snapshot counted as foreground the budget would be 100, which is below the floor, and the limiter would fall.
        qos.getEgressLimiter().pause(600 * mbPerTick);
        probes.use(0L, 800 * mbPerTick);
        tick.run();
        assertThat(qos.getEgressLimiter().getMBPerSec(), closeTo(600.0, 0.001));
    }

    public void testNotEnabledWithoutNodeBandwidthSettings() {
        final Settings settings = Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(settings, clusterSettings)
        );
        assertFalse(qos.isBackgroundQosEnabled());
    }

    public void testUploadExecutorFollowsTheSwitch() throws Exception {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            new FakeProbes().probes(),
            new AtomicLong()::get
        );
        // decided for each task, as the switch changes
        for (int i = 0; i < 4; i++) {
            final boolean adaptive = i % 2 == 1;
            clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), adaptive).build());
            final AtomicReference<String> executorName = new AtomicReference<>();
            final CountDownLatch done = new CountDownLatch(1);
            qos.getUploadExecutor().execute(() -> {
                executorName.set(EsExecutors.executorName(Thread.currentThread()));
                done.countDown();
            });
            safeAwait(done);
            assertThat(executorName.get(), equalTo(adaptive ? ThreadPool.Names.SNAPSHOT_UPLOAD : ThreadPool.Names.SNAPSHOT));
        }
    }

    public void testUploadConcurrencyBounds() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings)
        );
        // today's concurrency is the SNAPSHOT pool's, and the upload pool never has less
        final int snapshotMax = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(snapshotMax));
        assertThat(threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax(), greaterThan(0));
    }

    public void testUploadConcurrencyStaysAtDefaultWhenAdaptiveOff() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AtomicLong nanoTime = new AtomicLong();
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            new FakeProbes().probes(),
            nanoTime::get
        );
        final int defaultConcurrency = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(defaultConcurrency));

        // something else changed it: an adaptive-off interval puts it back
        qos.getUploadTaskRunner().setMaxRunningTasks(defaultConcurrency + 1);
        clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), false).build());
        for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            qos.tick();
        }
        assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(defaultConcurrency));

        // adaptive on with nothing queued holds at the default
        clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build());
        for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            qos.tick();
        }
        assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(defaultConcurrency));
    }

    public void testUploadConcurrencyMaxSetting() {
        assertThat(UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.EMPTY), equalTo(20));
        expectThrows(
            IllegalArgumentException.class,
            () -> UPLOAD_CONCURRENCY_MAX_SETTING.get(Settings.builder().put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), 0).build())
        );

        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AtomicLong nanoTime = new AtomicLong();
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            new FakeProbes().probes(),
            nanoTime::get
        );
        final int floor = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        final int nodeCeiling = Math.max(floor, threadPool.info(ThreadPool.Names.SNAPSHOT_UPLOAD).getMax());
        // the ceiling is the lower of the setting and what the node's size allows, never below today's concurrency
        assertThat(qos.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(20, nodeCeiling))));
        clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build());
        for (int max : new int[] { 1, floor, nodeCeiling, nodeCeiling + 50, randomIntBetween(1, 200) }) {
            clusterSettings.applySettings(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), max)
                    .build()
            );
            for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
                nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
                qos.tick();
            }
            assertThat(qos.getUploadConcurrencyCeiling(), equalTo(Math.max(floor, Math.min(max, nodeCeiling))));
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
            final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            final AtomicLong nanoTime = new AtomicLong();
            final BackgroundNetworkQos qos = new BackgroundNetworkQos(
                clusterSettings,
                guardedThreadPool,
                new RecoverySettings(Settings.EMPTY, clusterSettings),
                new FakeProbes().probes(),
                nanoTime::get
            );
            clusterSettings.applySettings(
                Settings.builder()
                    .put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true)
                    .put(UPLOAD_CONCURRENCY_MAX_SETTING.getKey(), randomIntBetween(1, 200))
                    .build()
            );
            for (int i = 0; i < UPLOAD_CONCURRENCY_INTERVAL_TICKS; i++) {
                nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
                qos.tick();
            }
            assertThat(qos.getUploadConcurrencyCeiling(), equalTo(snapshotMax));
            assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(snapshotMax));
        } finally {
            terminate(guardedThreadPool);
        }
    }

    public void testSwitchingOffRestoresTheDefaultAtOnce() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            new FakeProbes().probes(),
            new AtomicLong()::get
        );
        final int defaultConcurrency = threadPool.info(ThreadPool.Names.SNAPSHOT).getMax();
        clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), true).build());
        qos.getUploadTaskRunner().setMaxRunningTasks(defaultConcurrency + 5);
        clusterSettings.applySettings(Settings.builder().put(ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING.getKey(), false).build());
        assertThat(qos.getUploadTaskRunner().getMaxRunningTasks(), equalTo(defaultConcurrency));
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

        // write queue: mean wait of the tasks started in the interval
        assertThat(
            BackgroundNetworkQos.writeQueueWaitMillis(
                new BackgroundNetworkQos.QueueLatency(1_000_000L, 10L),
                new BackgroundNetworkQos.QueueLatency(7_000_000L, 20L)
            ),
            equalTo(OptionalDouble.of(0.6))
        );
        // no tasks started: no wait
        assertThat(
            BackgroundNetworkQos.writeQueueWaitMillis(
                new BackgroundNetworkQos.QueueLatency(5L, 10L),
                new BackgroundNetworkQos.QueueLatency(5L, 10L)
            ),
            equalTo(OptionalDouble.of(0.0))
        );
        assertThat(
            BackgroundNetworkQos.writeQueueWaitMillis(null, new BackgroundNetworkQos.QueueLatency(5L, 10L)),
            equalTo(OptionalDouble.empty())
        );
    }

    private static void assertLimiters(BackgroundNetworkQos qos, double netIn, double netOut) {
        assertThat(qos.getIngressLimiter().getMBPerSec(), closeTo(netIn, 0.001));
        assertThat(qos.getEgressLimiter().getMBPerSec(), closeTo(netOut, 0.001));
    }
}
