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
import org.elasticsearch.monitor.network.NetworkProbe;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.ADAPTIVE_UPLOAD_CONCURRENCY_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.BACKGROUND_QOS_ENABLED_SETTING;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.MIN_FLOOR_BYTES_PER_SEC;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.UPLOAD_CONCURRENCY_INTERVAL_TICKS;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeFloor;
import static org.elasticsearch.indices.recovery.BackgroundNetworkQos.computeRate;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING;
import static org.elasticsearch.indices.recovery.RecoverySettings.NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;

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
        final Settings settings = Settings.builder()
            .put(NODE_BANDWIDTH_RECOVERY_NETWORK_SETTING.getKey(), "1000mb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_READ_SETTING.getKey(), "2000mb")
            .put(NODE_BANDWIDTH_RECOVERY_DISK_WRITE_SETTING.getKey(), "2000mb")
            .build();
        final ClusterSettings clusterSettings = new ClusterSettings(settings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final RecoverySettings recoverySettings = new RecoverySettings(settings, clusterSettings);
        final AtomicReference<NetworkProbe.NetworkStats> networkStats = new AtomicReference<>(new NetworkProbe.NetworkStats(0L, 0L));
        final AtomicLong nanoTime = new AtomicLong(randomLong());
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            recoverySettings,
            networkStats::get,
            nanoTime::get,
            () -> 0,
            ByteSizeUnit.GB.toBytes(64)
        );
        final Runnable tick = () -> {
            nanoTime.addAndGet(TimeUnit.SECONDS.toNanos(1));
            qos.tick();
        };

        assertFalse(qos.isBackgroundQosEnabled());
        tick.run();
        assertLimiters(qos, 400);

        clusterSettings.applySettings(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), true).build());
        assertTrue(qos.isBackgroundQosEnabled());
        // idle foreground: up to 900MB/s in steps of 100MB/s
        for (int expected = 500; expected <= 900; expected += 100) {
            tick.run();
            assertLimiters(qos, expected);
        }
        tick.run();
        assertLimiters(qos, 900);

        // busy foreground: 800MB/s in each direction, straight down to the floor
        networkStats.set(new NetworkProbe.NetworkStats(800 * MB, 800 * MB));
        tick.run();
        assertLimiters(qos, 400);

        // probe unavailable: floor
        networkStats.set(null);
        tick.run();
        assertLimiters(qos, 400);

        // idle again, then switched off: back to the floor
        networkStats.set(new NetworkProbe.NetworkStats(800 * MB, 800 * MB));
        tick.run();
        tick.run();
        assertLimiters(qos, 500);
        clusterSettings.applySettings(Settings.builder().put(BACKGROUND_QOS_ENABLED_SETTING.getKey(), false).build());
        assertFalse(qos.isBackgroundQosEnabled());
        tick.run();
        assertLimiters(qos, 400);
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

    public void testUploadConcurrencyStaysAtDefaultWhenAdaptiveOff() {
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        final AtomicLong nanoTime = new AtomicLong();
        final BackgroundNetworkQos qos = new BackgroundNetworkQos(
            clusterSettings,
            threadPool,
            new RecoverySettings(Settings.EMPTY, clusterSettings),
            () -> null,
            nanoTime::get,
            () -> 0,
            ByteSizeUnit.GB.toBytes(64)
        );
        final int defaultConcurrency = ThreadPool.getDefaultSnapshotConcurrency(threadPool);
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

    private static void assertLimiters(BackgroundNetworkQos qos, long expectedMBPerSec) {
        assertThat(qos.getIngressLimiter().getMBPerSec(), closeTo(expectedMBPerSec, 0.001));
        assertThat(qos.getEgressLimiter().getMBPerSec(), closeTo(expectedMBPerSec, 0.001));
    }
}
