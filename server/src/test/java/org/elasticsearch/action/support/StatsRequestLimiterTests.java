/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.support;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.action.support.StatsRequestLimiter.MAX_CONCURRENT_STATS_REQUESTS_PER_NODE;

public class StatsRequestLimiterTests extends ESTestCase {

    public void testGrantsPermitsUpToMaxPermits() throws Exception {
        final int maxPermits = randomIntBetween(1, 5);
        final List<Thread> threads = new ArrayList<>(maxPermits);
        final CyclicBarrier barrier = new CyclicBarrier(1 + maxPermits);
        TriConsumer<Task, Integer, ActionListener<Integer>> execute = (task, i, actionListener) -> {
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (final BrokenBarrierException | InterruptedException e) {
                    fail("Exception occurred while waiting for the barrier to be lifted");
                }
                actionListener.onResponse(i);
            });
            thread.setName("thread-" + i);
            threads.add(thread);
            thread.start();
        };
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings settings = Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), maxPermits).build();
        StatsRequestLimiter statsRequestLimiter = new StatsRequestLimiter(settings, clusterSettings);

        for (int i = 0; i < maxPermits; i++) {
            PlainActionFuture<Integer> listener = new PlainActionFuture<>();
            statsRequestLimiter.tryToExecute(createTask(), i, listener, execute);
        }
        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        statsRequestLimiter.tryToExecute(createTask(), maxPermits, listener, execute);
        String expectedExceptionMessage = "this node is already coordinating ["
            + MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.get(settings)
            + "] stats requests and has reached the limit set by ["
            + MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey()
            + "]";
        expectThrows(EsRejectedExecutionException.class, expectedExceptionMessage, listener::actionGet);
        StatsRequestStats.Stats stats = getStats(statsRequestLimiter);
        assertEquals(maxPermits, stats.getCurrent());

        barrier.await();
        for (Thread thread : threads) {
            thread.join();
        }
        assertBusy(() -> assertTrue(statsRequestLimiter.tryAcquire()));
        stats = getStats(statsRequestLimiter);
        assertEquals(0, stats.getCurrent());
        assertEquals(maxPermits, stats.getCompleted());
        assertEquals(1, stats.getRejected());
    }

    public void testStatsRequestPermitCanBeDynamicallyUpdated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        StatsRequestLimiter statsRequestLimiter = new StatsRequestLimiter(
            Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build(),
            clusterSettings
        );

        assertTrue(statsRequestLimiter.tryAcquire());
        assertFalse(statsRequestLimiter.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 2).build());

        assertTrue(statsRequestLimiter.tryAcquire());

        clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build());

        assertFalse(statsRequestLimiter.tryAcquire());
        statsRequestLimiter.release();
        statsRequestLimiter.release();

        assertTrue(statsRequestLimiter.tryAcquire());
        assertFalse(statsRequestLimiter.tryAcquire());
    }

    public void testMaxConcurrentStatsRequestsPerNodeIsValidated() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        Settings invalidSetting = Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 0).build();
        expectThrows(IllegalArgumentException.class, () -> new StatsRequestLimiter(invalidSetting, clusterSettings));
        new StatsRequestLimiter(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build(), clusterSettings);
        expectThrows(
            IllegalArgumentException.class,
            () -> clusterSettings.applySettings(Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 0).build())
        );
    }

    public void testReleasingAfterException() {
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        StatsRequestLimiter statsRequestLimiter = new StatsRequestLimiter(
            Settings.builder().put(MAX_CONCURRENT_STATS_REQUESTS_PER_NODE.getKey(), 1).build(),
            clusterSettings
        );
        PlainActionFuture<Integer> listener = new PlainActionFuture<>();
        TriConsumer<Task, Integer, ActionListener<Integer>> execute = (task, input, actionListener) -> {
            // Verify that we hold the last permit
            assertFalse(statsRequestLimiter.tryAcquire());
            throw new RuntimeException("simulated");
        };
        expectThrows(RuntimeException.class, () -> statsRequestLimiter.tryToExecute(createTask(), 10, listener, execute));
        StatsRequestStats.Stats stats = getStats(statsRequestLimiter);
        assertEquals(0, stats.getCurrent());
        assertEquals(1, stats.getCompleted());
        assertTrue(statsRequestLimiter.tryAcquire());
    }

    private StatsRequestStats.Stats getStats(StatsRequestLimiter statsRequestLimiter) {
        return statsRequestLimiter.stats().iterator().next();
    }

    private Task createTask() {
        return new Task(
            randomLong(),
            "transport",
            "stats_action",
            "description",
            new TaskId(randomLong() + ":" + randomLong()),
            emptyMap()
        );
    }
}
