/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleCalculator;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleSettings;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleState;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class UploadQueueControllerServiceTests extends ESTestCase {
    public void testThrottleAndRemoveSteadyState() {
        var shardId = new ShardId(randomIndexName(), randomUUID(), 0);

        var time = new AtomicLong(0);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        var stats = new ShardCommitUploadStats() {
            long oldestCommitUploadStartTime = 0;

            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public Long oldestCommitUploadStartTimeRelativeMillis() {
                return oldestCommitUploadStartTime;
            }
        };

        var settings = new ThrottleSettings(TimeValue.timeValueMillis(20), TimeValue.timeValueMillis(20), 10);

        int iterations = randomIntBetween(1, 20);
        Map<ShardId, ThrottleState> currentState = Map.of();
        for (int i = 0; i < iterations; i++) {
            // Ensure that the period always passes for simplicity.
            long timePassed = randomLongBetween(settings.cooldownPeriodMs() + 1, 60);
            time.addAndGet(timePassed);

            // We will alternate between throttle and unthrottle conditions.
            // Upload start time further in the past indicates backlog and lead to throttling.
            long differenceWithCurrentTime;
            if (i % 2 == 0) {
                differenceWithCurrentTime = randomLongBetween(settings.activationThreshold().millis() + 1, 100);
            } else {
                differenceWithCurrentTime = randomLongBetween(0, settings.deactivationThreshold().millis() - 1);
            }
            stats.oldestCommitUploadStartTime = time.get() - differenceWithCurrentTime;

            currentState = calculator.newState(currentState, Stream.of(stats), settings);
            var shardState = currentState.get(shardId);
            if (i % 2 == 0) {
                assertEquals(Type.THROTTLED, shardState.latestDecision());
                assertEquals(time.get(), shardState.relativeApplicationTimeMs());
            } else {
                assertEquals(Type.THROTTLE_REMOVED, shardState.latestDecision());
                assertEquals(time.get(), shardState.relativeApplicationTimeMs());
            }
        }
    }

    public void testMultipleThrottlingPeriods() {
        var shardId = new ShardId(randomIndexName(), randomUUID(), 0);

        long initialTime = 1000;
        var time = new AtomicLong(initialTime);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        // With empty current state we can throttle if conditions are met.
        var stats = new ShardCommitUploadStats() {
            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public Long oldestCommitUploadStartTimeRelativeMillis() {
                // See value of `time`, this results in the age of the oldest commit being larger than the threshold in settings below.
                return 0L;
            }
        };
        var settings = new ThrottleSettings(TimeValue.timeValueMillis(20), TimeValue.timeValueMillis(10), 10);
        // Age of the oldest commit is 1000 which is higher than the threshold.
        Map<ShardId, ThrottleState> throttleState = calculator.newState(Map.of(), Stream.of(stats), settings);
        var throttleShardState = throttleState.get(shardId);
        assertEquals(Type.THROTTLED, throttleShardState.latestDecision());
        assertEquals(initialTime, throttleShardState.relativeApplicationTimeMs());
        assertEquals(1, throttleShardState.consecutiveApplications());
        assertEquals(1, throttler.history.size());
        assertEquals(shardId, throttler.history.get(0).shardId);
        assertTrue(throttler.history.get(0).throttled);

        // We should hold this state for the specified cooldown period.
        Map<ShardId, ThrottleState> throttleKeepState = throttleState;
        for (int i = 0; i < settings.cooldownPeriodMs(); i++) {
            time.incrementAndGet();

            // So this is all the same as above.
            throttleKeepState = calculator.newState(throttleState, Stream.of(stats), settings);
            var throttleKeepShardState = throttleKeepState.get(shardId);
            assertEquals(Type.THROTTLED, throttleKeepShardState.latestDecision());
            assertEquals(initialTime, throttleKeepShardState.relativeApplicationTimeMs());
            assertEquals(1, throttleKeepShardState.consecutiveApplications());
            assertEquals(1, throttler.history.size());
        }

        time.incrementAndGet();

        // We keep the throttle as long as needed if we still see commits being queued.
        time.addAndGet(settings.cooldownPeriodMs() + 1);

        Map<ShardId, ThrottleState> secondPeriodState = calculator.newState(throttleKeepState, Stream.of(stats), settings);
        var secondPeriodShardState = secondPeriodState.get(shardId);
        assertEquals(Type.THROTTLED, secondPeriodShardState.latestDecision());
        assertEquals(time.get(), secondPeriodShardState.relativeApplicationTimeMs());
        assertEquals(2, secondPeriodShardState.consecutiveApplications());
        // We don't reapply throttling if it's already there.
        assertEquals(1, throttler.history.size());
    }

    public void testRemoveThrottleCooldown() {
        var shardId = new ShardId(randomIndexName(), randomUUID(), 0);

        long initialTime = 1000;
        var time = new AtomicLong(initialTime);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        var stats = new ShardCommitUploadStats() {
            long oldestCommitUploadStartTime = 0;

            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public Long oldestCommitUploadStartTimeRelativeMillis() {
                return oldestCommitUploadStartTime;
            }
        };

        var settings = new ThrottleSettings(TimeValue.timeValueMillis(20), TimeValue.timeValueMillis(15), 10);

        // Upload start time that is in the past which leads to throttling.
        stats.oldestCommitUploadStartTime = 0;

        Map<ShardId, ThrottleState> throttledState = calculator.newState(Map.of(), Stream.of(stats), settings);
        var throttledShardState = throttledState.get(shardId);
        assertEquals(Type.THROTTLED, throttledShardState.latestDecision());
        assertEquals(initialTime, throttledShardState.relativeApplicationTimeMs());
        assertEquals(1, throttledShardState.consecutiveApplications());
        assertEquals(1, throttler.history.size());
        assertTrue(throttler.history.get(0).throttled);

        time.addAndGet(settings.cooldownPeriodMs() + 1);
        // Results in 1 ms age of the oldest commit which is lower than the deactivation threshold.
        // Leads to removal of throttling.
        stats.oldestCommitUploadStartTime = time.get() - 1;

        long throttleRemovedTime = time.get();
        Map<ShardId, ThrottleState> removeThrottleState = calculator.newState(throttledState, Stream.of(stats), settings);
        var removeThrottleStateShardState = removeThrottleState.get(shardId);
        assertEquals(Type.THROTTLE_REMOVED, removeThrottleStateShardState.latestDecision());
        assertEquals(throttleRemovedTime, removeThrottleStateShardState.relativeApplicationTimeMs());
        assertEquals(2, throttler.history.size());
        assertFalse(throttler.history.get(1).throttled);

        stats.oldestCommitUploadStartTime = 0;

        Map<ShardId, ThrottleState> state = removeThrottleState;
        for (int i = 0; i < settings.cooldownPeriodMs(); i++) {
            time.incrementAndGet();

            // We don't throttle again during grace period after the throttle removal.
            state = calculator.newState(state, Stream.of(stats), settings);
            var shardState = state.get(shardId);
            assertEquals(Type.THROTTLE_REMOVED, shardState.latestDecision());
            assertEquals(throttleRemovedTime, shardState.relativeApplicationTimeMs());
            assertEquals(2, throttler.history.size());
        }

        // Finally once the cooldown period passes, the throttle is applied again.
        time.incrementAndGet();
        state = calculator.newState(state, Stream.of(stats), settings);
        var shardState = state.get(shardId);
        assertEquals(Type.THROTTLED, shardState.latestDecision());
        assertEquals(time.get(), shardState.relativeApplicationTimeMs());
        assertEquals(1, shardState.consecutiveApplications());
        assertEquals(3, throttler.history.size());
        assertTrue(throttler.history.get(2).throttled);
    }

    public void testThrottleIsRemovedOnUndefinedOldestCommit() {
        var shardId = new ShardId(randomIndexName(), randomUUID(), 0);

        long initialTime = 1000;
        var time = new AtomicLong(initialTime);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        var stats = new ShardCommitUploadStats() {
            Long oldestCommitUploadStartTime = 0L;

            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public Long oldestCommitUploadStartTimeRelativeMillis() {
                return oldestCommitUploadStartTime;
            }
        };

        var settings = new ThrottleSettings(TimeValue.timeValueMillis(20), TimeValue.timeValueMillis(15), 10);

        // Upload start time that is in the past which leads to throttling.
        stats.oldestCommitUploadStartTime = 0L;

        Map<ShardId, ThrottleState> throttledState = calculator.newState(Map.of(), Stream.of(stats), settings);
        var throttledShardState = throttledState.get(shardId);
        assertEquals(Type.THROTTLED, throttledShardState.latestDecision());
        assertEquals(initialTime, throttledShardState.relativeApplicationTimeMs());
        assertEquals(1, throttledShardState.consecutiveApplications());
        assertEquals(1, throttler.history.size());
        assertTrue(throttler.history.get(0).throttled);

        time.addAndGet(settings.cooldownPeriodMs() + 1);

        // This happens when there are no pending commits and we should take throttle removal code path.
        stats.oldestCommitUploadStartTime = null;

        long throttleRemovedTime = time.get();
        Map<ShardId, ThrottleState> removeThrottleState = calculator.newState(throttledState, Stream.of(stats), settings);
        var removeThrottleStateShardState = removeThrottleState.get(shardId);
        assertEquals(Type.THROTTLE_REMOVED, removeThrottleStateShardState.latestDecision());
        assertEquals(throttleRemovedTime, removeThrottleStateShardState.relativeApplicationTimeMs());
        assertEquals(2, throttler.history.size());
        assertFalse(throttler.history.get(1).throttled);
    }

    public void testIndexingThrottler() {
        var indicesService = mock(IndicesService.class);

        var sut = new UploadQueueControllerService.IndexingThrottler(indicesService);

        // Happy case throttling.
        var shard1 = mock(IndexShard.class);
        var shardId1 = new ShardId(randomIndexName(), randomUUID(), 0);
        when(indicesService.getShardOrNull(shardId1)).thenReturn(shard1);

        sut.activate(shardId1);
        assertTrue(sut.getThrottledShards().contains(shard1));
        verify(shard1, times(1)).activateThrottling();

        // Shard doesn't exist.
        var shardId2 = new ShardId(randomIndexName(), randomUUID(), 1);

        // The call succeeds but nothing changes in state.
        sut.activate(shardId2);
        assertEquals(1, sut.getThrottledShards().size());

        // Deactivate works.
        sut.deactivate(shardId1);
        assertFalse(sut.getThrottledShards().contains(shard1));
        verify(shard1, times(1)).deactivateThrottling();

        // Do not deactivate throttling if it was never applied.
        var shard3 = mock(IndexShard.class);
        var shardId3 = new ShardId(randomIndexName(), randomUUID(), 2);
        when(indicesService.getShardOrNull(shardId3)).thenReturn(shard3);

        sut.deactivate(shardId3);
        assertFalse(sut.getThrottledShards().contains(shard3));
        verify(shard3, never()).deactivateThrottling();

        // closeShard() also works
        var shard4 = mock(IndexShard.class);
        var shardId4 = new ShardId(randomIndexName(), randomUUID(), 3);
        when(indicesService.getShardOrNull(shardId4)).thenReturn(shard4);

        sut.activate(shardId4);
        assertTrue(sut.getThrottledShards().contains(shard4));

        sut.closeShard(shard4);
        assertFalse(sut.getThrottledShards().contains(shard4));
    }

    static class MemorizingThrottler implements UploadQueueControllerService.Throttler {
        private final List<Decision> history = new ArrayList<>();

        @Override
        public void activate(ShardId shardId) {
            history.add(new Decision(shardId, true));
        }

        @Override
        public void deactivate(ShardId shardId) {
            history.add(new Decision(shardId, false));
        }
    }

    record Decision(ShardId shardId, boolean throttled) {}
}
