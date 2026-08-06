/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.commits;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleCalculator;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleSettings;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.ThrottleState;
import org.elasticsearch.xpack.stateless.commits.UploadQueueControllerService.Type;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class UploadQueueControllerServiceTests extends ESTestCase {
    public void testThrottleAndRemoveSteadyState() {
        var shardId = new ShardId(randomIndexName(), randomUUID(), 0);

        var time = new AtomicLong(0);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        var stats = new ShardCommitStats() {
            long pendingUploadBytes = 0;

            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public long pendingUploadBytes() {
                return pendingUploadBytes;
            }
        };

        var settings = new ThrottleSettings(20, 10, 10);

        int iterations = randomIntBetween(1, 20);
        Map<ShardId, ThrottleState> currentState = Map.of();
        for (int i = 0; i < iterations; i++) {
            // Ensure that the period always passes for simplicity.
            long timePassed = randomIntBetween(11, 60);
            time.addAndGet(timePassed);

            // We will alternate between high and low backlog.
            long pendingUploadBytes;
            if (i % 2 == 0) {
                pendingUploadBytes = randomLongBetween(ByteSizeValue.ofMb(21).getBytes(), ByteSizeValue.ofMb(100).getBytes());
            } else {
                pendingUploadBytes = randomLongBetween(0, ByteSizeValue.ofMb(10).getBytes() - 1);
            }
            stats.pendingUploadBytes = pendingUploadBytes;

            currentState = calculator.newState(currentState, List.of(stats), settings, 1);
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

        var time = new AtomicLong(0);
        var throttler = new MemorizingThrottler();
        var calculator = new ThrottleCalculator(time::get, throttler);

        // With empty current state we can throttle if conditions are met.
        var stats = new ShardCommitStats() {
            @Override
            public ShardId shardId() {
                return shardId;
            }

            @Override
            public long pendingUploadBytes() {
                return 50 * 1024 * 1024;
            }
        };
        var settings = new ThrottleSettings(20, 10, 10);
        // The backlog is 50 seconds with provided stats and throughput which is more than 20.
        Map<ShardId, ThrottleState> throttleState = calculator.newState(Map.of(), List.of(stats), settings, 1);
        var throttleShardState = throttleState.get(shardId);
        assertEquals(Type.THROTTLED, throttleShardState.latestDecision());
        assertEquals(0, throttleShardState.relativeApplicationTimeMs());
        assertEquals(1, throttleShardState.consecutiveApplications());
        assertEquals(1, throttler.history.size());
        assertEquals(shardId, throttler.history.get(0).shardId);
        assertTrue(throttler.history.get(0).throttled);

        // We should hold this state for the specified cooldown period.
        time.set(9);

        // So this is all the same as above.
        Map<ShardId, ThrottleState> throttleKeepState = calculator.newState(throttleState, List.of(stats), settings, 1);
        var throttleKeepShardState = throttleKeepState.get(shardId);
        assertEquals(Type.THROTTLED, throttleKeepShardState.latestDecision());
        assertEquals(0, throttleKeepShardState.relativeApplicationTimeMs());
        assertEquals(1, throttleKeepShardState.consecutiveApplications());
        // We don't reapply throttling if it's already there.
        assertEquals(1, throttler.history.size());

        // Once the period passes, we'll keep the throttle until we reach the maximum period count.
        time.set(11);

        Map<ShardId, ThrottleState> secondPeriodState = calculator.newState(throttleKeepState, List.of(stats), settings, 1);
        var secondPeriodShardState = secondPeriodState.get(shardId);
        assertEquals(Type.THROTTLED, secondPeriodShardState.latestDecision());
        assertEquals(11, secondPeriodShardState.relativeApplicationTimeMs());
        assertEquals(2, secondPeriodShardState.consecutiveApplications());
        // We don't reapply throttling if it's already there.
        assertEquals(1, throttler.history.size());

        time.set(22);

        Map<ShardId, ThrottleState> thirdPeriodState = calculator.newState(secondPeriodState, List.of(stats), settings, 1);
        var thirdPeriodShardState = thirdPeriodState.get(shardId);
        assertEquals(Type.THROTTLED, thirdPeriodShardState.latestDecision());
        assertEquals(22, thirdPeriodShardState.relativeApplicationTimeMs());
        assertEquals(3, thirdPeriodShardState.consecutiveApplications());
        // We don't reapply throttling if it's already there.
        assertEquals(1, throttler.history.size());

        time.set(33);

        Map<ShardId, ThrottleState> unthrottledDueToMaxPeriodsState = calculator.newState(thirdPeriodState, List.of(stats), settings, 1);
        var unthrottledDueToMaxPeriodsShardState = unthrottledDueToMaxPeriodsState.get(shardId);
        assertEquals(Type.THROTTLE_REMOVED, unthrottledDueToMaxPeriodsShardState.latestDecision());
        assertEquals(33, unthrottledDueToMaxPeriodsShardState.relativeApplicationTimeMs());
        assertEquals(1, unthrottledDueToMaxPeriodsShardState.consecutiveApplications());
        assertEquals(2, throttler.history.size());
        assertEquals(shardId, throttler.history.get(1).shardId);
        assertFalse(throttler.history.get(1).throttled);
    }

    public void testRemoveThrottleCooldown() {

    }

    static class MemorizingThrottler implements UploadQueueControllerService.Throttler {
        private final List<Decision> history = new ArrayList<>();
        private final Function<ShardId, Boolean> canActivate;

        MemorizingThrottler(Function<ShardId, Boolean> canActivate) {
            this.canActivate = canActivate;
        }

        MemorizingThrottler() {
            this.canActivate = shardId -> true;
        }

        @Override
        public boolean activate(ShardId shardId) {
            history.add(new Decision(shardId, true));
            return canActivate.apply(shardId);
        }

        @Override
        public void deactivate(ShardId shardId) {
            history.add(new Decision(shardId, false));
        }
    }

    record Decision(ShardId shardId, boolean throttled) {}
}
