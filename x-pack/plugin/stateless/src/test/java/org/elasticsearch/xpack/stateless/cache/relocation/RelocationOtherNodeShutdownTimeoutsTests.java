/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.cache.relocation;

import org.elasticsearch.cluster.routing.allocation.decider.ConcurrentRebalanceAllocationDecider;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.recovery.DataNodeRecoveryThrottlingSettings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.FakeTimeThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeout.TimeoutContext;
import org.elasticsearch.xpack.stateless.cache.SearchRecoveryTimeoutCalculationService;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.cache.relocation.OtherNodeShutdownClusterStates.ShuttingDownNode;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.xpack.stateless.cache.relocation.OtherNodeShutdownClusterStates.RELOCATION_TARGET_NODE;
import static org.elasticsearch.xpack.stateless.cache.relocation.OtherNodeShutdownClusterStates.relocationWithOtherNodeShuttingDown;
import static org.elasticsearch.xpack.stateless.cache.relocation.OtherNodeShutdownClusterStates.relocationWithOtherNodesShuttingDown;
import static org.elasticsearch.xpack.stateless.cache.relocation.OtherNodeShutdownClusterStates.slotSharingSettings;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class RelocationOtherNodeShutdownTimeoutsTests extends ESTestCase {

    private static final TimeValue PLAIN_TIMEOUT =
        SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING.getDefault(Settings.EMPTY);

    /// The shards with a later deadline use the slots as well, as the recoveries are not scheduled by the deadline. Node A has 1 relocating
    /// shard and 4s left, node B 5 relocating shards and 8s left, the limit is 2 concurrent relocations. The 2 shards due by A's deadline
    /// get `2 * 2 / 7` of the slots, so A gives `4s / 2 * 2 * 2 / 7 ≈ 1.1s`, and B gives `8s / 7 * 2 ≈ 2.3s`. The tightest of them
    /// decides.
    public void testChecksEachShutdownGraceDeadline() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final long now = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            // the grace cap is 10s: the deadline of the early node is `now + 6s` and the one of the late node `now + 10s`
            final var state = relocationWithOtherNodesShuttingDown(
                index,
                List.of(new ShuttingDownNode("early-node", 1, now - 4000), new ShuttingDownNode("late-node", 5, now))
            );
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            // equal share for the early node deadline: 4s / (1 + 1), with 2 * (1 + 1) / (1 + 5 + 1) of the slots
            assertThat(plan.timeout().millis(), equalTo(Math.round(4000 / 2.0 * (2.0 * 2 / 7))));

            // once the early deadline has passed its shard may still be queued for a slot, so the wait is over
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 6500);
            final var afterEarlyDeadline = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(afterEarlyDeadline.timeout().millis(), equalTo(0L));
        }
    }

    /// The warming cap does not free a slot: once the deadline of a shutting-down node has passed, its relocations may still be queued
    /// for the slots of the target, so the wait is over, instead of getting the renewable plain timeout. When all the recoveries fit in
    /// the slots, nobody is queued, and the plain timeout stays.
    public void testStopsWaitingOnceThePassedDeadlineRelocationsAreStillQueued() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final long now = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            // the grace cap is 10s: the deadline has passed 1s ago
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 11_000);

            // 3 relocating shards and this one do not fit in the 2 slots
            final var queued = timeouts.timeout(relocationWithOtherNodeShuttingDown(3, index, now), RELOCATION_TARGET_NODE, null);
            assertThat(queued.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(queued.timeout().millis(), equalTo(0L));

            // 1 relocating shard and this one fit in the 2 slots
            final var fitting = timeouts.timeout(relocationWithOtherNodeShuttingDown(1, index, now), RELOCATION_TARGET_NODE, null);
            assertThat(fitting.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE));
            assertThat(fitting.timeout(), equalTo(PLAIN_TIMEOUT));
        }
    }

    /// A relocation with a later deadline competes for the slots of an earlier deadline, as the recoveries are not scheduled by the
    /// deadline. With 2 slots, this shard, 1 shard due in 60s and 1 shard due in 600s, the shard that is due first could queue behind
    /// both of the others. It gets `2 * 2 / 3` of the slots, so the wait is `60s / 2 * 2 * 2 / 3 = 40s`, instead of the 400s that the later
    /// deadline allows.
    public void testALaterDeadlineRelocationCompetesForTheSlotsOfAnEarlierDeadline() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, withGraceCap("10m"));
            threadPool.setCurrentTimeInMillis(randomLongBetween(1_000_000, 2_000_000));
            final long now = threadPool.absoluteTimeInMillis();
            final var state = relocationWithOtherNodesShuttingDown(
                new Index("idx", randomUUID()),
                List.of(new ShuttingDownNode("early-node", 1, now - 540_000), new ShuttingDownNode("late-node", 1, now))
            );

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout().millis(), equalTo(40_000L));
        }
    }

    /// A relocation from a source that is not shutting down while another node shuts down: the target accepts 2 incoming recoveries,
    /// and this shard and the one relocating shard of the shutting-down node fit in them concurrently, so nobody queues and the shard keeps
    /// the full relocation-with-shutdown timeout.
    public void testKeepsFullTimeoutWhenSlotsAreAvailable() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final var state = relocationWithOtherNodeShuttingDown(1, new Index("idx", randomUUID()), threadPool.absoluteTimeInMillis());

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE));
            assertThat(plan.timeout(), equalTo(PLAIN_TIMEOUT));
            assertThat(plan.extendable(), is(true));
        }
    }

    /// With 3 search shards relocating from the shutting-down node, this shard and a limit of 2 concurrent relocations, the 4 competing
    /// shards share the time left until the grace deadline (8s of the 10s cap), 2 at a time: equal share = 8s / 4 * 2 = 4s. The shard
    /// waits for that share, as a longer wait would take slots that the shards of the shutting-down node need.
    public void testSharesFreeSlotsWhenSlotsAreScarce() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodeShuttingDown(3, new Index("idx", randomUUID()), threadPool.absoluteTimeInMillis());
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout().millis(), equalTo(4000L));
        }
    }

    /// The recoveries on the target that are not relocations, as the ones of unassigned shards, use its slots as well. With 3 relocating
    /// shards, this shard and 1 such recovery, the slots (2) are split between 5 recoveries: the 4 competing shards get `2 * 4 / 5 = 1.6`
    /// of them, so the equal share is `8s / 4 * 1.6 = 3.2s`, instead of 4s without the other recovery.
    public void testCountsTheOtherRecoveriesOnTheTargetAsUsingTheSlots() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodesShuttingDown(
                new Index("idx", randomUUID()),
                List.of(new ShuttingDownNode("shutting-node", 3, threadPool.absoluteTimeInMillis())),
                1
            );
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout().millis(), equalTo(3200L));
        }
    }

    /// The critical recoveries do not use the slots for the non-critical ones, only the overall limit. With 4 slots, 2 of them for the
    /// non-critical recoveries, this shard, 1 relocating shard and 1 critical recovery all run at once, so nobody queues and the shard
    /// keeps the plain timeout.
    public void testCriticalRecoveriesDoNotUseTheSlotsOfTheNonCriticalRecoveries() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var settings = Settings.builder()
                .put(slotSharingSettings())
                .put(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(), 4)
                .put(
                    DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_NON_CRITICAL_INCOMING_RECOVERIES_PROPORTION_SETTING.getKey(),
                    "50%"
                )
                .build();
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, settings);
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodesShuttingDown(
                new Index("idx", randomUUID()),
                List.of(new ShuttingDownNode("shutting-node", 1, threadPool.absoluteTimeInMillis())),
                0,
                1
            );
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE));
            assertThat(plan.timeout(), equalTo(PLAIN_TIMEOUT));
        }
    }

    /// The critical recoveries use the overall limit of the slots. With 2 slots, 1 critical recovery leaves 1 for this shard and the
    /// relocating one: the 2 shards get `2 * 1 / 2 = 1` slot between them, so the equal share is `8s / 2 * 1 = 4s`, and not the plain
    /// timeout of the case where both fit.
    public void testCriticalRecoveriesUseTheOverallLimitOfTheSlots() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodesShuttingDown(
                new Index("idx", randomUUID()),
                List.of(new ShuttingDownNode("shutting-node", 1, threadPool.absoluteTimeInMillis())),
                0,
                1
            );
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout().millis(), equalTo(4000L));
        }
    }

    /// The equal share can be longer than the relocation-with-shutdown timeout (60s), when there is a lot of time left. With a 10m grace
    /// period, 5 shards relocating from the shutting-down node and a limit of 2 the equal share is `600s / 6 * 2 = 200s`, and the plan
    /// can be extended when the slots get free.
    public void testEqualShareCanExceedThePlainTimeout() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, withGraceCap("10m"));
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodeShuttingDown(5, new Index("idx", randomUUID()), threadPool.absoluteTimeInMillis());

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout(), equalTo(TimeValue.timeValueSeconds(200)));
            assertThat(plan.extendable(), is(true));
        }
    }

    /// The previous plan was computed while there was no slot contention, so it budgets no per-shard share. The new plan hands out the
    /// equal share as for a first plan.
    public void testComputesLikeAFirstPlanWithoutABudgetedShare() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, withGraceCap("10m"));
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodeShuttingDown(5, new Index("idx", randomUUID()), threadPool.absoluteTimeInMillis());
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 100_000);

            final var withoutShare = new SearchRecoveryTimeout(
                TimeValue.timeValueSeconds(60),
                TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE
            );
            // 500s left, shared by this shard and the 5 relocating ones, 2 at a time
            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, withoutShare);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(plan.timeout().millis(), equalTo(Math.round(500_000 / 6.0 * 2)));
            assertThat(plan.perShardShareMs(), closeTo(500_000 / 6.0, 1e-6));
        }
    }

    /// The shard does not get more than the equal share while the shards of the shutting-down node still compete for the slots. Once all
    /// but one of them are through and everything fits in the slots, the re-evaluation gives the plain relocation-with-shutdown timeout.
    public void testGivesThePlainTimeoutOnceTheSlotsGetFree() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final long shutdownStartedAt = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            // 3 relocating shards compete with this one for 2 slots: equal share = 8s / 4 * 2 = 4s
            final var firstPlan = timeouts.timeout(
                relocationWithOtherNodeShuttingDown(3, index, shutdownStartedAt),
                RELOCATION_TARGET_NODE,
                null
            );
            assertThat(firstPlan.timeout().millis(), equalTo(4000L));
            assertThat(firstPlan.extendable(), is(true));

            // all but one of the shutting-down node's shards are through: this shard and that one fit in the 2 slots
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 6000);
            final var free = relocationWithOtherNodeShuttingDown(1, index, shutdownStartedAt);
            final var extended = timeouts.timeout(free, RELOCATION_TARGET_NODE, firstPlan);
            assertThat(extended.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE));
            assertThat(extended.timeout(), equalTo(PLAIN_TIMEOUT));
        }
    }

    /// A re-evaluation extends the wait only by the time that the shards of the shutting-down node saved by finishing earlier than
    /// budgeted. With a 10m grace period, 5 shards relocating from the shutting-down node and 2 slots, the first plan waits
    /// `600s / 6 * 2 = 200s` and budgets 100s per shard. After those 200s the fresh per-shard share is `400s / (shards left + 1)`. Only
    /// when it is above 100s did shards finish early, and the extension is the difference times the 2 slots. A fresh equal share every
    /// time would add up to the whole time left (200s + 133s + 89s + ...) and hold the slot until the deadline.
    public void testExtendsByTheTimeSavedByFinishedShards() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, withGraceCap("10m"));
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final long shutdownStartedAt = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final var firstPlan = timeouts.timeout(
                relocationWithOtherNodeShuttingDown(5, index, shutdownStartedAt),
                RELOCATION_TARGET_NODE,
                null
            );
            assertThat(firstPlan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(firstPlan.timeout(), equalTo(TimeValue.timeValueSeconds(200)));
            assertThat(firstPlan.perShardShareMs(), closeTo(100_000.0, 1e-6));

            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 200_000);
            // none, the one expected, or one more than that finished: the fresh share is not above the budgeted one
            for (final int relocatingShards : List.of(5, 4, 3)) {
                final var state = relocationWithOtherNodeShuttingDown(relocatingShards, index, shutdownStartedAt);
                assertThat(
                    "no time should be added with " + relocatingShards + " relocating shards, as no time was saved",
                    timeouts.timeout(state, RELOCATION_TARGET_NODE, firstPlan).timeout().millis(),
                    equalTo(0L)
                );
            }

            // 3 of the 5 finished: 400s left for this shard and 2 relocating ones, `400s / 3 - 100s = 33.3s` per slot, for 2 slots
            final var state = relocationWithOtherNodeShuttingDown(2, index, shutdownStartedAt);
            final var extended = timeouts.timeout(state, RELOCATION_TARGET_NODE, firstPlan);
            assertThat(extended.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(extended.timeout().millis(), equalTo(Math.round((400_000 / 3.0 - 100_000) * 2)));
            assertThat(extended.perShardShareMs(), closeTo(400_000 / 3.0, 1e-6));
        }
    }

    /// The slots of the target are shared by all its recoveries, also the ones that are not due by the deadline, so the plan budgets the
    /// time per recovery. When those recoveries finish, the time saved is seen, though the shards due by the deadline are still the same.
    /// With 2 slots, this shard, 1 shard of the shutting-down node and 4 other recoveries, the first plan waits `600s * 2 / 6 = 200s`
    /// and budgets `600s / 6 = 100s` per recovery.
    public void testExtendsWhenRecoveriesThatAreNotDueByTheDeadlineFinish() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, withGraceCap("10m"));
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final long shutdownStartedAt = threadPool.absoluteTimeInMillis();
            final var index = new Index("idx", randomUUID());
            final var shuttingDownNodes = List.of(new ShuttingDownNode("shutting-node", 1, shutdownStartedAt));
            final var firstPlan = timeouts.timeout(
                relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 4),
                RELOCATION_TARGET_NODE,
                null
            );
            assertThat(firstPlan.timeout(), equalTo(TimeValue.timeValueSeconds(200)));
            assertThat(firstPlan.perShardShareMs(), closeTo(100_000.0, 1e-6));

            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 200_000);
            // 2 of the 4 other recoveries finished: 400s / 4 = 100s per recovery, as budgeted
            final var asPlanned = timeouts.timeout(
                relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 2),
                RELOCATION_TARGET_NODE,
                firstPlan
            );
            assertThat(asPlanned.timeout().millis(), equalTo(0L));

            // 3 of them finished: 400s / 3 = 133.3s per recovery, and this shard and the other one get 2 slots of the 3 recoveries
            final var saved = timeouts.timeout(
                relocationWithOtherNodesShuttingDown(index, shuttingDownNodes, 1),
                RELOCATION_TARGET_NODE,
                firstPlan
            );
            assertThat(saved.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_EQUAL_SHARE));
            assertThat(saved.timeout().millis(), equalTo(Math.round((400_000 / 3.0 - 100_000) * 2)));
        }
    }

    /// While a shutting-down node still holds shards that the master has not begun to relocate, as when it throttles the relocations, the
    /// relocations to the target that the cluster state shows are not all the target will get, so the shard waits for the plain
    /// relocation-with-shutdown timeout. The shards that already relocate are enough to share the slots otherwise.
    public void testFallsBackToPlainTimeoutWhileAShuttingDownNodeHoldsPendingShards() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, slotSharingSettings());
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            // the same scenario as when everything relocates: 3 relocating shards, which would share the slots, 8s of the grace left
            final var state = relocationWithOtherNodesShuttingDown(
                new Index("idx", randomUUID()),
                List.of(new ShuttingDownNode("shutting-node", 3, randomIntBetween(1, 3), threadPool.absoluteTimeInMillis()))
            );
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(
                plan.timeoutContext(),
                equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_CLUSTER_SHUTDOWN_METADATA_PRESENT)
            );
            assertThat(plan.timeout(), equalTo(PLAIN_TIMEOUT));
        }
    }

    /// Without a data node limit the target has no slots to share, as all the relocations run at once, so the shard keeps the full
    /// relocation-with-shutdown timeout.
    public void testKeepsFullTimeoutWithoutDataNodeLimit() {
        try (var threadPool = new FakeTimeThreadPool(getTestName(), randomNonNegativeLong() / 2)) {
            final var noDataNodeLimit = Settings.builder()
                .put(slotSharingSettings())
                .putNull(DataNodeRecoveryThrottlingSettings.INDICES_RECOVERY_MAX_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey())
                .build();
            final var timeouts = newRelocationOtherNodeShutdownTimeouts(threadPool, noDataNodeLimit);
            final long shutdownStartedAtMillis = randomLongBetween(1, 100_000);
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis);
            final var state = relocationWithOtherNodeShuttingDown(3, new Index("idx", randomUUID()), threadPool.absoluteTimeInMillis());
            threadPool.setCurrentTimeInMillis(shutdownStartedAtMillis + 2000);

            final var plan = timeouts.timeout(state, RELOCATION_TARGET_NODE, null);
            assertThat(plan.timeoutContext(), equalTo(TimeoutContext.RELOCATION_SOURCE_NOT_SHUTTING_DOWN_SHUTDOWN_SLOTS_AVAILABLE));
            assertThat(plan.timeout(), equalTo(PLAIN_TIMEOUT));
        }
    }

    private static Settings withGraceCap(String gracePeriodCap) {
        return Settings.builder()
            .put(slotSharingSettings())
            .put(SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING.getKey(), gracePeriodCap)
            .build();
    }

    private static RelocationOtherNodeShutdownTimeouts newRelocationOtherNodeShutdownTimeouts(ThreadPool threadPool, Settings settings) {
        final Set<Setting<?>> registeredSettings = new HashSet<>(DataNodeRecoveryThrottlingSettings.settings());
        registeredSettings.addAll(
            List.of(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING,
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING,
                ConcurrentRebalanceAllocationDecider.CLUSTER_ROUTING_ALLOCATION_CLUSTER_CONCURRENT_REBALANCE_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_TIMEOUT_RELOCATION_WITH_SHUTDOWN_SETTING,
                SharedBlobCacheWarmingService.SEARCH_RECOVERY_WARMING_GRACE_PERIOD_CAP_SETTING,
                SearchRecoveryTimeoutCalculationService.OFFLINE_WARMING_SHARE_SLOTS_WITH_SHUTTING_DOWN_NODES_SETTING
            )
        );
        final var clusterSettings = new ClusterSettings(settings, registeredSettings);
        return new RelocationOtherNodeShutdownTimeouts(
            new TargetRelocationSlotsEstimator(clusterSettings, Settings.EMPTY, ByteSizeValue.ZERO),
            new ShutdownWarmingDeadline(clusterSettings),
            threadPool,
            clusterSettings
        );
    }
}
