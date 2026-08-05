/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RerouteService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentCaptor.forClass;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests derive commitments from a fixed cache size and the watermark percentage under test, so a commitment can never land on a
 * watermark boundary. The clock is a manually-advanced {@link AtomicLong}, never a real time source, so throttling assertions are
 * deterministic.
 */
@TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.SharedCacheCapacityMonitor:DEBUG", reason = "debug log for test")
public class SharedCacheCapacityMonitorTests extends ESTestCase {

    private static final long CACHE_SIZE_IN_BYTES = 1000L;
    private static final int LOW_WATERMARK_PERCENT = 75;
    private static final int HIGH_WATERMARK_PERCENT = 95;

    private static final String EXCEEDED_HIGH_WATERMARK_REASON = SharedCacheCapacityMonitor.RerouteDecision.EXCEEDED_HIGH_WATERMARK_REASON;
    private static final String NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON =
        SharedCacheCapacityMonitor.RerouteDecision.NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON;
    private static final String DROPPED_BELOW_LOW_WATERMARK_REASON =
        SharedCacheCapacityMonitor.RerouteDecision.DROPPED_BELOW_LOW_WATERMARK_REASON;

    private static final DiscoveryNode SEARCH_0 = searchNode("search-0");
    private static final DiscoveryNode SEARCH_1 = searchNode("search-1");
    private static final DiscoveryNode SEARCH_2 = searchNode("search-2");
    private static final DiscoveryNode INDEX_NODE = DiscoveryNodeUtils.builder("index-node")
        .name("index-node")
        .roles(Set.of(DiscoveryNodeRole.INDEX_ROLE))
        .build();

    private RerouteService rerouteService;
    private AtomicLong currentTimeMillis;

    @Before
    public void initMocksAndClock() {
        rerouteService = mock(RerouteService.class);
        // lastRerouteTimeMillis starts at 0. The clock must start well beyond the longest reroute_interval used in this file (30
        // seconds), or a small random starting value could make the very first reroute look throttled.
        currentTimeMillis = new AtomicLong(randomLongBetween(TimeValue.timeValueMinutes(10).millis(), 1_000_000_000));
    }

    // -----------------------------------------------------------------------------------------------------------------------
    // Classification behavior, asserted directly against decideReroute's RerouteDecision and NodeWatermarkTransitions.
    // -----------------------------------------------------------------------------------------------------------------------

    public void testRerouteWhenNodeExceedsHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // All nodes start below the high watermark. search-1's low-watermark transition is recorded but has nothing to relieve.
        var currentCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        var previousCommitments = Map.<DiscoveryNode, NodeCacheSizeAndCommitments>of();
        final boolean intervalElapsed = false;
        final SharedCacheCapacityMonitor.RerouteDecision initialDecision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );
        assertThat(initialDecision.shouldReroute(), equalTo(false));
        assertThat(initialDecision.reason(), equalTo(null));
        assertThat(initialDecision.transitions().nodesOverHighWatermark(), equalTo(Set.of()));
        assertThat(initialDecision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(initialDecision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
        assertThat(initialDecision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));

        // One node crosses the high watermark while the anchor node stays below the low watermark, so a reroute fires. The
        // interval has not elapsed, but a newly observed transition bypasses the throttle regardless.
        previousCommitments = currentCommitments;
        currentCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log names the newly over-subscribed node by its short description",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "cache commitments exceeded the high watermark for nodes [search-0/search-0]*"
                )
            );
            final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
                currentCommitments,
                previousCommitments,
                intervalElapsed
            );
            mockLog.assertAllExpectationsMatched();
            assertThat(decision.shouldReroute(), equalTo(true));
            assertThat(decision.reason(), equalTo(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON));
            assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0)));
            assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of(SEARCH_0)));
            assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
            assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
        }
    }

    public void testNoRerouteWhenNodeStaysAboveHighWatermarkAndIntervalNotElapsed() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // The same node reports a slightly higher, still-over-the-high-watermark, commitment. That is not a new transition, so
        // the retry rule applies, and it is blocked while the interval has not elapsed.
        var currentCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 2, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        var previousCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        final boolean intervalElapsed = false;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0)));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
    }

    public void testRetryWhenNodeStaysAboveHighWatermarkAndIntervalElapses() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // Same scenario as above, but the interval has elapsed, so the still-outstanding condition is retried since the
        // earlier reroute may not have relieved the pressure.
        var currentCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 2, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        var previousCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        final boolean intervalElapsed = true;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(true));
        assertThat(decision.reason(), equalTo(EXCEEDED_HIGH_WATERMARK_REASON));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0)));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
    }

    public void testRerouteWhenAdditionalNodeExceedsHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // A second node now also exceeds the high watermark. That is a new transition, so it triggers a reroute, and the debug
        // log names only that newly transitioned node, not the first node's already-known condition.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        final boolean intervalElapsed = false;

        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log names only the newly over-subscribed node, not the one that was already over the watermark",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "cache commitments exceeded the high watermark for nodes [search-1/search-1]*"
                )
            );
            final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
                currentCommitments,
                previousCommitments,
                intervalElapsed
            );
            mockLog.assertAllExpectationsMatched();
            assertThat(decision.shouldReroute(), equalTo(true));
            assertThat(decision.reason(), equalTo(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON));
            assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1)));
            assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of(SEARCH_1)));
            assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
            assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_2)));
        }
    }

    public void testNoRerouteWhenHighWatermarkSetOnlyShrinks() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // Every node that was over the high watermark drops back below it, staying above the low watermark. No node remains
        // over the high watermark, so there is nothing to retry even though the interval has elapsed.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        final boolean intervalElapsed = true;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_2)));
    }

    public void testNoRerouteWhenNoNodeRemainsOverHighWatermarkAndIntervalNotElapsed() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // Same scenario as above, but the interval has not elapsed either. The outcome is identical, since there is nothing
        // outstanding to retry regardless of the interval.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        final boolean intervalElapsed = false;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_2)));
    }

    public void testRerouteWhenNodeDropsBelowLowWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // search-1 drops back below the low watermark while search-2 is over the high watermark, freeing capacity for it and
        // warranting a reroute that names search-1 by its short description in the debug log.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, HIGH_WATERMARK_PERCENT + 1)
        );
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT + 1, SEARCH_2, HIGH_WATERMARK_PERCENT + 1)
        );
        final boolean intervalElapsed = false;

        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "log names the node that dropped below the low watermark by its short description",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "cache commitments dropped below the low watermark for nodes [search-1/search-1]*"
                )
            );
            final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
                currentCommitments,
                previousCommitments,
                intervalElapsed
            );
            mockLog.assertAllExpectationsMatched();
            assertThat(decision.shouldReroute(), equalTo(true));
            assertThat(decision.reason(), equalTo(DROPPED_BELOW_LOW_WATERMARK_REASON));
            assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_2)));
            assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
            assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
            assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1)));
        }
    }

    public void testNoRerouteWhenNodeDropsBelowLowWatermarkButNoNodeIsOverHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // search-0 drops below the low watermark, but no search node is over the high watermark, so the drop has nothing to
        // relieve and must not trigger a reroute on its own.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        final boolean intervalElapsed = false;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of(SEARCH_0)));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1, SEARCH_2)));
    }

    public void testNoRerouteWhenNodeDepartsRatherThanDropping() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // search-0 leaves the cluster entirely rather than its commitment dropping. Its absence must not be mistaken for a drop
        // below the low watermark.
        var currentCommitments = commitmentsAt(Map.of(SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1));
        var previousCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );
        final boolean intervalElapsed = true;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1, SEARCH_2)));
    }

    public void testRerouteWhenNewNodeJoinsAlreadyAboveHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // search-2 joins already above the high watermark. It has no prior commitment to compare against, but it must still be
        // reported as newly exceeding the high watermark.
        var currentCommitments = commitmentsAt(
            Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, HIGH_WATERMARK_PERCENT + 1)
        );
        var previousCommitments = commitmentsAt(Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        final boolean intervalElapsed = false;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(true));
        assertThat(decision.reason(), equalTo(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_2)));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of(SEARCH_2)));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1)));
    }

    public void testNoRerouteWhenAllSearchNodesOverSubscribed() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO);

        // Both nodes exceed the high watermark simultaneously, so no search node is below the low watermark and there is nowhere
        // to move shards to. The monitor must not reroute even though both nodes newly crossed the high watermark.
        var currentCommitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1));
        var previousCommitments = commitmentsAt(Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        final boolean intervalElapsed = true;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(false));
        assertThat(decision.reason(), equalTo(null));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1)));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of(SEARCH_0, SEARCH_1)));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of()));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of()));
    }

    public void testTotalAccountingModeSumsBoostedAndUnboosted() {
        final SharedCacheCapacityMonitor monitor = createMonitor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL,
            TimeValue.ZERO
        );

        // Neither the boosted nor the unboosted commitment alone exceeds the high watermark, but their sum does. Only the TOTAL
        // accounting mode should catch this.
        final long halfOfHighWatermark = bytesForPercent(HIGH_WATERMARK_PERCENT) / 2 + 1;
        var currentCommitments = Map.of(
            SEARCH_0,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, halfOfHighWatermark, halfOfHighWatermark),
            SEARCH_1,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L)
        );
        var previousCommitments = Map.<DiscoveryNode, NodeCacheSizeAndCommitments>of();
        final boolean intervalElapsed = false;

        final SharedCacheCapacityMonitor.RerouteDecision decision = monitor.decideReroute(
            currentCommitments,
            previousCommitments,
            intervalElapsed
        );

        assertThat(decision.shouldReroute(), equalTo(true));
        assertThat(decision.reason(), equalTo(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON));
        assertThat(decision.transitions().nodesOverHighWatermark(), equalTo(Set.of(SEARCH_0)));
        assertThat(decision.transitions().nodesNewlyExceedingHighWatermark(), equalTo(Set.of(SEARCH_0)));
        assertThat(decision.transitions().nodesNewlyDroppedBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
        assertThat(decision.transitions().nodesBelowLowWatermark(), equalTo(Set.of(SEARCH_1)));
    }

    // -----------------------------------------------------------------------------------------------------------------------
    // onNewInfo behavior tests
    // -----------------------------------------------------------------------------------------------------------------------

    public void testNoRerouteWhenStateIsNotRecovered() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, () -> {
            final ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE);
            builder.blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build());
            return builder.build();
        });

        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);
    }

    public void testNoRerouteWhenDisabled() {
        final SharedCacheCapacityMonitor monitor = createMonitor(false, TimeValue.ZERO, this::twoSearchNodeState);

        // search-0 is fully committed and search-1 sits below the low watermark, so there is somewhere to move shards to, and
        // every other condition for a reroute is satisfied. Only the disabled guard explains why no reroute happens.
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, 100, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);
    }

    public void testNoRerouteWhenCanRemainDisabled() {
        // The decider as a whole is enabled, but canRemain specifically is not, so a reroute would give canRemain no chance to
        // act on the high watermark.
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey(), false).build()
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::twoSearchNodeState,
            rerouteService
        );

        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);
    }

    public void testDisablingResetsRecordedCommitments() {
        // A non-zero reroute interval, with the clock never advanced across this test, blocks the retry path entirely, so a
        // reroute on the final call can only be explained by the recorded commitments having been reset, not by the interval
        // having elapsed.
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            rerouteInterval
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::twoSearchNodeState,
            rerouteService
        );

        // search-0 crosses the high watermark while enabled, recording it as already known on the next call.
        var commitments = commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1));
        monitor.onNewInfo(clusterInfoOf(commitments));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // Disabling the decider must reset the recorded commitments, not just suppress the reroute.
        clusterSettings.applySettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), false).build());
        monitor.onNewInfo(clusterInfoOf(commitments));
        verifyNoInteractions(rerouteService);

        // Re-enabling and reporting the exact same commitments, with search-0 still over the high watermark, must reroute
        // again as a fresh transition. Had the recorded commitments not been reset, search-0 would look unchanged, and with
        // the interval nowhere near elapsed, this call would see no reason to reroute at all.
        clusterSettings.applySettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), true).build());
        monitor.onNewInfo(clusterInfoOf(commitments));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testNonSearchNodesAreIgnoredEvenWhenConditionsWouldOtherwiseWarrantReroute() {
        final SharedCacheCapacityMonitor monitor = createMonitor(
            true,
            TimeValue.ZERO,
            () -> ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(SEARCH_0).add(INDEX_NODE)).build()
        );

        // A first call establishes search-0 as already below the low watermark, isolating the index node's effect as the only
        // variable under test on the second call.
        monitor.onNewInfo(
            clusterInfoOf(
                Map.of(SEARCH_0, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L))
            )
        );
        verifyNoInteractions(rerouteService);
        reset(rerouteService);

        // search-0 still sits below the low watermark and the index node is fully committed, exceeding both watermarks. Every
        // condition for a reroute is satisfied, but reroute should ignore index nodes.
        var commitments = Map.of(
            SEARCH_0,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L),
            INDEX_NODE,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(100), 0L)
        );
        monitor.onNewInfo(clusterInfoOf(commitments));
        verifyNoInteractions(rerouteService);
    }

    public void testSearchNodeMissingFromClusterInfoIsSkipped() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::twoSearchNodeState);

        // A first call establishes search-0 as already below the low watermark, isolating search-1's missing ClusterInfo entry
        // as the only variable under test on the second call.
        monitor.onNewInfo(
            clusterInfoOf(
                Map.of(SEARCH_0, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L))
            )
        );
        verifyNoInteractions(rerouteService);
        reset(rerouteService);

        // search-1 is present in cluster state but has no recorded cache commitments yet. The monitor must not throw and must
        // simply classify search-0 alone, which stays below the low watermark, so no reroute is warranted.
        monitor.onNewInfo(
            clusterInfoOf(
                Map.of(SEARCH_0, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L))
            )
        );
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteWithinIntervalWhenAnotherNodeNewlyExceedsHighWatermark() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // A different node now also crosses the high watermark, and the clock has advanced by less than the reroute interval.
        // A newly observed transition always bypasses the throttle, so the reroute still fires.
        currentTimeMillis.addAndGet(rerouteInterval.millis() - 1);
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testNoRetryWithinIntervalWhenNoNodeNewlyExceedsHighWatermark() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // The same commitments are reported again with the clock advanced by less than the reroute interval. search-0 remains
        // over the high watermark, but that is not a new transition, so the retry is blocked until the interval elapses.
        currentTimeMillis.addAndGet(randomLongBetween(1, rerouteInterval.millis() - 1));
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        verifyNoInteractions(rerouteService);
    }

    public void testRetryAfterIntervalElapsesWhenNoNodeNewlyExceedsHighWatermark() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // Once the interval has fully elapsed, search-0 remaining over the high watermark is retried even though nothing newly
        // crossed a watermark this time. The earlier reroute may not have relieved the pressure, for example due to
        // concurrent-movement throttling elsewhere that has since cleared.
        currentTimeMillis.addAndGet(randomLongBetween(rerouteInterval.millis(), rerouteInterval.millis() * 10));
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testNoRetryWhenAllSearchNodesOverSubscribedEvenAfterIntervalElapses() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::twoSearchNodeState);

        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // search-1 also crosses the high watermark, and the interval has fully elapsed, but every search node is now over the
        // low watermark, so there is nowhere left to move shards to. The retry is suppressed the same way a new transition
        // would be.
        currentTimeMillis.addAndGet(randomLongBetween(rerouteInterval.millis(), rerouteInterval.millis() * 10));
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1))));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteWhenNewNodeJoinsBelowLowWatermarkAfterAllSearchNodesWereOverSubscribed() {
        // search-3 joins from the second call onward, already below the low watermark. This is new capacity information, just
        // like an existing node dropping below the low watermark, and must bypass the throttle immediately.
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(threeSearchNodeState());
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, clusterState::get);

        // search-0, search-1, and search-2 all exceed the high watermark, so there is nowhere to move shards to. The reroute is
        // suppressed even though this is a genuine transition.
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, HIGH_WATERMARK_PERCENT + 1, SEARCH_2, HIGH_WATERMARK_PERCENT + 1)
                )
            )
        );
        verifyNoInteractions(rerouteService);

        // search-3 joins, already below the low watermark, freeing up somewhere to move shards to. search-0, search-1, and
        // search-2's over-high-watermark condition is not new, and the interval has not elapsed, but search-3's own
        // low-watermark transition is new and bypasses the throttle.
        final DiscoveryNode search3 = searchNode("search-3");
        clusterState.set(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(DiscoveryNodes.builder().add(SEARCH_0).add(SEARCH_1).add(SEARCH_2).add(search3))
                .build()
        );
        final Map<DiscoveryNode, NodeCacheSizeAndCommitments> commitmentsWithSearch3 = new HashMap<>();
        commitmentsWithSearch3.put(
            SEARCH_0,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L)
        );
        commitmentsWithSearch3.put(
            SEARCH_1,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L)
        );
        commitmentsWithSearch3.put(
            SEARCH_2,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L)
        );
        commitmentsWithSearch3.put(
            search3,
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L)
        );
        monitor.onNewInfo(clusterInfoOf(commitmentsWithSearch3));
        assertRerouted(DROPPED_BELOW_LOW_WATERMARK_REASON);
    }

    public void testRerouteForLowWatermarkDropBypassesIntervalButIsNotRetried() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT + 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // search-1 drops back below the low watermark, and the clock has advanced by less than the interval. A newly observed
        // transition always bypasses the throttle, so the reroute still fires.
        currentTimeMillis.addAndGet(rerouteInterval.millis() - 1);
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(DROPPED_BELOW_LOW_WATERMARK_REASON);
        reset(rerouteService);

        // The same commitments are reported again with no further change. search-0's over-high condition is retried once the
        // interval elapses, but the low-watermark drop is a one-shot signal and is not retried.
        currentTimeMillis.addAndGet(rerouteInterval.millis());
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
                )
            )
        );
        assertRerouted(EXCEEDED_HIGH_WATERMARK_REASON);
    }

    // -----------------------------------------------------------------------------------------------------------------------
    // Live settings updates. These apply a follow-up setting change to the same monitor instance and observe the effect on
    // the very next onNewInfo call, so they drive onNewInfo rather than decideReroute directly.
    // -----------------------------------------------------------------------------------------------------------------------

    public void testEnabledSettingChangeIsObservedOnTheSameInstance() {
        final ClusterSettings clusterSettings = clusterSettingsFor(
            false,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::twoSearchNodeState,
            rerouteService
        );

        // The decider is disabled at construction time, so an over-subscribed node does not trigger a reroute yet.
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);

        // Enabling the setting on the live cluster settings, without constructing a new monitor, changes behavior immediately.
        clusterSettings.applySettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), true).build());
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testCanRemainEnabledSettingChangeIsObservedOnTheSameInstance() {
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey(), false).build()
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::twoSearchNodeState,
            rerouteService
        );

        // canRemain is disabled at construction time, so an over-subscribed node does not trigger a reroute yet.
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);

        // Enabling the setting on the live cluster settings, without constructing a new monitor, changes behavior immediately.
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey(), true).build()
        );
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testAccountingModeSettingChangeIsObservedOnTheSameInstance() {
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(twoSearchNodeState());
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            clusterState::get,
            rerouteService
        );

        // search-0 and search-1 are both newly observed below the low watermark, but no node is over the high watermark, so
        // there is nothing to relieve yet.
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);

        // Switch the same live instance to TOTAL accounting before search-2 is ever observed.
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), "TOTAL").build()
        );

        // search-2 joins. Neither its boosted nor its unboosted commitment alone exceeds the high watermark, but their sum
        // does. Only TOTAL accounting, already in effect when search-2 is first classified, reports it as newly exceeding.
        clusterState.set(threeSearchNodeState());
        final long halfOfHighWatermark = bytesForPercent(HIGH_WATERMARK_PERCENT) / 2 + 1;
        var commitments = new HashMap<DiscoveryNode, NodeCacheSizeAndCommitments>();
        commitments.put(SEARCH_0, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put(SEARCH_1, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put(SEARCH_2, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, halfOfHighWatermark, halfOfHighWatermark));
        monitor.onNewInfo(clusterInfoOf(commitments));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testHighWatermarkSettingChangeIsObservedOnTheSameInstance() {
        // A node whose commitment is unchanged is judged the same way regardless of when a setting changed, since both its
        // current and prior state are evaluated against whichever watermark is live at classification time. search-1 joins for
        // the first time in the same call the lowered watermark first applies, so its lack of a prior commitment is what lets
        // the setting change explain the reroute, rather than a change in search-1's own commitment.
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(SEARCH_0)).build()
        );
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            clusterState::get,
            rerouteService
        );

        // search-0's commitment sits below the low watermark, anchoring it as somewhere shards could move to, but no node is
        // over the high watermark, so there is nothing to relieve yet.
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1))));
        verifyNoInteractions(rerouteService);

        // Lower the high watermark on the same live instance before search-1 is ever observed.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), (HIGH_WATERMARK_PERCENT - 2) + "%")
                .build()
        );

        // search-1 joins with a commitment that exceeds the lowered high watermark but not the original one, while search-0
        // stays below the low watermark. Only the watermark live when search-1 is first classified can explain the reroute.
        clusterState.set(
            ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(SEARCH_0).add(SEARCH_1)).build()
        );
        monitor.onNewInfo(clusterInfoOf(commitmentsAt(Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, HIGH_WATERMARK_PERCENT - 1))));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
    }

    public void testLowWatermarkSettingChangeIsObservedOnTheSameInstance() {
        // A fourth node joins on the final call, isolating the setting change as the only variable, for the same reason given
        // in testAccountingModeSettingChangeIsObservedOnTheSameInstance.
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(threeSearchNodeState());
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.ZERO
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            clusterState::get,
            rerouteService
        );

        // search-2 starts above the high watermark, while search-0 sits below the still-unchanged 75% low watermark, so this
        // call must reroute.
        monitor.onNewInfo(
            clusterInfoOf(
                commitmentsAt(
                    Map.of(SEARCH_0, LOW_WATERMARK_PERCENT - 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, HIGH_WATERMARK_PERCENT + 1)
                )
            )
        );
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // Lower the low watermark below search-0's unchanged commitment before search-3 is ever observed.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING.getKey(), (LOW_WATERMARK_PERCENT - 2) + "%")
                .build()
        );

        // search-3 joins, already above the high watermark. With the lowered low watermark now in effect, search-0's unchanged
        // commitment no longer counts as below it, so there is nowhere to move shards to and the reroute is suppressed.
        final DiscoveryNode search3 = searchNode("search-3");
        clusterState.set(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(DiscoveryNodes.builder().add(SEARCH_0).add(SEARCH_1).add(SEARCH_2).add(search3))
                .build()
        );
        var commitments = new HashMap<DiscoveryNode, NodeCacheSizeAndCommitments>();
        commitments.put(SEARCH_0, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put(SEARCH_1, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put(SEARCH_2, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L));
        commitments.put(search3, new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L));
        monitor.onNewInfo(clusterInfoOf(commitments));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteIntervalSettingChangeIsObservedOnTheSameInstance() {
        // A newly joining over-watermark node would bypass the throttle and reroute regardless of the interval setting, so the
        // same already-over-watermark node is reused with an unchanged commitment across every call, isolating the retry rule's
        // response to the interval setting.
        final TimeValue initialRerouteInterval = TimeValue.timeValueSeconds(30);
        final long delta = randomLongBetween(1, initialRerouteInterval.millis() - 1);
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            initialRerouteInterval
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::threeSearchNodeState,
            rerouteService
        );

        var commitments = commitmentsAt(
            Map.of(SEARCH_0, HIGH_WATERMARK_PERCENT + 1, SEARCH_1, LOW_WATERMARK_PERCENT - 1, SEARCH_2, LOW_WATERMARK_PERCENT - 1)
        );

        monitor.onNewInfo(clusterInfoOf(commitments));
        assertRerouted(NEW_NODES_EXCEEDED_HIGH_WATERMARK_REASON);
        reset(rerouteService);

        // The same commitments are reported again. search-0 remains over the high watermark, but the clock has advanced by
        // only delta milliseconds against the still-unchanged 30 second interval, so the retry is throttled.
        currentTimeMillis.addAndGet(delta);
        monitor.onNewInfo(clusterInfoOf(commitments));
        verifyNoInteractions(rerouteService);

        // Lowering the interval on the same live instance, to less than the delta already elapsed since the last reroute, must
        // remove the throttle on the very next call even though the clock does not advance any further and no node's commitment
        // changes again.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(delta - 1))
                .build()
        );
        monitor.onNewInfo(clusterInfoOf(commitments));
        assertRerouted(EXCEEDED_HIGH_WATERMARK_REASON);
    }

    /**
     * Asserts that exactly one reroute was requested, with the given reason, at {@link Priority#NORMAL}.
     */
    private void assertRerouted(String expectedReason) {
        final var reasonCaptor = forClass(String.class);
        verify(rerouteService).reroute(reasonCaptor.capture(), eq(Priority.NORMAL), any());
        assertThat(reasonCaptor.getValue(), equalTo(expectedReason));
    }

    private ClusterState twoSearchNodeState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE).nodes(DiscoveryNodes.builder().add(SEARCH_0).add(SEARCH_1)).build();
    }

    private ClusterState threeSearchNodeState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(DiscoveryNodes.builder().add(SEARCH_0).add(SEARCH_1).add(SEARCH_2))
            .build();
    }

    private static DiscoveryNode searchNode(String id) {
        return DiscoveryNodeUtils.builder(id).name(id).ephemeralId(id).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build();
    }

    private ClusterInfo clusterInfoOf(Map<DiscoveryNode, NodeCacheSizeAndCommitments> commitmentsByNode) {
        final Map<String, NodeCacheSizeAndCommitments> commitmentsById = commitmentsByNode.entrySet()
            .stream()
            .collect(Collectors.toMap(entry -> entry.getKey().getId(), Map.Entry::getValue));
        return ClusterInfo.builder().nodeCacheSizeAndCommitments(commitmentsById).build();
    }

    /**
     * Builds a commitments map keyed by node, with each node's commitment expressed as a percentage of {@link #CACHE_SIZE_IN_BYTES}
     * recorded entirely as a boosted commitment.
     */
    private static Map<DiscoveryNode, NodeCacheSizeAndCommitments> commitmentsAt(Map<DiscoveryNode, Integer> percentByNode) {
        return percentByNode.entrySet()
            .stream()
            .collect(
                Collectors.toMap(
                    Map.Entry::getKey,
                    entry -> new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(entry.getValue()), 0L)
                )
            );
    }

    private static long bytesForPercent(int percent) {
        return CACHE_SIZE_IN_BYTES * percent / 100;
    }

    private SharedCacheCapacityMonitor createMonitor(boolean enabled, TimeValue rerouteInterval) {
        return createMonitor(enabled, SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED, rerouteInterval);
    }

    private SharedCacheCapacityMonitor createMonitor(
        boolean enabled,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        TimeValue rerouteInterval
    ) {
        return createMonitor(enabled, accountingMode, rerouteInterval, () -> {
            throw new AssertionError("cluster state supplier should not be invoked");
        });
    }

    private SharedCacheCapacityMonitor createMonitor(
        boolean enabled,
        TimeValue rerouteInterval,
        Supplier<ClusterState> clusterStateSupplier
    ) {
        return createMonitor(
            enabled,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            rerouteInterval,
            clusterStateSupplier
        );
    }

    private SharedCacheCapacityMonitor createMonitor(
        boolean enabled,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        TimeValue rerouteInterval,
        Supplier<ClusterState> clusterStateSupplier
    ) {
        final ClusterSettings clusterSettings = clusterSettingsFor(enabled, accountingMode, rerouteInterval);
        return new SharedCacheCapacityMonitor(clusterSettings, currentTimeMillis::get, clusterStateSupplier, rerouteService);
    }

    /**
     * Builds a live, mutable {@link ClusterSettings} rather than a one-shot {@link Settings} object, so a test can apply a follow-up
     * setting update to the same instance a monitor was constructed with and observe the effect on the very next {@code onNewInfo}
     * call, without constructing a second monitor.
     */
    private ClusterSettings clusterSettingsFor(
        boolean enabled,
        SharedCacheCapacityAllocationDecider.CacheAccountingMode accountingMode,
        TimeValue rerouteInterval
    ) {
        return new ClusterSettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), enabled)
                .put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), accountingMode.name())
                .put(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING.getKey(), LOW_WATERMARK_PERCENT + "%")
                .put(SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), HIGH_WATERMARK_PERCENT + "%")
                .put(SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING.getKey(), rerouteInterval)
                .build(),
            Set.of(
                SharedCacheCapacityAllocationDecider.ENABLED_SETTING,
                SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING,
                SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING,
                SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING
            )
        );
    }
}
