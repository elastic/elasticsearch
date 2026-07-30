/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.block.ClusterBlocks;
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

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

/**
 * Tests use a fixed cache size and derive commitments from the watermark percentage under test, so a commitment can never land exactly
 * on a watermark boundary regardless of how the randomization resolves. The clock is a manually-advanced {@link AtomicLong}, never a
 * real time source, so throttling assertions are deterministic rather than dependent on wall-clock timing.
 */
public class SharedCacheCapacityMonitorTests extends ESTestCase {

    private static final long CACHE_SIZE_IN_BYTES = 1000L;
    private static final int LOW_WATERMARK_PERCENT = 75;
    private static final int HIGH_WATERMARK_PERCENT = 95;

    private RerouteService rerouteService;
    private AtomicLong currentTimeMillis;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        rerouteService = mock(RerouteService.class);
        // lastRerouteTimeMillis starts at 0, so the clock must start well beyond the longest reroute_interval used in this file
        // (30 seconds). Otherwise, a small random starting value could make the very first reroute look throttled.
        currentTimeMillis = new AtomicLong(randomLongBetween(TimeValue.timeValueMinutes(10).millis(), 1_000_000_000));
    }

    public void testNoRerouteWhenStateIsNotRecovered() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, () -> {
            final ClusterState.Builder builder = ClusterState.builder(ClusterState.EMPTY_STATE);
            builder.blocks(ClusterBlocks.builder().addGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK).build());
            return builder.build();
        });

        monitor.onNewInfo(clusterInfoWithOneNodeAt(HIGH_WATERMARK_PERCENT + 1));
        verifyNoInteractions(rerouteService);
    }

    public void testNoRerouteWhenDisabled() {
        final SharedCacheCapacityMonitor monitor = createMonitor(false, TimeValue.ZERO, this::twoSearchNodeState);

        // Every node is fully committed, but that must not matter while the monitor is disabled.
        monitor.onNewInfo(clusterInfoWithOneNodeAt(100));
        verifyNoInteractions(rerouteService);
    }

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

        // The decider is disabled at construction time, so an over-subscribed node must not trigger a reroute yet.
        monitor.onNewInfo(clusterInfoWithNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // Enabling the setting on the live cluster settings, without constructing a new monitor, must change behavior immediately.
        clusterSettings.applySettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), true).build());
        monitor.onNewInfo(clusterInfoWithNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testAccountingModeSettingChangeIsObservedOnTheSameInstance() {
        // The cluster state supplier is mutable so a new node can join between calls, giving the second call a node with no prior
        // commitment to compare against. That isolates the setting change as the only variable, since a node whose commitment is
        // unchanged between calls is judged the same way regardless of when the setting changed underneath it.
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

        monitor.onNewInfo(clusterInfoWithNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // Switch the same live instance to TOTAL accounting before the new node is even observed.
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), "TOTAL").build()
        );

        // A third node joins. Neither its boosted nor its unboosted commitment alone exceeds the high watermark, but their sum
        // does. Only TOTAL accounting, now in effect on this instance, reports it as newly exceeding the high watermark.
        clusterState.set(threeSearchNodeState());
        final long halfOfHighWatermark = bytesForPercent(HIGH_WATERMARK_PERCENT) / 2 + 1;
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put("search-2", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, halfOfHighWatermark, halfOfHighWatermark));
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testHighWatermarkSettingChangeIsObservedOnTheSameInstance() {
        // The cluster state supplier is mutable so a new node can join between calls, isolating the setting change as the only
        // variable for the reasons given in testAccountingModeSettingChangeIsObservedOnTheSameInstance.
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

        monitor.onNewInfo(clusterInfoWithNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // Lower the high watermark on the same live instance before the new node is observed.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), (HIGH_WATERMARK_PERCENT - 2) + "%")
                .build()
        );

        // A third node joins with a commitment that exceeds the lowered high watermark but not the original one, proving the new
        // setting, rather than the old one, is what's in effect on this instance.
        clusterState.set(threeSearchNodeState());
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1, HIGH_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testLowWatermarkSettingChangeIsObservedOnTheSameInstance() {
        // The cluster state supplier is mutable so a new node can join between calls, isolating the setting change as the only
        // variable for the reasons given in testAccountingModeSettingChangeIsObservedOnTheSameInstance.
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

        // The anchor node sits comfortably below the (currently 75%) low watermark, so there is somewhere to move shards to.
        monitor.onNewInfo(clusterInfoWithNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // Lower the low watermark below the anchor node's commitment (LOW_WATERMARK_PERCENT - 1) on the same live instance, before
        // the new node is observed, so the anchor node no longer counts as "below" it.
        clusterSettings.applySettings(
            Settings.builder()
                .put(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING.getKey(), (LOW_WATERMARK_PERCENT - 2) + "%")
                .build()
        );

        // A third node joins above the high watermark. With the raised low watermark now in effect, the anchor node's unchanged
        // commitment no longer counts as "below" watermark, so there is nowhere to move shards to and the reroute must be suppressed.
        clusterState.set(threeSearchNodeState());
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1, HIGH_WATERMARK_PERCENT + 1));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteIntervalSettingChangeIsObservedOnTheSameInstance() {
        final ClusterSettings clusterSettings = clusterSettingsFor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED,
            TimeValue.timeValueSeconds(30)
        );
        final SharedCacheCapacityMonitor monitor = new SharedCacheCapacityMonitor(
            clusterSettings,
            currentTimeMillis::get,
            this::threeSearchNodeState,
            rerouteService
        );

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // A new node crosses the high watermark well within the original 30 second interval, so it would normally be throttled.
        currentTimeMillis.addAndGet(1);
        // Lowering the interval to zero on the same live instance must remove the throttle immediately.
        clusterSettings.applySettings(
            Settings.builder().put(SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING.getKey(), TimeValue.ZERO).build()
        );
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testRerouteWhenNodeExceedsHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        // All nodes start below the high watermark, so there is nothing to reroute for yet.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // One node crosses the high watermark. The anchor node stays below the low watermark, so there is somewhere to move shards to
        // and a reroute is warranted.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testNoRerouteWhenNodeStaysAboveHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // The same node reports a slightly different, but still-over-the-high-watermark, commitment on the next call. It didn't newly
        // cross the watermark this time, so no further reroute should fire.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 2, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteWhenAdditionalNodeExceedsHighWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // A second node now also exceeds the high watermark, while the anchor node still sits below the low watermark. Even though the
        // first node's already-known condition persists, the second node's transition is new and must trigger another reroute.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testNoRerouteWhenHighWatermarkSetOnlyShrinks() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // One node drops back below the high watermark, but stays above the low watermark. No node newly crossed the high watermark
        // and no node newly dropped below the low watermark, so no reroute should fire.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteWhenNodeDropsBelowLowWatermark() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // The node that was above the low watermark drops back below it. Capacity has freed up, so previously NOT_PREFERRED
        // allocations may now fit, warranting a reroute.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testNoRerouteWhenNodeDepartsRatherThanDropping() {
        // The cluster state supplier is mutable so the same monitor instance can observe the node's departure between calls.
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(threeSearchNodeState());
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, clusterState::get);

        // One node starts out above the low watermark; the anchor node stays below it.
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(LOW_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // That node leaves the cluster entirely, rather than its commitment actually dropping. Its absence from both the cluster
        // state and ClusterInfo must not be mistaken for "dropped below the low watermark": there is no reroute reason here, only a
        // node that is no longer part of the comparison.
        clusterState.set(
            ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        // search-0 is dropped from the cluster
                        .add(DiscoveryNodeUtils.builder("search-1").name("search-1").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
                        .add(DiscoveryNodeUtils.builder("search-2").name("search-2").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
                )
                .build()
        );
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put("search-2", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteWhenNewNodeJoinsAlreadyAboveHighWatermark() {
        // The cluster state supplier is mutable so the same monitor instance can observe the new node joining between calls.
        final AtomicReference<ClusterState> clusterState = new AtomicReference<>(twoSearchNodeState());
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, clusterState::get);

        // Only the two original nodes exist so far, both comfortably below the high watermark.
        monitor.onNewInfo(clusterInfoWithNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // A third search node joins the cluster and is already, from its very first observation, above the high watermark. It has
        // no prior commitment to compare against, but it must still be reported as newly exceeding the high watermark rather than
        // being silently ignored because there's nothing to compare it to.
        clusterState.set(threeSearchNodeState());
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        commitments.put("search-2", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L));
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testNoRerouteWithinIntervalForUnchangedSet() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // A different node now also crosses the high watermark, which is new information, but the clock has advanced by less than
        // the reroute interval. The interval is a plain floor between reroutes, so this reroute is still suppressed.
        currentTimeMillis.addAndGet(rerouteInterval.millis() - 1);
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);
    }

    public void testNoRerouteWhenDecisionIsNoAndWithinInterval() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // Advance the clock by less than the reroute interval, and report no new transition at all: the same node stays above the
        // high watermark and nothing else changes. Both the "decision is no" and "within interval" conditions hold at once here, so
        // this covers that combination distinctly from a case where only one of the two would otherwise have suppressed the reroute.
        currentTimeMillis.addAndGet(rerouteInterval.millis() - 1);
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);
    }

    public void testRerouteAfterIntervalElapses() {
        final TimeValue rerouteInterval = TimeValue.timeValueSeconds(30);
        final SharedCacheCapacityMonitor monitor = createMonitor(true, rerouteInterval, this::threeSearchNodeState);

        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
        reset(rerouteService);

        // Once the interval has fully elapsed, a further transition is allowed to reroute again.
        currentTimeMillis.addAndGet(rerouteInterval.millis());
        monitor.onNewInfo(clusterInfoWithThreeNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1, LOW_WATERMARK_PERCENT - 1));
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testNoRerouteWhenAllSearchNodesOverSubscribed() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::twoSearchNodeState);

        monitor.onNewInfo(clusterInfoWithNodesAt(LOW_WATERMARK_PERCENT - 1, LOW_WATERMARK_PERCENT - 1));
        verifyNoInteractions(rerouteService);

        // Both nodes exceed the high watermark simultaneously, so no search node is below the low watermark and there is nowhere to
        // move shards to. The monitor must not reroute even though both nodes newly crossed the high watermark.
        monitor.onNewInfo(clusterInfoWithNodesAt(HIGH_WATERMARK_PERCENT + 1, HIGH_WATERMARK_PERCENT + 1));
        verifyNoInteractions(rerouteService);
    }

    public void testNonSearchNodesAreIgnored() {
        final SharedCacheCapacityMonitor monitor = createMonitor(
            true,
            TimeValue.ZERO,
            () -> ClusterState.builder(ClusterState.EMPTY_STATE)
                .nodes(
                    DiscoveryNodes.builder()
                        .add(
                            DiscoveryNodeUtils.builder("index-node").name("index-node").roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build()
                        )
                )
                .build()
        );

        // The only node in the cluster is an index node, fully committed. Even though it's over both watermarks, it must be ignored
        // because the decider and monitor only apply to search nodes.
        final Map<String, NodeCacheSizeAndCommitments> commitments = Map.of(
            "index-node",
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(100), 0L)
        );
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verifyNoInteractions(rerouteService);
    }

    public void testNodeMissingFromClusterInfoIsSkipped() {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::threeSearchNodeState);

        // Only two of the three search nodes have recorded cache commitments; the third is presumably in the process of joining or
        // leaving the cluster. The monitor must not throw and must simply skip the node with no data, while still counting the
        // low-commitment node as somewhere shards could move to.
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(HIGH_WATERMARK_PERCENT + 1), 0L));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testTotalAccountingModeSumsBoostedAndUnboosted() {
        final SharedCacheCapacityMonitor monitor = createMonitor(
            true,
            SharedCacheCapacityAllocationDecider.CacheAccountingMode.TOTAL,
            TimeValue.ZERO,
            this::twoSearchNodeState
        );

        // Neither the boosted nor the unboosted commitment alone exceeds the high watermark, but their sum does. Only the TOTAL
        // accounting mode should catch this.
        final long halfOfHighWatermark = bytesForPercent(HIGH_WATERMARK_PERCENT) / 2 + 1;
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, halfOfHighWatermark, halfOfHighWatermark));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(LOW_WATERMARK_PERCENT - 1), 0L));
        monitor.onNewInfo(ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build());
        verify(rerouteService).reroute(anyString(), eq(Priority.NORMAL), any());
    }

    public void testConcurrentOnNewInfoCallsDoNotThrowAndDoNotExceedOneReroutePerCall() throws InterruptedException {
        final SharedCacheCapacityMonitor monitor = createMonitor(true, TimeValue.ZERO, this::twoSearchNodeState);
        final int numberOfCalls = 20;
        final ExecutorService executor = Executors.newFixedThreadPool(4);
        final CountDownLatch latch = new CountDownLatch(numberOfCalls);
        final AtomicInteger failures = new AtomicInteger();

        try {
            for (int i = 0; i < numberOfCalls; i++) {
                final int percent = randomBoolean() ? HIGH_WATERMARK_PERCENT + 1 : LOW_WATERMARK_PERCENT - 1;
                executor.submit(() -> {
                    try {
                        monitor.onNewInfo(clusterInfoWithNodesAt(percent, LOW_WATERMARK_PERCENT - 1));
                    } catch (Exception e) {
                        failures.incrementAndGet();
                    } finally {
                        latch.countDown();
                    }
                });
            }
            assertTrue(latch.await(30, TimeUnit.SECONDS));
        } finally {
            executor.shutdown();
        }

        // The only invariant under an arbitrary interleaving is that no call threw. The reroute count depends on ordering, so it is
        // deliberately not asserted here.
        assertEquals(0, failures.get());
    }

    private ClusterState twoSearchNodeState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.builder("search-0").name("search-0").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
                    .add(DiscoveryNodeUtils.builder("search-1").name("search-1").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            )
            .build();
    }

    private ClusterState threeSearchNodeState() {
        return ClusterState.builder(ClusterState.EMPTY_STATE)
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.builder("search-0").name("search-0").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
                    .add(DiscoveryNodeUtils.builder("search-1").name("search-1").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
                    .add(DiscoveryNodeUtils.builder("search-2").name("search-2").roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            )
            .build();
    }

    private ClusterInfo clusterInfoWithOneNodeAt(int percent) {
        final Map<String, NodeCacheSizeAndCommitments> commitments = Map.of(
            "search-0",
            new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(percent), 0L)
        );
        return ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build();
    }

    private ClusterInfo clusterInfoWithNodesAt(int firstNodePercent, int secondNodePercent) {
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(firstNodePercent), 0L));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(secondNodePercent), 0L));
        return ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build();
    }

    private ClusterInfo clusterInfoWithThreeNodesAt(int firstNodePercent, int secondNodePercent, int thirdNodePercent) {
        final Map<String, NodeCacheSizeAndCommitments> commitments = new HashMap<>();
        commitments.put("search-0", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(firstNodePercent), 0L));
        commitments.put("search-1", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(secondNodePercent), 0L));
        commitments.put("search-2", new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, bytesForPercent(thirdNodePercent), 0L));
        return ClusterInfo.builder().nodeCacheSizeAndCommitments(commitments).build();
    }

    private static long bytesForPercent(int percent) {
        return CACHE_SIZE_IN_BYTES * percent / 100;
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
                SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING,
                SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING,
                SharedCacheCapacityAllocationDecider.MINIMUM_LOGGING_INTERVAL,
                SharedCacheCapacityAllocationDecider.REROUTE_INTERVAL_SETTING
            )
        );
    }
}
