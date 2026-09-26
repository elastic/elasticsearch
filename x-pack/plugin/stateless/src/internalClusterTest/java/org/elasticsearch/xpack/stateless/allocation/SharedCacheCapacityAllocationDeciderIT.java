/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.CacheSizesAndCommitmentStats;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SharedCacheCapacityAllocationDeciderIT extends AbstractStatelessPluginIntegTestCase {

    private static final long CACHE_SIZE_IN_BYTES = 1000L;

    private static final String MONITOR_TRIGGERING_REROUTE_LOG_MESSAGE =
        "cache commitments exceeded the high watermark for nodes * triggering reroute";
    private static final String MONITOR_NOT_REROUTING_ALL_NODES_OVER_LOW_WATERMARK_LOG_MESSAGE =
        "not rerouting for nodes * over the high watermark because all search nodes exceed the low watermark";
    private static final String MONITOR_SKIPPED_WHILE_DISABLED_LOG_MESSAGE =
        "skipping monitor as the shared cache capacity decider or its canRemain check is disabled";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockInternalClusterInfoService.TestPlugin.class))
            .collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(SharedCacheCapacityAllocationDecider.ENABLED_SETTING.getKey(), true)
            .put(SharedCacheCapacityAllocationDecider.LOW_WATERMARK_SETTING.getKey(), "75%")
            .put(SharedCacheCapacityAllocationDecider.HIGH_WATERMARK_SETTING.getKey(), "95%");
    }

    private static long bytesForPercent(int percent) {
        return CACHE_SIZE_IN_BYTES * percent / 100;
    }

    public void testCanAllocateDeprioritizesOversubscribedNode() {
        startMasterOnlyNode();
        startIndexNode();
        final var overSubscribedSearchNode = startSearchNode();
        final var healthySearchNode = startSearchNode();
        ensureStableCluster(4);

        final String overSubscribedNodeId = getNodeId(overSubscribedSearchNode);
        final String healthyNodeId = getNodeId(healthySearchNode);

        // The oversubscribed node is already above the 75% low watermark. The healthy node has no commitment at all. NOT_PREFERRED
        // does not block allocation outright. With two otherwise-equal candidate search nodes, the balancer should still place the
        // search-only replica on the healthy one.
        final long overSubscribedBoostedCommitmentBytes = bytesForPercent(80);
        final long healthyBoostedCommitmentBytes = 0L;
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                overSubscribedNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, overSubscribedBoostedCommitmentBytes, noUnboostedCommitmentBytes),
                healthyNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, healthyBoostedCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(healthyNodeId));

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(healthyNodeId);
        explainRequest.includeYesDecisions(true);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));

        final List<NodeAllocationResult> nodeDecisions = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getNodeDecisions();
        assertNotNull(nodeDecisions);

        // The overall move-decision reason is WORSE_BALANCE (the shard is already well-placed), but the per-decider breakdown for the
        // oversubscribed node still shows this decider firing with NOT_PREFERRED.
        final NodeAllocationResult overSubscribedResult = nodeDecisions.stream()
            .filter(result -> result.getNode().getId().equals(overSubscribedNodeId))
            .findFirst()
            .orElseThrow();
        assertTrue(
            overSubscribedResult.getCanAllocateDecision()
                .getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.type() == Decision.Type.NOT_PREFERRED
                        && decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().contains("already exceeds the low watermark")
                )
        );
    }

    public void testCanRemainDeprioritizesOversubscribedNode() {
        startMasterOnlyNode();
        startIndexNode();
        // Start with only one search node, so the shard's initial host is unambiguous and there is no alternative node an incidental
        // reroute (triggered for any of the many reasons unrelated to this decider) could relocate it to before the oversubscription
        // is faked and asserted below.
        final var hostedSearchNode = startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final String hostedNodeId = getNodeId(hostedSearchNode);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(hostedNodeId));

        // The (only) search node is over the 95% high watermark.
        final long hostedBoostedCommitmentBytes = bytesForPercent(97);
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                hostedNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, hostedBoostedCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(hostedNodeId);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));

        final Decision canRemainDecision = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getCanRemainDecision();
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertTrue(
            canRemainDecision.getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().contains("exceeds the high watermark")
                )
        );

        // Only now introduce a healthy alternative node. Nothing could have relocated the shard prematurely before this point, since
        // no alternative existed. There's no need to fake anything for the new node, as the decider treats a node it has no cache data
        // for as healthy. Joining the cluster triggers a reroute of its own, so the reconciler picks up the existing oversubscription
        // and relocates the shard without any explicit reroute call here.
        final var healthySearchNode = startSearchNode();
        ensureStableCluster(4);
        final String healthyNodeId = getNodeId(healthySearchNode);

        awaitClusterState(
            state -> state.getRoutingNodes().node(healthyNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isPresent()
                && state.getRoutingNodes().node(hostedNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isEmpty()
        );
    }

    public void testCanAllocateNotPreferredWhenShardRequirementWouldExceedWatermark() {
        startMasterOnlyNode();
        startIndexNode();
        final var tippedNode = startSearchNode();
        final var healthyNode = startSearchNode();
        ensureStableCluster(4);

        final String tippedNodeId = getNodeId(tippedNode);
        final String healthyNodeId = getNodeId(healthyNode);

        // Create the index with no replica yet, so the search-only copy's cache requirement can be faked against its exact ShardId
        // before the copy is allocated.
        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 0).build());
        ensureGreen(indexName);
        final ShardId shardId = new ShardId(resolveIndex(indexName), 0);

        // Neither node is already over the 75% low watermark on its own, but the shard's own cache requirement would push the
        // tipped node over while leaving the healthy node comfortably under.
        final long tippedNodeExistingCommitmentBytes = bytesForPercent(70);
        final long healthyNodeExistingCommitmentBytes = 0L;
        final long shardBoostedRequirementBytes = bytesForPercent(10);
        final long noUnboostedCommitmentBytes = 0L;
        fakeCacheSizesAndCommitments(
            Map.of(shardId, new BoostedAndUnboostedCacheRequirements(shardBoostedRequirementBytes, noUnboostedCommitmentBytes)),
            Map.of(
                tippedNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, tippedNodeExistingCommitmentBytes, noUnboostedCommitmentBytes),
                healthyNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, healthyNodeExistingCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        setReplicaCount(1, indexName);
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(healthyNodeId));

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(healthyNodeId);
        explainRequest.includeYesDecisions(true);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));

        final List<NodeAllocationResult> nodeDecisions = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getNodeDecisions();
        assertNotNull(nodeDecisions);

        // The tipped node's own commitment stays under the low watermark, but adding this shard's own requirement would exceed it.
        final NodeAllocationResult tippedResult = nodeDecisions.stream()
            .filter(result -> result.getNode().getId().equals(tippedNodeId))
            .findFirst()
            .orElseThrow();
        assertTrue(
            tippedResult.getCanAllocateDecision()
                .getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.type() == Decision.Type.NOT_PREFERRED
                        && decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().contains("would raise its cache commitment")
                        && decision.getExplanation().contains("exceeding the low")
                )
        );
    }

    public void testCanUpdateAccountingModeDynamically() {
        startMasterOnlyNode();
        startIndexNode();
        final var divergentNode = startSearchNode();
        final var healthyNode = startSearchNode();
        ensureStableCluster(4);

        final String divergentNodeId = getNodeId(divergentNode);
        final String healthyNodeId = getNodeId(healthyNode);

        // Assert default accounting mode (BOOSTED) up front so switching to TOTAL below is a genuine, visible change.
        assertThat(
            clusterService().getClusterSettings().get(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING),
            equalTo(SharedCacheCapacityAllocationDecider.CacheAccountingMode.BOOSTED)
        );

        // Switch accounting mode to TOTAL at runtime, exercising the setting's dynamic nature.
        updateClusterSettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), "TOTAL"));

        // The divergent node's boosted commitment alone is low, but its unboosted commitment is high. In TOTAL mode the combined
        // 90% already exceeds the 75% low watermark, even though BOOSTED mode alone would consider this node healthy.
        final long divergentNodeBoostedCommitmentBytes = bytesForPercent(10);
        final long divergentNodeUnboostedCommitmentBytes = bytesForPercent(80);
        final long healthyBoostedCommitmentBytes = 0L;
        final long healthyUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                divergentNodeId,
                new NodeCacheSizeAndCommitments(
                    CACHE_SIZE_IN_BYTES,
                    divergentNodeBoostedCommitmentBytes,
                    divergentNodeUnboostedCommitmentBytes
                ),
                healthyNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, healthyBoostedCommitmentBytes, healthyUnboostedCommitmentBytes)
            )
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(healthyNodeId));

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(healthyNodeId);
        explainRequest.includeYesDecisions(true);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));

        final List<NodeAllocationResult> nodeDecisions = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getNodeDecisions();
        assertNotNull(nodeDecisions);

        final NodeAllocationResult divergentResult = nodeDecisions.stream()
            .filter(result -> result.getNode().getId().equals(divergentNodeId))
            .findFirst()
            .orElseThrow();
        assertTrue(
            divergentResult.getCanAllocateDecision()
                .getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.type() == Decision.Type.NOT_PREFERRED
                        && decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().contains("accounting mode [TOTAL]")
                )
        );
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.SharedCacheCapacityMonitor:DEBUG", reason = "debug log for test")
    public void testCanRemainDisabledThenEnabledDynamically() {
        startMasterOnlyNode();
        startIndexNode();
        final var searchNodeA = startSearchNode();
        final var searchNodeB = startSearchNode();
        ensureStableCluster(4);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final String searchNodeAId = getNodeId(searchNodeA);
        final String searchNodeBId = getNodeId(searchNodeB);
        final String hostedNodeId = findSearchShard(indexName).routingEntry().currentNodeId();
        final String otherNodeId = hostedNodeId.equals(searchNodeAId) ? searchNodeBId : searchNodeAId;

        // canRemain is enabled by default; assert that up front so disabling it below is a genuine, visible state change.
        assertTrue(clusterService().getClusterSettings().get(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING));

        // Disable canRemain before faking any oversubscription. Reroute is triggered for all sorts of reasons outside this test's
        // control, so if the oversubscription were faked first, an incidental reroute landing in the window before this setting
        // update takes effect could see canRemain still enabled and relocate the shard prematurely, before the "must not relocate
        // while disabled" assertions below even run.
        updateClusterSettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey(), false));

        // The hosting node is heavily over the 95% high watermark. No need to fake anything for the other node, as the decider
        // treats a node with no cache data as healthy (YES), and the monitor never gets far enough to look at any node's
        // commitment while canRemain is disabled.
        final long hostedBoostedCommitmentBytes = bytesForPercent(97);
        final long noUnboostedCommitmentBytes = 0L;
        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "monitor skipped because canRemain is disabled",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    MONITOR_SKIPPED_WHILE_DISABLED_LOG_MESSAGE
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no reroute while canRemain is disabled",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    MONITOR_TRIGGERING_REROUTE_LOG_MESSAGE
                )
            );
            fakeNodeCacheSizeAndCommitments(
                Map.of(
                    hostedNodeId,
                    new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, hostedBoostedCommitmentBytes, noUnboostedCommitmentBytes)
                )
            );
            mockLog.assertAllExpectationsMatched();
        }

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(hostedNodeId);
        explainRequest.includeYesDecisions(true);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));
        final Decision canRemainDecision = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getCanRemainDecision();
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.YES));
        assertTrue(
            canRemainDecision.getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().equals("shared cache capacity decider's canRemain check is disabled")
                )
        );

        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(hostedNodeId));

        // Re-enabling canRemain dynamically, without restarting any node, should let the same fake oversubscription trigger
        // relocation. No explicit reroute is needed, since a settings update triggers its own reroute once applied,
        // which picks up the still-oversubscribed node under the now-enabled check.
        updateClusterSettings(Settings.builder().putNull(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey()));
        awaitClusterState(
            state -> state.getRoutingNodes().node(otherNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isPresent()
                && state.getRoutingNodes().node(hostedNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isEmpty()
        );
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.SharedCacheCapacityMonitor:DEBUG", reason = "debug log for test")
    public void testMonitorTriggersRerouteWhenNodeExceedsHighWatermark() {
        startMasterOnlyNode();
        startIndexNode();
        final var searchNodeA = startSearchNode();
        final var searchNodeB = startSearchNode();
        ensureStableCluster(4);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final String searchNodeAId = getNodeId(searchNodeA);
        final String searchNodeBId = getNodeId(searchNodeB);
        final String hostedNodeId = findSearchShard(indexName).routingEntry().currentNodeId();
        final String otherNodeId = hostedNodeId.equals(searchNodeAId) ? searchNodeBId : searchNodeAId;

        // The hosting node crosses the 95% high watermark, and the other node is faked as well below the low watermark, so the
        // monitor has somewhere to relieve the pressure onto. No settings update is involved here, so the only possible trigger
        // for the relocation below is the monitor reacting to the ClusterInfo refresh inside the fake call.
        final long hostedBoostedCommitmentBytes = bytesForPercent(97);
        final long otherNodeBoostedCommitmentBytes = 0L;
        final long noUnboostedCommitmentBytes = 0L;
        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reroute due to cache commitments exceeding the high watermark",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    MONITOR_TRIGGERING_REROUTE_LOG_MESSAGE
                )
            );
            fakeNodeCacheSizeAndCommitments(
                Map.of(
                    hostedNodeId,
                    new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, hostedBoostedCommitmentBytes, noUnboostedCommitmentBytes),
                    otherNodeId,
                    new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, otherNodeBoostedCommitmentBytes, noUnboostedCommitmentBytes)
                )
            );
            mockLog.assertAllExpectationsMatched();
        }

        awaitClusterState(
            state -> state.getRoutingNodes().node(otherNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isPresent()
                && state.getRoutingNodes().node(hostedNodeId).shardsWithState(indexName, ShardRoutingState.STARTED).findAny().isEmpty()
        );
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.SharedCacheCapacityMonitor:DEBUG", reason = "debug log for test")
    public void testCanRemainNotPreferredButShardStaysAssignedWithNoAlternativeNode() {
        startMasterOnlyNode();
        startIndexNode();
        final var soleSearchNode = startSearchNode();
        ensureStableCluster(3);
        final String soleSearchNodeId = getNodeId(soleSearchNode);

        // Fake the sole search node as heavily oversubscribed before the index (and its search-only replica) even exists. That way,
        // the replica's initial allocation is also exercised against the oversubscription, demonstrating that canAllocate lets the
        // shard be allocated there too (NOT_PREFERRED is only a soft deprioritization), since there is no alternative node.
        final long overWatermarkCommitmentBytes = bytesForPercent(97);
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                soleSearchNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, overWatermarkCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(soleSearchNodeId));

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(soleSearchNodeId);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));
        final Decision canRemainDecision = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getCanRemainDecision();
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertTrue(
            canRemainDecision.getDecisions()
                .stream()
                .anyMatch(
                    decision -> decision.label().equals(SharedCacheCapacityAllocationDecider.NAME)
                        && decision.getExplanation().contains("exceeds the high watermark")
                )
        );

        // NOT_PREFERRED is only a soft deprioritization. With no alternative node available, the shard must remain assigned and
        // started rather than being left unassigned. The monitor should decline to reroute too, since there is no node below the
        // low watermark to relieve the pressure onto.
        try (MockLog mockLog = MockLog.capture(SharedCacheCapacityMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "no reroute when there is no alternative node below the low watermark",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    MONITOR_NOT_REROUTING_ALL_NODES_OVER_LOW_WATERMARK_LOG_MESSAGE
                )
            );
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no reroute log line when there is no alternative node below the low watermark",
                    SharedCacheCapacityMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    MONITOR_TRIGGERING_REROUTE_LOG_MESSAGE
                )
            );
            refreshClusterInfo();
            mockLog.assertAllExpectationsMatched();
        }

        ClusterRerouteUtils.reroute(client());
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(soleSearchNodeId));
        assertTrue(findSearchShard(indexName).routingEntry().started());
    }

    private void fakeNodeCacheSizeAndCommitments(Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments) {
        fakeCacheSizesAndCommitments(Map.of(), nodeCacheSizeAndCommitments);
    }

    private void fakeCacheSizesAndCommitments(
        Map<ShardId, BoostedAndUnboostedCacheRequirements> shardCacheRequirements,
        Map<String, NodeCacheSizeAndCommitments> nodeCacheSizeAndCommitments
    ) {
        final MockInternalClusterInfoService clusterInfoService = (MockInternalClusterInfoService) internalCluster()
            .getCurrentMasterNodeInstance(ClusterInfoService.class);
        clusterInfoService.setCacheSizesAndCommitmentStatsFunctionAndRefresh(
            ignored -> new CacheSizesAndCommitmentStats(shardCacheRequirements, nodeCacheSizeAndCommitments)
        );
    }
}
