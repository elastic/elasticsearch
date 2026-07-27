/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteRequest;
import org.elasticsearch.action.admin.cluster.reroute.TransportClusterRerouteAction;
import org.elasticsearch.cluster.BoostedAndUnboostedCacheRequirements;
import org.elasticsearch.cluster.CacheSizesAndCommitmentStats;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.MockInternalClusterInfoService;
import org.elasticsearch.cluster.NodeCacheSizeAndCommitments;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
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

    public void testCanAllocateDeprioritizesOversubscribedNode() throws Exception {
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
        final long overSubscribedBoostedCommitmentBytes = 800L;
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
                        && decision.getExplanation().contains("already exceeds the low watermark")
                )
        );
    }

    public void testCanRemainDeprioritizesOversubscribedNode() throws Exception {
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

        // The hosting node is over the 95% high watermark. The other search node has no commitment at all.
        final long hostedBoostedCommitmentBytes = 970L;
        final long otherBoostedCommitmentBytes = 0L;
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                hostedNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, hostedBoostedCommitmentBytes, noUnboostedCommitmentBytes),
                otherNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, otherBoostedCommitmentBytes, noUnboostedCommitmentBytes)
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
            canRemainDecision.getDecisions().stream().anyMatch(decision -> decision.getExplanation().contains("exceeds the high watermark"))
        );

        // The reconciler only moves shards away from a node when canRemain returns NO/NOT_PREFERRED and a better target exists. Drive
        // reroute passes until it picks up the fake oversubscription and relocates the shard to the healthy node.
        assertBusy(() -> {
            safeGet(
                client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
            );
            assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(otherNodeId));
        });
    }

    public void testCanAllocateNotPreferredWhenShardRequirementWouldExceedWatermark() throws Exception {
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
        final long tippedNodeExistingCommitmentBytes = 700L;
        final long healthyNodeExistingCommitmentBytes = 0L;
        final long shardBoostedRequirementBytes = 100L;
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
                        && decision.getExplanation().contains("would raise its cache commitment")
                        && decision.getExplanation().contains("exceeding the low")
                )
        );
    }

    public void testCanAllocateUpdateAccountingModeMidTest() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final var divergentNode = startSearchNode();
        final var healthyNode = startSearchNode();
        ensureStableCluster(4);

        final String divergentNodeId = getNodeId(divergentNode);
        final String healthyNodeId = getNodeId(healthyNode);

        // Switch accounting mode to TOTAL at runtime, exercising the setting's dynamic nature.
        updateClusterSettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.ACCOUNTING_MODE_SETTING.getKey(), "TOTAL"));

        // The divergent node's boosted commitment alone is low, but its unboosted commitment is high. In TOTAL mode the combined
        // 90% already exceeds the 75% low watermark, even though BOOSTED mode alone would consider this node healthy.
        final long divergentNodeBoostedCommitmentBytes = 100L;
        final long divergentNodeUnboostedCommitmentBytes = 800L;
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
                        && decision.getExplanation().contains("accounting mode [TOTAL]")
                )
        );
    }

    public void testCanRemainDisabledThenEnabledDynamically() throws Exception {
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

        // The hosting node is heavily over the 95% high watermark. The other search node has no commitment at all.
        final long hostedBoostedCommitmentBytes = 970L;
        final long otherBoostedCommitmentBytes = 0L;
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                hostedNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, hostedBoostedCommitmentBytes, noUnboostedCommitmentBytes),
                otherNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, otherBoostedCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        // Disable canRemain specifically, even though the decider as a whole, and the fake oversubscription, remain in place.
        updateClusterSettings(Settings.builder().put(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey(), false));

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
                .anyMatch(decision -> decision.getExplanation().equals("shared cache capacity decider's canRemain check is disabled"))
        );

        // A reroute pass must not relocate the shard while canRemain is disabled, despite the fake oversubscription.
        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );
        ensureGreen(indexName);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(hostedNodeId));

        // Re-enabling canRemain dynamically, without restarting any node, should let the same fake oversubscription trigger
        // relocation.
        updateClusterSettings(Settings.builder().putNull(SharedCacheCapacityAllocationDecider.CAN_REMAIN_ENABLED_SETTING.getKey()));
        assertBusy(() -> {
            safeGet(
                client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
            );
            assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(otherNodeId));
        });
    }

    public void testCanRemainNotPreferredButShardStaysAssignedWithNoAlternativeNode() throws Exception {
        startMasterOnlyNode();
        startIndexNode();
        final var soleSearchNode = startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).build());
        ensureGreen(indexName);

        final String soleSearchNodeId = getNodeId(soleSearchNode);
        assertThat(findSearchShard(indexName).routingEntry().currentNodeId(), equalTo(soleSearchNodeId));

        // The sole search node is heavily oversubscribed, but there is no alternative search node to move the shard to.
        final long overWatermarkCommitmentBytes = 970L;
        final long noUnboostedCommitmentBytes = 0L;
        fakeNodeCacheSizeAndCommitments(
            Map.of(
                soleSearchNodeId,
                new NodeCacheSizeAndCommitments(CACHE_SIZE_IN_BYTES, overWatermarkCommitmentBytes, noUnboostedCommitmentBytes)
            )
        );

        final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        explainRequest.setIndex(indexName).setShard(0).setPrimary(false).setCurrentNode(soleSearchNodeId);
        final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));
        final Decision canRemainDecision = explainResponse.getExplanation()
            .getShardAllocationDecision()
            .getMoveDecision()
            .getCanRemainDecision();
        assertThat(canRemainDecision.type(), equalTo(Decision.Type.NOT_PREFERRED));
        assertTrue(
            canRemainDecision.getDecisions().stream().anyMatch(decision -> decision.getExplanation().contains("exceeds the high watermark"))
        );

        // NOT_PREFERRED is only a soft deprioritization. With no alternative node available, the shard must remain assigned and
        // started rather than being left unassigned.
        safeGet(
            client().execute(TransportClusterRerouteAction.TYPE, new ClusterRerouteRequest(TEST_REQUEST_TIMEOUT, TEST_REQUEST_TIMEOUT))
        );
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
