/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.decider;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.action.admin.indices.ResizeIndexTestUtils;
import org.elasticsearch.action.admin.indices.shrink.ResizeType;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.RoutingNode;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.RoutingAllocation;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ClusterPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.not;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ResizeAllocationDeciderIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(TestPlugin.class);
    }

    private static class BlockIndexAllocationDecider extends AllocationDecider {
        private static final String NAME = "block_index";
        private final AtomicReference<String> indexToBlock;

        private BlockIndexAllocationDecider(AtomicReference<String> indexToBlock) {
            this.indexToBlock = indexToBlock;
        }

        @Override
        public Decision canAllocate(ShardRouting shardRouting, RoutingNode node, RoutingAllocation allocation) {
            if (shardRouting.getIndexName().equals(indexToBlock.get())) {
                return allocation.decision(Decision.NO, NAME, "blocked");
            } else {
                return Decision.YES;
            }
        }
    }

    public static class TestPlugin extends Plugin implements ClusterPlugin {
        private final AtomicReference<String> indexToBlock = new AtomicReference<>();

        @Override
        public Collection<AllocationDecider> createAllocationDeciders(Settings settings, ClusterSettings clusterSettings) {
            return List.of(new BlockIndexAllocationDecider(indexToBlock));
        }
    }

    public void testDeletedSourceIndexShouldNotBreakDesiredBalance() {
        internalCluster().startNode();
        final String sourceIndex = randomIdentifier("source-");
        createIndex(sourceIndex, 1, 0);
        indexRandom(randomBoolean(), sourceIndex, between(10, 100));
        updateIndexSettings(Settings.builder().put("index.blocks.write", true), sourceIndex);

        // Clone the source index to the target index but block its allocation initially
        final String targetIndex = randomIdentifier("target-");
        final var testPlugin = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(TestPlugin.class)
            .findFirst()
            .orElseThrow();
        testPlugin.indexToBlock.set(targetIndex);

        // Wait for the target index to be created in the cluster state so that we can delete the source index
        final var indexCreatedLatch = new CountDownLatch(1);
        ClusterServiceUtils.addMasterTemporaryStateListener(state -> {
            if (state.metadata().getProject(ProjectId.DEFAULT).hasIndex(targetIndex)) {
                indexCreatedLatch.countDown();
                return true;
            } else {
                return false;
            }
        });
        ResizeIndexTestUtils.executeResize(ResizeType.CLONE, sourceIndex, targetIndex, Settings.builder());
        safeAwait(indexCreatedLatch);

        // Delete the source index and unblock the allocation for the target index
        safeGet(indicesAdmin().prepareDelete(sourceIndex).execute());
        testPlugin.indexToBlock.set(null);

        // Create another index should work and not blocked by the target index breaking the desired balance computation
        final String anotherIndex = randomIdentifier("another-");
        createIndex(anotherIndex, 1, 0);
        ensureGreen(anotherIndex);

        // Run an allocation explain to ensure no decider is broken due to the same issue of missing source index
        final var request = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        request.setIndex(targetIndex).setPrimary(true).setShard(0);
        final var explanation = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, request)).getExplanation();
        assertThat(explanation.getShardState(), equalTo(ShardRoutingState.UNASSIGNED));
        final var nodeDecisions = explanation.getShardAllocationDecision().getAllocateDecision().getNodeDecisions().getFirst();
        final var deciderDecisions = nodeDecisions.getCanAllocateDecision().getDecisions();
        assertThat(deciderDecisions.stream().map(Decision::label).toList(), not(hasItem(BlockIndexAllocationDecider.NAME)));
        final var decision = deciderDecisions.stream()
            .filter(d -> ResizeAllocationDecider.NAME.equals(d.label()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("expected decision from resize allocation decider, but none found"));
        assertThat(
            decision.getExplanation(),
            allOf(containsString("resize source index [[" + sourceIndex), containsString("doesn't exist"))
        );

        // Delete the target index to clean up
        safeGet(indicesAdmin().prepareDelete(targetIndex).execute());
    }
}
