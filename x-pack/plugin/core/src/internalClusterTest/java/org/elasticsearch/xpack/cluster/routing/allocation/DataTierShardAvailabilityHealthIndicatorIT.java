/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.cluster.routing.allocation;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplanation;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.routing.RoutingNodesHelper;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.DataTier;
import org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.health.Diagnosis;
import org.elasticsearch.health.GetHealthAction;
import org.elasticsearch.health.HealthIndicatorResult;
import org.elasticsearch.health.HealthStatus;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.elasticsearch.test.NodeRoles.onlyRole;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;

/**
 * Contains all integration tests for the {@link org.elasticsearch.cluster.routing.allocation.ShardsAvailabilityHealthIndicatorService}
 * that require the data tiers allocation decider logic.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataTierShardAvailabilityHealthIndicatorIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateCompositeXPackPlugin.class);
    }

    /**
     * Verify that the health API returns an "increase tier capacity" diagnosis when an index is created but there aren't enough nodes in
     * a tier to host the desired replicas on unique nodes.
     */
    public void testIncreaseTierCapacityDiagnosisWhenCreated() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startNodes(1, onlyRole(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        ElasticsearchAssertions.assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
            )
        );
        ensureYellow("test");
        GetHealthAction.Response healthResponse = client().execute(
            GetHealthAction.INSTANCE,
            new GetHealthAction.Request(ShardsAvailabilityHealthIndicatorService.NAME, true, 1000)
        ).get();
        HealthIndicatorResult indicatorResult = healthResponse.findIndicator(ShardsAvailabilityHealthIndicatorService.NAME);
        assertThat(indicatorResult.status(), equalTo(HealthStatus.YELLOW));
        assertThat(
            indicatorResult.diagnosisList(),
            hasItem(
                new Diagnosis(
                    ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(DataTier.DATA_HOT),
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("test")))
                )
            )
        );
    }

    /**
     * Verify that the health API returns an "increase tier capacity" diagnosis when enough nodes in a tier leave such that the tier cannot
     * host all of an index's replicas on unique nodes.
     */
    public void testIncreaseTierCapacityDiagnosisWhenTierShrinksUnexpectedly() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startNodes(2, onlyRole(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        ElasticsearchAssertions.assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0)
            )
        );
        ensureGreen("test");
        indexRandomData("test");
        internalCluster().stopNode(findNodeWithReplicaShard("test", 0));
        ensureYellow("test");
        GetHealthAction.Response healthResponse = client().execute(
            GetHealthAction.INSTANCE,
            new GetHealthAction.Request(ShardsAvailabilityHealthIndicatorService.NAME, true, 1000)
        ).get();
        ClusterAllocationExplanation explain = clusterAdmin().prepareAllocationExplain()
            .setIndex("test")
            .setShard(0)
            .setPrimary(false)
            .get()
            .getExplanation();
        logger.info(XContentHelper.toXContent(explain, XContentType.JSON, true).utf8ToString());
        HealthIndicatorResult indicatorResult = healthResponse.findIndicator(ShardsAvailabilityHealthIndicatorService.NAME);
        assertThat(indicatorResult.status(), equalTo(HealthStatus.YELLOW));
        assertThat(
            indicatorResult.diagnosisList(),
            hasItem(
                new Diagnosis(
                    ShardsAvailabilityHealthIndicatorService.ACTION_INCREASE_TIER_CAPACITY_LOOKUP.get(DataTier.DATA_HOT),
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("test")))
                )
            )
        );
    }

    /**
     * Verify that the health API returns a "YELLOW" status when a node disappears and a shard is unassigned because it is delayed.
     */
    public void testRemovingNodeReturnsYellowForDelayedIndex() throws Exception {
        internalCluster().startMasterOnlyNodes(1);
        internalCluster().startNodes(3, onlyRole(DiscoveryNodeRole.DATA_HOT_NODE_ROLE));
        ElasticsearchAssertions.assertAcked(
            prepareCreate("test").setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 1)
                    .put(DataTier.TIER_PREFERENCE, DataTier.DATA_HOT)
                    .put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), TimeValue.timeValueMinutes(30))
            )
        );
        ensureGreen("test");
        indexRandomData("test");
        internalCluster().stopNode(findNodeWithPrimaryShard("test", 0));
        ensureYellow("test");
        GetHealthAction.Response healthResponse = client().execute(
            GetHealthAction.INSTANCE,
            new GetHealthAction.Request(ShardsAvailabilityHealthIndicatorService.NAME, true, 1000)
        ).get();
        HealthIndicatorResult indicatorResult = healthResponse.findIndicator(ShardsAvailabilityHealthIndicatorService.NAME);
        assertThat(indicatorResult.status(), equalTo(HealthStatus.YELLOW));
        assertThat(indicatorResult.diagnosisList().size(), equalTo(1));
        assertThat(
            indicatorResult.diagnosisList(),
            hasItem(
                new Diagnosis(
                    ShardsAvailabilityHealthIndicatorService.DIAGNOSIS_WAIT_FOR_OR_FIX_DELAYED_SHARDS,
                    List.of(new Diagnosis.Resource(Diagnosis.Resource.Type.INDEX, List.of("test")))
                )
            )
        );
    }

    private void indexRandomData(String indexName) throws Exception {
        int numDocs = scaledRandomIntBetween(100, 1000);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < builders.length; i++) {
            builders[i] = client().prepareIndex(indexName).setSource("field", "value");
        }
        // we want to test both full divergent copies of the shard in terms of segments, and
        // a case where they are the same (using sync flush), index Random does all this goodness
        // already
        indexRandom(true, builders);
    }

    private String findNodeWithPrimaryShard(String indexName, int shard) {
        return findNodeWithShard(indexName, shard, true);
    }

    private String findNodeWithReplicaShard(String indexName, int shard) {
        return findNodeWithShard(indexName, shard, false);
    }

    private String findNodeWithShard(final String indexName, final int shard, final boolean primary) {
        ClusterState state = clusterAdmin().prepareState().get().getState();
        List<ShardRouting> startedShards = RoutingNodesHelper.shardsWithState(state.getRoutingNodes(), ShardRoutingState.STARTED);
        startedShards = startedShards.stream()
            .filter(shardRouting -> shardRouting.getIndexName().equals(indexName))
            .filter(shardRouting -> shard == shardRouting.getId())
            .filter(shardRouting -> primary == shardRouting.primary())
            .collect(Collectors.toList());
        Collections.shuffle(startedShards, random());
        return state.nodes().get(startedShards.get(0).currentNodeId()).getName();
    }
}
