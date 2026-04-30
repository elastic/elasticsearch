/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.allocation;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthRequest;
import org.elasticsearch.action.admin.cluster.state.ClusterStateRequest;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancedShardsAllocator;
import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class StatelessBalancingWeightsFactoryIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(TestTelemetryPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings()
            // Tests below can set the other weights all to 0, resulting in 0 node weight, which is not currently supported. Therefore,
            // index balance will be set to a value >0 to ensure there is always some node weight balance.
            .put(BalancedShardsAllocator.INDEX_BALANCE_FACTOR_SETTING.getKey(), 0.55f)
            .put(StatelessBalancingWeightsFactory.SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING.getKey(), true);
    }

    public void testShardCountIsConfigurablePerTier() throws Exception {
        String indexNode1 = startMasterAndIndexNode();
        String indexNode2 = startMasterAndIndexNode();
        String searchNode1 = startSearchNode();
        String searchNode2 = startSearchNode();

        // Zero one of the tiers
        final boolean zeroSearchTier = randomBoolean();
        final String setting = zeroSearchTier
            ? StatelessBalancingWeightsFactory.SEARCH_TIER_SHARD_BALANCE_FACTOR_SETTING.getKey()
            : StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING.getKey();
        updateClusterSettings(
            Settings.builder()
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.45)
                // Zero write load to isolate the effect of shard count
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
                .put(StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
                .put(setting, 0.0)
        );

        // Create some indices
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(10, 100));
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(10, 100));
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(10, 100));
        ensureGreen();

        final TestTelemetryPlugin indexNodeTelemetry = internalCluster().getCurrentMasterNodeInstance(PluginsService.class)
            .filterPlugins(TestTelemetryPlugin.class)
            .findFirst()
            .orElseThrow();

        indexNodeTelemetry.collect();

        // Calculate total shards across all nodes for the avgShardsPerNode calculation
        List<Measurement> shardCountMeasurements = indexNodeTelemetry.getLongGaugeMeasurement(
            DesiredBalanceMetrics.CURRENT_NODE_SHARD_COUNT_METRIC_NAME
        );
        int totalNumShards = (int) shardCountMeasurements.stream().mapToLong(Measurement::getLong).sum();
        int totalNumberNodes = shardCountMeasurements.size();

        // Assert weights are what we expect
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            indexNode1,
            zeroSearchTier ? BalancerSettings.DEFAULT.getShardBalanceFactor() : 0.0f,
            totalNumShards,
            totalNumberNodes
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            indexNode2,
            zeroSearchTier ? BalancerSettings.DEFAULT.getShardBalanceFactor() : 0.0f,
            totalNumShards,
            totalNumberNodes
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            searchNode1,
            zeroSearchTier ? 0.0f : BalancerSettings.DEFAULT.getShardBalanceFactor(),
            totalNumShards,
            totalNumberNodes
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            searchNode2,
            zeroSearchTier ? 0.0f : BalancerSettings.DEFAULT.getShardBalanceFactor(),
            totalNumShards,
            totalNumberNodes
        );
    }

    public void testThatBalanceThresholdIsConfigurablePerTier() throws Exception {
        final String indexNode1 = startMasterAndIndexNode();
        final String indexNode2 = startMasterAndIndexNode();
        final String searchNode1 = startSearchNode();
        final String searchNode2 = startSearchNode();

        // Loosen the threshold of one of the tiers, and pin the other tier to a low threshold so it rebalances normally. Both tier
        // thresholds and the indexing-tier shard balance factor must be set explicitly because StatelessPlugin overrides their defaults
        // (the threshold to a very large value and the shard balance factor to 0), which would otherwise prevent the indexing tier from
        // rebalancing even when it is not the tier being loosened.
        final boolean loosenSearchTier = randomBoolean();
        updateClusterSettings(
            Settings.builder()
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_BALANCING_THRESHOLD_SETTING.getKey(), loosenSearchTier ? 1.0f : 100.0f)
                .put(StatelessBalancingWeightsFactory.SEARCH_TIER_BALANCING_THRESHOLD_SETTING.getKey(), loosenSearchTier ? 100.0f : 1.0f)
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_SHARD_BALANCE_FACTOR_SETTING.getKey(), 0.45f)
                // Zero write load to isolate the effect of shard count
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
                .put(StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
        );

        // Ensure allocation is skewed to begin with
        updateClusterSettings(Settings.builder().put("cluster.routing.allocation.include._name", indexNode1 + ", " + searchNode1));

        // Create some indices
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(1, 10));
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(1, 10));
        indexDocsAndRefresh(randomIdentifier(), randomIntBetween(1, 10));
        ensureGreen();

        // Verify that allocation is skewed
        var clusterStateResponse = safeGet(
            internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT))
        );
        var routingNodes = clusterStateResponse.getState().getRoutingNodes();
        assertThat(routingNodes.node(getNodeId(indexNode1)).size(), greaterThan(0));
        assertThat(routingNodes.node(getNodeId(indexNode2)).size(), equalTo(0));
        assertThat(routingNodes.node(getNodeId(searchNode1)).size(), greaterThan(0));
        assertThat(routingNodes.node(getNodeId(searchNode2)).size(), equalTo(0));

        // Allow rebalancing, this should trigger a re-route
        updateClusterSettings(Settings.builder().putNull("cluster.routing.allocation.include._name"));

        // Wait for balancing to complete
        safeGet(
            internalCluster().client()
                .admin()
                .cluster()
                .health(new ClusterHealthRequest(TEST_REQUEST_TIMEOUT).waitForNoRelocatingShards(true))
        );

        // Check that the tier whose threshold was increased remains skewed and the other is balanced by shard count
        clusterStateResponse = safeGet(internalCluster().client().admin().cluster().state(new ClusterStateRequest(TEST_REQUEST_TIMEOUT)));
        routingNodes = clusterStateResponse.getState().getRoutingNodes();
        assertThat(routingNodes.node(getNodeId(indexNode1)).size(), greaterThan(0));
        assertThat(routingNodes.node(getNodeId(indexNode2)).size(), loosenSearchTier ? greaterThan(0) : equalTo(0));
        assertThat(routingNodes.node(getNodeId(searchNode1)).size(), greaterThan(0));
        assertThat(routingNodes.node(getNodeId(searchNode2)).size(), loosenSearchTier ? equalTo(0) : greaterThan(0));
    }

    private void assertThatWeightsAreExpected(
        TestTelemetryPlugin testTelemetryPlugin,
        String nodeName,
        float shardBalance,
        int totalNumShards,
        int totalNumberNodes
    ) {
        long shardCount = getMeasurementForNode(
            testTelemetryPlugin.getLongGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_SHARD_COUNT_METRIC_NAME),
            nodeName
        ).getLong();
        double weight = getMeasurementForNode(
            testTelemetryPlugin.getDoubleGaugeMeasurement(DesiredBalanceMetrics.CURRENT_NODE_WEIGHT_METRIC_NAME),
            nodeName
        ).getDouble();

        WeightFunction weightFunction = new WeightFunction(
            shardBalance,
            BalancerSettings.DEFAULT.getIndexBalanceFactor(),
            0.0f,
            BalancerSettings.DEFAULT.getDiskUsageBalanceFactor()
        );
        double expectedWeight = weightFunction.calculateNodeWeight((int) shardCount, (float) totalNumShards / totalNumberNodes, 0, 0, 0, 0);
        assertThat(weight, equalTo(expectedWeight));
    }

    private Measurement getMeasurementForNode(List<Measurement> measurements, String nodeName) {
        return measurements.stream().filter(m -> nodeName.equals(m.attributes().get("node_name"))).findFirst().orElseThrow();
    }
}
