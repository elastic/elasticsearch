/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.allocation;

import co.elastic.elasticsearch.stateless.AbstractServerlessStatelessPluginIntegTestCase;

import org.elasticsearch.cluster.routing.allocation.allocator.BalancerSettings;
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.cluster.routing.allocation.allocator.WeightFunction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;

import java.util.Collection;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class StatelessBalancingWeightsFactoryIT extends AbstractServerlessStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.concatLists(List.of(TestTelemetryPlugin.class), super.nodePlugins());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(StatelessBalancingWeightsFactory.SEPARATE_WEIGHTS_PER_TIER_ENABLED_SETTING.getKey(), true);
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
                .put(setting, 0.0)
                // Zero write load to isolate the effect of shard count
                .put(StatelessBalancingWeightsFactory.INDEXING_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
                .put(StatelessBalancingWeightsFactory.SEARCH_TIER_WRITE_LOAD_BALANCE_FACTOR_SETTING.getKey(), 0.0)
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

        // Assert weights are what we expect
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            indexNode1,
            zeroSearchTier ? BalancerSettings.DEFAULT.getShardBalanceFactor() : 0.0f
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            indexNode2,
            zeroSearchTier ? BalancerSettings.DEFAULT.getShardBalanceFactor() : 0.0f
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            searchNode1,
            zeroSearchTier ? 0.0f : BalancerSettings.DEFAULT.getShardBalanceFactor()
        );
        assertThatWeightsAreExpected(
            indexNodeTelemetry,
            searchNode2,
            zeroSearchTier ? 0.0f : BalancerSettings.DEFAULT.getShardBalanceFactor()
        );
    }

    private void assertThatWeightsAreExpected(TestTelemetryPlugin testTelemetryPlugin, String nodeName, float shardBalance) {
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
        int totalNumShards = 36;    // 3 indices * 6 shards * 2 tiers
        int totalNumberNodes = 4;
        double expectedWeight = weightFunction.calculateNodeWeight((int) shardCount, (float) totalNumShards / totalNumberNodes, 0, 0, 0, 0);
        assertThat(weight, equalTo(expectedWeight));
    }

    private Measurement getMeasurementForNode(List<Measurement> measurements, String nodeName) {
        return measurements.stream().filter(m -> nodeName.equals(m.attributes().get("node_name"))).findFirst().orElseThrow();
    }
}
