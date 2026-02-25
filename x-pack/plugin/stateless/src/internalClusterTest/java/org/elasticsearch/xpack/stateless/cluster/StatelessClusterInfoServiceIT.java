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

package org.elasticsearch.xpack.stateless.cluster;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.EstimatedHeapUsage;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.ShardAndIndexHeapUsage;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.greaterThan;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class StatelessClusterInfoServiceIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.frequency",
            TimeValue.timeValueMillis(10)
        ).put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true);
    }

    public void testClusterInfoIncludesHeapUsage() throws Exception {
        startMasterAndIndexNode();
        startMasterAndIndexNode();
        startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomIdentifier();
        indexDocsAndRefresh(indexName, between(1, 1_000));
        ensureGreen(indexName);

        final InternalClusterInfoService infoService = asInstanceOf(
            InternalClusterInfoService.class,
            internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class)
        );

        ClusterInfoServiceUtils.setUpdateFrequency(infoService, TimeValue.timeValueMillis(100));

        // Wait for heap metrics with shard mapping sizes to be received by the master
        assertBusy(() -> {
            final ClusterInfo info = ClusterInfoServiceUtils.refresh(infoService);
            final Map<String, EstimatedHeapUsage> nodesHeapUsage = info.getEstimatedHeapUsages();
            final Map<ShardId, ShardAndIndexHeapUsage> shardsHeapUsage = info.getEstimatedShardHeapUsages();
            assertThat(nodesHeapUsage.size(), greaterThan(0));
            assertThat(shardsHeapUsage.size(), greaterThan(0));
            // Verify at least one node has estimated heap usage
            assertTrue(
                "Expected at least one node to have estimated heap usage",
                nodesHeapUsage.values().stream().anyMatch(usage -> usage.estimatedUsageBytes() > 0)
            );
        });
    }
}
