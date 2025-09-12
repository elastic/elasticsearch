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

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.decider.ThrottlingAllocationDecider;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.recovery.StatelessPrimaryRelocationAction;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.concurrent.CountDownLatch;

import static co.elastic.elasticsearch.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING;
import static org.hamcrest.CoreMatchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class StatelessThrottlingConcurrentRecoveriesAllocationDeciderIT extends AbstractStatelessIntegTestCase {

    public void testConcurrentRecoveriesNotAllowedOnSmallNodes() throws Exception {
        Settings settings = Settings.builder()
            .put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), "256gb")
            // This needs to be enabled in order for the periodic cluster info poll to fetch node stats
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
            // Ensure concurrent recoveries are not limited by the default settings
            .put(ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_RECOVERIES_SETTING.getKey(), randomIntBetween(2, 5))
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_INCOMING_RECOVERIES_SETTING.getKey(),
                randomIntBetween(2, 5)
            )
            .put(
                ThrottlingAllocationDecider.CLUSTER_ROUTING_ALLOCATION_NODE_CONCURRENT_OUTGOING_RECOVERIES_SETTING.getKey(),
                randomIntBetween(2, 5)
            )
            .build();
        String indexNode1 = startMasterAndIndexNode(settings);
        String indexNode2 = startMasterAndIndexNode(settings);
        ensureStableCluster(2);

        // Make sure the heap per node is filled out in the cluster info
        var clusterInfoService = (InternalClusterInfoService) internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class);
        ClusterInfoServiceUtils.setUpdateFrequency(clusterInfoService, TimeValue.timeValueMillis(100));
        ClusterInfoServiceUtils.refresh(clusterInfoService);
        var info = clusterInfoService.getClusterInfo();
        assertThat(info.getMaxHeapSizePerNode().size(), equalTo(2));

        createIndex("index1", indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build());
        createIndex("index2", indexSettings(1, 0).put("index.routing.allocation.require._name", indexNode1).build());
        ensureGreen();
        indexDocs("index1", randomIntBetween(20, 50));
        indexDocs("index2", randomIntBetween(20, 50));

        var continueRelocation = new CountDownLatch(1);
        MockTransportService.getInstance(indexNode1)
            .addRequestHandlingBehavior(
                TransportStatelessPrimaryRelocationAction.START_RELOCATION_ACTION_NAME,
                (handler, request, channel, task) -> {
                    logger.info("--> Got a relocation request for {}", ((StatelessPrimaryRelocationAction.Request) request).shardId());
                    safeAwait(continueRelocation);
                    handler.messageReceived(request, channel, task);
                }
            );
        // At most one shard can relocate concurrently
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2), "index1");
        awaitClusterState(
            state -> state.routingTable(ProjectId.DEFAULT)
                .index("index1")
                .shard(0)
                .activeShards()
                .get(0)
                .state()
                .equals(ShardRoutingState.RELOCATING)
        );
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2), "index2");
        assertBusy(() -> {
            final var explanation1 = safeGet(
                client().execute(
                    TransportClusterAllocationExplainAction.TYPE,
                    new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex("index2").setShard(0).setPrimary(true)
                )
            ).getExplanation();
            var moveDecision = explanation1.getShardAllocationDecision().getMoveDecision();
            assertNotNull(moveDecision);
            assertThat(moveDecision.getAllocationDecision(), equalTo(AllocationDecision.THROTTLED));
        });
        // reducing the value should allow a second recovery
        updateClusterSettings(Settings.builder().put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), "1mb"));
        awaitClusterState(cs -> {
            var routingTable = internalCluster().clusterService().state().routingTable(ProjectId.DEFAULT);
            return routingTable.index("index1").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING)
                && routingTable.index("index2").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING);
        });
        continueRelocation.countDown();
        ensureGreen();
        // Clean up the setting
        updateClusterSettings(Settings.builder().putNull(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey()));
    }
}
