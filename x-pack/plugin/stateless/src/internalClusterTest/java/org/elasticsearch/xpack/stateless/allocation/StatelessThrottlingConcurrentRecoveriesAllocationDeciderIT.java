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

package org.elasticsearch.xpack.stateless.allocation;

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
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.recovery.TransportStatelessPrimaryRelocationAction;

import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider.CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB;
import static org.elasticsearch.xpack.stateless.allocation.StatelessThrottlingConcurrentRecoveriesAllocationDecider.MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING;
import static org.hamcrest.CoreMatchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0)
public class StatelessThrottlingConcurrentRecoveriesAllocationDeciderIT extends AbstractStatelessPluginIntegTestCase {

    public void testConcurrentRecoveriesNotAllowedOnSmallNodes() throws Exception {
        String minHeapRequiredLargeValue = "256gb";
        String minHeapRequiredSmallValue = "1mb";
        Settings settings = Settings.builder()
            // This is the setting we are testing.
            .put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), minHeapRequiredLargeValue)
            // Set to high value to make sure we never throttle based on this setting
            .put(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(), Integer.MAX_VALUE)
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
        updateClusterSettings(
            Settings.builder().put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), minHeapRequiredSmallValue)
        );
        awaitClusterState(cs -> {
            var routingTable = internalCluster().clusterService().state().routingTable(ProjectId.DEFAULT);
            return routingTable.index("index1").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING)
                && routingTable.index("index2").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING);
        });
        continueRelocation.countDown();
        ensureGreen();
        // Clean up the setting
        updateClusterSettings(Settings.builder().putNull(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey()));
        updateClusterSettings(Settings.builder().putNull(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey()));
    }

    /**
     * Test that cluster.routing.allocation.concurrent_primary_recoveries_per_heap_gb will throttle allocations correctly.
     * The basic process is:
     * 1. concurrent_primary_recoveries_per_heap_gb = a small value to make sure concurrent relocations will throttle
     * 2. Start relocation for "index-1", this is will not be throttled because we always allow a single relocation to run
     * 3. Start relocation for "index-2" and make sure it is throttled (on concurrent_primary_recoveries_per_heap_gb)
     * 4. Update concurrent_primary_recoveries_per_heap_gb to a high value to allow many concurrent relocations
     *    and make sure that "index-2" is unthrottled
     */
    public void testConcurrentRecoveriesIsThrottledByRecoveriesPerGbHeap2() throws Exception {
        Settings settings = Settings.builder()
            // This is the setting we are testing, start with a small value to make sure we throttle
            // A node with heap size of 256GB would allow one concurrent recovery
            .put(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(), 1.0 / 256)
            // Make sure we don't throttle on small node size
            .put(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey(), "0")
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
        // Start one relocation which will not be throttled
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

        // Start another relocation
        updateIndexSettings(Settings.builder().put("index.routing.allocation.require._name", indexNode2), "index2");
        // ... and assert that it is throttled
        assertBusy(() -> {
            final var explanation = safeGet(
                client().execute(
                    TransportClusterAllocationExplainAction.TYPE,
                    new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT).setIndex("index2").setShard(0).setPrimary(true)
                )
            ).getExplanation();
            var moveDecision = explanation.getShardAllocationDecision().getMoveDecision();
            assertNotNull(moveDecision);
            assertThat(moveDecision.getAllocationDecision(), equalTo(AllocationDecision.THROTTLED));
        });
        // increasing the value should allow another recovery
        updateClusterSettings(Settings.builder().put(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey(), 100.0));
        awaitClusterState(cs -> {
            var routingTable = internalCluster().clusterService().state().routingTable(ProjectId.DEFAULT);
            return routingTable.index("index1").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING)
                && routingTable.index("index2").shard(0).activeShards().get(0).state().equals(ShardRoutingState.RELOCATING);
        });

        continueRelocation.countDown();
        ensureGreen();
        // Clean up the setting
        updateClusterSettings(Settings.builder().putNull(MIN_HEAP_REQUIRED_FOR_CONCURRENT_PRIMARY_RECOVERIES_SETTING.getKey()));
        updateClusterSettings(Settings.builder().putNull(CONCURRENT_PRIMARY_RECOVERIES_PER_HEAP_GB.getKey()));
    }
}
