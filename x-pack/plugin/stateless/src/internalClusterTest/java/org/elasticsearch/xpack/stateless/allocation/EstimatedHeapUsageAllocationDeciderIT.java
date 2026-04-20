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

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.cluster.routing.allocation.decider.Decision;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.memory.HeapMemoryUsage;
import org.elasticsearch.xpack.stateless.memory.PublishHeapMemoryMetricsRequest;
import org.elasticsearch.xpack.stateless.memory.ShardMappingSize;
import org.elasticsearch.xpack.stateless.memory.ShardsMappingSizeCollector;
import org.elasticsearch.xpack.stateless.memory.StatelessMemoryMetricsService;
import org.elasticsearch.xpack.stateless.memory.TransportPublishHeapMemoryMetrics;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.xpack.stateless.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EstimatedHeapUsageAllocationDeciderIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            // Set up fast publishing of new memory metrics, so the tests can run fast.
            ShardsMappingSizeCollector.PUBLISHING_FREQUENCY_SETTING.getKey(),
            TimeValue.timeValueMillis(10)
        )
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
            // Ensure the decider is enabled even for the small (512 MB) test JVM.
            .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), "100mb");
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageMonitor:DEBUG", reason = "debug log for test")
    public void testEstimatedHeapAllocationDecider() {
        final var masterNodeName = startMasterOnlyNode();
        final var nodeNameA = startIndexNode();
        final var nodeNameB = startIndexNode();
        ensureStableCluster(3);

        // Override the WORKLOAD_MEMORY_OVERHEAD of 500MB because testing often runs 512MB nodes.
        // Shards need more space to be assigned than what's leftover with the default node overheads. A value of 100 is arbitrary.
        internalCluster().getInstance(StatelessMemoryMetricsService.class, internalCluster().getMasterName())
            .setWorkloadMemoryOverheadOverrideForTesting(100);

        // Place index shards on both index nodes.
        final var indexNameA = randomIdentifier();
        createIndex(indexNameA, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameA).build());
        final var indexNameB = randomIdentifier();
        createIndex(indexNameB, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameB).build());

        final Map<String, Long> nodeHeapMaxLookupById = getNodeMaxHeapSizes(nodeNameA, nodeNameB);

        // Fake large heap memory usages to block allocation
        final var interceptor = new HeapUsagePublicationInterceptor(
            MockTransportService.getInstance(masterNodeName),
            nodeHeapMaxLookupById
        );
        interceptor.injectAll(nodeHeapMaxLookupById.keySet());
        interceptor.waitForNextPublication();

        final ClusterInfo clusterInfo = refreshClusterInfo();
        assertTrue(
            "expect all estimated heap usages to be greater than 100%, but got " + clusterInfo.getEstimatedHeapUsages(),
            clusterInfo.getEstimatedHeapUsages()
                .values()
                .stream()
                .allMatch(estimatedHeapUsage -> estimatedHeapUsage.estimatedUsageAsPercentage() > 100.0)
        );

        // canRemain should return NO for assigned shards because the nodes are above the heap usage watermark.
        {
            final var explainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
            explainRequest.setIndex(indexNameA).setPrimary(true).setShard(0);
            final var explainResponse = safeGet(client().execute(TransportClusterAllocationExplainAction.TYPE, explainRequest));
            var canRemainDecision = explainResponse.getExplanation().getShardAllocationDecision().getMoveDecision().getCanRemainDecision();
            assertThat(canRemainDecision.getDecisions().toString(), canRemainDecision.type(), equalTo(Decision.Type.NO));
        }

        // canAllocate should return NO for new shards because all the nodes are above the heap usage watermark.
        final String indexName = "index";
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(indexSettings(1, 0).put(SETTING_ALLOCATION_MAX_RETRY.getKey(), "0"))
                .setWaitForActiveShards(0)
                .execute()
        );
        ensureRed(indexName);

        final var allocationExplainRequest = new ClusterAllocationExplainRequest(TEST_REQUEST_TIMEOUT);
        allocationExplainRequest.setIndex(indexName).setPrimary(true).setShard(0);
        final var allocationExplainResponse = safeGet(
            client().execute(TransportClusterAllocationExplainAction.TYPE, allocationExplainRequest)
        );

        assertThat(
            allocationExplainResponse.getExplanation().getShardAllocationDecision().getAllocateDecision().getAllocationStatus(),
            is(UnassignedInfo.AllocationStatus.DECIDERS_NO)
        );
        final List<NodeAllocationResult> nodeDecisions = allocationExplainResponse.getExplanation()
            .getShardAllocationDecision()
            .getAllocateDecision()
            .getNodeDecisions();
        assertNotNull(nodeDecisions);
        assertTrue(
            "unexpected node decisions:\n"
                + Strings.collectionToDelimitedString(nodeDecisions.stream().map(Strings::toString).toList(), "\n"),
            nodeDecisions.stream()
                .allMatch(
                    nodeDecision -> nodeDecision.getNodeDecision() == AllocationDecision.NO
                        && nodeDecision.getCanAllocateDecision()
                            .getDecisions()
                            .stream()
                            .anyMatch(decision -> decision.getExplanation().startsWith("insufficient estimated heap"))
                )
        );

        // Stop faking large heap memory usages and unblock allocation
        final String nodeIdToUnblock = getNodeId(randomFrom(nodeNameA, nodeNameB));

        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reroute due to heap usages dropped below low watermark",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "estimated heap usages dropped below the low watermark * triggering reroute"
                )
            );

            // Remove the node from the injection set before waiting for a new publication. This is important to ensure the order of
            // node-removed-from-injection -> set-new-publication-latch -> handle-new-publication.
            interceptor.stopInjecting(nodeIdToUnblock);
            interceptor.waitForNextPublication();

            final ClusterInfo clusterInfo2 = refreshClusterInfo();
            assertTrue(
                "unexpected estimated heap usages " + clusterInfo2.getEstimatedHeapUsages(),
                clusterInfo2.getEstimatedHeapUsages().entrySet().stream().allMatch(entry -> {
                    if (entry.getKey().equals(nodeIdToUnblock)) {
                        return entry.getValue().estimatedUsageAsPercentage() < 100.0;
                    } else {
                        return entry.getValue().estimatedUsageAsPercentage() > 100.0;
                    }
                })
            );

            ensureGreen(indexName);
            mockLog.assertAllExpectationsMatched();
        }

        final IndexShard indexShard = findIndexShard(indexName);
        assertThat(indexShard.routingEntry().currentNodeId(), equalTo(nodeIdToUnblock));
    }

    @TestLogging(value = "org.elasticsearch.xpack.stateless.allocation.EstimatedHeapUsageMonitor:DEBUG", reason = "debug log for test")
    public void testEstimatedHeapHighWatermarkMonitorTriggersReroute() {
        final var masterNodeName = startMasterOnlyNode();
        final var nodeNameA = startIndexNode();
        final var nodeNameB = startIndexNode();
        ensureStableCluster(3);

        // Override the WORKLOAD_MEMORY_OVERHEAD of 500MB because testing often runs 512MB nodes.
        internalCluster().getInstance(StatelessMemoryMetricsService.class, internalCluster().getMasterName())
            .setWorkloadMemoryOverheadOverrideForTesting(100);

        // Place index shards on both index nodes so that both publish memory metrics.
        final var indexNameA = randomIdentifier();
        createIndex(indexNameA, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameA).build());
        final var indexNameB = randomIdentifier();
        createIndex(indexNameB, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameB).build());

        final Map<String, Long> nodeHeapMaxLookupById = getNodeMaxHeapSizes(nodeNameA, nodeNameB);

        // Use a conventional watermark configuration: low (allocation blocked) at 70%, high (shards moved) at 80%.
        updateClusterSettings(
            Settings.builder()
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_LOW_WATERMARK.getKey(), "70%")
                .put(EstimatedHeapUsageAllocationDecider.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_HIGH_WATERMARK.getKey(), "80%")
        );

        // No injection yet — nodes start well below the 80% high watermark.
        final var interceptor = new HeapUsagePublicationInterceptor(
            MockTransportService.getInstance(masterNodeName),
            nodeHeapMaxLookupById
        );
        interceptor.waitForNextPublication();
        final ClusterInfo initialClusterInfo = refreshClusterInfo();
        assertTrue(
            "expected all estimated heap usages to be below the 80% high watermark initially, but got "
                + initialClusterInfo.getEstimatedHeapUsages(),
            initialClusterInfo.getEstimatedHeapUsages()
                .values()
                .stream()
                .allMatch(estimatedHeapUsage -> estimatedHeapUsage.estimatedUsageAsPercentage() < 80.0)
        );

        // Inject large heap memory usages to push all nodes above the 80% high watermark.
        // The monitor should detect the new nodes exceeding the high watermark and trigger a reroute.
        try (MockLog mockLog = MockLog.capture(EstimatedHeapUsageMonitor.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "reroute due to heap usages exceeded high watermark",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "estimated heap usages exceeded the high watermark * triggering reroute"
                )
            );

            interceptor.injectAll(nodeHeapMaxLookupById.keySet());
            interceptor.waitForNextPublication();
            refreshClusterInfo();

            mockLog.assertAllExpectationsMatched();

            // Stop injecting for one node so it drops below the high watermark.
            // The set of nodes exceeding the high watermark has changed, but it has not grown, so no reroute should be triggered.
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation(
                    "no reroute when a node drops below the high watermark",
                    EstimatedHeapUsageMonitor.class.getCanonicalName(),
                    Level.DEBUG,
                    "estimated heap usages exceeded the high watermark * triggering reroute"
                )
            );

            interceptor.stopInjecting(nodeHeapMaxLookupById.keySet().iterator().next());
            interceptor.waitForNextPublication();
            refreshClusterInfo();

            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Get the max heap size for the specified nodes
     *
     * @param nodeNames The names of the nodes to get the max heap size for
     * @return A map of the node IDs to their max heap sizes
     */
    private static Map<String, Long> getNodeMaxHeapSizes(String... nodeNames) {
        final var nodesStatsResponse = safeGet(
            client().admin()
                .cluster()
                .prepareNodesStats(Arrays.stream(nodeNames).map(ESIntegTestCase::getNodeId).toArray(String[]::new))
                .setJvm(true)
                .execute()
        );
        return nodesStatsResponse.getNodes()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    nodeStats -> nodeStats.getNode().getId(),
                    nodeStats -> nodeStats.getJvm().getMem().getHeapMax().getBytes()
                )
            );
    }

    /**
     * Intercepts {@link TransportPublishHeapMemoryMetrics} requests on the master and optionally replaces the reported mapping sizes with
     * the node's full heap max, simulating a node that is fully saturated. Nodes are added to and removed from injection via
     * {@link #injectAll(Collection)} and {@link #stopInjecting(String)}. Use {@link #waitForNextPublication()} to synchronize
     * with the next publication cycle from every tracked node.
     */
    private class HeapUsagePublicationInterceptor {

        private final Map<String, Long> nodeHeapMaxLookupById;
        private final Set<String> nodesToInject = ConcurrentCollections.newConcurrentSet();
        private final ConcurrentHashMap<String, CountDownLatch> nodeIdToPublicationLatch;

        HeapUsagePublicationInterceptor(MockTransportService masterMockTransportService, Map<String, Long> nodeHeapMaxLookupById) {
            this.nodeHeapMaxLookupById = nodeHeapMaxLookupById;
            this.nodeIdToPublicationLatch = new ConcurrentHashMap<>(
                Maps.transformValues(nodeHeapMaxLookupById, v -> new CountDownLatch(1))
            );
            masterMockTransportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, this::handlePublication);
        }

        /** Start injecting saturated heap usage for the specified nodes. */
        void injectAll(Collection<String> nodeIds) {
            nodesToInject.addAll(nodeIds);
        }

        /** Stop injecting saturated heap usage for the given node, letting it report real usage again. */
        void stopInjecting(String nodeId) {
            nodesToInject.remove(nodeId);
        }

        /**
         * Resets the per-node latches and blocks until each tracked node has completed one publication cycle. This ensures callers
         * observe a publication that started after any injection-set changes were made.
         */
        void waitForNextPublication() {
            nodeIdToPublicationLatch.keySet().forEach(nodeId -> nodeIdToPublicationLatch.put(nodeId, new CountDownLatch(1)));
            nodeIdToPublicationLatch.forEach((nodeId, latch) -> {
                logger.info("--> waiting for publication from node [{}]", nodeId);
                safeAwait(latch);
            });
        }

        private void handlePublication(
            TransportRequestHandler<TransportRequest> handler,
            TransportRequest request,
            TransportChannel channel,
            Task task
        ) throws Exception {
            final var publishHeapMemoryMetricsRequest = asInstanceOf(PublishHeapMemoryMetricsRequest.class, request);
            final var heapMemoryUsage = publishHeapMemoryMetricsRequest.getHeapMemoryUsage();
            final var shardMappingSizes = heapMemoryUsage.shardMappingSizes();
            if (shardMappingSizes.isEmpty()) {
                handler.messageReceived(request, channel, task);
                return;
            }

            final String nodeId = nodeIdFromShardMappingSizes(heapMemoryUsage);
            // We use a latch for each node to wait for new publication. It is important that this publication gets a reference to the latch
            // only after the latch is set in waitForPublication. Otherwise, the publication may be stale, i.e. it starts before the latch
            // is set, and the test can fail. Therefore, we take the latch before handling the publication. If a new latch is set halfway
            // through the publication handling, it will not be pulled by the existing publication, but the next one, which is expected.
            final CountDownLatch latch = nodeIdToPublicationLatch.get(nodeId);
            if (nodesToInject.contains(nodeId)) {
                assertThat(nodeHeapMaxLookupById, hasKey(nodeId));
                final long nodeHeapMax = nodeHeapMaxLookupById.get(nodeId);
                final var updatedShardMappingSizes = shardMappingSizes.entrySet()
                    .stream()
                    .collect(
                        Collectors.toUnmodifiableMap(
                            Map.Entry::getKey,
                            entry -> new ShardMappingSize(
                                nodeHeapMax,
                                entry.getValue().numSegments(),
                                entry.getValue().totalFields(),
                                entry.getValue().postingsInMemoryBytes(),
                                entry.getValue().liveDocsBytes(),
                                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                                nodeId
                            )
                        )
                    );
                handler.messageReceived(
                    new PublishHeapMemoryMetricsRequest(
                        new HeapMemoryUsage(
                            heapMemoryUsage.publicationSeqNo(),
                            updatedShardMappingSizes,
                            heapMemoryUsage.clusterStateVersion()
                        )
                    ),
                    channel,
                    task
                );
            } else {
                handler.messageReceived(request, channel, task);
            }
            latch.countDown();
        }

        private static String nodeIdFromShardMappingSizes(HeapMemoryUsage heapMemoryUsage) {
            final Set<String> nodeIds = heapMemoryUsage.shardMappingSizes()
                .values()
                .stream()
                .map(ShardMappingSize::nodeId)
                .collect(Collectors.toUnmodifiableSet());
            assertThat(nodeIds, hasSize(1));
            return nodeIds.iterator().next();
        }
    }
}
