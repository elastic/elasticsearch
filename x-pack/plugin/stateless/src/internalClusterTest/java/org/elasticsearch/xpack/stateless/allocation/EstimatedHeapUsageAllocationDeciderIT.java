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
import co.elastic.elasticsearch.stateless.autoscaling.memory.HeapMemoryUsage;
import co.elastic.elasticsearch.stateless.autoscaling.memory.PublishHeapMemoryMetricsRequest;
import co.elastic.elasticsearch.stateless.autoscaling.memory.ShardMappingSize;
import co.elastic.elasticsearch.stateless.autoscaling.memory.TransportPublishHeapMemoryMetrics;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.admin.cluster.allocation.ClusterAllocationExplainRequest;
import org.elasticsearch.action.admin.cluster.allocation.TransportClusterAllocationExplainAction;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.cluster.ClusterInfoService;
import org.elasticsearch.cluster.ClusterInfoServiceUtils;
import org.elasticsearch.cluster.InternalClusterInfoService;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.AllocationDecision;
import org.elasticsearch.cluster.routing.allocation.NodeAllocationResult;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.test.transport.MockTransportService;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class EstimatedHeapUsageAllocationDeciderIT extends AbstractStatelessIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(MockTransportService.TestPlugin.class)).collect(Collectors.toList());
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(
            "serverless.autoscaling.memory_metrics.indices_mapping_size.publication.frequency",
            TimeValue.timeValueMillis(10)
        )
            .put(InternalClusterInfoService.CLUSTER_ROUTING_ALLOCATION_ESTIMATED_HEAP_THRESHOLD_DECIDER_ENABLED.getKey(), true)
            // Ensure it's enabled for this test
            .put(EstimatedHeapUsageAllocationDecider.MINIMUM_HEAP_SIZE_FOR_ENABLEMENT.getKey(), "100mb");
    }

    @TestLogging(value = "co.elastic.elasticsearch.stateless.allocation.EstimatedHeapUsageMonitor:DEBUG", reason = "debug log for test")
    public void testEstimatedHeapAllocationDecider() {
        final var masterNodeName = startMasterOnlyNode();
        final var nodeNameA = startIndexNode();
        final var nodeNameB = startIndexNode();
        ensureStableCluster(3);

        final var indexNameA = randomIdentifier();
        createIndex(indexNameA, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameA).build());

        final var indexNameB = randomIdentifier();
        createIndex(indexNameB, indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + "._name", nodeNameB).build());

        final var nodesStatsResponse = safeGet(
            client().admin().cluster().prepareNodesStats(getNodeId(nodeNameA), getNodeId(nodeNameB)).setJvm(true).execute()
        );
        final Map<String, Long> nodeHeapMaxLookupById = nodesStatsResponse.getNodes()
            .stream()
            .collect(
                Collectors.toUnmodifiableMap(
                    nodeStats -> nodeStats.getNode().getId(),
                    nodeStats -> nodeStats.getJvm().getMem().getHeapMax().getBytes()
                )
            );

        // Fake large heap memory usages to block allocation
        final var nodesToInjectEstimatedHeapUsage = ConcurrentCollections.newConcurrentSet();
        nodesToInjectEstimatedHeapUsage.addAll(nodeHeapMaxLookupById.keySet());
        final var nodeIdToPublicationLatch = new ConcurrentHashMap<>(
            Maps.transformValues(nodeHeapMaxLookupById, v -> new CountDownLatch(1))
        );
        final var masterMockTransportService = MockTransportService.getInstance(masterNodeName);
        masterMockTransportService.addRequestHandlingBehavior(TransportPublishHeapMemoryMetrics.NAME, (handler, request, channel, task) -> {
            final var publishHeapMemoryMetricsRequest = asInstanceOf(PublishHeapMemoryMetricsRequest.class, request);
            final var heapMemoryUsage = publishHeapMemoryMetricsRequest.getHeapMemoryUsage();
            final var shardMappingSizes = heapMemoryUsage.shardMappingSizes();
            if (shardMappingSizes.isEmpty()) {
                handler.messageReceived(request, channel, task);
                return;
            }

            final String nodeId = nodeIdFromHeapMemoryUsage(heapMemoryUsage);
            // We use a latch for each node to wait for new publication. It is important that the publication pulls the latch
            // starts strictly after the latch is set in waitForRefreshedHeapMemoryUsagePublication. Otherwise, the publication
            // may be stale, i.e. it starts before the latch is set, and the test can fail. Therefore, we get the latch once
            // before handling the publication. If a new latch is set halfway through the publication handling, it will not
            // be pulled by the existing publication, but the next one which is the expected behavior.
            final CountDownLatch latch = nodeIdToPublicationLatch.get(nodeId);
            if (nodesToInjectEstimatedHeapUsage.contains(nodeId)) {
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
        });

        waitForRefreshedHeapMemoryUsagePublication(nodeIdToPublicationLatch);

        final var infoService = asInstanceOf(
            InternalClusterInfoService.class,
            internalCluster().getCurrentMasterNodeInstance(ClusterInfoService.class)
        );
        ClusterInfoServiceUtils.setUpdateFrequency(infoService, TimeValue.timeValueMillis(100));
        final ClusterInfo clusterInfo = ClusterInfoServiceUtils.refresh(infoService);
        assertTrue(
            "expect all estimated heap usages to be greater than 100%, but got " + clusterInfo.getEstimatedHeapUsages(),
            clusterInfo.getEstimatedHeapUsages()
                .values()
                .stream()
                .allMatch(estimatedHeapUsage -> estimatedHeapUsage.estimatedUsageAsPercentage() > 100.0)
        );

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

            // Remove the node from the injection set before wait for new publication. This is important to ensure the order of
            // node-removed-from-injection -> set-new-publication-latch -> handle-new-publication.
            nodesToInjectEstimatedHeapUsage.remove(nodeIdToUnblock);
            waitForRefreshedHeapMemoryUsagePublication(nodeIdToPublicationLatch);

            final ClusterInfo clusterInfo2 = ClusterInfoServiceUtils.refresh(infoService);
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

    private void waitForRefreshedHeapMemoryUsagePublication(Map<String, CountDownLatch> nodeIdToPublicationLatch) {
        nodeIdToPublicationLatch.keySet().forEach(nodeId -> nodeIdToPublicationLatch.put(nodeId, new CountDownLatch(1)));
        nodeIdToPublicationLatch.forEach((nodeId, latch) -> {
            logger.info("--> waiting for publication from node [{}]", nodeId);
            safeAwait(latch);
        });
    }

    private String nodeIdFromHeapMemoryUsage(HeapMemoryUsage heapMemoryUsage) {
        final Set<String> nodeIds = heapMemoryUsage.shardMappingSizes()
            .values()
            .stream()
            .map(ShardMappingSize::nodeId)
            .collect(Collectors.toUnmodifiableSet());
        assertThat(nodeIds, hasSize(1));
        return nodeIds.iterator().next();
    }
}
