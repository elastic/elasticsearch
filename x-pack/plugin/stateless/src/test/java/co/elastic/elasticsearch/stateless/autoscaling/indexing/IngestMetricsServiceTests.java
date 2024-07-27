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

package co.elastic.elasticsearch.stateless.autoscaling.indexing;

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.NodesShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata;
import org.elasticsearch.cluster.metadata.SingleNodeShutdownMetadata.Type;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.ACCURATE_LOAD_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.SHUTDOWN_ATTENUATION_ENABLED;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.STALE_LOAD_WINDOW;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class IngestMetricsServiceTests extends ESTestCase {

    private MemoryMetricsService memoryMetricsService;

    @Before
    public void init() {
        memoryMetricsService = new MemoryMetricsService(
            System::nanoTime,
            new ClusterSettings(
                Settings.EMPTY,
                Sets.addToCopy(
                    ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
                    MemoryMetricsService.STALE_METRICS_CHECK_DURATION_SETTING,
                    MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
                    MemoryMetricsService.SHARD_MEMORY_OVERHEAD_SETTING
                )
            )
        );
    }

    public void testServiceOnlyReturnDataWhenLocalNodeIsElectedAsMaster() {
        var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var remoteNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var nodes = DiscoveryNodes.builder().add(localNode).add(remoteNode).localNodeId(localNode.getId()).build();
        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService);
        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        // If the node is not elected as master (i.e. we haven't got any cluster state notification) it shouldn't return any info
        assertThat(indexTierMetrics.getNodesLoad(), is(empty()));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node not elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(nodes)
            )
        );

        var indexTierMetricsAfterClusterStateEvent = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(indexTierMetricsAfterClusterStateEvent.getNodesLoad(), is(empty()));
    }

    public void testOnlyIndexNodesAreTracked() {
        final var localNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();

        final var nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .localNodeId(localNode.getId())
            .build();

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService);

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()),
                clusterState(nodes)
            )
        );
        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        var metricQualityCount = indexTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeIngestLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(indexTierMetrics.toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));
    }

    public void testIngestionLoadIsKeptDuringNodeLifecycle() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var indexNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        var fakeClock = new AtomicLong();

        var inaccurateMetricTime = TimeValue.timeValueSeconds(25);
        var staleLoadWindow = TimeValue.timeValueMinutes(10);
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(ACCURATE_LOAD_WINDOW.getKey(), inaccurateMetricTime)
                    .put(STALE_LOAD_WINDOW.getKey(), staleLoadWindow)
                    .build()
            ),
            fakeClock::get,
            memoryMetricsService
        );

        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState(nodesWithElectedMaster), clusterState(nodes)));

        // Take into account the case where the index node sends the metric to the new master node before it applies the new cluster state
        if (randomBoolean()) {
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.trackNodeIngestLoad(indexNode.getId(), 1, 0.5);
        }

        var nodesWithIndexingNode = DiscoveryNodes.builder(nodesWithElectedMaster).add(indexNode).build();

        service.clusterChanged(
            new ClusterChangedEvent("index node joins", clusterState(nodesWithIndexingNode), clusterState(nodesWithElectedMaster))
        );

        fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
        service.trackNodeIngestLoad(indexNode.getId(), 2, 1.5);

        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(1.5)));
        assertThat(indexTierMetrics.toString(), indexNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));

        if (randomBoolean()) {
            service.clusterChanged(
                new ClusterChangedEvent("index node leaves", clusterState(nodesWithElectedMaster), clusterState(nodesWithIndexingNode))
            );
        }

        fakeClock.addAndGet(inaccurateMetricTime.getNanos());

        var indexTierMetricsAfterNodeMetricIsInaccurate = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(indexTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad(), hasSize(1));

        var indexNodeLoadAfterMissingMetrics = indexTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad().get(0);
        assertThat(indexNodeLoadAfterMissingMetrics.load(), is(equalTo(1.5)));
        assertThat(indexNodeLoadAfterMissingMetrics.metricQuality(), is(equalTo(MetricQuality.MINIMUM)));

        // The node re-joins before the metric is considered to be inaccurate
        if (randomBoolean()) {
            service.clusterChanged(
                new ClusterChangedEvent("index node re-joins", clusterState(nodesWithIndexingNode), clusterState(nodesWithElectedMaster))
            );
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.trackNodeIngestLoad(indexNode.getId(), 3, 0.5);

            var indexTierMetricsAfterNodeReJoins = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(indexTierMetricsAfterNodeReJoins.getNodesLoad(), hasSize(1));

            var indexNodeLoadAfterRejoining = indexTierMetricsAfterNodeReJoins.getNodesLoad().get(0);
            assertThat(indexNodeLoadAfterRejoining.load(), is(equalTo(0.5)));
            assertThat(indexNodeLoadAfterRejoining.metricQuality(), is(equalTo(MetricQuality.EXACT)));
        } else {
            // The node do not re-join after the max time
            fakeClock.addAndGet(staleLoadWindow.getNanos());

            var indexTierMetricsAfterTTLExpires = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
            assertThat(indexTierMetricsAfterTTLExpires.getNodesLoad(), hasSize(0));
        }
    }

    public void testOutOfOrderMetricsAreDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var indexNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(indexNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService);

        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState(nodesWithElectedMaster), clusterState(nodes)));

        var maxSeqNo = randomIntBetween(10, 20);
        var maxSeqNoIngestionLoad = randomIngestionLoad();
        service.trackNodeIngestLoad(indexNode.getId(), maxSeqNo, maxSeqNoIngestionLoad);

        var numberOfOutOfOrderMetricSamples = randomIntBetween(1, maxSeqNo);
        var unorderedSeqNos = IntStream.of(numberOfOutOfOrderMetricSamples).boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(unorderedSeqNos, random());
        for (long seqNo : unorderedSeqNos) {
            service.trackNodeIngestLoad(indexNode.getId(), seqNo, randomIngestionLoad());
        }

        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(maxSeqNoIngestionLoad)));
        assertThat(indexNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));
    }

    public void testServiceStopsReturningInfoAfterMasterTakeover() {
        final var localNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();

        final var remoteNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build();
        final var nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(remoteNode)
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .localNodeId(localNode.getId())
            .build();

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService);

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()),
                clusterState(nodes)
            )
        );
        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        var metricQualityCount = indexTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeIngestLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(indexTierMetrics.getNodesLoad().toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Remote node elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build())
            )
        );

        var indexTierMetricsAfterMasterHandover = service.getIndexTierMetrics(ClusterState.EMPTY_STATE);
        assertThat(indexTierMetricsAfterMasterHandover.getNodesLoad(), is(empty()));
    }

    public void testIngestLoadsMetricsAttenuatedForShutdownMetadata() {
        // Initial state
        final ClusterState state1;
        {
            final List<DiscoveryNode> nodes = IntStream.range(0, between(1, 8))
                .mapToObj(
                    i -> DiscoveryNodeUtils.builder("node-" + i)
                        .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                        .build()
                )
                .toList();

            final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
            for (int i = 0; i < nodes.size(); i++) {
                final DiscoveryNode node = nodes.get(i);
                if (i == 0) {
                    builder.add(node).localNodeId(node.getId()).masterNodeId(node.getId());
                } else {
                    builder.add(node);
                }
            }
            state1 = clusterState(builder.build());
        }

        var service = new IngestMetricsService(
            clusterSettings(Settings.builder().put(SHUTDOWN_ATTENUATION_ENABLED.getKey(), true).build()),
            () -> 0,
            memoryMetricsService
        );
        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        final List<Double> ingestionLoads1 = trackRandomIngestionLoads(service, seqNoSupplier, state1);
        assertExactIngestionLoads(service, ingestionLoads1, state1);

        // Simulate nodes shutting down and new nodes joining
        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(
            Math.min(between(1, 3), state1.nodes().size()),
            state1.nodes().getAllNodes()
        );
        final var nodeShutdownMetadata = createShutdownMetadata(shuttingDownNodes);
        final List<DiscoveryNode> newNodes = IntStream.range(state1.nodes().size(), state1.nodes().size() + shuttingDownNodes.size())
            .mapToObj(
                i -> DiscoveryNodeUtils.builder("node-" + i)
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                    .build()
            )
            .toList();

        final ClusterState state2, state3;
        final List<Double> ingestionLoads3;
        if (randomBoolean()) {
            // shutdown first and then add new nodes
            state2 = ClusterState.builder(state1)
                .metadata(Metadata.builder(state1.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state2, state1));
            final List<Double> ingestionLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            assertAttenuatedIngestionLoads(service, ingestionLoads2, state2);

            state3 = stateWithNewNodes(state2, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state3, state2));
            ingestionLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, state3);
            assertAttenuatedIngestionLoads(service, ingestionLoads3, state3);
        } else {
            // add new nodes first then shutdown
            state2 = stateWithNewNodes(state1, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state2, state1));
            final List<Double> ingestionLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            assertExactIngestionLoads(service, ingestionLoads2, state2);

            state3 = ClusterState.builder(state2)
                .metadata(Metadata.builder(state1.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state3, state2));
            ingestionLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, state3);
            assertAttenuatedIngestionLoads(service, ingestionLoads3, state3);
        }

        final ClusterState state4;
        if (randomBoolean()) {
            // shutdown nodes left the cluster
            final var builder = DiscoveryNodes.builder(state3.nodes());
            shuttingDownNodes.forEach(node -> builder.remove(node.getId()));
            if (shuttingDownNodes.contains(state3.nodes().getMasterNode())) {
                final DiscoveryNode newMasterNode = randomValueOtherThanMany(
                    shuttingDownNodes::contains,
                    () -> randomFrom(state3.nodes().getAllNodes())
                );
                builder.localNodeId(newMasterNode.getId()).masterNodeId(newMasterNode.getId());
            }
            // Randomly also remove the shutdown metadata for some of the nodes that have left.
            final boolean shutdownMetadataRemoved = randomBoolean();
            if (shutdownMetadataRemoved) {
                state4 = ClusterState.builder(state3)
                    .nodes(builder.build())
                    .metadata(Metadata.builder(state3.metadata()).removeCustom(NodesShutdownMetadata.TYPE))
                    .build();
            } else {
                state4 = ClusterState.builder(state3).nodes(builder.build()).build();
            }
            service.clusterChanged(new ClusterChangedEvent("node-left", state4, state3));
            final List<NodeIngestLoadSnapshot> nodesLoad4 = service.getIndexTierMetrics(state4).getNodesLoad();
            if (shutdownMetadataRemoved) {
                assertThat(nodesLoad4, hasSize(state3.nodes().size()));
                // Nodes left the cluster have their metric quality as minimum
                assertThat(
                    nodesLoad4.stream().filter(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.MINIMUM).count(),
                    equalTo((long) shuttingDownNodes.size())
                );
                assertThat(
                    nodesLoad4.stream().filter(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.EXACT).count(),
                    equalTo((long) state4.nodes().size())
                );
            } else {
                // Metrics are attenuated because the nodes left still have their shutdown metadata around
                assertThat(nodesLoad4, hasSize(state4.nodes().size()));
                doAssertAttenuatedIngestionLoads(ingestionLoads3, nodesLoad4);
            }
        } else {
            // shutdown metadata removed, i.e. shutdown cancelled and nodes remain in the cluster
            state4 = ClusterState.builder(state3)
                .metadata(Metadata.builder(state3.metadata()).removeCustom(NodesShutdownMetadata.TYPE))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown-cancelled", state4, state3));
            final List<Double> ingestionLoads4 = trackRandomIngestionLoads(service, seqNoSupplier, state4);
            assertExactIngestionLoads(service, ingestionLoads4, state4);
        }
    }

    private ClusterState stateWithNewNodes(ClusterState state, List<DiscoveryNode> nodes) {
        final var builder = DiscoveryNodes.builder(state.nodes());
        nodes.forEach(builder::add);
        return ClusterState.builder(state).nodes(builder.build()).build();
    }

    private List<Double> trackRandomIngestionLoads(IngestMetricsService service, LongSupplier seqNoSupplier, ClusterState state) {
        final List<Double> ingestionLoads = randomList(
            state.nodes().size(),
            state.nodes().size(),
            IngestMetricsServiceTests::randomIngestionLoad
        );
        final Iterator<Double> ingestionLoadsIterator = ingestionLoads.iterator();
        state.nodes().forEach(node -> service.trackNodeIngestLoad(node.getId(), seqNoSupplier.getAsLong(), ingestionLoadsIterator.next()));
        return ingestionLoads;
    }

    private void assertExactIngestionLoads(IngestMetricsService service, List<Double> allIngestionLoads, ClusterState state) {
        final List<NodeIngestLoadSnapshot> newNodeLoadList = service.getIndexTierMetrics(state).getNodesLoad();
        assertThat(newNodeLoadList.size(), equalTo(state.nodes().size()));
        assertTrue(newNodeLoadList.stream().allMatch(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.EXACT));
        assertArrayEquals(
            allIngestionLoads.stream().mapToDouble(Double::doubleValue).sorted().toArray(),
            newNodeLoadList.stream().mapToDouble(NodeIngestLoadSnapshot::load).sorted().toArray(),
            0.0000001
        );
    }

    private void assertAttenuatedIngestionLoads(IngestMetricsService service, List<Double> allIngestionLoads, ClusterState state) {
        final List<NodeIngestLoadSnapshot> newNodeLoadList = service.getIndexTierMetrics(state).getNodesLoad();
        assertThat(newNodeLoadList.size(), equalTo(state.nodes().size() - state.metadata().nodeShutdowns().getAllNodeIds().size()));
        doAssertAttenuatedIngestionLoads(allIngestionLoads, newNodeLoadList);
    }

    private static void doAssertAttenuatedIngestionLoads(List<Double> allIngestionLoads, List<NodeIngestLoadSnapshot> newNodeLoadList) {
        newNodeLoadList.forEach(nodeLoad -> assertThat(nodeLoad.metricQuality(), is(MetricQuality.MINIMUM)));
        final List<Double> newLoadValues = newNodeLoadList.stream().map(NodeIngestLoadSnapshot::load).toList();
        // All nodeLoads that are _not_ reported have higher load values
        final double epsilon = 0.0000001;
        allIngestionLoads.stream()
            // Filter for metrics that are _not_ returned in the new report
            .filter(
                nodeLoad -> newLoadValues.stream()
                    .noneMatch(newLoadValue -> Double.compare(newLoadValue, nodeLoad) == 0 || Math.abs(newLoadValue - nodeLoad) <= epsilon)
            )
            .forEach(
                ignoredNodeLoad -> assertThat(
                    "newNodeLoadList " + newNodeLoadList + " has higher load value than " + ignoredNodeLoad,
                    newNodeLoadList.stream().allMatch(nodeLoad -> nodeLoad.load() < ignoredNodeLoad),
                    is(true)
                )
            );
    }

    private static NodesShutdownMetadata createShutdownMetadata(List<DiscoveryNode> shuttingDownNodes) {
        return new NodesShutdownMetadata(shuttingDownNodes.stream().collect(Collectors.toMap(DiscoveryNode::getId, node -> {
            final var shutdownType = randomFrom(Type.values());
            final var singleShutdownMetadataBuilder = SingleNodeShutdownMetadata.builder()
                .setNodeId(node.getId())
                .setReason("test")
                .setType(shutdownType)
                .setStartedAtMillis(randomNonNegativeLong());
            if (shutdownType.equals(Type.REPLACE)) {
                singleShutdownMetadataBuilder.setTargetNodeName(randomIdentifier());
            } else if (shutdownType.equals(Type.SIGTERM)) {
                singleShutdownMetadataBuilder.setGracePeriod(TimeValue.MAX_VALUE);
            }
            return singleShutdownMetadataBuilder.build();
        })));
    }

    private static double randomIngestionLoad() {
        return randomDoubleBetween(0, 16, true);
    }

    private static ClusterState clusterState(DiscoveryNodes nodes) {
        assert nodes != null;
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, Set.of(ACCURATE_LOAD_WINDOW, STALE_LOAD_WINDOW, SHUTDOWN_ATTENUATION_ENABLED));
    }
}
