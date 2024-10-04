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

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.RawAndAdjustedNodeIngestLoadSnapshots;
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
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.MetricRecorder;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.Instrument;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.ACCURATE_LOAD_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.STALE_LOAD_WINDOW;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class IngestMetricsServiceTests extends ESTestCase {

    private static final double EPSILON = 0.0000001;
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
                    MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING
                )
            ),
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE
        );
    }

    public void testServiceOnlyReturnDataWhenLocalNodeIsElectedAsMaster() {
        var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var remoteNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var nodes = DiscoveryNodes.builder().add(localNode).add(remoteNode).localNodeId(localNode.getId()).build();
        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
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

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService, MeterRegistry.NOOP);

        final var state = clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build());
        service.clusterChanged(new ClusterChangedEvent("Local node elected as master", state, clusterState(nodes)));
        var indexTierMetrics = service.getIndexTierMetrics(state);
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
            memoryMetricsService,
            MeterRegistry.NOOP
        );

        final var clusterState1 = clusterState(nodesWithElectedMaster);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState1, clusterState(nodes)));

        // Take into account the case where the index node sends the metric to the new master node before it applies the new cluster state
        if (randomBoolean()) {
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), 1, 0.5);
        }

        var nodesWithIndexingNode = DiscoveryNodes.builder(nodesWithElectedMaster).add(indexNode).build();
        final var clusterState2 = clusterState(nodesWithIndexingNode);
        service.clusterChanged(new ClusterChangedEvent("index node joins", clusterState2, clusterState(nodesWithElectedMaster)));

        fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), 2, 1.5);

        var indexTierMetrics = service.getIndexTierMetrics(clusterState2);
        assertThat(indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(1.5)));
        assertThat(indexTierMetrics.toString(), indexNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));

        final var indexNodeLeaves = randomBoolean();
        if (indexNodeLeaves) {
            service.clusterChanged(
                new ClusterChangedEvent("index node leaves", clusterState(nodesWithElectedMaster), clusterState(nodesWithIndexingNode))
            );
        }

        fakeClock.addAndGet(inaccurateMetricTime.getNanos());

        final var currentClusterState = indexNodeLeaves ? clusterState(nodesWithElectedMaster) : clusterState(nodesWithIndexingNode);
        var indexTierMetricsAfterNodeMetricIsInaccurate = service.getIndexTierMetrics(currentClusterState);
        if (indexNodeLeaves) {
            // We clean it up
            assertThat(indexTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad(), hasSize(0));
        } else {
            assertThat(indexTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad(), hasSize(1));
            var indexNodeLoadAfterMissingMetrics = indexTierMetricsAfterNodeMetricIsInaccurate.getNodesLoad().get(0);
            assertThat(indexNodeLoadAfterMissingMetrics.load(), is(equalTo(1.5)));
            assertThat(indexNodeLoadAfterMissingMetrics.metricQuality(), is(equalTo(MetricQuality.MINIMUM)));
        }

        // The node re-joins before the metric is considered to be inaccurate
        if (randomBoolean()) {
            final var clusterState3 = clusterState(nodesWithIndexingNode);
            service.clusterChanged(new ClusterChangedEvent("index node re-joins", clusterState3, clusterState(nodesWithElectedMaster)));
            fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
            service.trackNodeIngestLoad(clusterState3, indexNode.getId(), indexNode.getName(), 3, 0.5);

            var indexTierMetricsAfterNodeReJoins = service.getIndexTierMetrics(clusterState3);
            assertThat(indexTierMetricsAfterNodeReJoins.getNodesLoad(), hasSize(1));

            var indexNodeLoadAfterRejoining = indexTierMetricsAfterNodeReJoins.getNodesLoad().get(0);
            assertThat(indexNodeLoadAfterRejoining.load(), is(equalTo(0.5)));
            assertThat(indexNodeLoadAfterRejoining.metricQuality(), is(equalTo(MetricQuality.EXACT)));
        } else {
            // The node do not re-join after the max time
            fakeClock.addAndGet(staleLoadWindow.getNanos());

            var indexTierMetricsAfterTTLExpires = service.getIndexTierMetrics(currentClusterState);
            assertThat(indexTierMetricsAfterTTLExpires.getNodesLoad(), hasSize(0));
        }
    }

    public void testOutOfOrderMetricsAreDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var indexNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(indexNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        final var clusterState = clusterState(nodesWithElectedMaster);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState, clusterState(nodes)));

        var maxSeqNo = randomIntBetween(10, 20);
        var maxSeqNoIngestionLoad = randomIngestionLoad();
        service.trackNodeIngestLoad(clusterState, indexNode.getId(), indexNode.getName(), maxSeqNo, maxSeqNoIngestionLoad);

        var numberOfOutOfOrderMetricSamples = randomIntBetween(1, maxSeqNo);
        var unorderedSeqNos = IntStream.of(numberOfOutOfOrderMetricSamples).boxed().collect(Collectors.toCollection(ArrayList::new));
        Collections.shuffle(unorderedSeqNos, random());
        for (long seqNo : unorderedSeqNos) {
            service.trackNodeIngestLoad(clusterState, indexNode.getId(), indexNode.getName(), seqNo, randomIngestionLoad());
        }

        var indexTierMetrics = service.getIndexTierMetrics(clusterState);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(maxSeqNoIngestionLoad)));
        assertThat(indexNodeLoad.metricQuality(), is(equalTo(MetricQuality.EXACT)));
    }

    public void testDelayedMetricForRemovedNodeIsDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
            .build();
        final var indexNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
            .build();

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(indexNode).localNodeId(masterNode.getId()).build();
        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();
        final var clusterState1 = clusterState(nodesWithElectedMaster);
        final var settingsBuilder = Settings.builder();
        settingsBuilder.put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true));
        settingsBuilder.put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true));
        var service = new IngestMetricsService(clusterSettings(settingsBuilder.build()), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState1, ClusterState.EMPTY_STATE));

        var seqNo = randomNonNegativeLong();
        service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), seqNo, randomIngestionLoad());
        final var masterNodeLoad = randomIngestionLoad();
        service.trackNodeIngestLoad(clusterState1, masterNode.getId(), masterNode.getName(), seqNo, masterNodeLoad);

        var indexTierMetrics = service.getIndexTierMetrics(clusterState1);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
        assertTrue(indexTierMetrics.getNodesLoad().stream().allMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));

        final var nodes2 = DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build();
        final var nodesWithElectedMaster2 = DiscoveryNodes.builder(nodes2).masterNodeId(masterNode.getId()).build();
        final var shutdownType = randomFrom(Arrays.stream(Type.values()).filter(Type::isRemovalType).toList());
        final var singleShutdownMetadataBuilder = SingleNodeShutdownMetadata.builder()
            .setNodeId(indexNode.getId())
            .setReason("test")
            .setType(shutdownType)
            .setStartedAtMillis(randomNonNegativeLong());
        if (shutdownType.equals(Type.REPLACE)) {
            singleShutdownMetadataBuilder.setTargetNodeName(randomIdentifier());
        } else if (shutdownType.equals(Type.SIGTERM)) {
            singleShutdownMetadataBuilder.setGracePeriod(TimeValue.MAX_VALUE);
        }
        final var nodeShutdownMetadata = new NodesShutdownMetadata(Map.of(indexNode.getId(), singleShutdownMetadataBuilder.build()));
        final var clusterState2 = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(nodesWithElectedMaster2)
            .metadata(Metadata.builder(clusterState1.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
            .build();
        service.clusterChanged(new ClusterChangedEvent("node removed", clusterState2, clusterState1));

        indexTierMetrics = service.getIndexTierMetrics(clusterState2);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
        assertEquals(indexTierMetrics.getNodesLoad().get(0).load(), masterNodeLoad, EPSILON);
        assertEquals(indexTierMetrics.getNodesLoad().get(0).metricQuality(), MetricQuality.EXACT);

        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), seqNo + 1, randomIngestionLoad());

        indexTierMetrics = service.getIndexTierMetrics(clusterState2);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
        assertEquals(indexTierMetrics.getNodesLoad().get(0).load(), masterNodeLoad, EPSILON);
        assertEquals(indexTierMetrics.getNodesLoad().get(0).metricQuality(), MetricQuality.EXACT);
    }

    public void testDelayedMetricForDroppingNodeIsNotDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
            .build();
        final var indexNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID())
            .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
            .build();

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(indexNode).localNodeId(masterNode.getId()).build();
        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();
        final var clusterState1 = clusterState(nodesWithElectedMaster);
        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState1, ClusterState.EMPTY_STATE));

        var seqNo = randomNonNegativeLong();
        service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), seqNo, randomIngestionLoad());
        service.trackNodeIngestLoad(clusterState1, masterNode.getId(), masterNode.getName(), seqNo, randomIngestionLoad());

        var indexTierMetrics = service.getIndexTierMetrics(clusterState1);
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
        assertTrue(indexTierMetrics.getNodesLoad().stream().allMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));

        final var nodes2 = DiscoveryNodes.builder().add(masterNode).localNodeId(masterNode.getId()).build();
        final var nodesWithElectedMaster2 = DiscoveryNodes.builder(nodes2).masterNodeId(masterNode.getId()).build();
        final var clusterState2Builder = ClusterState.builder(ClusterName.DEFAULT).nodes(nodesWithElectedMaster2);
        final var leftUnassignedShards = randomBoolean();
        if (leftUnassignedShards) {
            var index = new Index((randomIdentifier()), randomUUID());
            clusterState2Builder.routingTable(
                RoutingTable.builder()
                    .add(IndexRoutingTable.builder(index).addShard(createUnassignedShardRouting(index, indexNode.getId())).build())
                    .build()
            );
        }
        final var clusterState2 = clusterState2Builder.build();
        service.clusterChanged(new ClusterChangedEvent("node dropped", clusterState2, clusterState1));

        indexTierMetrics = service.getIndexTierMetrics(clusterState2);
        if (leftUnassignedShards) {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.MINIMUM)));
        } else {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
        }

        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), seqNo + 1, randomIngestionLoad());
        indexTierMetrics = service.getIndexTierMetrics(clusterState2);
        if (leftUnassignedShards) {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.MINIMUM)));
            final var clusterStateWithoutUnassignedShards = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(nodesWithElectedMaster2)
                .build();
            service.clusterChanged(new ClusterChangedEvent("shard assigned", clusterStateWithoutUnassignedShards, clusterState2));
            indexTierMetrics = service.getIndexTierMetrics(clusterStateWithoutUnassignedShards);
        }
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
        assertTrue(indexTierMetrics.getNodesLoad().stream().allMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
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

        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService, MeterRegistry.NOOP);

        final var clusterStateWithLocalNodeElectedAsMaster = clusterState(
            DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()
        );
        service.clusterChanged(
            new ClusterChangedEvent("Local node elected as master", clusterStateWithLocalNodeElectedAsMaster, clusterState(nodes))
        );
        var indexTierMetrics = service.getIndexTierMetrics(clusterStateWithLocalNodeElectedAsMaster);
        var metricQualityCount = indexTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeIngestLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(indexTierMetrics.getNodesLoad().toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));

        final var clusterStateWithRemoteNodeElectedAsMaster = clusterState(
            DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()
        );
        service.clusterChanged(
            new ClusterChangedEvent(
                "Remote node elected as master",
                clusterStateWithRemoteNodeElectedAsMaster,
                clusterStateWithLocalNodeElectedAsMaster
            )
        );

        var indexTierMetricsAfterMasterHandover = service.getIndexTierMetrics(clusterStateWithRemoteNodeElectedAsMaster);
        assertThat(indexTierMetricsAfterMasterHandover.getNodesLoad(), is(empty()));
    }

    public void testIngestLoadsMetricsAdjustmentForNodesShutdown() {
        final List<DiscoveryNode> indexNodes = IntStream.range(0, between(1, 8))
            .mapToObj(
                i -> DiscoveryNodeUtils.builder("node-" + i)
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                    .build()
            )
            .toList();

        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        for (int i = 0; i < indexNodes.size(); i++) {
            final DiscoveryNode node = indexNodes.get(i);
            if (i == 0) {
                builder.add(node).localNodeId(node.getId()).masterNodeId(node.getId());
            } else {
                builder.add(node);
            }
        }
        // The number of search nodes and whether they are shutting down should not change anything w.r.t. ingestion load adjustment
        // during indexing tier scaling events.
        final List<DiscoveryNode> searchNodes = IntStream.range(0, randomIntBetween(0, 3))
            .mapToObj(i -> DiscoveryNodeUtils.builder("search-node-" + i).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .toList();
        searchNodes.forEach(builder::add);
        final var initialState = clusterState(builder.build());

        final var meterRegistry = new RecordingMeterRegistry();
        final MetricRecorder<Instrument> metricRecorder = meterRegistry.getRecorder();
        final AtomicLong currentTimeInNanos = new AtomicLong(System.nanoTime());
        final TimeValue adjustmentAfterScalingWindow = TimeValue.timeValueSeconds(randomLongBetween(0, 30));
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW.getKey(), adjustmentAfterScalingWindow)
                    .build()
            ),
            currentTimeInNanos::get,
            memoryMetricsService,
            meterRegistry
        );
        service.clusterChanged(new ClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        final Map<String, Double> publishedLoads1 = trackRandomIngestionLoads(service, seqNoSupplier, initialState);
        assertExactIngestionLoads(service, publishedLoads1.values(), initialState);
        assertMetricsForRawIngestLoads(service, metricRecorder, publishedLoads1);

        // Simulate nodes shutting down and new nodes joining
        final int numShuttingDownIndexingNodes = Math.min(between(1, 3), indexNodes.size());
        List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(numShuttingDownIndexingNodes, indexNodes);
        shuttingDownNodes.addAll(randomSubsetOf(searchNodes));
        final var nodeShutdownMetadata = createShutdownMetadata(shuttingDownNodes);
        final List<DiscoveryNode> newNodes = IntStream.range(
            initialState.nodes().size(),
            initialState.nodes().size() + shuttingDownNodes.size()
        )
            .mapToObj(
                i -> DiscoveryNodeUtils.builder("node-" + i)
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                    .build()
            )
            .toList();

        metricRecorder.resetCalls();
        final ClusterState state2, state3;
        final Map<String, Double> publishedLoads3;
        if (randomBoolean()) {
            // shutdown first and then add new nodes
            state2 = ClusterState.builder(initialState)
                .metadata(Metadata.builder(initialState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state2, initialState));
            final Map<String, Double> publishedLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            final var readMetrics2 = service.getIndexTierMetrics(state2).getNodesLoad();
            assertEquals(publishedLoads2.size(), readMetrics2.size());
            assertIngestionLoadWeightApplied(service, publishedLoads2.values(), readMetrics2, numShuttingDownIndexingNodes);
            assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads2, readMetrics2);

            state3 = stateWithNewNodes(state2, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state3, state2));
        } else {
            // add new nodes first then shutdown
            state2 = stateWithNewNodes(initialState, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state2, initialState));
            final Map<String, Double> ingestionLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            assertExactIngestionLoads(service, ingestionLoads2.values(), state2);
            assertMetricsForRawIngestLoads(service, metricRecorder, ingestionLoads2);

            state3 = ClusterState.builder(state2)
                .metadata(Metadata.builder(initialState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state3, state2));
        }
        metricRecorder.resetCalls();
        publishedLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, state3);
        final var readMetrics3 = service.getIndexTierMetrics(state3).getNodesLoad();
        assertEquals(publishedLoads3.size(), readMetrics3.size());
        assertIngestionLoadWeightApplied(service, publishedLoads3.values(), readMetrics3, numShuttingDownIndexingNodes);
        assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads3, readMetrics3);

        // Shutting down node left
        final DiscoveryNodes.Builder b = DiscoveryNodes.builder(state3.nodes());
        state3.nodes().forEach(node -> {
            if (shuttingDownNodes.contains(node)) {
                b.remove(node);
                if (node.isMasterNode()) {
                    b.localNodeId(newNodes.get(0).getId()).masterNodeId(newNodes.get(0).getId());
                }
            }
        });
        final ClusterState.Builder clusterStateBuilder = ClusterState.builder(state3).nodes(b.build());
        // Whether shutdown markers are removed should have no impact
        if (randomBoolean()) {
            clusterStateBuilder.metadata(Metadata.builder(state3.metadata()).removeCustom(NodesShutdownMetadata.TYPE));
        }
        final ClusterState state4 = clusterStateBuilder.build();
        service.clusterChanged(new ClusterChangedEvent("shutdown-node-left", state4, state3));
        final Map<String, Double> publishedLoads4 = trackRandomIngestionLoads(service, seqNoSupplier, state4);

        metricRecorder.resetCalls();
        if (adjustmentAfterScalingWindow.equals(TimeValue.ZERO)) {
            assertExactIngestionLoads(service, publishedLoads4.values(), state4);
            assertMetricsForRawIngestLoads(service, metricRecorder, publishedLoads4);
        } else {
            // After-scaling adjustment since we are still within the time window
            final var readMetrics4 = service.getIndexTierMetrics(state4).getNodesLoad();
            assertEquals(publishedLoads4.size(), readMetrics4.size());
            assertIngestionLoadWeightApplied(service, publishedLoads4.values(), readMetrics4, 1);
            assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads4, readMetrics4);

            // Move the time forward to be beyond the adjustment after scaling window
            currentTimeInNanos.addAndGet(adjustmentAfterScalingWindow.getNanos() + randomLongBetween(1, 100));
            metricRecorder.resetCalls();
            assertExactIngestionLoads(service, publishedLoads4.values(), state4);
            assertMetricsForRawIngestLoads(service, metricRecorder, publishedLoads4);
        }
    }

    // Tests that if during shutdown some nodes leave (w/o having shutdown marker), we'd still consider them if they leave
    // unassigned shards behind.
    public void testIngestLoadMetricsAdjustmentForUnexpectedNodeLeaves() {
        // Initial state
        final List<DiscoveryNode> nodes = IntStream.range(0, between(1, 8))
            .mapToObj(
                i -> DiscoveryNodeUtils.builder("node-" + i)
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                    .build()
            )
            .toList();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        nodes.forEach(builder::add);
        builder.localNodeId(nodes.get(0).getId()).masterNodeId(nodes.get(0).getId());
        final ClusterState initialState = clusterState(builder.build());

        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .build()
            ),
            () -> 0,
            memoryMetricsService,
            MeterRegistry.NOOP
        );
        service.clusterChanged(new ClusterChangedEvent("initial", initialState, ClusterState.EMPTY_STATE));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        final Collection<Double> publishedLoads1 = trackRandomIngestionLoads(service, seqNoSupplier, initialState).values();
        assertExactIngestionLoads(service, publishedLoads1, initialState);

        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(1, initialState.nodes().getAllNodes());
        final var nodeShutdownMetadata = createShutdownMetadata(shuttingDownNodes);

        final List<DiscoveryNode> newNodes = IntStream.range(
            initialState.nodes().size(),
            initialState.nodes().size() + shuttingDownNodes.size()
        )
            .mapToObj(
                i -> DiscoveryNodeUtils.builder("node-" + i)
                    .roles(Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE))
                    .build()
            )
            .toList();
        // we're scaling up. new nodes are added and some are marked for shutdown
        final var stateWithShutDowns = ClusterState.builder(initialState)
            .metadata(Metadata.builder(initialState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
            .build();
        service.clusterChanged(new ClusterChangedEvent("shutdown", stateWithShutDowns, initialState));
        final Collection<Double> publishedLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, stateWithShutDowns).values();
        final var readMetrics2 = service.getIndexTierMetrics(stateWithShutDowns).getNodesLoad();
        assertFalse(readMetrics2.isEmpty());
        assertEquals(publishedLoads2.size(), readMetrics2.size());
        assertIngestionLoadWeightApplied(service, publishedLoads2, readMetrics2, shuttingDownNodes.size());

        final var stateWithAllNodes = stateWithNewNodes(stateWithShutDowns, newNodes);
        service.clusterChanged(new ClusterChangedEvent("node-join", stateWithAllNodes, stateWithShutDowns));
        final var publishedLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, stateWithAllNodes).values();
        final var readMetrics3 = service.getIndexTierMetrics(stateWithAllNodes).getNodesLoad();
        assertFalse(readMetrics3.isEmpty());
        assertEquals(publishedLoads3.size(), readMetrics3.size());
        assertIngestionLoadWeightApplied(service, publishedLoads3, readMetrics3, shuttingDownNodes.size());

        // some non-shutting down nodes disappears
        final var droppingNodes = randomSubsetOf(1, newNodes);
        final ClusterState stateWithSomeNodesMissing = stateWithNewNodes(
            stateWithShutDowns,
            newNodes.stream().filter(n -> droppingNodes.contains(n) == false).toList()
        );
        final var nodesWithUnassignedShards = randomSubsetOf(droppingNodes);
        final var clusterStateBuilderWithUnassignedShards = ClusterState.builder(stateWithSomeNodesMissing);
        final var routingTableBuilder = RoutingTable.builder();
        nodesWithUnassignedShards.forEach(node -> {
            var index = new Index(randomIdentifier(), randomUUID());
            routingTableBuilder.add(IndexRoutingTable.builder(index).addShard(createUnassignedShardRouting(index, node.getId())).build());
        });
        final var clusterStateWithUnassignedShards = clusterStateBuilderWithUnassignedShards.routingTable(routingTableBuilder).build();
        assertThat(stateWithSomeNodesMissing.nodes().size(), lessThan(stateWithAllNodes.nodes().size()));
        service.clusterChanged(new ClusterChangedEvent("nodes-dropped", clusterStateWithUnassignedShards, stateWithAllNodes));
        final var publishedLoads4 = trackRandomIngestionLoads(service, seqNoSupplier, clusterStateWithUnassignedShards);
        final var readMetrics4 = service.getIndexTierMetrics(clusterStateWithUnassignedShards).getNodesLoad();
        assertThat(readMetrics4.size(), equalTo(publishedLoads4.size() + nodesWithUnassignedShards.size()));
        assertTrue(readMetrics4.stream().allMatch(load -> load.metricQuality().equals(MetricQuality.MINIMUM)));
    }

    public void testMaybeAdjustIngestLoadsForShuttingDownNodes() {
        var noneShuttingDownNodesCount = between(0, 2);
        var shuttingDownNodesCount = between(noneShuttingDownNodesCount == 0 ? 1 : 0, 2);
        final var nodeRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);
        final var nodes = IntStream.range(0, noneShuttingDownNodesCount + shuttingDownNodesCount)
            .mapToObj(i -> DiscoveryNodeUtils.builder("node-" + i).roles(nodeRoles).build())
            .toList();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        nodes.forEach(builder::add);
        builder.localNodeId(nodes.get(0).getId()).masterNodeId(nodes.get(0).getId());
        final List<DiscoveryNode> shuttingDownNodes = randomSubsetOf(shuttingDownNodesCount, nodes);
        final var nodeShutdownMetadata = createShutdownMetadata(shuttingDownNodes);
        final var state = ClusterState.builder(ClusterName.DEFAULT)
            .nodes(builder)
            .metadata(Metadata.builder().putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
            .build();
        var lowWeight = randomDoubleBetween(0.0, 1.0, true);
        var highWeight = randomDoubleBetween(0.0, 1.0, true);
        final var lowLoads = IntStream.range(0, noneShuttingDownNodesCount)
            .mapToDouble(i -> randomDoubleBetween(0.0, 8.0, true))
            .boxed()
            .toList();
        final var highLoads = IntStream.range(0, shuttingDownNodesCount)
            .mapToDouble(i -> randomDoubleBetween(8.0, 16.0, false))
            .boxed()
            .toList();
        List<NodeIngestLoadSnapshot> publishedLoads = new ArrayList<>(noneShuttingDownNodesCount + shuttingDownNodesCount);
        lowLoads.forEach(
            l -> publishedLoads.add(
                new NodeIngestLoadSnapshot(randomIdentifier(), randomIdentifier(), l, randomFrom(MetricQuality.values()))
            )
        );
        highLoads.forEach(
            l -> publishedLoads.add(
                new NodeIngestLoadSnapshot(randomIdentifier(), randomIdentifier(), l, randomFrom(MetricQuality.values()))
            )
        );
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), highWeight)
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), lowWeight)
                    .build()
            ),
            () -> 0,
            memoryMetricsService,
            MeterRegistry.NOOP
        );
        var calculatedLoads = service.maybeAdjustIngestLoads(state, publishedLoads);
        assertThat(calculatedLoads.size(), equalTo(publishedLoads.size()));
        if (shuttingDownNodesCount == 0 || (lowWeight == 1.0 && highWeight == 1.0)) {
            assertEquals(publishedLoads, calculatedLoads);
        } else {
            lowLoads.forEach(load -> assertTrue(calculatedLoads.stream().anyMatch(e -> doublesEquals(e.load(), load * lowWeight))));
            highLoads.forEach(load -> assertTrue(calculatedLoads.stream().anyMatch(e -> doublesEquals(e.load(), load * highWeight))));
        }
    }

    private static ShardRouting createUnassignedShardRouting(Index index, String lastAllocationNodeId) {
        return ShardRouting.newUnassigned(
            new ShardId(index, 0),
            true,
            RecoverySource.ExistingStoreRecoverySource.INSTANCE,
            new UnassignedInfo(
                UnassignedInfo.Reason.NODE_LEFT,
                null,
                null,
                0,
                0,
                0,
                false,
                UnassignedInfo.AllocationStatus.NO_ATTEMPT,
                Set.of(),
                lastAllocationNodeId
            ),
            ShardRouting.Role.INDEX_ONLY
        );
    }

    private ClusterState stateWithNewNodes(ClusterState state, List<DiscoveryNode> nodes) {
        final var builder = DiscoveryNodes.builder(state.nodes());
        nodes.forEach(builder::add);
        return ClusterState.builder(state).nodes(builder.build()).build();
    }

    private Map<String, Double> trackRandomIngestionLoads(IngestMetricsService service, LongSupplier seqNoSupplier, ClusterState state) {
        final List<DiscoveryNode> indexingNodes = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .toList();
        final Map<String, Double> ingestionLoads = indexingNodes.stream()
            .collect(Collectors.toUnmodifiableMap(DiscoveryNode::getId, ignore -> randomIngestionLoad()));
        indexingNodes.forEach(
            node -> service.trackNodeIngestLoad(
                state,
                node.getId(),
                node.getName(),
                seqNoSupplier.getAsLong(),
                ingestionLoads.get(node.getId())
            )
        );
        return ingestionLoads;
    }

    private void assertExactIngestionLoads(IngestMetricsService service, Collection<Double> publishedLoads, ClusterState state) {
        final List<DiscoveryNode> indexingNodes = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .toList();
        final List<NodeIngestLoadSnapshot> readMetrics = service.getIndexTierMetrics(state).getNodesLoad();
        assertThat(readMetrics.size(), equalTo(indexingNodes.size()));
        assertTrue(readMetrics.stream().allMatch(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.EXACT));
        assertArrayEquals(
            publishedLoads.stream().mapToDouble(Double::doubleValue).sorted().toArray(),
            readMetrics.stream().mapToDouble(NodeIngestLoadSnapshot::load).sorted().toArray(),
            EPSILON
        );
    }

    private void assertIngestionLoadWeightApplied(
        IngestMetricsService service,
        Collection<Double> publishedLoads,
        List<NodeIngestLoadSnapshot> readMetrics,
        int numForHighWeight
    ) {
        final double highWeight = service.getHighIngestionLoadWeightDuringScaling();
        final double lowWeight = service.getLowIngestionLoadWeightDuringScaling();
        boolean weightApplied = highWeight < 1.0 || lowWeight < 1.0;
        double totalPublishedIngestionLoad = publishedLoads.stream().reduce(Double::sum).get();
        double totalReadMetric = readMetrics.stream().map(NodeIngestLoadSnapshot::load).reduce(Double::sum).get();
        if (weightApplied) {
            assertTrue(readMetrics.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.MINIMUM)));
            assertThat(totalReadMetric, lessThan(totalPublishedIngestionLoad));
            final List<Double> sortedPublishedLoads = publishedLoads.stream().sorted().toList();
            for (int i = 0; i < sortedPublishedLoads.size() - numForHighWeight; i++) {
                assertTrue(doublesEquals(sortedPublishedLoads.get(i) * lowWeight, readMetrics.get(i).load()));
            }
            for (int i = sortedPublishedLoads.size() - numForHighWeight; i < sortedPublishedLoads.size(); i++) {
                assertTrue(doublesEquals(sortedPublishedLoads.get(i) * highWeight, readMetrics.get(i).load()));
            }
        } else {
            assertTrue(readMetrics.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.EXACT)));
            assertEquals(totalReadMetric, totalPublishedIngestionLoad, EPSILON);
        }
    }

    private static void assertMetricsForRawIngestLoads(
        IngestMetricsService service,
        MetricRecorder<Instrument> metricRecorder,
        Map<String, Double> publishedLoads
    ) {
        assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads, null);
    }

    private static void assertMetricsForRawAndAdjustedIngestLoads(
        IngestMetricsService service,
        MetricRecorder<Instrument> metricRecorder,
        Map<String, Double> publishedLoads,
        @Nullable List<NodeIngestLoadSnapshot> readMetrics
    ) {
        // We should always observe the raw ingest loads
        final RawAndAdjustedNodeIngestLoadSnapshots lastNodeIngestLoadSnapshots = service.getLastNodeIngestLoadSnapshots();
        assertThat(
            lastNodeIngestLoadSnapshots.raw()
                .stream()
                .collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load)),
            equalTo(publishedLoads)
        );

        metricRecorder.collect();
        final List<Measurement> measurements = metricRecorder.getMeasurements(
            InstrumentType.DOUBLE_GAUGE,
            NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME
        );
        assertThat(
            measurements.stream().filter(measurement -> (boolean) measurement.attributes().get("adjusted") == false).peek(measurement -> {
                assertThat(measurement.attributes().get("quality"), is(MetricQuality.EXACT.getLabel()));
                assertThat(measurement.attributes().get("node_name"), notNullValue());
            }).collect(Collectors.toUnmodifiableMap(m -> m.attributes().get("node_id"), Measurement::getDouble)),
            equalTo(publishedLoads)
        );

        // Based on whether there are shutting down nodes and whether weights are configured, we may observe adjusted ingest loads
        boolean weightApplied = service.getHighIngestionLoadWeightDuringScaling() < 1.0
            || service.getLowIngestionLoadWeightDuringScaling() < 1.0;
        final boolean expectAdjustedLoads = readMetrics != null && weightApplied;
        if (expectAdjustedLoads) {
            assertThat(lastNodeIngestLoadSnapshots.adjusted(), notNullValue());
            assertThat(
                lastNodeIngestLoadSnapshots.adjusted()
                    .stream()
                    .collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load)),
                equalTo(
                    readMetrics.stream().collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load))
                )
            );
            assertThat(measurements, hasSize(publishedLoads.size() * 2));
            assertThat(measurements.stream().filter(measurement -> (boolean) measurement.attributes().get("adjusted")).peek(measurement -> {
                assertThat(measurement.attributes().get("quality"), is(MetricQuality.MINIMUM.getLabel()));
                assertThat(measurement.attributes().get("node_name"), notNullValue());
            }).collect(Collectors.toUnmodifiableMap(m -> m.attributes().get("node_id"), Measurement::getDouble)),
                equalTo(
                    readMetrics.stream().collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load))
                )
            );
        } else {
            assertThat(lastNodeIngestLoadSnapshots.adjusted(), nullValue());
            assertThat(measurements, hasSize(publishedLoads.size()));
        }
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
        return new ClusterSettings(
            settings,
            Set.of(
                ACCURATE_LOAD_WINDOW,
                STALE_LOAD_WINDOW,
                HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING,
                LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING,
                LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW
            )
        );
    }

    private static boolean doublesEquals(double expected, double actual) {
        return Math.abs(expected - actual) < EPSILON;
    }
}
