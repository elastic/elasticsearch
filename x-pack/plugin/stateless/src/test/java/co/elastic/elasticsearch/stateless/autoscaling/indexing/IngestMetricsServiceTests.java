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
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestionLoad.ExecutorIngestionLoad;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestionLoad.ExecutorStats;
import co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestionLoad.NodeIngestionLoad;
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
import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
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
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.DoubleSupplier;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.AverageWriteLoadSampler.WRITE_EXECUTORS;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.IngestMetricType.ADJUSTED;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.IngestMetricType.SINGLE;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.IngestMetricType.UNADJUSTED;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.NodeIngestionLoadTracker.ACCURATE_LOAD_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.NodeIngestionLoadTracker.INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.NodeIngestionLoadTracker.STALE_LOAD_WINDOW;
import static co.elastic.elasticsearch.stateless.autoscaling.indexing.NodeIngestionLoadTracker.USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING;
import static org.elasticsearch.core.TimeValue.timeValueMillis;
import static org.hamcrest.Matchers.closeTo;
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
                    MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_SETTING,
                    MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY_SETTING,
                    MemoryMetricsService.INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_ENABLED_SETTING,
                    MemoryMetricsService.MERGE_MEMORY_ESTIMATE_ENABLED_SETTING,
                    MemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING,
                    MemoryMetricsService.SELF_REPORTED_SHARD_MEMORY_OVERHEAD_ENABLED_SETTING
                )
            ),
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE,
            MeterRegistry.NOOP
        );
    }

    public void testServiceOnlyReturnDataWhenLocalNodeIsElectedAsMaster() {
        var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var remoteNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var nodes = DiscoveryNodes.builder().add(localNode).add(remoteNode).localNodeId(localNode.getId()).build();
        var service = new IngestMetricsService(emptyClusterSettings(), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        var indexTierMetrics = service.getIndexTierMetrics(ClusterState.EMPTY_STATE, randomDesiredBalanceStats());
        // If the node is not elected as master (i.e. we haven't got any cluster state notification) it shouldn't return any info
        assertThat(indexTierMetrics.getNodesLoad(), is(empty()));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node not elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(nodes)
            )
        );

        var indexTierMetricsAfterClusterStateEvent = service.getIndexTierMetrics(ClusterState.EMPTY_STATE, randomDesiredBalanceStats());
        assertThat(indexTierMetricsAfterClusterStateEvent.getNodesLoad(), is(empty()));
    }

    public void testOnlyIndexNodesAreTracked() {
        final var localNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();

        final var indexNodeId = UUIDs.randomBase64UUID();
        final var nodes = DiscoveryNodes.builder()
            .add(localNode)
            .add(DiscoveryNodeUtils.builder(indexNodeId).roles(Set.of(DiscoveryNodeRole.INDEX_ROLE)).build())
            .add(DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.SEARCH_ROLE)).build())
            .localNodeId(localNode.getId())
            .build();

        var service = new IngestMetricsService(emptyClusterSettings(), () -> 0, memoryMetricsService, MeterRegistry.NOOP);

        final var state = clusterState(DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build());
        service.clusterChanged(new ClusterChangedEvent("Local node elected as master", state, clusterState(nodes)));
        var trackedIngestLoads = service.getNodesIngestLoad();
        assertEquals(1, trackedIngestLoads.size());
        assertNotNull(trackedIngestLoads.get(indexNodeId));
        assertEquals(NodeIngestionLoad.EMPTY, trackedIngestLoads.get(indexNodeId).getIngestLoad());
        var indexTierMetrics = service.getIndexTierMetrics(state, randomDesiredBalanceStats());
        var metricQualityCount = indexTierMetrics.getNodesLoad()
            .stream()
            .collect(Collectors.groupingBy(NodeIngestLoadSnapshot::metricQuality, Collectors.counting()));

        // When the node hasn't published a metric yet, we consider it as missing
        assertThat(indexTierMetrics.toString(), metricQualityCount.get(MetricQuality.MISSING), is(equalTo(1L)));
    }

    public void testIngestionLoadIsKeptDuringNodeLifecycleWithExactMetrics() {
        final var maxUndesiredShardsProportionForScaleDown = randomMaxUndesiredShardsProportionForScaleDown();
        runIngestionLoadDuringNodeLifecycleTest(
            maxUndesiredShardsProportionForScaleDown,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown),
            MetricQuality.EXACT
        );
    }

    public void testIngestionLoadIsKeptDuringNodeLifecycleWithInexactMetrics() {
        final var totalShards = randomLongBetween(1, 1000);
        final var undesiredShards = randomLongBetween(1, totalShards);
        final var maxUndesiredShardsProportionForScaleDown = randomDoubleBetween(0.0, (undesiredShards - 0.5) / (double) totalShards, true);
        runIngestionLoadDuringNodeLifecycleTest(
            maxUndesiredShardsProportionForScaleDown,
            new DesiredBalanceMetrics.AllocationStats(
                randomNonNegativeLong(),
                Map.of(ShardRouting.Role.INDEX_ONLY, new DesiredBalanceMetrics.RoleAllocationStats(totalShards, undesiredShards))
            ),
            MetricQuality.MINIMUM
        );
    }

    private void runIngestionLoadDuringNodeLifecycleTest(
        double maxUndesiredShardsProportionForScaleDown,
        DesiredBalanceMetrics.AllocationStats allocationStats,
        MetricQuality bestMetricQuality
    ) {
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
                    .put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), maxUndesiredShardsProportionForScaleDown)
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
            service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), 1, mockedNodeIngestionLoad(0.5));
        }

        var nodesWithIndexingNode = DiscoveryNodes.builder(nodesWithElectedMaster).add(indexNode).build();
        final var clusterState2 = clusterState(nodesWithIndexingNode);
        service.clusterChanged(new ClusterChangedEvent("index node joins", clusterState2, clusterState(nodesWithElectedMaster)));

        fakeClock.addAndGet(TimeValue.timeValueSeconds(1).nanos());
        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), 2, mockedNodeIngestionLoad(1.5));

        var indexTierMetrics = service.getIndexTierMetrics(clusterState2, allocationStats);
        assertThat(indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(1.5)));
        assertThat(indexTierMetrics.toString(), indexNodeLoad.metricQuality(), is(equalTo(bestMetricQuality)));

        final var indexNodeLeaves = randomBoolean();
        if (indexNodeLeaves) {
            service.clusterChanged(
                new ClusterChangedEvent("index node leaves", clusterState(nodesWithElectedMaster), clusterState(nodesWithIndexingNode))
            );
        }

        fakeClock.addAndGet(inaccurateMetricTime.getNanos());

        final var currentClusterState = indexNodeLeaves ? clusterState(nodesWithElectedMaster) : clusterState(nodesWithIndexingNode);
        var indexTierMetricsAfterNodeMetricIsInaccurate = service.getIndexTierMetrics(
            currentClusterState,
            randomDesiredBalanceStats() // expect MINIMUM, so it's ok to randomly use stats with undesired shards
        );
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
            service.trackNodeIngestLoad(clusterState3, indexNode.getId(), indexNode.getName(), 3, mockedNodeIngestionLoad(0.5));

            var indexTierMetricsAfterNodeReJoins = service.getIndexTierMetrics(clusterState3, allocationStats);
            assertThat(indexTierMetricsAfterNodeReJoins.getNodesLoad(), hasSize(1));

            var indexNodeLoadAfterRejoining = indexTierMetricsAfterNodeReJoins.getNodesLoad().get(0);
            assertThat(indexNodeLoadAfterRejoining.load(), is(equalTo(0.5)));
            assertThat(indexNodeLoadAfterRejoining.metricQuality(), is(equalTo(bestMetricQuality)));
        } else {
            // The node do not re-join after the max time
            fakeClock.addAndGet(staleLoadWindow.getNanos());

            var indexTierMetricsAfterTTLExpires = service.getIndexTierMetrics(currentClusterState, randomDesiredBalanceStats());
            assertThat(indexTierMetricsAfterTTLExpires.getNodesLoad(), hasSize(0));
        }
    }

    public void testOutOfOrderMetricsAreDiscarded() {
        final var masterNode = DiscoveryNodeUtils.builder(UUIDs.randomBase64UUID()).roles(Set.of(DiscoveryNodeRole.MASTER_ROLE)).build();
        final var indexNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());

        final var nodes = DiscoveryNodes.builder().add(masterNode).add(indexNode).localNodeId(masterNode.getId()).build();

        final var nodesWithElectedMaster = DiscoveryNodes.builder(nodes).masterNodeId(masterNode.getId()).build();

        final var clusterSettings = emptyClusterSettings();
        var service = new IngestMetricsService(clusterSettings, () -> 0, memoryMetricsService, MeterRegistry.NOOP);
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

        var indexTierMetrics = service.getIndexTierMetrics(
            clusterState,
            randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
        );
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));

        var indexNodeLoad = indexTierMetrics.getNodesLoad().get(0);
        assertThat(indexNodeLoad.load(), is(equalTo(maxSeqNoIngestionLoad.totalIngestionLoad())));
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
        final var maxUndesiredShardsProportionForScaleDown = randomMaxUndesiredShardsProportionForScaleDown();
        settingsBuilder.put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), maxUndesiredShardsProportionForScaleDown);
        var service = new IngestMetricsService(clusterSettings(settingsBuilder.build()), () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState1, ClusterState.EMPTY_STATE));

        var seqNo = randomNonNegativeLong();
        service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), seqNo, randomIngestionLoad());
        final var masterNodeLoad = randomIngestionLoad();
        service.trackNodeIngestLoad(clusterState1, masterNode.getId(), masterNode.getName(), seqNo, masterNodeLoad);

        var indexTierMetrics = service.getIndexTierMetrics(
            clusterState1,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        );
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

        indexTierMetrics = service.getIndexTierMetrics(
            clusterState2,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        );
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
        assertEquals(indexTierMetrics.getNodesLoad().get(0).load(), masterNodeLoad.totalIngestionLoad(), EPSILON);
        assertEquals(indexTierMetrics.getNodesLoad().get(0).metricQuality(), MetricQuality.EXACT);

        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), seqNo + 1, randomIngestionLoad());

        indexTierMetrics = service.getIndexTierMetrics(
            clusterState2,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        );
        assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
        assertEquals(indexTierMetrics.getNodesLoad().get(0).load(), masterNodeLoad.totalIngestionLoad(), EPSILON);
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
        final var clusterSettings = emptyClusterSettings();
        var service = new IngestMetricsService(clusterSettings, () -> 0, memoryMetricsService, MeterRegistry.NOOP);
        service.clusterChanged(new ClusterChangedEvent("master node elected", clusterState1, ClusterState.EMPTY_STATE));

        var seqNo = randomNonNegativeLong();
        service.trackNodeIngestLoad(clusterState1, indexNode.getId(), indexNode.getName(), seqNo, randomIngestionLoad());
        service.trackNodeIngestLoad(clusterState1, masterNode.getId(), masterNode.getName(), seqNo, randomIngestionLoad());

        var indexTierMetrics = service.getIndexTierMetrics(
            clusterState1,
            randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
        );
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

        indexTierMetrics = service.getIndexTierMetrics(
            clusterState2,
            randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
        );
        if (leftUnassignedShards) {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.MINIMUM)));
        } else {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(1));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
        }

        service.trackNodeIngestLoad(clusterState2, indexNode.getId(), indexNode.getName(), seqNo + 1, randomIngestionLoad());
        indexTierMetrics = service.getIndexTierMetrics(
            clusterState2,
            randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
        );
        if (leftUnassignedShards) {
            assertThat(indexTierMetrics.getNodesLoad().toString(), indexTierMetrics.getNodesLoad(), hasSize(2));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.EXACT)));
            assertTrue(indexTierMetrics.getNodesLoad().stream().anyMatch(load -> load.metricQuality().equals(MetricQuality.MINIMUM)));
            final var clusterStateWithoutUnassignedShards = ClusterState.builder(ClusterName.DEFAULT)
                .nodes(nodesWithElectedMaster2)
                .build();
            service.clusterChanged(new ClusterChangedEvent("shard assigned", clusterStateWithoutUnassignedShards, clusterState2));
            indexTierMetrics = service.getIndexTierMetrics(
                clusterStateWithoutUnassignedShards,
                randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
            );
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

        final var clusterSettings = emptyClusterSettings();
        var service = new IngestMetricsService(clusterSettings, () -> 0, memoryMetricsService, MeterRegistry.NOOP);

        final var clusterStateWithLocalNodeElectedAsMaster = clusterState(
            DiscoveryNodes.builder(nodes).masterNodeId(localNode.getId()).build()
        );
        service.clusterChanged(
            new ClusterChangedEvent("Local node elected as master", clusterStateWithLocalNodeElectedAsMaster, clusterState(nodes))
        );
        var indexTierMetrics = service.getIndexTierMetrics(
            clusterStateWithLocalNodeElectedAsMaster,
            randomDesiredBalanceStatsForExactMetrics(clusterSettings.get(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN))
        );
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

        var indexTierMetricsAfterMasterHandover = service.getIndexTierMetrics(
            clusterStateWithRemoteNodeElectedAsMaster,
            randomDesiredBalanceStats()
        );
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
        final var maxUndesiredShardsProportionForScaleDown = randomMaxUndesiredShardsProportionForScaleDown();
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW.getKey(), adjustmentAfterScalingWindow)
                    .put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), maxUndesiredShardsProportionForScaleDown)
                    .build()
            ),
            currentTimeInNanos::get,
            memoryMetricsService,
            meterRegistry
        );
        service.clusterChanged(new ClusterChangedEvent("test", initialState, ClusterState.EMPTY_STATE));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        final Map<String, NodeIngestionLoad> publishedLoads1 = trackRandomIngestionLoads(service, seqNoSupplier, initialState);
        assertExactIngestionLoads(service, publishedLoads1.values(), initialState, maxUndesiredShardsProportionForScaleDown);
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
        final Map<String, NodeIngestionLoad> publishedLoads3;
        if (randomBoolean()) {
            // shutdown first and then add new nodes
            state2 = ClusterState.builder(initialState)
                .metadata(Metadata.builder(initialState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state2, initialState));
            final Map<String, NodeIngestionLoad> publishedLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            final var readMetrics2 = service.getIndexTierMetrics(
                state2,
                randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
            ).getNodesLoad();
            assertEquals(publishedLoads2.size(), readMetrics2.size());
            assertIngestionLoadWeightApplied(service, publishedLoads2.values(), readMetrics2, numShuttingDownIndexingNodes);
            assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads2, readMetrics2);

            state3 = stateWithNewNodes(state2, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state3, state2));
        } else {
            // add new nodes first then shutdown
            state2 = stateWithNewNodes(initialState, newNodes);
            service.clusterChanged(new ClusterChangedEvent("node-join", state2, initialState));
            final Map<String, NodeIngestionLoad> ingestionLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, state2);
            assertExactIngestionLoads(service, ingestionLoads2.values(), state2, maxUndesiredShardsProportionForScaleDown);
            assertMetricsForRawIngestLoads(service, metricRecorder, ingestionLoads2);

            state3 = ClusterState.builder(state2)
                .metadata(Metadata.builder(initialState.metadata()).putCustom(NodesShutdownMetadata.TYPE, nodeShutdownMetadata))
                .build();
            service.clusterChanged(new ClusterChangedEvent("shutdown", state3, state2));
        }
        metricRecorder.resetCalls();
        publishedLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, state3);
        final var readMetrics3 = service.getIndexTierMetrics(
            state3,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        ).getNodesLoad();
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
        final Map<String, NodeIngestionLoad> publishedLoads4 = trackRandomIngestionLoads(service, seqNoSupplier, state4);

        metricRecorder.resetCalls();
        if (adjustmentAfterScalingWindow.equals(TimeValue.ZERO)) {
            assertExactIngestionLoads(service, publishedLoads4.values(), state4, maxUndesiredShardsProportionForScaleDown);
            assertMetricsForRawIngestLoads(service, metricRecorder, publishedLoads4);
        } else {
            // After-scaling adjustment since we are still within the time window
            final var readMetrics4 = service.getIndexTierMetrics(
                state4,
                randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
            ).getNodesLoad();
            assertEquals(publishedLoads4.size(), readMetrics4.size());
            assertIngestionLoadWeightApplied(service, publishedLoads4.values(), readMetrics4, 1);
            assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads4, readMetrics4);

            // Move the time forward to be beyond the adjustment after scaling window
            currentTimeInNanos.addAndGet(adjustmentAfterScalingWindow.getNanos() + randomLongBetween(1, 100));
            metricRecorder.resetCalls();
            assertExactIngestionLoads(service, publishedLoads4.values(), state4, maxUndesiredShardsProportionForScaleDown);
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

        final var maxUndesiredShardsProportionForScaleDown = randomMaxUndesiredShardsProportionForScaleDown();
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), randomDoubleBetween(0.0, 1.0, true))
                    .put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), maxUndesiredShardsProportionForScaleDown)
                    .build()
            ),
            () -> 0,
            memoryMetricsService,
            MeterRegistry.NOOP
        );
        service.clusterChanged(new ClusterChangedEvent("initial", initialState, ClusterState.EMPTY_STATE));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        final Collection<NodeIngestionLoad> publishedLoads1 = trackRandomIngestionLoads(service, seqNoSupplier, initialState).values();
        assertExactIngestionLoads(service, publishedLoads1, initialState, maxUndesiredShardsProportionForScaleDown);

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
        final Collection<NodeIngestionLoad> publishedLoads2 = trackRandomIngestionLoads(service, seqNoSupplier, stateWithShutDowns)
            .values();
        final var readMetrics2 = service.getIndexTierMetrics(
            stateWithShutDowns,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        ).getNodesLoad();
        assertFalse(readMetrics2.isEmpty());
        assertEquals(publishedLoads2.size(), readMetrics2.size());
        assertIngestionLoadWeightApplied(service, publishedLoads2, readMetrics2, shuttingDownNodes.size());

        final var stateWithAllNodes = stateWithNewNodes(stateWithShutDowns, newNodes);
        service.clusterChanged(new ClusterChangedEvent("node-join", stateWithAllNodes, stateWithShutDowns));
        final var publishedLoads3 = trackRandomIngestionLoads(service, seqNoSupplier, stateWithAllNodes).values();
        final var readMetrics3 = service.getIndexTierMetrics(
            stateWithAllNodes,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        ).getNodesLoad();
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
        final var readMetrics4 = service.getIndexTierMetrics(
            clusterStateWithUnassignedShards,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        ).getNodesLoad();
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
        final var maxUndesiredShardsProportionForScaleDown = randomMaxUndesiredShardsProportionForScaleDown();
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), highWeight)
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), lowWeight)
                    .put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), maxUndesiredShardsProportionForScaleDown)
                    .build()
            ),
            () -> 0,
            memoryMetricsService,
            MeterRegistry.NOOP
        );
        var calculatedLoads = service.maybeAdjustIngestLoadsAndQuality(
            state,
            publishedLoads,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        );
        assertThat(calculatedLoads.size(), equalTo(publishedLoads.size()));
        if (shuttingDownNodesCount == 0 || (lowWeight == 1.0 && highWeight == 1.0)) {
            assertEquals(publishedLoads, calculatedLoads);
        } else {
            lowLoads.forEach(load -> assertTrue(calculatedLoads.stream().anyMatch(e -> doublesEquals(e.load(), load * lowWeight))));
            highLoads.forEach(load -> assertTrue(calculatedLoads.stream().anyMatch(e -> doublesEquals(e.load(), load * highWeight))));
        }
    }

    public void testUseTierWideWriteAverageTaskExecTimeDuringScaling() {
        final String NODE0 = "node-0", NODE1 = "node-1", NODE2 = "node-2";
        // Two initial nodes
        final var nodeRoles = Set.of(DiscoveryNodeRole.MASTER_ROLE, DiscoveryNodeRole.INDEX_ROLE);
        final var initialNodes = IntStream.range(0, 2)
            .mapToObj(i -> DiscoveryNodeUtils.builder("node-" + i).name("node-" + i).roles(nodeRoles).build())
            .toList();
        final DiscoveryNodes.Builder builder = DiscoveryNodes.builder();
        initialNodes.forEach(builder::add);
        builder.localNodeId(initialNodes.get(0).getId()).masterNodeId(initialNodes.get(0).getId());
        AtomicLong now = new AtomicLong(System.currentTimeMillis());
        final var initialScalingWindowConsideredUnstable = randomTimeValue(1, 5, TimeUnit.MINUTES);
        var service = new IngestMetricsService(
            clusterSettings(
                Settings.builder()
                    .put(USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING.getKey(), true)
                    .put(INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE.getKey(), initialScalingWindowConsideredUnstable)
                    // no other adjustments
                    .put(HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), 1.0)
                    .put(LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING.getKey(), 1.0)
                    .put(ACCURATE_LOAD_WINDOW.getKey(), TimeValue.ONE_HOUR)
                    .put(IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE.getKey(), TimeValue.timeValueSeconds(1))
                    .put(IngestLoadProbe.MAX_MANAGEABLE_QUEUED_WORK.getKey(), TimeValue.ZERO)
                    .put(IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR.getKey(), 1000.0) // so that there are no limits
                    .build()
            ),
            now::get,
            memoryMetricsService,
            MeterRegistry.NOOP
        );
        final var initialState = clusterState(builder.build());
        service.clusterChanged(new ClusterChangedEvent("initial", initialState, ClusterState.EMPTY_STATE));
        final var nodeIngestLoadTracker = service.getNodeIngestionLoadTracker();
        assertThat(nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().isPresent(), is(false));
        final LongSupplier seqNoSupplier = new AtomicLong(randomLongBetween(0, 100))::getAndIncrement;
        var writeLoadPerNode = Map.of(NODE0, 0.0, NODE1, 0.0);
        var queueSizePerNode = Map.of(NODE0, 0.0, NODE1, 0.0);
        var avgTaskExecTimePerNode = Map.of(NODE0, 0.0, NODE1, 0.0);
        final var ingestionLoadWithZeroStableExecTimePerNode = Map.of(
            NODE0,
            nodeIngestionLoadWithWriteActivity(
                writeLoadPerNode.get(NODE0),
                queueSizePerNode.get(NODE0),
                avgTaskExecTimePerNode.get(NODE0),
                OptionalDouble.of(0)
            ),
            NODE1,
            nodeIngestionLoadWithWriteActivity(
                writeLoadPerNode.get(NODE1),
                queueSizePerNode.get(NODE1),
                avgTaskExecTimePerNode.get(NODE1),
                OptionalDouble.of(0)
            )
        );
        trackIngestionLoads(service, seqNoSupplier, initialState, ingestionLoadWithZeroStableExecTimePerNode::get);
        assertThat(nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().isPresent(), is(false));

        DoubleSupplier randomAvgWriteLoad = () -> randomDoubleBetween(0.1, 4.0, true);
        DoubleSupplier randomAvgQueueSize = () -> randomDoubleBetween(10, 10_000, true);
        DoubleSupplier randomStableAvgExecTimeNanos = () -> timeValueMillis(randomLongBetween(1, 100)).nanos();
        DoubleSupplier randomUnstableAvgExecTimeNanos = () -> timeValueMillis(randomLongBetween(101, 10_000)).nanos();

        final double node0StableExecTimeNanos = randomStableAvgExecTimeNanos.getAsDouble();
        final double node1StableExecTimeNanos = randomStableAvgExecTimeNanos.getAsDouble();
        writeLoadPerNode = Map.of(NODE0, randomAvgWriteLoad.getAsDouble(), NODE1, randomAvgWriteLoad.getAsDouble());
        queueSizePerNode = Map.of(NODE0, randomAvgQueueSize.getAsDouble(), NODE1, randomAvgQueueSize.getAsDouble());
        avgTaskExecTimePerNode = Map.of(NODE0, node0StableExecTimeNanos, NODE1, node1StableExecTimeNanos);
        final var ingestionLoadPerNode = Map.of(
            NODE0,
            nodeIngestionLoadWithWriteActivity(
                writeLoadPerNode.get(NODE0),
                queueSizePerNode.get(NODE0),
                avgTaskExecTimePerNode.get(NODE0),
                OptionalDouble.of(node0StableExecTimeNanos)
            ),
            NODE1,
            nodeIngestionLoadWithWriteActivity(
                writeLoadPerNode.get(NODE1),
                queueSizePerNode.get(NODE1),
                avgTaskExecTimePerNode.get(NODE1),
                OptionalDouble.of(node1StableExecTimeNanos)
            )
        );
        trackIngestionLoads(service, seqNoSupplier, initialState, ingestionLoadPerNode::get);
        // tier-wide average is available from the two nodes. Reported ingestion loads are not adjusted.
        assertThat(
            nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().get(),
            equalTo(average(node0StableExecTimeNanos, node1StableExecTimeNanos))
        );
        var ingestionLoadSnapshots = service.getIndexTierMetrics(initialState, null).getNodesLoad();
        var lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        assertThat(ingestionLoadSnapshots.size(), equalTo(initialState.nodes().size()));
        assertThat(lastRawAndAdjustedLoads.raw().size(), equalTo(initialState.nodes().size()));
        assertThat(lastRawAndAdjustedLoads.adjusted(), is(nullValue()));
        // New node joins and initially publishes no stable average task execution time
        final var newNode = DiscoveryNodeUtils.builder(NODE2).name(NODE2).roles(nodeRoles).build();
        final var stateWithNewNode = stateWithNewNodes(initialState, List.of(newNode));
        service.clusterChanged(new ClusterChangedEvent("node-join", stateWithNewNode, initialState));
        final double node2UnstableExecTimeNanos = randomUnstableAvgExecTimeNanos.getAsDouble();
        writeLoadPerNode = Maps.copyMapWithAddedEntry(writeLoadPerNode, NODE2, randomAvgWriteLoad.getAsDouble());
        queueSizePerNode = Maps.copyMapWithAddedEntry(queueSizePerNode, NODE2, randomAvgQueueSize.getAsDouble());
        avgTaskExecTimePerNode = Maps.copyMapWithAddedEntry(avgTaskExecTimePerNode, NODE2, node2UnstableExecTimeNanos);
        var node2IngestionLoad1 = nodeIngestionLoadWithWriteActivity(
            writeLoadPerNode.get(NODE2),
            queueSizePerNode.get(NODE2),
            avgTaskExecTimePerNode.get(NODE2),
            OptionalDouble.empty()
        );
        if (randomBoolean()) {
            // if the existing nodes have published zero as their stable avg task execution times, the tier-wide average is not available
            final var ingestionLoadWithZeroStableExecTimePerNodeWithNewNode = Maps.copyMapWithAddedEntry(
                ingestionLoadWithZeroStableExecTimePerNode,
                NODE2,
                node2IngestionLoad1
            );
            trackIngestionLoads(service, seqNoSupplier, stateWithNewNode, ingestionLoadWithZeroStableExecTimePerNodeWithNewNode::get);
            assertThat(nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().isPresent(), is(false));
            // no adjustment should happen
            ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithNewNode, null).getNodesLoad();
            lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
            assertNull(lastRawAndAdjustedLoads.adjusted());
            assertTrue(
                ingestionLoadSnapshots.toString(),
                ingestionLoadSnapshots.stream().allMatch(l -> l.metricQuality() == MetricQuality.EXACT)
            );
        }
        final var nonEmptyIngestionLoadPerNode = new HashMap<>(ingestionLoadPerNode);
        nonEmptyIngestionLoadPerNode.put(NODE2, node2IngestionLoad1);
        trackIngestionLoads(service, seqNoSupplier, stateWithNewNode, nonEmptyIngestionLoadPerNode::get);
        // tier-wide average is available from the initial two nodes. Reported ingestion load for the new node is adjusted.
        assertThat(
            nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().get(),
            equalTo(average(node0StableExecTimeNanos, node1StableExecTimeNanos))
        );
        ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithNewNode, null).getNodesLoad();
        lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        assertThat(lastRawAndAdjustedLoads.raw().size(), equalTo(stateWithNewNode.nodes().size()));
        assertNotNull(lastRawAndAdjustedLoads.adjusted());
        assertThat(lastRawAndAdjustedLoads.adjusted().size(), equalTo(stateWithNewNode.nodes().size()));
        assertThat(ingestionLoadSnapshots.size(), equalTo(stateWithNewNode.nodes().size()));
        for (var adjustedLoad : ingestionLoadSnapshots) {
            if (adjustedLoad.nodeId().equals(newNode.getId())) {
                assertThat(adjustedLoad.metricQuality(), equalTo(MetricQuality.MINIMUM));
                final double threadsForQueue = queueThreadsNeeded(
                    queueSizePerNode.get(NODE2),
                    average(node0StableExecTimeNanos, node1StableExecTimeNanos)
                );
                assertThat(adjustedLoad.load(), closeTo(writeLoadPerNode.get(NODE2) + threadsForQueue, 0.01));
                assertThat(adjustedLoad.load(), lessThan(node2IngestionLoad1.totalIngestionLoad()));
            } else {
                assertThat(adjustedLoad.metricQuality(), equalTo(MetricQuality.EXACT));
                assertThat(adjustedLoad.load(), equalTo(ingestionLoadPerNode.get(adjustedLoad.nodeId()).totalIngestionLoad()));
            }
        }
        // after INITIAL_INTERVAL_TO_CONSIDER_NODE_AVG_TASK_EXEC_TIME_UNSTABLE, the new node also reports
        // a stable write average task execution time. Just advance time here...
        now.addAndGet(randomTimeValue(1, 5, TimeUnit.MINUTES).millis());
        final double node2StableExecTime = randomStableAvgExecTimeNanos.getAsDouble();
        avgTaskExecTimePerNode = Maps.copyMapWithAddedOrReplacedEntry(avgTaskExecTimePerNode, NODE2, node2StableExecTime);
        var node2IngestionLoad2 = nodeIngestionLoadWithWriteActivity(
            writeLoadPerNode.get(NODE2),
            queueSizePerNode.get(NODE2),
            avgTaskExecTimePerNode.get(NODE2),
            OptionalDouble.of(node2StableExecTime)
        );
        service.trackNodeIngestLoad(stateWithNewNode, newNode.getId(), newNode.getName(), seqNoSupplier.getAsLong(), node2IngestionLoad2);
        ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithNewNode, null).getNodesLoad();
        lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        // No adjustment should happen since there is no scaling and all nodes have stable average task execution times
        assertThat(lastRawAndAdjustedLoads.adjusted(), is(nullValue()));
        assertThat(ingestionLoadSnapshots.size(), equalTo(stateWithNewNode.nodes().size()));
        assertTrue(ingestionLoadSnapshots.stream().allMatch(l -> l.metricQuality() == MetricQuality.EXACT));
        // the tier-wide average is updated
        assertThat(
            nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().get(),
            equalTo(average(node0StableExecTimeNanos, node1StableExecTimeNanos, node2StableExecTime))
        );
        // two nodes are marked for shutdown, all nodes used tier wide and adjusted loads are returned
        final var shutdownMetadata = createShutdownMetadata(initialNodes);
        final var stateWithShutdowns = ClusterState.builder(stateWithNewNode)
            .metadata(Metadata.builder(stateWithNewNode.metadata()).putCustom(NodesShutdownMetadata.TYPE, shutdownMetadata))
            .build();
        service.clusterChanged(new ClusterChangedEvent("shutdown", stateWithShutdowns, stateWithNewNode));
        assertThat(nodeIngestLoadTracker.withinInitialScalingWindow(), is(true));
        ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithShutdowns, null).getNodesLoad();
        lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        // All reported loads are adjusted
        assertNotNull(lastRawAndAdjustedLoads.adjusted());
        assertThat(lastRawAndAdjustedLoads.raw().size(), equalTo(stateWithShutdowns.nodes().size()));
        assertThat(lastRawAndAdjustedLoads.adjusted().size(), equalTo(stateWithShutdowns.nodes().size()));
        assertThat(ingestionLoadSnapshots.size(), equalTo(stateWithShutdowns.nodes().size()));
        double tierWideAvgExecTime = average(node0StableExecTimeNanos, node1StableExecTimeNanos, node2StableExecTime);
        for (var adjustedLoad : ingestionLoadSnapshots) {
            final var nodeId = adjustedLoad.nodeId();
            assertThat(adjustedLoad.metricQuality(), equalTo(MetricQuality.MINIMUM));
            final double threadsForQueue = queueThreadsNeeded(queueSizePerNode.get(nodeId), tierWideAvgExecTime);
            assertThat(adjustedLoad.load(), closeTo(writeLoadPerNode.get(nodeId) + threadsForQueue, 0.01));
        }
        // One node leaves and the tier wide average task execution time is updated
        final var stateWithNodeLeft = ClusterState.builder(stateWithShutdowns)
            .nodes(DiscoveryNodes.builder(stateWithShutdowns.nodes()).remove(initialNodes.get(1).getId()))
            .build();
        service.clusterChanged(new ClusterChangedEvent("node-left", stateWithNodeLeft, stateWithShutdowns));
        assertThat(
            nodeIngestLoadTracker.getTierWideAverageWriteThreadpoolTaskExecutionTime().get(),
            equalTo(average(node0StableExecTimeNanos, node2StableExecTime))
        );
        ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithNodeLeft, null).getNodesLoad();
        lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        assertThat(ingestionLoadSnapshots.size(), equalTo(stateWithNodeLeft.nodes().size()));
        assertNotNull(lastRawAndAdjustedLoads.adjusted());
        assertThat(lastRawAndAdjustedLoads.raw().size(), equalTo(stateWithNodeLeft.nodes().size()));
        assertThat(lastRawAndAdjustedLoads.adjusted().size(), equalTo(stateWithNodeLeft.nodes().size()));
        tierWideAvgExecTime = average(node0StableExecTimeNanos, node2StableExecTime);
        for (var adjustedLoad : ingestionLoadSnapshots) {
            final var nodeId = adjustedLoad.nodeId();
            assertThat(adjustedLoad.metricQuality(), equalTo(MetricQuality.MINIMUM));
            final double threadsForQueue = queueThreadsNeeded(queueSizePerNode.get(nodeId), tierWideAvgExecTime);
            assertThat(adjustedLoad.load(), closeTo(writeLoadPerNode.get(nodeId) + threadsForQueue, 0.01));
        }

        // INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE passes, and we should go back to no adjustments
        now.addAndGet(initialScalingWindowConsideredUnstable.getNanos() + 1);
        ingestionLoadSnapshots = service.getIndexTierMetrics(stateWithNodeLeft, null).getNodesLoad();
        lastRawAndAdjustedLoads = service.getLastNodeIngestLoadSnapshots();
        assertThat(lastRawAndAdjustedLoads.adjusted(), is(nullValue()));
        assertThat(ingestionLoadSnapshots.size(), equalTo(stateWithNodeLeft.nodes().size()));
        assertTrue(ingestionLoadSnapshots.stream().allMatch(l -> l.metricQuality() == MetricQuality.EXACT));
    }

    private double average(double... values) {
        return Arrays.stream(values).average().orElse(0.0);
    }

    private static double queueThreadsNeeded(double avgQueueSize, double avgTaskExecTimeNanos) {
        // assumes MAX_TIME_TO_CLEAR_QUEUE = 1s
        return TimeValue.timeValueNanos((long) (avgQueueSize * avgTaskExecTimeNanos)).secondsFrac();
    }

    private static NodeIngestionLoad nodeIngestionLoadWithWriteActivity(
        double averageWriteLoad,
        double averageQueueSize,
        double averageTaskExecTimeNanos,
        OptionalDouble stableAvgTaskExecTimeNanos
    ) {
        final var queueThreadsNeeded = queueThreadsNeeded(averageQueueSize, averageTaskExecTimeNanos);
        return nodeIngestionLoadWithWriteActivity(
            new ExecutorStats(averageWriteLoad, averageTaskExecTimeNanos, (int) averageQueueSize, averageQueueSize, 4),
            stableAvgTaskExecTimeNanos,
            new ExecutorIngestionLoad(averageWriteLoad, queueThreadsNeeded),
            averageWriteLoad + queueThreadsNeeded
        );
    }

    private static NodeIngestionLoad nodeIngestionLoadWithWriteActivity(
        ExecutorStats writeExecutorStats,
        OptionalDouble writeStableAvgTaskExecTime,
        ExecutorIngestionLoad writeExecutorIngestionLoad,
        double totalIngestionLoad
    ) {
        final var emptyExecutorStats = new ExecutorStats(0.0, 0.0, 0, 0.0, 4);
        final var emptyExecutorIngestionLoad = new ExecutorIngestionLoad(0.0, 0.0);
        return new NodeIngestionLoad(
            WRITE_EXECUTORS.stream()
                .collect(
                    Collectors.toMap(name -> name, name -> name.equals(ThreadPool.Names.WRITE) ? writeExecutorStats : emptyExecutorStats)
                ),
            writeStableAvgTaskExecTime.isEmpty()
                ? Map.of()
                : WRITE_EXECUTORS.stream()
                    .collect(
                        Collectors.toMap(
                            name -> name,
                            name -> name.equals(ThreadPool.Names.WRITE) ? writeStableAvgTaskExecTime.getAsDouble() : 0.0
                        )
                    ),
            WRITE_EXECUTORS.stream()
                .collect(
                    Collectors.toMap(
                        name -> name,
                        name -> name.equals(ThreadPool.Names.WRITE) ? writeExecutorIngestionLoad : emptyExecutorIngestionLoad
                    )
                ),
            totalIngestionLoad
        );
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

    private Map<String, NodeIngestionLoad> trackRandomIngestionLoads(
        IngestMetricsService service,
        LongSupplier seqNoSupplier,
        ClusterState state
    ) {
        return trackIngestionLoads(service, seqNoSupplier, state, nodeId -> randomIngestionLoad());
    }

    private Map<String, NodeIngestionLoad> trackIngestionLoads(
        IngestMetricsService service,
        LongSupplier seqNoSupplier,
        ClusterState state,
        Function<String, NodeIngestionLoad> ingestionLoadSupplier
    ) {
        final List<DiscoveryNode> indexingNodes = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .toList();
        final Map<String, NodeIngestionLoad> ingestionLoads = indexingNodes.stream()
            .collect(Collectors.toUnmodifiableMap(DiscoveryNode::getId, node -> ingestionLoadSupplier.apply(node.getId())));
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

    private void assertExactIngestionLoads(
        IngestMetricsService service,
        Collection<NodeIngestionLoad> publishedLoads,
        ClusterState state,
        double maxUndesiredShardsProportionForScaleDown
    ) {
        final List<DiscoveryNode> indexingNodes = state.nodes()
            .stream()
            .filter(node -> node.getRoles().contains(DiscoveryNodeRole.INDEX_ROLE))
            .toList();
        final List<NodeIngestLoadSnapshot> readMetrics = service.getIndexTierMetrics(
            state,
            randomDesiredBalanceStatsForExactMetrics(maxUndesiredShardsProportionForScaleDown)
        ).getNodesLoad();
        assertThat(readMetrics.size(), equalTo(indexingNodes.size()));
        assertTrue(readMetrics.stream().allMatch(nodeLoad -> nodeLoad.metricQuality() == MetricQuality.EXACT));
        assertArrayEquals(
            publishedLoads.stream().mapToDouble(NodeIngestionLoad::totalIngestionLoad).sorted().toArray(),
            readMetrics.stream().mapToDouble(NodeIngestLoadSnapshot::load).sorted().toArray(),
            EPSILON
        );
    }

    private void assertIngestionLoadWeightApplied(
        IngestMetricsService service,
        Collection<NodeIngestionLoad> publishedLoads,
        List<NodeIngestLoadSnapshot> readMetrics,
        int numForHighWeight
    ) {
        final double highWeight = service.getHighIngestionLoadWeightDuringScaling();
        final double lowWeight = service.getLowIngestionLoadWeightDuringScaling();
        boolean weightApplied = highWeight < 1.0 || lowWeight < 1.0;
        double totalPublishedIngestionLoad = publishedLoads.stream().map(NodeIngestionLoad::totalIngestionLoad).reduce(Double::sum).get();
        double totalReadMetric = readMetrics.stream().map(NodeIngestLoadSnapshot::load).reduce(Double::sum).get();
        if (weightApplied) {
            assertTrue(readMetrics.stream().allMatch(l -> l.metricQuality().equals(MetricQuality.MINIMUM)));
            assertThat(totalReadMetric, lessThan(totalPublishedIngestionLoad));
            final List<Double> sortedPublishedLoads = publishedLoads.stream().map(NodeIngestionLoad::totalIngestionLoad).sorted().toList();
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
        Map<String, NodeIngestionLoad> publishedLoads
    ) {
        assertMetricsForRawAndAdjustedIngestLoads(service, metricRecorder, publishedLoads, null);
    }

    private static void assertMetricsForRawAndAdjustedIngestLoads(
        IngestMetricsService service,
        MetricRecorder<Instrument> metricRecorder,
        Map<String, NodeIngestionLoad> publishedLoads,
        @Nullable List<NodeIngestLoadSnapshot> readMetrics
    ) {
        Map<String, Double> publishedTotalLoads = publishedLoads.entrySet()
            .stream()
            .collect(Collectors.toUnmodifiableMap(Map.Entry::getKey, e -> e.getValue().totalIngestionLoad()));
        // We should always observe the raw ingest loads
        final RawAndAdjustedNodeIngestLoadSnapshots lastNodeIngestLoadSnapshots = service.getLastNodeIngestLoadSnapshots();
        assertThat(
            lastNodeIngestLoadSnapshots.raw()
                .stream()
                .collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load)),
            equalTo(publishedTotalLoads)
        );

        metricRecorder.collect();
        final List<Measurement> measurements = metricRecorder.getMeasurements(
            InstrumentType.DOUBLE_GAUGE,
            NODE_INGEST_LOAD_SNAPSHOTS_METRIC_NAME
        );
        assertThat(
            measurements.stream()
                .filter(measurement -> ADJUSTED.key().equals(measurement.attributes().get("type")) == false)
                .peek(measurement -> {
                    assertThat(measurement.attributes().get("quality"), is(MetricQuality.EXACT.getLabel()));
                    assertThat(measurement.attributes().get("node_name"), notNullValue());
                })
                .collect(Collectors.toUnmodifiableMap(m -> m.attributes().get("node_id"), Measurement::getDouble)),
            equalTo(publishedTotalLoads)
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
            assertThat(measurements, hasSize(publishedTotalLoads.size() * 2));
            assertThat(
                measurements.stream()
                    .filter(measurement -> ADJUSTED.key().equals(measurement.attributes().get("type")))
                    .peek(measurement -> {
                        assertThat(measurement.attributes().get("quality"), is(MetricQuality.MINIMUM.getLabel()));
                        assertThat(measurement.attributes().get("node_name"), notNullValue());
                    })
                    .collect(Collectors.toUnmodifiableMap(m -> m.attributes().get("node_id"), Measurement::getDouble)),
                equalTo(
                    readMetrics.stream().collect(Collectors.toUnmodifiableMap(NodeIngestLoadSnapshot::nodeId, NodeIngestLoadSnapshot::load))
                )
            );
            // All values have type=adjusted or type=unadjusted
            assertTrue(
                measurements.stream()
                    .allMatch(
                        measurement -> UNADJUSTED.key().equals(measurement.attributes().get("type"))
                            || ADJUSTED.key().equals(measurement.attributes().get("type"))
                    )
            );
        } else {
            assertThat(lastNodeIngestLoadSnapshots.adjusted(), nullValue());
            assertThat(measurements, hasSize(publishedTotalLoads.size()));
            assertTrue(measurements.stream().allMatch(measurement -> SINGLE.key().equals(measurement.attributes().get("type"))));
        }
    }

    public static NodesShutdownMetadata createShutdownMetadata(List<DiscoveryNode> shuttingDownNodes) {
        return new NodesShutdownMetadata(shuttingDownNodes.stream().collect(Collectors.toMap(DiscoveryNode::getId, node -> {
            final var shutdownType = randomFrom(Arrays.stream(Type.values()).filter(Type::isRemovalType).toList());
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

    private static NodeIngestionLoad randomIngestionLoad() {
        return mockedNodeIngestionLoad(randomDoubleBetween(0, 16, true));
    }

    private static ClusterState clusterState(DiscoveryNodes nodes) {
        assert nodes != null;
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
    }

    private static ClusterSettings emptyClusterSettings() {
        return clusterSettings(
            Settings.builder()
                .put(MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN.getKey(), randomMaxUndesiredShardsProportionForScaleDown())
                .build()
        );
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(
            settings,
            Set.of(
                ACCURATE_LOAD_WINDOW,
                STALE_LOAD_WINDOW,
                HIGH_INGESTION_LOAD_WEIGHT_DURING_SCALING,
                LOW_INGESTION_LOAD_WEIGHT_DURING_SCALING,
                LOAD_ADJUSTMENT_AFTER_SCALING_WINDOW,
                MAX_UNDESIRED_SHARDS_PROPORTION_FOR_SCALE_DOWN,
                INITIAL_SCALING_WINDOW_TO_CONSIDER_AVG_TASK_EXEC_TIMES_UNSTABLE,
                USE_TIER_WIDE_AVG_TASK_EXEC_TIME_DURING_SCALING,
                IngestLoadProbe.MAX_TIME_TO_CLEAR_QUEUE,
                IngestLoadProbe.MAX_QUEUE_CONTRIBUTION_FACTOR,
                IngestLoadProbe.INCLUDE_WRITE_COORDINATION_EXECUTORS_ENABLED,
                IngestLoadProbe.MAX_MANAGEABLE_QUEUED_WORK
            )
        );
    }

    private static boolean doublesEquals(double expected, double actual) {
        return Math.abs(expected - actual) < EPSILON;
    }

    private static double randomMaxUndesiredShardsProportionForScaleDown() {
        return switch (between(1, 3)) {
            case 1 -> 0.0;
            case 2 -> 1.0;
            default -> randomDoubleBetween(0.0, 1.0, true);
        };
    }

    private static DesiredBalanceMetrics.AllocationStats randomDesiredBalanceStatsForExactMetrics(
        double maxUndesiredShardsProportionForScaleDown
    ) {
        final var totalShards = randomLongBetween(0, 1000);
        final long undesiredAllocations = randomLongBetween(0L, (long) (totalShards * maxUndesiredShardsProportionForScaleDown));
        return new DesiredBalanceMetrics.AllocationStats(
            randomNonNegativeLong(),
            Map.of(ShardRouting.Role.INDEX_ONLY, new DesiredBalanceMetrics.RoleAllocationStats(totalShards, undesiredAllocations))
        );
    }

    private static DesiredBalanceMetrics.AllocationStats randomDesiredBalanceStats() {
        if (randomBoolean()) {
            return null;
        }
        return new DesiredBalanceMetrics.AllocationStats(
            randomNonNegativeLong(),
            randomSubsetOf(List.of(ShardRouting.Role.INDEX_ONLY, ShardRouting.Role.SEARCH_ONLY)).stream()
                .collect(Collectors.toUnmodifiableMap(Function.identity(), r -> randomRoleAllocationStats()))
        );
    }

    private static DesiredBalanceMetrics.RoleAllocationStats randomRoleAllocationStats() {
        final long totalShards = randomBoolean() ? 0 : randomNonNegativeLong();
        final long undesiredAllocations = randomBoolean() ? 0 : randomLongBetween(0L, totalShards);
        return new DesiredBalanceMetrics.RoleAllocationStats(totalShards, undesiredAllocations);
    }

    // TODO: return a value that is consistent internally at least w.r.t. the executor ingestion loads
    private static NodeIngestionLoad mockedNodeIngestionLoad(double totalIngestionLoad) {
        return new NodeIngestionLoad(Map.of(), Map.of(), Map.of(), totalIngestionLoad);
    }
}
