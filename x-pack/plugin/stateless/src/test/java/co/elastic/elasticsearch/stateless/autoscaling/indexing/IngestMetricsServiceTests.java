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
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.indexing.IngestMetricsService.ACCURATE_LOAD_WINDOW;
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
                    MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING
                )
            )
        );
    }

    public void testServiceOnlyReturnDataWhenLocalNodeIsElectedAsMaster() {
        var localNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var remoteNode = DiscoveryNodeUtils.create(UUIDs.randomBase64UUID());
        var nodes = DiscoveryNodes.builder().add(localNode).add(remoteNode).localNodeId(localNode.getId()).build();
        var service = new IngestMetricsService(clusterSettings(Settings.EMPTY), () -> 0, memoryMetricsService);
        var indexTierMetrics = service.getIndexTierMetrics();
        // If the node is not elected as master (i.e. we haven't got any cluster state notification) it shouldn't return any info
        assertThat(indexTierMetrics.getNodesLoad(), is(empty()));

        service.clusterChanged(
            new ClusterChangedEvent(
                "Local node not elected as master",
                clusterState(DiscoveryNodes.builder(nodes).masterNodeId(remoteNode.getId()).build()),
                clusterState(nodes)
            )
        );

        var indexTierMetricsAfterClusterStateEvent = service.getIndexTierMetrics();
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
        var indexTierMetrics = service.getIndexTierMetrics();
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

        var indexTierMetrics = service.getIndexTierMetrics();
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

        var indexTierMetricsAfterNodeMetricIsInaccurate = service.getIndexTierMetrics();
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

            var indexTierMetricsAfterNodeReJoins = service.getIndexTierMetrics();
            assertThat(indexTierMetricsAfterNodeReJoins.getNodesLoad(), hasSize(1));

            var indexNodeLoadAfterRejoining = indexTierMetricsAfterNodeReJoins.getNodesLoad().get(0);
            assertThat(indexNodeLoadAfterRejoining.load(), is(equalTo(0.5)));
            assertThat(indexNodeLoadAfterRejoining.metricQuality(), is(equalTo(MetricQuality.EXACT)));
        } else {
            // The node do not re-join after the max time
            fakeClock.addAndGet(staleLoadWindow.getNanos());

            var indexTierMetricsAfterTTLExpires = service.getIndexTierMetrics();
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

        var indexTierMetrics = service.getIndexTierMetrics();
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
        var indexTierMetrics = service.getIndexTierMetrics();
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

        var indexTierMetricsAfterMasterHandover = service.getIndexTierMetrics();
        assertThat(indexTierMetricsAfterMasterHandover.getNodesLoad(), is(empty()));
    }

    private static double randomIngestionLoad() {
        return randomDoubleBetween(0, 16, true);
    }

    private static ClusterState clusterState(DiscoveryNodes nodes) {
        assert nodes != null;
        return ClusterState.builder(ClusterName.DEFAULT).nodes(nodes).build();
    }

    private static ClusterSettings clusterSettings(Settings settings) {
        return new ClusterSettings(settings, Set.of(ACCURATE_LOAD_WINDOW, STALE_LOAD_WINDOW));
    }
}
