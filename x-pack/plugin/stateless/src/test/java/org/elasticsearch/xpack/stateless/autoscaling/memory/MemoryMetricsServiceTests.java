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

package co.elastic.elasticsearch.stateless.autoscaling.memory;

import co.elastic.elasticsearch.serverless.constants.ProjectType;
import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class MemoryMetricsServiceTests extends ESTestCase {

    private static final Index INDEX = new Index("test-index-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");
    private static final ClusterSettings CLUSTER_SETTINGS = new ClusterSettings(
        Settings.EMPTY,
        Sets.addToCopy(
            ClusterSettings.BUILT_IN_CLUSTER_SETTINGS,
            MemoryMetricsService.STALE_METRICS_CHECK_DURATION_SETTING,
            MemoryMetricsService.STALE_METRICS_CHECK_INTERVAL_SETTING,
            MemoryMetricsService.SHARD_MEMORY_OVERHEAD_SETTING
        )
    );

    private static ExecutorService executorService;

    private MemoryMetricsService service;

    @BeforeClass
    public static void setupThreadPool() {
        executorService = Executors.newFixedThreadPool(2);
    }

    @AfterClass
    public static void tearDownThreadPool() {
        terminate(executorService);
    }

    @Before
    public void init() {
        service = new MemoryMetricsService(System::nanoTime, CLUSTER_SETTINGS, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE);
    }

    public void testReduceFinalIndexMappingSize() {
        // get access to internals
        Map<ShardId, MemoryMetricsService.ShardMemoryMetrics> map = service.getShardMemoryMetrics();
        long expectedSizeInBytes = 0;
        int numberOfIndices = randomIntBetween(10, 1000);
        for (int nameSuffix = 1; nameSuffix <= numberOfIndices; nameSuffix++) {
            long mappingSize = randomIntBetween(0, 1000);
            int numSegments = randomNonNegativeInt();
            int totalFields = randomNonNegativeInt();
            Index index = new Index("name-" + nameSuffix, "uuid-" + nameSuffix);
            var metric = new MemoryMetricsService.ShardMemoryMetrics(
                mappingSize,
                numSegments,
                totalFields,
                randomNonNegativeLong(),
                MetricQuality.EXACT,
                randomIdentifier(),
                randomNonNegativeLong()
            );
            map.put(new ShardId(index, 0), metric);
            expectedSizeInBytes += mappingSize;
        }
        var result = service.calculateTotalIndicesMappingSize();
        assertThat(result.sizeInBytes(), equalTo(expectedSizeInBytes));
        assertThat(result.metricQuality(), equalTo(MetricQuality.EXACT));

        // simulate MINIMUM `quality` attribute on a random metric
        int nameSuffix = randomIntBetween(1, numberOfIndices);
        ShardId shardId = new ShardId(new Index("name-" + nameSuffix, "uuid-" + nameSuffix), 0);
        long oldMappingSize = map.get(shardId).getMappingSizeInBytes();
        long newMappingSize = randomNonNegativeLong();
        map.put(
            shardId,
            new MemoryMetricsService.ShardMemoryMetrics(
                newMappingSize,
                randomNonNegativeInt(),
                randomNonNegativeInt(),
                randomNonNegativeLong(),
                MetricQuality.MINIMUM,
                randomIdentifier(),
                randomNonNegativeLong()
            )
        );
        expectedSizeInBytes += (newMappingSize - oldMappingSize);
        result = service.calculateTotalIndicesMappingSize();
        assertThat(result.sizeInBytes(), equalTo(expectedSizeInBytes));
        // verify that the whole batch has MISSING `quality` attribute
        assertThat(result.metricQuality(), equalTo(MetricQuality.MINIMUM));
    }

    public void testConcurrentUpdateMetricHigherSeqNoWins() throws InterruptedException {

        int numberOfConcurrentUpdates = 10000;
        final CountDownLatch latch = new CountDownLatch(numberOfConcurrentUpdates);

        // init value
        ShardId shardId = new ShardId(INDEX, between(0, 5));
        service.getShardMemoryMetrics()
            .put(shardId, new MemoryMetricsService.ShardMemoryMetrics(0, 0, 0, 0L, MetricQuality.MISSING, "node-0", 0));

        // simulate concurrent updates
        for (int i = 0; i < numberOfConcurrentUpdates; i++) {
            long mappingSize, seqNo;
            final int numSegments;
            final int totalFields;
            if (randomBoolean()) {
                mappingSize = 100;
                seqNo = 10;
                numSegments = 50;
                totalFields = 100;
            } else {
                mappingSize = 200;
                seqNo = 1;
                numSegments = 5;
                totalFields = 10;
            }
            executorService.execute(() -> {
                HeapMemoryUsage metric = new HeapMemoryUsage(
                    seqNo,
                    Map.of(shardId, new ShardMappingSize(mappingSize, numSegments, totalFields, "node-0"))
                );
                service.updateShardsMappingSize(metric);
                latch.countDown();
            });
        }

        safeAwait(latch);
        var metrics = service.getShardMemoryMetrics().get(shardId);
        assertThat(metrics.getMappingSizeInBytes(), equalTo(100L));
        assertThat(metrics.getNumSegments(), equalTo(50));
        assertThat(metrics.getTotalFields(), equalTo(100));
    }

    public void testReportNonExactMetricsInTotalIndicesMappingSize() throws Exception {
        long currentTime = System.nanoTime();
        MemoryMetricsService customService = new MemoryMetricsService(
            () -> currentTime,
            CLUSTER_SETTINGS,
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE
        );

        long updateTime = currentTime - TimeUnit.MINUTES.toNanos(5) - TimeUnit.SECONDS.toNanos(1);
        for (int i = 0; i < 100; i++) {
            customService.getShardMemoryMetrics()
                .put(
                    new ShardId(new Index(randomIdentifier(), randomUUID()), 0),
                    new MemoryMetricsService.ShardMemoryMetrics(2, 1, 5, 0L, MetricQuality.MISSING, "node-0", updateTime)
                );
        }
        int count = randomIntBetween(10, 100);
        List<ShardId> shardsToSkip = randomSubsetOf(count, customService.getShardMemoryMetrics().keySet());
        for (var entry : customService.getShardMemoryMetrics().entrySet()) {
            if (shardsToSkip.contains(entry.getKey()) == false) {
                entry.getValue()
                    .update(
                        randomNonNegativeLong(),
                        randomNonNegativeInt(),
                        randomNonNegativeInt(),
                        randomIntBetween(1, 100),
                        "node-0",
                        currentTime
                    );
            }
        }

        try (var mockLog = MockLog.capture(MemoryMetricsService.class)) {
            for (ShardId shard : shardsToSkip) {
                mockLog.addExpectation(
                    new MockLog.SeenEventExpectation(
                        "expected warn log about state index",
                        MemoryMetricsService.class.getName(),
                        Level.WARN,
                        Strings.format(
                            "Memory metrics are stale for shard %s=ShardMemoryMetrics{mappingSizeInBytes=2, numSegments=1,"
                                + " totalFields=5, seqNo=0, metricQuality=MISSING, metricShardNodeId='node-0', updateTimestampNanos='%d'}",
                            shard,
                            updateTime
                        )
                    )
                );
            }

            customService.calculateTotalIndicesMappingSize();
            mockLog.assertAllExpectationsMatched();

            // Second call doesn't result in duplicate logs
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            customService.calculateTotalIndicesMappingSize();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testNoStaleMetricsInTotalIndicesMappingSize() throws Exception {
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            service.getShardMemoryMetrics()
                .put(
                    new ShardId(new Index(randomIdentifier(), randomUUID()), between(0, 2)),
                    new MemoryMetricsService.ShardMemoryMetrics(
                        randomNonNegativeLong(),
                        randomNonNegativeInt(),
                        randomNonNegativeInt(),
                        randomNonNegativeLong(),
                        MetricQuality.EXACT,
                        "node-0",
                        System.nanoTime()
                    )
                );
        }

        try (var mockLog = MockLog.capture(MemoryMetricsService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            service.calculateTotalIndicesMappingSize();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDoNotReportNonExactMetricsAsStaleImmediatelyOnStartup() {
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            service.getShardMemoryMetrics()
                .put(
                    new ShardId(new Index(randomIdentifier(), randomUUID()), randomIntBetween(0, 2)),
                    new MemoryMetricsService.ShardMemoryMetrics(
                        randomNonNegativeLong(),
                        randomNonNegativeInt(),
                        randomNonNegativeInt(),
                        0,
                        MetricQuality.MISSING,
                        "node-0",
                        System.nanoTime()
                    )
                );
        }

        try (var mockLog = MockLog.capture(MemoryMetricsService.class)) {
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            service.calculateTotalIndicesMappingSize();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDoNotThrowMissedIndicesUpdateExceptionOnOutOfOrderMessages() {
        ShardId shardId = new ShardId(INDEX, between(0, 2));
        service.getShardMemoryMetrics()
            .put(shardId, new MemoryMetricsService.ShardMemoryMetrics(0, 0, 0, 0L, MetricQuality.MISSING, "node-0", 0));

        int amount = randomIntBetween(50, 100);
        List<Integer> seqIds = IntStream.range(1, amount + 1).mapToObj(Integer::valueOf).collect(Collectors.toList());
        Collections.shuffle(seqIds, random());
        for (int i = 0; i < amount; i++) {
            service.updateShardsMappingSize(
                new HeapMemoryUsage(
                    seqIds.get(i),
                    Map.of(
                        shardId,
                        new ShardMappingSize(randomIntBetween(0, 100), randomIntBetween(0, 100), randomIntBetween(0, 100), "node-0")
                    )
                )
            );
        }

        assertThat(service.getShardMemoryMetrics().get(shardId).getMetricQuality(), equalTo(MetricQuality.EXACT));
    }

    public void testThrowMissedIndicesUpdateExceptionOnMissedIndex() {
        ShardId shardId = new ShardId(INDEX, between(0, 2));
        service.getShardMemoryMetrics()
            .put(shardId, new MemoryMetricsService.ShardMemoryMetrics(0, 0, 0, 0, MetricQuality.MISSING, "node-0", 0));

        ShardId otherShardId = randomValueOtherThan(shardId, () -> new ShardId(INDEX, between(0, 2)));
        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateShardsMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(otherShardId, new ShardMappingSize(randomIntBetween(0, 100), 0, 0, "node-0"))
                )
            );
        });
    }

    public void testThrowMissedIndicesUpdateExceptionOnMissedNode() {
        ShardId shardId = new ShardId(INDEX, 0);
        String currentNode = randomIdentifier();
        service.getShardMemoryMetrics()
            .put(
                shardId,
                new MemoryMetricsService.ShardMemoryMetrics(
                    randomNonNegativeLong(),
                    randomNonNegativeInt(),
                    randomNonNegativeInt(),
                    0,
                    MetricQuality.MISSING,
                    currentNode,
                    0
                )
            );

        String newNode = randomValueOtherThan(currentNode, ESTestCase::randomIdentifier);
        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateShardsMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(shardId, new ShardMappingSize(randomNonNegativeLong(), randomNonNegativeInt(), randomNonNegativeInt(), newNode))
                )
            );
        });
    }

    public void testSpecificValues() {
        long size = randomBoolean() ? between(1, 1000) : randomLongBetween(ByteSizeUnit.GB.toBytes(1), ByteSizeUnit.GB.toBytes(2));
        var clusterState = createClusterStateWithIndices(1, 1);
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE);
        service.clusterChanged(event);
        assertEquals(1, clusterState.metadata().indices().size());
        var index = clusterState.metadata().indices().values().iterator().next().getIndex();
        var node = clusterState.nodes().getLocalNode().getId();
        int numSegments = randomIntBetween(0, 5);
        int numFields = randomIntBetween(0, 10);
        service.getShardMemoryMetrics()
            .put(
                new ShardId(index, 0),
                new MemoryMetricsService.ShardMemoryMetrics(size, numSegments, numFields, 0L, MetricQuality.EXACT, node, 0)
            );

        MemoryMetrics memoryMetrics = service.getMemoryMetrics();
        assertThat(
            memoryMetrics.nodeMemoryInBytes(),
            equalTo((MemoryMetricsService.INDEX_MEMORY_OVERHEAD + MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD) * 2)
        );
        assertThat(
            memoryMetrics.totalMemoryInBytes(),
            equalTo(HeapToSystemMemory.tier(size + service.shardMemoryOverhead.getBytes(), ProjectType.ELASTICSEARCH_GENERAL_PURPOSE))
        );

        // a relatively high starting point, coming from 500MB heap work * 2 (for memory)
        assertThat(memoryMetrics.nodeMemoryInBytes(), lessThan(ByteSizeUnit.MB.toBytes(1200)));
    }

    public void testManyShards() {
        int numberOfIndices = 300;
        var clusterState = createClusterStateWithIndices(numberOfIndices, 1);
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE);
        service.clusterChanged(event);
        assertThat(service.getShardMemoryMetrics().size(), equalTo(numberOfIndices));
        var node = clusterState.nodes().getLocalNode().getId();
        int numSegments = randomIntBetween(0, 5);
        int numFields = randomIntBetween(0, 10);
        for (var indexMetadata : clusterState.metadata().indices().values()) {
            service.getShardMemoryMetrics()
                .put(
                    new ShardId(indexMetadata.getIndex(), 0),
                    new MemoryMetricsService.ShardMemoryMetrics(0, numSegments, numFields, 0, MetricQuality.EXACT, node, 0)
                );
        }

        MemoryMetrics memoryMetrics = service.getMemoryMetrics();
        assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(ByteSizeUnit.GB.toBytes(2)));
        assertThat(memoryMetrics.totalMemoryInBytes(), lessThan(ByteSizeUnit.GB.toBytes(4)));
        // * 2 to go from heap to system memory
        assertThat(memoryMetrics.totalMemoryInBytes(), equalTo(numberOfIndices * service.shardMemoryOverhead.getBytes() * 2));

        // show that one 4GB node is not enough, but 1.5 node would be.
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, greaterThan(ByteSizeUnit.GB.toBytes(4)));
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, lessThan(ByteSizeUnit.GB.toBytes(6)));
    }

    // TODO: update this test once when switch to use
    public void testEstimateUsingSegmentFields() {
        int numberOfIndices = between(1, 5);
        int numberOfShards = between(1, 2);
        ClusterState clusterState = createClusterStateWithIndices(numberOfIndices, numberOfShards);
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE);
        service.clusterChanged(event);
        var shardMetrics = service.getShardMemoryMetrics();
        assertThat(shardMetrics.size(), equalTo(numberOfIndices * numberOfShards));
        long expectedMappingSize = 0;
        for (var index : clusterState.metadata().indices().values()) {
            for (int id = 0; id < numberOfShards; id++) {
                ShardId shardId = new ShardId(index.getIndex(), id);
                var metrics = shardMetrics.get(shardId);
                assertNotNull(metrics);
                long mappingSizeInBytes = randomLongBetween(1, 1000);
                int numSegments = randomNonNegativeInt();
                int numFields = randomNonNegativeInt();
                service.updateShardsMappingSize(
                    new HeapMemoryUsage(
                        randomNonNegativeLong(),
                        Map.of(shardId, new ShardMappingSize(mappingSizeInBytes, numSegments, numFields, metrics.getMetricShardNodeId()))
                    )
                );
                if (id == 0) {
                    expectedMappingSize += mappingSizeInBytes;
                }
            }
        }
        assertThat(service.calculateTotalIndicesMappingSize().sizeInBytes(), equalTo(expectedMappingSize));
    }

    /** Creates a cluster state for a one node cluster, having the given number of indices in its metadata. */
    private ClusterState createClusterStateWithIndices(int numberOfIndices, int numberOfShards) {
        var indices = new HashMap<String, IndexMetadata>();
        var routingTableBuilder = RoutingTable.builder();
        IntStream.range(0, numberOfIndices).forEach(i -> {
            var indexMetadata = IndexMetadata.builder("index" + i)
                .settings(indexSettings(numberOfShards, 1).put("index.version.created", 1))
                .build();
            indices.put("index" + i, indexMetadata);
            routingTableBuilder.add(
                new RoutingTableGenerator().genIndexRoutingTable(indexMetadata, new RoutingTableGenerator.ShardCounter())
            );
        });
        return ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(DiscoveryNodeUtils.create("node_0")).localNodeId("node_0").masterNodeId("node_0"))
            .routingTable(routingTableBuilder)
            .metadata(Metadata.builder().indices(indices))
            .build();
    }
}
