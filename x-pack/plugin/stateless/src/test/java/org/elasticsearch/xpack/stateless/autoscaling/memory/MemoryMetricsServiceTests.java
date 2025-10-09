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
import co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ShardMergeMemoryEstimatePublication;
import co.elastic.elasticsearch.stateless.autoscaling.memory.MergeMemoryEstimateCollector.ShardMergeMemoryEstimate;

import org.apache.logging.log4j.Level;
import org.elasticsearch.action.support.replication.ClusterStateCreationUtils;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.GlobalRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.routing.RoutingTableGenerator;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_FIELD_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_SEGMENT_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.ADAPTIVE_SHARD_MEMORY_OVERHEAD;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.MemoryMetricsService.SHARD_MEMORY_OVERHEAD_OVERRIDE_ENABLED_SETTING;
import static co.elastic.elasticsearch.stateless.autoscaling.memory.ShardMappingSize.UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;

public class MemoryMetricsServiceTests extends ESTestCase {

    private static final Index INDEX = new Index("test-index-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");

    private static ExecutorService executorService;

    private ClusterSettings clusterSettings;
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
        clusterSettings = new ClusterSettings(
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
                MemoryMetricsService.SHARD_MEMORY_OVERHEAD_OVERRIDE_ENABLED_SETTING
            )
        );
        service = new MemoryMetricsService(
            System::nanoTime,
            clusterSettings,
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE,
            MeterRegistry.NOOP
        );
    }

    public void testReduceFinalIndexMappingSize() {
        // get access to internals
        Map<ShardId, MemoryMetricsService.ShardMemoryMetrics> map = service.getShardMemoryMetrics();
        long expectedSizeInBytes = 0;
        int numberOfIndices = randomIntBetween(10, 1000);
        for (int nameSuffix = 1; nameSuffix <= numberOfIndices; nameSuffix++) {
            long mappingSize = randomIntBetween(0, 1000);
            int numSegments = between(1, 10);
            int totalFields = between(1, 100);
            long postingsMemoryBytes = between(1, 512);
            long liveDocsBytes = between(1, 512);
            Index index = new Index("name-" + nameSuffix, "uuid-" + nameSuffix);
            var metric = new MemoryMetricsService.ShardMemoryMetrics(
                mappingSize,
                numSegments,
                totalFields,
                postingsMemoryBytes,
                liveDocsBytes,
                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                randomNonNegativeLong(),
                MetricQuality.EXACT,
                randomIdentifier(),
                randomNonNegativeLong()
            );
            map.put(new ShardId(index, 0), metric);
            expectedSizeInBytes += mappingSize;
            expectedSizeInBytes += service.fixedShardMemoryOverhead.getBytes();
        }
        var result = service.estimateTierMemoryUsage();
        assertThat(result.totalBytes(), equalTo(expectedSizeInBytes));
        assertThat(result.metricQuality(), equalTo(MetricQuality.EXACT));

        // simulate MINIMUM `quality` attribute on a random metric
        int nameSuffix = randomIntBetween(1, numberOfIndices);
        ShardId shardId = new ShardId(new Index("name-" + nameSuffix, "uuid-" + nameSuffix), 0);
        var oldMetric = map.get(shardId);
        long newMappingSize = between(1, 100000);
        int newFields = between(1, 10);
        int newSegments = between(1, 100);
        long postingsMemoryBytes = between(1, 512);
        long liveDocsBytes = between(1, 512);
        map.put(
            shardId,
            new MemoryMetricsService.ShardMemoryMetrics(
                newMappingSize,
                newSegments,
                newFields,
                postingsMemoryBytes,
                liveDocsBytes,
                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                randomNonNegativeLong(),
                MetricQuality.MINIMUM,
                randomIdentifier(),
                randomNonNegativeLong()
            )
        );
        expectedSizeInBytes += (newMappingSize - oldMetric.getMappingSizeInBytes());
        result = service.estimateTierMemoryUsage();
        assertThat(result.totalBytes(), equalTo(expectedSizeInBytes));
        // verify that the whole batch has MISSING `quality` attribute
        assertThat(result.metricQuality(), equalTo(MetricQuality.MINIMUM));
    }

    public void testConcurrentUpdateMetricHigherSeqNoWins() throws InterruptedException {

        int numberOfConcurrentUpdates = 10000;
        final CountDownLatch latch = new CountDownLatch(numberOfConcurrentUpdates);

        // init value
        ShardId shardId = new ShardId(INDEX, between(0, 5));
        service.getShardMemoryMetrics()
            .put(
                shardId,
                new MemoryMetricsService.ShardMemoryMetrics(
                    0,
                    0,
                    0,
                    0L,
                    0L,
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    0L,
                    MetricQuality.MISSING,
                    "node-0",
                    0
                )
            );

        // simulate concurrent updates
        for (int i = 0; i < numberOfConcurrentUpdates; i++) {
            long mappingSize, seqNo;
            final int numSegments;
            final int totalFields;
            final long postingsInMemoryBytes;
            final long liveDocsBytes;
            final long shardMemoryOverheadBytes;
            if (randomBoolean()) {
                mappingSize = 100;
                seqNo = 10;
                numSegments = 50;
                totalFields = 100;
                postingsInMemoryBytes = 100L;
                liveDocsBytes = 40L;
                shardMemoryOverheadBytes = 200L;
            } else {
                mappingSize = 200;
                seqNo = 1;
                numSegments = 5;
                totalFields = 10;
                postingsInMemoryBytes = 30L;
                liveDocsBytes = 20L;
                shardMemoryOverheadBytes = UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
            }
            executorService.execute(() -> {
                HeapMemoryUsage metric = new HeapMemoryUsage(
                    seqNo,
                    Map.of(
                        shardId,
                        new ShardMappingSize(
                            mappingSize,
                            numSegments,
                            totalFields,
                            postingsInMemoryBytes,
                            liveDocsBytes,
                            shardMemoryOverheadBytes,
                            "node-0"
                        )
                    )
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
        assertThat(metrics.getPostingsInMemoryBytes(), equalTo(100L));
        assertThat(metrics.getLiveDocsBytes(), equalTo(40L));
        assertThat(metrics.getShardMemoryOverheadBytes(), is(200L));
    }

    public void testReportNonExactMetricsInTotalIndicesMappingSize() throws Exception {
        long currentTime = System.nanoTime();
        MemoryMetricsService customService = new MemoryMetricsService(
            () -> currentTime,
            clusterSettings,
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE,
            MeterRegistry.NOOP
        );

        long updateTime = currentTime - TimeUnit.MINUTES.toNanos(5) - TimeUnit.SECONDS.toNanos(1);
        for (int i = 0; i < 100; i++) {
            customService.getShardMemoryMetrics()
                .put(
                    new ShardId(new Index(randomIdentifier(), randomUUID()), 0),
                    new MemoryMetricsService.ShardMemoryMetrics(2, 1, 5, 30L, 23L, 40L, 0L, MetricQuality.MISSING, "node-0", updateTime)
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
                        randomNonNegativeLong(),
                        randomNonNegativeInt(),
                        randomShardMemoryOverheadBytes(),
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
                                + " totalFields=5, postingsInMemoryBytes=30, liveDocsBytes=23, shardMemoryOverheadBytes=40, seqNo=0,"
                                + " metricQuality=MISSING, metricShardNodeId='node-0', updateTimestampNanos='%d'}",
                            shard,
                            updateTime
                        )
                    )
                );
            }

            customService.estimateTierMemoryUsage();
            mockLog.assertAllExpectationsMatched();

            // Second call doesn't result in duplicate logs
            mockLog.addExpectation(
                new MockLog.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            customService.estimateTierMemoryUsage();
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
                        randomNonNegativeLong(),
                        randomShardMemoryOverheadBytes(),
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
            service.estimateTierMemoryUsage();
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
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomShardMemoryOverheadBytes(),
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
            service.estimateTierMemoryUsage();
            mockLog.assertAllExpectationsMatched();
        }
    }

    public void testDoNotThrowMissedIndicesUpdateExceptionOnOutOfOrderMessages() {
        ShardId shardId = new ShardId(INDEX, between(0, 2));
        service.getShardMemoryMetrics()
            .put(
                shardId,
                new MemoryMetricsService.ShardMemoryMetrics(
                    0,
                    0,
                    0,
                    0L,
                    0L,
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    0L,
                    MetricQuality.MISSING,
                    "node-0",
                    0
                )
            );

        int amount = randomIntBetween(50, 100);
        List<Integer> seqIds = IntStream.range(1, amount + 1).mapToObj(Integer::valueOf).collect(Collectors.toList());
        Collections.shuffle(seqIds, random());
        for (int i = 0; i < amount; i++) {
            service.updateShardsMappingSize(
                new HeapMemoryUsage(
                    seqIds.get(i),
                    Map.of(
                        shardId,
                        new ShardMappingSize(
                            randomIntBetween(0, 100),
                            randomIntBetween(0, 100),
                            randomIntBetween(0, 100),
                            randomLongBetween(0, 100),
                            randomLongBetween(0, 100),
                            randomShardMemoryOverheadBytes(1, 100),
                            "node-0"
                        )
                    )
                )
            );
        }

        assertThat(service.getShardMemoryMetrics().get(shardId).getMetricQuality(), equalTo(MetricQuality.EXACT));
    }

    public void testThrowMissedIndicesUpdateExceptionOnMissedIndex() {
        ShardId shardId = new ShardId(INDEX, between(0, 2));
        service.getShardMemoryMetrics()
            .put(
                shardId,
                new MemoryMetricsService.ShardMemoryMetrics(
                    0,
                    0,
                    0,
                    0,
                    0,
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    0,
                    MetricQuality.MISSING,
                    "node-0",
                    0
                )
            );

        ShardId otherShardId = randomValueOtherThan(shardId, () -> new ShardId(INDEX, between(0, 2)));
        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateShardsMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(
                        otherShardId,
                        new ShardMappingSize(randomIntBetween(0, 100), 0, 0, 0, 0, UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES, "node-0")
                    )
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
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomShardMemoryOverheadBytes(),
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
                    Map.of(
                        shardId,
                        new ShardMappingSize(
                            randomNonNegativeLong(),
                            randomNonNegativeInt(),
                            randomNonNegativeInt(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomShardMemoryOverheadBytes(),
                            newNode
                        )
                    )
                )
            );
        });
    }

    public void testSpecificValues() {
        long size = randomBoolean() ? between(1, 1000) : randomLongBetween(ByteSizeUnit.GB.toBytes(1), ByteSizeUnit.GB.toBytes(2));
        var clusterState = createClusterStateWithIndices(1, 1);
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE);
        service.clusterChanged(event);
        assertEquals(1, clusterState.metadata().getTotalNumberOfIndices());
        var index = clusterState.metadata().indicesAllProjects().iterator().next().getIndex();
        var node = clusterState.nodes().getLocalNode().getId();
        int numSegments = 3;
        int numFields = 200;
        long postingsInMemoryBytes = 150L;
        long liveDocsBytes = 50L;
        service.getShardMemoryMetrics()
            .put(
                new ShardId(index, 0),
                new MemoryMetricsService.ShardMemoryMetrics(
                    size,
                    numSegments,
                    numFields,
                    postingsInMemoryBytes,
                    0L,
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    liveDocsBytes,
                    MetricQuality.EXACT,
                    node,
                    0
                )
            );

        MemoryMetrics memoryMetrics = service.getSearchTierMemoryMetrics();
        assertThat(
            memoryMetrics.nodeMemoryInBytes(),
            equalTo((MemoryMetricsService.INDEX_MEMORY_OVERHEAD + MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD) * 2)
        );
        long estimateBytes = size + service.fixedShardMemoryOverhead.getBytes();
        assertThat(
            memoryMetrics.totalMemoryInBytes(),
            equalTo(HeapToSystemMemory.tier(estimateBytes, ProjectType.ELASTICSEARCH_GENERAL_PURPOSE))
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
        int numSegments = 50;
        int numFields = 1200;
        for (var indexMetadata : clusterState.metadata().indicesAllProjects()) {
            service.getShardMemoryMetrics()
                .put(
                    new ShardId(indexMetadata.getIndex(), 0),
                    new MemoryMetricsService.ShardMemoryMetrics(
                        0,
                        numSegments,
                        numFields,
                        0,
                        0,
                        UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                        0,
                        MetricQuality.EXACT,
                        node,
                        0
                    )
                );
        }

        MemoryMetrics memoryMetrics = service.getSearchTierMemoryMetrics();
        assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(ByteSizeUnit.GB.toBytes(2)));
        assertThat(memoryMetrics.totalMemoryInBytes(), lessThan(ByteSizeUnit.GB.toBytes(4)));
        // show that one 4GB node is not enough, but 1.5 node would be.
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, greaterThan(ByteSizeUnit.GB.toBytes(4)));
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, lessThan(ByteSizeUnit.GB.toBytes(6)));
    }

    public void testEstimateMethods() {
        final double adaptiveExtraOverheadRatio = randomDoubleBetween(0, 1, true);
        final boolean shardMemoryOverheadOverrideEnabled = randomBoolean();
        clusterSettings.applySettings(
            Settings.builder()
                .put(MemoryMetricsService.ADAPTIVE_EXTRA_OVERHEAD_SETTING.getKey(), adaptiveExtraOverheadRatio)
                .put(SHARD_MEMORY_OVERHEAD_OVERRIDE_ENABLED_SETTING.getKey(), shardMemoryOverheadOverrideEnabled)
                .build()
        );
        service = new MemoryMetricsService(
            System::nanoTime,
            clusterSettings,
            ProjectType.ELASTICSEARCH_GENERAL_PURPOSE,
            MeterRegistry.NOOP
        );

        int numberOfIndices = between(1, 5);
        int numberOfShards = between(1, 2);
        ClusterState clusterState = createClusterStateWithIndices(numberOfIndices, numberOfShards);
        ClusterChangedEvent event = new ClusterChangedEvent("test", clusterState, ClusterState.EMPTY_STATE);
        service.clusterChanged(event);

        var shardMetrics = service.getShardMemoryMetrics();
        assertThat(shardMetrics.size(), equalTo(numberOfIndices * numberOfShards));
        long totalMappingSizeInBytes = 0;
        int totalShards = 0;
        int totalSegments = 0;
        int totalFields = 0;
        long maxPostingsInMemoryBytes = 0L;
        long totalLiveDocsBytes = 0L;
        for (var index : clusterState.metadata().indicesAllProjects()) {
            for (int id = 0; id < numberOfShards; id++) {
                ShardId shardId = new ShardId(index.getIndex(), id);
                totalShards++;
                var metrics = shardMetrics.get(shardId);
                assertNotNull(metrics);
                long mappingSizeInBytes = randomLongBetween(1, 1000);
                int numSegments = between(1, 10);
                totalSegments += numSegments;
                int numFields = between(1, 1000);
                totalFields += numFields;
                long postingsInMemoryBytes = randomLongBetween(1, 1000);
                maxPostingsInMemoryBytes = Math.max(postingsInMemoryBytes, maxPostingsInMemoryBytes);
                long liveDocsBytes = randomLongBetween(1, 1000);
                totalLiveDocsBytes += liveDocsBytes;
                service.updateShardsMappingSize(
                    new HeapMemoryUsage(
                        between(1, 10000),
                        Map.of(
                            shardId,
                            new ShardMappingSize(
                                mappingSizeInBytes,
                                numSegments,
                                numFields,
                                postingsInMemoryBytes,
                                liveDocsBytes,
                                UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                                metrics.getMetricShardNodeId()
                            )
                        )
                    )
                );
                totalMappingSizeInBytes += mappingSizeInBytes;
            }
        }

        // defaults to the fixed method
        long fixedEstimateBytes = totalMappingSizeInBytes + totalShards * FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT.getBytes();
        assertThat(service.estimateTierMemoryUsage().totalBytes(), equalTo(fixedEstimateBytes));
        // switch to the adaptive method
        service.fixedShardMemoryOverhead = ByteSizeValue.MINUS_ONE;
        long adaptiveEstimateBytes = totalShards * ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + totalSegments
            * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes() + totalFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + totalLiveDocsBytes;
        long extraBytes = (long) (adaptiveEstimateBytes * adaptiveExtraOverheadRatio);
        assertThat(service.estimateTierMemoryUsage().totalBytes(), equalTo(totalMappingSizeInBytes + adaptiveEstimateBytes + extraBytes));
        // switch back to the fixed method
        ByteSizeValue newOverhead = ByteSizeValue.ofBytes(between(1, 1000));
        service.fixedShardMemoryOverhead = newOverhead;
        var newFixedEstimateBytes = totalMappingSizeInBytes + totalShards * newOverhead.getBytes();
        assertThat(service.estimateTierMemoryUsage().totalBytes(), equalTo(newFixedEstimateBytes));

        assertThat(service.postingsMemoryEstimation(), equalTo(maxPostingsInMemoryBytes));

        // Test handling of shard memory overhead overrides. E.g. this is used for hollow shards.
        int overrideCount = randomIntBetween(1, numberOfIndices * numberOfShards);
        List<ShardId> shardsToOverride = randomSubsetOf(overrideCount, service.getShardMemoryMetrics().keySet());
        long totalOverheadOverrideBytes = 0;
        long maxPostingsInMemoryBytesAfterOverride = 0L;
        for (ShardId shardId : service.getShardMemoryMetrics().keySet()) {
            MemoryMetricsService.ShardMemoryMetrics metrics = service.getShardMemoryMetrics().get(shardId);
            if (shardsToOverride.contains(shardId)) {
                long overheadOverrideBytes = randomLongBetween(1, 1000);
                totalOverheadOverrideBytes += overheadOverrideBytes;
                service.updateShardsMappingSize(
                    new HeapMemoryUsage(
                        metrics.getSeqNo() + 1,
                        Map.of(
                            shardId,
                            new ShardMappingSize(
                                metrics.getMappingSizeInBytes(),
                                metrics.getNumSegments(),
                                metrics.getTotalFields(),
                                metrics.getPostingsInMemoryBytes(),
                                metrics.getLiveDocsBytes(),
                                overheadOverrideBytes,
                                metrics.getMetricShardNodeId()
                            )
                        )
                    )
                );
                // We skip regular logic when we have an override value supplied
                totalSegments -= metrics.getNumSegments();
                totalFields -= metrics.getTotalFields();
                totalLiveDocsBytes -= metrics.getLiveDocsBytes();
            } else {
                maxPostingsInMemoryBytesAfterOverride = Math.max(maxPostingsInMemoryBytesAfterOverride, metrics.getPostingsInMemoryBytes());
            }
        }
        var nonOverrideCount = totalShards - overrideCount;
        var fixedEstimateBytesWithOverride = totalMappingSizeInBytes + nonOverrideCount * newOverhead.getBytes()
            + totalOverheadOverrideBytes;
        if (shardMemoryOverheadOverrideEnabled) {
            assertThat(service.estimateTierMemoryUsage().totalBytes(), equalTo(fixedEstimateBytesWithOverride));
        } else {
            // No change as overrides are ignored
            assertThat(service.estimateTierMemoryUsage().totalBytes(), equalTo(newFixedEstimateBytes));
        }
        // Switch to the adaptive method and check with the updated aggregates
        service.fixedShardMemoryOverhead = ByteSizeValue.MINUS_ONE;
        long adaptiveEstimateBytesWithOverride = nonOverrideCount * ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + totalSegments
            * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes() + totalFields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes() + totalLiveDocsBytes;
        long extraBytesWithHollow = (long) (adaptiveEstimateBytesWithOverride * adaptiveExtraOverheadRatio);
        if (shardMemoryOverheadOverrideEnabled) {
            assertThat(
                service.estimateTierMemoryUsage().totalBytes(),
                equalTo(totalMappingSizeInBytes + totalOverheadOverrideBytes + adaptiveEstimateBytesWithOverride + extraBytesWithHollow)
            );
            assertThat(service.postingsMemoryEstimation(), equalTo(maxPostingsInMemoryBytesAfterOverride));
        } else {
            assertThat(
                service.estimateTierMemoryUsage().totalBytes(),
                equalTo(totalMappingSizeInBytes + adaptiveEstimateBytes + extraBytes)
            );
            assertThat(service.postingsMemoryEstimation(), equalTo(maxPostingsInMemoryBytes));
        }
    }

    public void testAdaptiveEstimateValues() {
        record Stat(String id, int shards, int segments, int fields, int actualMB) {

        }
        List<Stat> stats = List.of(
            new Stat("bcd2dc79ea2e4f0d801aa769fdee3dc2", 845, 13623, 1437797, 1986),
            new Stat("f0d408b52c7e4d43a0f60c6b9e039f08", 123, 2375, 515282, 668),
            new Stat("f0d408b52c7e4d43a0f60c6b9e039f08", 188, 4415, 793688, 1006),
            new Stat("e30fc2594a1e44e08c036faf6a3aca46", 188, 2263, 125951, 154),
            new Stat("e30fc2594a1e44e08c036faf6a3aca46", 197, 2359, 137145, 226),
            new Stat("b58f9bba9cbc4aa994e40ee38086be8a", 27, 147, 5760, 18),
            new Stat("d54bfd3da1424828972223c87e9f096f", 53, 475, 35732, 128),
            new Stat("e949317afc464134b5efef9210c21413", 867, 14127, 3050663, 4841),
            new Stat("e6cb34ca60a74a3cab2d90dcc561bd71", 123, 702, 35532, 58),
            new Stat("c028d3e13c3440f5b3e0c99943162c6b", 47, 1383, 1298352, 1460),
            new Stat("b0e6a8c015c54edbaacc9705746e4c85", 378, 10491, 867764, 1409),
            new Stat("e6f04f207dbd4187b3c07ef14b92294f", 291, 8210, 618282, 979)
        );
        StringBuilder sb = new StringBuilder();
        sb.append("| Project Id                      |shards|segments|  fields | actual |  fixed |adaptive|adjusted|");
        sb.append(System.lineSeparator());
        sb.append("-------------------------------------------------------------------------------------------------");
        sb.append(System.lineSeparator());
        String format = "| %-32s| %4s | %6s | %7s | %6s | %6s | %6s | %6s |%n";
        for (var stat : stats) {
            service.fixedShardMemoryOverhead = FIXED_SHARD_MEMORY_OVERHEAD_DEFAULT;
            var fixedEstimate = service.estimateShardMemoryUsageInBytes(stat.shards, stat.segments, stat.fields, 0);
            long adaptiveEstimate = stat.shards * ADAPTIVE_SHARD_MEMORY_OVERHEAD.getBytes() + stat.segments
                * ADAPTIVE_SEGMENT_MEMORY_OVERHEAD.getBytes() + stat.fields * ADAPTIVE_FIELD_MEMORY_OVERHEAD.getBytes();
            service.fixedShardMemoryOverhead = ByteSizeValue.MINUS_ONE;
            var adjustedEstimate = service.estimateShardMemoryUsageInBytes(stat.shards, stat.segments, stat.fields, 0);
            sb.append(
                String.format(
                    Locale.ROOT,
                    format,
                    stat.id,
                    stat.shards,
                    stat.segments,
                    stat.fields,
                    stat.actualMB + "mb",
                    (fixedEstimate / 1024 / 1024) + "mb",
                    (adaptiveEstimate / 1024 / 1024) + "mb",
                    (adjustedEstimate / 1024 / 1024) + "mb"
                )
            );
        }
        String expectedOutput = """
            | Project Id                      |shards|segments|  fields | actual |  fixed |adaptive|adjusted|
            -------------------------------------------------------------------------------------------------
            | bcd2dc79ea2e4f0d801aa769fdee3dc2|  845 |  13623 | 1437797 | 1986mb | 5070mb | 2197mb | 3296mb |
            | f0d408b52c7e4d43a0f60c6b9e039f08|  123 |   2375 |  515282 |  668mb |  738mb |  639mb |  959mb |
            | f0d408b52c7e4d43a0f60c6b9e039f08|  188 |   4415 |  793688 | 1006mb | 1128mb | 1025mb | 1538mb |
            | e30fc2594a1e44e08c036faf6a3aca46|  188 |   2263 |  125951 |  154mb | 1128mb |  258mb |  387mb |
            | e30fc2594a1e44e08c036faf6a3aca46|  197 |   2359 |  137145 |  226mb | 1182mb |  275mb |  412mb |
            | b58f9bba9cbc4aa994e40ee38086be8a|   27 |    147 |    5760 |   18mb |  162mb |   15mb |   23mb |
            | d54bfd3da1424828972223c87e9f096f|   53 |    475 |   35732 |  128mb |  318mb |   64mb |   96mb |
            | e949317afc464134b5efef9210c21413|  867 |  14127 | 3050663 | 4841mb | 5202mb | 3801mb | 5702mb |
            | e6cb34ca60a74a3cab2d90dcc561bd71|  123 |    702 |   35532 |   58mb |  738mb |   81mb |  122mb |
            | c028d3e13c3440f5b3e0c99943162c6b|   47 |   1383 | 1298352 | 1460mb |  282mb | 1345mb | 2018mb |
            | b0e6a8c015c54edbaacc9705746e4c85|  378 |  10491 |  867764 | 1409mb | 2268mb | 1438mb | 2157mb |
            | e6f04f207dbd4187b3c07ef14b92294f|  291 |   8210 |  618282 |  979mb | 1746mb | 1066mb | 1599mb |
            """;
        assertThat(sb.toString(), equalTo(expectedOutput));
    }

    public void testDoNotThrowMissedIndicesUpdateExceptionWhenSourceClusterStateVersionIsLessThanOrEqualToLocalClusterStateVersion() {
        setupStateAndPublishFirstMetrics((currentClusterStateVersion, shardId, currentNode, lastSequenceNumber) -> {
            // received sequence number > last sequence number
            long receivedSeqNum = randomLongBetween(lastSequenceNumber + 1, Long.MAX_VALUE);
            // received cluster state <= current cluster state version
            long receivedClusterStateVersion = randomBoolean()
                ? currentClusterStateVersion
                : randomLongBetween(1, currentClusterStateVersion);
            // Different node
            String newNode = randomValueOtherThan(currentNode, ESTestCase::randomIdentifier);

            // Should not throw
            service.updateShardsMappingSize(
                new HeapMemoryUsage(receivedSeqNum, Map.of(shardId, randomShardMappingSize(newNode)), receivedClusterStateVersion)
            );
            // should not apply
            assertEquals(lastSequenceNumber, service.getShardMemoryMetrics().get(shardId).getSeqNo());
        });
    }

    public void testThrowMissedIndicesUpdateExceptionWhenSourceClusterStateVersionIsGreaterThanLocalClusterStateVersion() {
        setupStateAndPublishFirstMetrics((currentClusterStateVersion, shardId, currentNode, lastSequenceNumber) -> {
            // received sequence number > last sequence number
            long receivedSeqNum = randomLongBetween(lastSequenceNumber + 1, Long.MAX_VALUE);
            // received cluster state > current cluster state version
            long receivedClusterStateVersion = randomLongBetween(currentClusterStateVersion + 1, Long.MAX_VALUE);
            // Different node
            String newNode = randomValueOtherThan(currentNode, ESTestCase::randomIdentifier);

            // Should throw
            assertThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
                service.updateShardsMappingSize(
                    new HeapMemoryUsage(receivedSeqNum, Map.of(shardId, randomShardMappingSize(newNode)), receivedClusterStateVersion)
                );
            });
            // should not apply
            assertEquals(lastSequenceNumber, service.getShardMemoryMetrics().get(shardId).getSeqNo());
        });
    }

    public void testIgnoreOnConflictIsConservative() {
        final AtomicReference<ClusterState> currentClusterState = new AtomicReference<>();
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final int iterations = randomIntBetween(20, 100);
        final int numberOfShards = randomIntBetween(30, 80);
        logger.info("--> Running {} iterations with {} shards", iterations, numberOfShards);
        try (TestThreadPool threadPool = new TestThreadPool(getClass().getSimpleName())) {
            Future<?> applierFuture = threadPool.generic()
                .submit(() -> clusterStateApplier(iterations, currentClusterState, barrier, numberOfShards));
            Future<?> metricUpdaterFuture = threadPool.generic()
                .submit(() -> shardMetricsUpdater(iterations, currentClusterState, barrier));
            safeGet(metricUpdaterFuture);
            safeGet(applierFuture);
        }
    }

    public void testIndexingMemoryRequirements() {
        var fakeClock = new AtomicLong();
        var projectType = randomFrom(ProjectType.values());
        service = new MemoryMetricsService(fakeClock::get, clusterSettings, projectType, MeterRegistry.NOOP);
        final var indexingTierMemoryMetricsBeforeIndexRequirements = service.getIndexingTierMemoryMetrics();

        assertThat(indexingTierMemoryMetricsBeforeIndexRequirements, is(equalTo(service.getSearchTierMemoryMetrics())));

        long requiredBytesForIndexingOperations = ByteSizeValue.ofMb(512).getBytes();
        service.updateIndexingOperationsHeapMemoryRequirements(requiredBytesForIndexingOperations);
        var indexingMemoryMetrics = service.getIndexingTierMemoryMetrics();
        var searchMemoryMetrics = service.getSearchTierMemoryMetrics();
        long extraMemoryNeededForIndexingOperations = HeapToSystemMemory.tier(requiredBytesForIndexingOperations, projectType);
        assertThat(
            indexingMemoryMetrics.nodeMemoryInBytes(),
            is(equalTo(indexingTierMemoryMetricsBeforeIndexRequirements.nodeMemoryInBytes() + extraMemoryNeededForIndexingOperations))
        );
        assertThat(indexingMemoryMetrics.nodeMemoryInBytes(), is(greaterThan(searchMemoryMetrics.nodeMemoryInBytes())));

        // The new requirement is below the previous one, so the service will keep reporting the largest one until it expires
        fakeClock.incrementAndGet();
        service.updateIndexingOperationsHeapMemoryRequirements(ByteSizeValue.ofMb(256).getBytes());

        assertThat(service.getIndexingTierMemoryMetrics().nodeMemoryInBytes(), is(equalTo(indexingMemoryMetrics.nodeMemoryInBytes())));

        // Once the previous memory requirement expires, the system stops requesting for the extra memory
        fakeClock.addAndGet(MemoryMetricsService.DEFAULT_INDEXING_OPERATIONS_MEMORY_REQUIREMENTS_VALIDITY.nanos());
        assertThat(
            service.getIndexingTierMemoryMetrics().nodeMemoryInBytes(),
            is(equalTo(indexingTierMemoryMetricsBeforeIndexRequirements.nodeMemoryInBytes()))
        );

        // If the previous requirement was expired, the service should accept the new requirement and report it back
        fakeClock.incrementAndGet();
        long newIndexingHeapMemoryRequirements = ByteSizeValue.ofMb(256).getBytes();
        service.updateIndexingOperationsHeapMemoryRequirements(newIndexingHeapMemoryRequirements);
        long newExtraMemoryNeededForIndexingOperations = HeapToSystemMemory.tier(newIndexingHeapMemoryRequirements, projectType);
        assertThat(
            service.getIndexingTierMemoryMetrics().nodeMemoryInBytes(),
            is(equalTo(indexingTierMemoryMetricsBeforeIndexRequirements.nodeMemoryInBytes() + newExtraMemoryNeededForIndexingOperations))
        );
    }

    public void testMergeMemoryEstimate() {
        var fakeClock = new AtomicLong();
        final var validityNanos = DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY.nanos();
        var seqNoGenerator = new AtomicLong();
        var node0 = DiscoveryNodeUtils.create("node0");
        var node0EphemeralId = node0.getEphemeralId();
        var node1 = DiscoveryNodeUtils.create("node1");
        var node1EphemeralId = node1.getEphemeralId();
        service = new MemoryMetricsService(fakeClock::get, clusterSettings, randomFrom(ProjectType.values()), MeterRegistry.NOOP);

        assertThat(service.mergeMemoryEstimation(), equalTo(0L));

        final var clusterState1 = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(node0).add(node1).localNodeId("node1").masterNodeId("node1"))
            .version(randomLongBetween(0, 1000))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState1, ClusterState.EMPTY_STATE));
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));

        var pub1 = new ShardMergeMemoryEstimatePublication(
            seqNoGenerator.getAndIncrement(),
            node0EphemeralId,
            new ShardMergeMemoryEstimate("merge1", 1024)
        );
        service.updateMergeMemoryEstimate(pub1);
        assertThat(service.mergeMemoryEstimation(), equalTo(1024L));

        var pub2 = new ShardMergeMemoryEstimatePublication(
            seqNoGenerator.getAndIncrement(),
            node1EphemeralId,
            new ShardMergeMemoryEstimate("merge2", 512)
        );
        service.updateMergeMemoryEstimate(pub2);
        assertThat(service.mergeMemoryEstimation(), equalTo(1024L));
        // out of order publication
        service.updateMergeMemoryEstimate(
            new ShardMergeMemoryEstimatePublication(
                0,
                randomBoolean() ? node0EphemeralId : node1EphemeralId,
                new ShardMergeMemoryEstimate("merge3", 2048)
            )
        );
        assertThat(service.mergeMemoryEstimation(), equalTo(1024L));

        fakeClock.addAndGet(validityNanos + randomLongBetween(0, 1000));
        assertThat(service.mergeMemoryEstimation(), equalTo(1024L));

        final var clusterState2 = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(node1).localNodeId("node1").masterNodeId("node1"))
            .version(clusterState1.version() + 1)
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState2, clusterState1));
        var randomTicks = randomLongBetween(0, validityNanos);
        fakeClock.addAndGet(randomTicks);
        assertThat(service.mergeMemoryEstimation(), equalTo(1024L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(2));
        var node0Entry = service.getMaxShardMergeMemoryEstimatePerNode().get(node0EphemeralId);
        assertNotNull(node0Entry);
        assertTrue(node0Entry.hasNodeLeft());
        assertThat(node0Entry.estimate().estimateInBytes(), equalTo(1024L));
        if (randomBoolean()) {
            // after the node leaves, a late publication with a higher merge arrives
            var pub3 = new ShardMergeMemoryEstimatePublication(
                seqNoGenerator.getAndIncrement(),
                node0EphemeralId,
                new ShardMergeMemoryEstimate("merge3", 2048)
            );
            service.updateMergeMemoryEstimate(pub3);
            // Estimate is update, but we still keep the timestamp for when the node left
            node0Entry = service.getMaxShardMergeMemoryEstimatePerNode().get(node0EphemeralId);
            assertNotNull(node0Entry);
            assertTrue(node0Entry.hasNodeLeft());
            assertThat(node0Entry.estimate().estimateInBytes(), equalTo(2048L));
            assertThat(service.mergeMemoryEstimation(), equalTo(2048L));
        }
        // once validity period for the departed node0 is finished, we clean it up.
        fakeClock.addAndGet(validityNanos - randomTicks + randomLongBetween(0, 1000));
        assertThat(service.mergeMemoryEstimation(), equalTo(512L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));

        service.updateMergeMemoryEstimate(
            new ShardMergeMemoryEstimatePublication(seqNoGenerator.getAndIncrement(), node1EphemeralId, ShardMergeMemoryEstimate.NO_MERGES)
        );
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        assertThat(
            service.getMaxShardMergeMemoryEstimatePerNode().get(node1EphemeralId).estimate(),
            equalTo(ShardMergeMemoryEstimate.NO_MERGES)
        );
    }

    public void testNodeBrieflyLeavingTheClusterAndRejoining() {
        var fakeClock = new AtomicLong();
        final var validityNanos = DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY.nanos();
        var seqNoGenerator = new AtomicLong();
        var node0 = DiscoveryNodeUtils.create("node0");
        var node0EphemeralId = node0.getEphemeralId();
        var node1 = DiscoveryNodeUtils.create("node1");
        service = new MemoryMetricsService(fakeClock::get, clusterSettings, randomFrom(ProjectType.values()), MeterRegistry.NOOP);
        final var clusterState1 = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(node0).add(node1).localNodeId("node1").masterNodeId("node1"))
            .version(randomLongBetween(0, 1000))
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState1, ClusterState.EMPTY_STATE));
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        if (randomBoolean()) {
            var pub0 = new ShardMergeMemoryEstimatePublication(
                seqNoGenerator.getAndIncrement(),
                node0EphemeralId,
                new ShardMergeMemoryEstimate("merge1", 512)
            );
            service.updateMergeMemoryEstimate(pub0);
        }
        // publish a new merge estimate and its matching finish but they arrive out of order
        var pub1 = new ShardMergeMemoryEstimatePublication(
            seqNoGenerator.getAndIncrement(),
            node0EphemeralId,
            new ShardMergeMemoryEstimate("merge2", 1024)
        );
        var pub2 = new ShardMergeMemoryEstimatePublication(
            seqNoGenerator.getAndIncrement(),
            node0EphemeralId,
            ShardMergeMemoryEstimate.NO_MERGES
        );
        service.updateMergeMemoryEstimate(pub2);
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        // node0 temporarily leaves and comes back before DEFAULT_REMOVED_NODE_MERGE_MEMORY_ESTIMATION_VALIDITY
        final var clusterState2 = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(node1).localNodeId("node1").masterNodeId("node1"))
            .version(clusterState1.version() + 1)
            .build();
        fakeClock.addAndGet(randomIntBetween(1, 1000));
        service.clusterChanged(new ClusterChangedEvent("test", clusterState2, clusterState1));
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        assertThat(
            service.getMaxShardMergeMemoryEstimatePerNode().get(node0EphemeralId).nodeLeftTimestampNanos(),
            equalTo(fakeClock.get())
        );
        // the queued event publication arrives
        service.updateMergeMemoryEstimate(pub1);
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        final var clusterState3 = ClusterState.builder(new ClusterName("test"))
            .nodes(DiscoveryNodes.builder().add(node0).add(node1).localNodeId("node1").masterNodeId("node1"))
            .version(clusterState1.version() + 1)
            .build();
        service.clusterChanged(new ClusterChangedEvent("test", clusterState3, clusterState2));
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        fakeClock.addAndGet(validityNanos);
        assertThat(service.mergeMemoryEstimation(), equalTo(0L));
        assertThat(service.getMaxShardMergeMemoryEstimatePerNode().size(), equalTo(1));
        assertThat(
            service.getMaxShardMergeMemoryEstimatePerNode().get(node0EphemeralId).estimate(),
            equalTo(ShardMergeMemoryEstimate.NO_MERGES)
        );
        assertFalse(service.getMaxShardMergeMemoryEstimatePerNode().get(node0EphemeralId).hasNodeLeft());
    }

    public void testEstimatedHeapMemoryCalculations() {
        ClusterState clusterState1 = randomInitialTwoNodeClusterState(4);
        var discoveryNodes = clusterState1.getNodes();
        var node0 = discoveryNodes.get("node_0");
        var node1 = discoveryNodes.get("node_1");
        service.clusterChanged(new ClusterChangedEvent("test", clusterState1, ClusterState.EMPTY_STATE));

        final long node0EstimateBeforeUpdate;
        final long node1EstimateBeforeUpdate;
        // Record the baseline heap usage for node 0 and 1, before any additional information is received
        {
            Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1.nodes());
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateBeforeUpdate = perNodeMemoryMetrics.get(node0.getId());
            node1EstimateBeforeUpdate = perNodeMemoryMetrics.get(node1.getId());
        }

        // We receive a shard mappings update from node 0
        final var node0MetricsUpdate = randomMemoryMetrics(node0, clusterState1);
        final var node0PostingsSize = node0MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(2, node0MetricsUpdate));

        // Node 0 heap estimate should have increased
        // Note that hollow shards can reduce the initial estimate, but we don't test this here
        long node0EstimateAfterUpdate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1.nodes());
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            node0EstimateAfterUpdate = perNodeMemoryMetrics.get(node0.getId());
            assertThat(node0EstimateAfterUpdate, greaterThan(node0EstimateBeforeUpdate));
            // PostingsMemorySize is the max across all nodes, so node1's estimate should have increased by that amount
            assertThat(perNodeMemoryMetrics.get(node1.getId()), equalTo(node1EstimateBeforeUpdate + node0PostingsSize));
        }

        // We receive a shard mappings update from node 1
        final var node1MetricsUpdate = randomMemoryMetrics(node1, clusterState1);
        final var node1PostingsSize = node1MetricsUpdate.values().stream().mapToLong(ShardMappingSize::postingsInMemoryBytes).sum();
        service.updateShardsMappingSize(new HeapMemoryUsage(1, node1MetricsUpdate));

        // Node 1 heap estimate should have increased
        final long node1EstimateAfterUpdate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1.nodes());
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            // PostingsMemorySize is the max across all nodes so that node0's estimate can increase if node1 has larger postings size
            if (node0PostingsSize < node1PostingsSize) {
                node0EstimateAfterUpdate += node1PostingsSize - node0PostingsSize;
            }
            assertThat(perNodeMemoryMetrics.get(node0.getId()), equalTo(node0EstimateAfterUpdate));
            node1EstimateAfterUpdate = perNodeMemoryMetrics.get(node1.getId());
            assertThat(node1EstimateAfterUpdate, greaterThan(node1EstimateBeforeUpdate));
        }

        // we receive a merge estimate from node 0
        final long node0MergeEstimate = randomLongBetween(10_000, 100_000);
        service.updateMergeMemoryEstimate(
            new ShardMergeMemoryEstimatePublication(
                randomLongBetween(100, 1000),
                node0.getEphemeralId(),
                new ShardMergeMemoryEstimate(randomIdentifier(), node0MergeEstimate)
            )
        );

        // All heap estimates should have increased
        final long node0EstimateAfterMergeEstimate, node1EstimateAfterMergeEstimate;
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1.nodes());
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));

            node0EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node0.getId());
            assertThat(node0EstimateAfterMergeEstimate - node0EstimateAfterUpdate, equalTo(node0MergeEstimate));

            node1EstimateAfterMergeEstimate = perNodeMemoryMetrics.get(node1.getId());
            assertThat(node1EstimateAfterMergeEstimate - node1EstimateAfterUpdate, equalTo(node0MergeEstimate));
        }

        // update indexing operations heap memory requirement
        final long indexingOperationsHeapMemoryRequirements = randomLongBetween(1_000, 100_000);
        service.updateIndexingOperationsHeapMemoryRequirements(indexingOperationsHeapMemoryRequirements);

        // All nodes' heap estimate should have increased
        {
            final Map<String, Long> perNodeMemoryMetrics = service.getPerNodeMemoryMetrics(clusterState1.nodes());
            assertThat(perNodeMemoryMetrics.size(), equalTo(2));
            assertThat(
                perNodeMemoryMetrics.get(node0.getId()) - node0EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
            assertThat(
                perNodeMemoryMetrics.get(node1.getId()) - node1EstimateAfterMergeEstimate,
                equalTo(indexingOperationsHeapMemoryRequirements)
            );
        }
    }

    private ClusterState randomInitialTwoNodeClusterState(int numberOfIndices) {
        DiscoveryNodes discoveryNodes = DiscoveryNodes.builder()
            .add(DiscoveryNodeUtils.create("node_0"))
            .add(DiscoveryNodeUtils.create("node_1"))
            .localNodeId("node_0")
            .masterNodeId("node_0")
            .build();
        String[] indices = IntStream.range(0, numberOfIndices).mapToObj(i -> randomIdentifier()).toArray(String[]::new);
        Tuple<ProjectMetadata.Builder, RoutingTable.Builder> projectAndRt = ClusterStateCreationUtils
            .projectWithAssignedPrimariesAndReplicas(ProjectId.DEFAULT, indices, 2, 0, discoveryNodes);
        return ClusterState.builder(new ClusterName("test"))
            .nodes(discoveryNodes)
            .routingTable(GlobalRoutingTable.builder().put(ProjectId.DEFAULT, projectAndRt.v2()).build())
            .metadata(Metadata.builder().put(projectAndRt.v1()))
            .build();
    }

    private Map<ShardId, ShardMappingSize> randomMemoryMetrics(DiscoveryNode node, ClusterState clusterState) {
        Map<ShardId, ShardMappingSize> result = new HashMap<>();
        Map<Index, Long> indexMappingSizes = new HashMap<>();
        clusterState.getRoutingNodes().node(node.getId()).forEach(r -> {
            long mappingSize = indexMappingSizes.computeIfAbsent(
                r.shardId().getIndex(),
                i -> ByteSizeValue.ofKb(randomLongBetween(1, 200)).getBytes()
            );
            result.put(
                r.shardId(),
                new ShardMappingSize(
                    mappingSize,
                    randomIntBetween(1, 1_000),
                    randomIntBetween(1, 100),
                    randomLongBetween(1, 100),
                    randomIntBetween(1, 100),
                    UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES,
                    node.getId()
                )
            );
        });
        return result;
    }

    private void clusterStateApplier(
        int iterations,
        AtomicReference<ClusterState> currentClusterState,
        CyclicBarrier barrier,
        int numberOfShards
    ) {
        ClusterState priorClusterState = ClusterState.EMPTY_STATE;
        long clusterStateVersion = randomLongBetween(1_000, 100_000);
        logger.info("--> Initial cluster state version: {}", clusterStateVersion);
        for (int i = 0; i < iterations; i++) {
            ClusterState clusterState = createClusterStateWithIndices(1, numberOfShards, clusterStateVersion++);
            currentClusterState.set(clusterState);
            safeAwait(barrier);
            service.clusterChanged(new ClusterChangedEvent("test", clusterState, priorClusterState));
            priorClusterState = clusterState;
        }
    }

    /**
     * Every time we submit metrics, we know we're submitting metrics from a cluster state version later than the last fully applied
     * version because of the barrier. For this reason, the metrics should never be ignored. Occasionally there will be a mismatch due
     * to a partially applied cluster state, in which case a retry should be request by throwing
     * {@link AutoscalingMissedIndicesUpdateException}.
     */
    private void shardMetricsUpdater(int iterations, AtomicReference<ClusterState> currentClusterState, CyclicBarrier barrier) {
        for (int i = 0; i < iterations; i++) {
            safeAwait(barrier);
            ClusterState clusterState = currentClusterState.get();
            Map<ShardId, ShardMappingSize> shardsToUpdate = clusterState.routingTable()
                .index("index0")
                .allShards()
                .filter(s -> randomBoolean())
                .map(IndexShardRoutingTable::primaryShard)
                .collect(Collectors.toMap(ShardRouting::shardId, sr -> randomShardMappingSize(sr.currentNodeId())));
            // Every update should be either applied or retried, we should never ignore
            try {
                long publicationSeqNo = randomLongBetween(100, Long.MAX_VALUE);
                service.updateShardsMappingSize(
                    new HeapMemoryUsage(
                        publicationSeqNo,  // it's a new node every time, this should always apply
                        shardsToUpdate,
                        clusterState.version()
                    )
                );
                // All submitted shards should have been updated if we get here
                String failureMessage = "Failed updating metrics for cluster state version " + clusterState.version();
                shardsToUpdate.keySet()
                    .forEach(
                        shardId -> assertEquals(failureMessage, publicationSeqNo, service.getShardMemoryMetrics().get(shardId).getSeqNo())
                    );
            } catch (AutoscalingMissedIndicesUpdateException e) {
                // This is fine, there was a conflict and we requested a retry
            }
        }
    }

    private void setupStateAndPublishFirstMetrics(TestStateConsumer testStateConsumer) {
        final long initialClusterStateVersion = randomLongBetween(1_000, 100_000);
        final ClusterState clusterStateWithIndices = createClusterStateWithIndices(1, 1, initialClusterStateVersion);
        final ShardRouting shardRouting = clusterStateWithIndices.routingTable().allShards().findFirst().orElseThrow();
        final String currentNode = shardRouting.currentNodeId();
        final ShardId shardId = shardRouting.shardId();

        service.clusterChanged(new ClusterChangedEvent("test", clusterStateWithIndices, ClusterState.EMPTY_STATE));
        int initialSeqNum = randomIntBetween(1, 1000);
        service.updateShardsMappingSize(new HeapMemoryUsage(initialSeqNum, Map.of(shardId, randomShardMappingSize(currentNode))));
        assertEquals(initialSeqNum, service.getShardMemoryMetrics().get(shardId).getSeqNo());

        testStateConsumer.accept(initialClusterStateVersion, shardId, currentNode, initialSeqNum);
    }

    private interface TestStateConsumer {
        void accept(long currentClusterStateVersion, ShardId shardId, String currentNode, long lastSequenceNumber);
    }

    private ShardMappingSize randomShardMappingSize(String nodeId) {
        return new ShardMappingSize(
            randomNonNegativeLong(),
            randomNonNegativeInt(),
            randomNonNegativeInt(),
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomShardMemoryOverheadBytes(),
            nodeId
        );
    }

    private ClusterState createClusterStateWithIndices(int numberOfShards, int numberOfReplicas) {
        return createClusterStateWithIndices(numberOfShards, numberOfReplicas, randomIntBetween(1, 1000));
    }

    /** Creates a cluster state for a one node cluster, having the given number of indices in its metadata. */
    private ClusterState createClusterStateWithIndices(int numberOfIndices, int numberOfShards, long version) {
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
            .version(version)
            .build();
    }

    private long randomShardMemoryOverheadBytes() {
        return randomShardMemoryOverheadBytes(1, Long.MAX_VALUE);
    }

    private long randomShardMemoryOverheadBytes(long min, long max) {
        if (randomBoolean()) {
            return UNDEFINED_SHARD_MEMORY_OVERHEAD_BYTES;
        } else {
            return randomLongBetween(min, max);
        }
    }
}
