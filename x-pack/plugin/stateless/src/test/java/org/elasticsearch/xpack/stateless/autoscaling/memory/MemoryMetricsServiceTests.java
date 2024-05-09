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

import co.elastic.elasticsearch.stateless.autoscaling.MetricQuality;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Strings;
import org.elasticsearch.index.Index;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
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
        service = new MemoryMetricsService(System::nanoTime, CLUSTER_SETTINGS);
    }

    public void testReduceFinalIndexMappingSize() {
        // get access to internals
        Map<Index, MemoryMetricsService.IndexMemoryMetrics> map = service.getIndicesMemoryMetrics();
        long expectedSizeInBytes = 0;
        int numberOfIndices = randomIntBetween(10, 1000);
        for (int nameSuffix = 1; nameSuffix <= numberOfIndices; nameSuffix++) {
            expectedSizeInBytes += nameSuffix;
            map.put(
                new Index("name-" + nameSuffix, "uuid-" + nameSuffix),
                new MemoryMetricsService.IndexMemoryMetrics(nameSuffix, MetricQuality.EXACT, System::nanoTime)
            );
        }

        MemoryMetricsService.IndexMemoryMetrics result;
        result = service.getTotalIndicesMappingSize();
        assertThat(result.getSizeInBytes(), equalTo(expectedSizeInBytes));
        assertThat(result.getMetricQuality(), equalTo(MetricQuality.EXACT));

        // simulate MINIMUM `quality` attribute on a random metric
        int nameSuffix = randomIntBetween(1, numberOfIndices);
        map.put(
            new Index("name-" + nameSuffix, "uuid-" + nameSuffix),
            new MemoryMetricsService.IndexMemoryMetrics(nameSuffix, MetricQuality.MINIMUM, System::nanoTime)
        );
        result = service.getTotalIndicesMappingSize();
        assertThat(result.getSizeInBytes(), equalTo(expectedSizeInBytes));
        // verify that the whole batch has MISSING `quality` attribute
        assertThat(result.getMetricQuality(), equalTo(MetricQuality.MINIMUM));
    }

    public void testConcurrentUpdateMetricHigherSeqNoWins() throws InterruptedException {

        int numberOfConcurrentUpdates = 10000;
        final CountDownLatch latch = new CountDownLatch(numberOfConcurrentUpdates);

        // init value
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", 0));

        // simulate concurrent updates
        for (int i = 0; i < numberOfConcurrentUpdates; i++) {
            long size, seqNo;
            if (randomBoolean()) {
                size = 100;
                seqNo = 10;
            } else {
                size = 200;
                seqNo = 1;
            }
            executorService.execute(() -> {
                HeapMemoryUsage metric = new HeapMemoryUsage(seqNo, Map.of(INDEX, new IndexMappingSize(size, "node-0")));
                service.updateIndicesMappingSize(metric);
                latch.countDown();
            });
        }

        safeAwait(latch);

        assertThat(100L, equalTo(service.getTotalIndicesMappingSize().getSizeInBytes()));
    }

    public void testReportNonExactMetricsInTotalIndicesMappingSize() throws Exception {
        long currentTime = System.nanoTime();
        MemoryMetricsService customService = new MemoryMetricsService(() -> currentTime, CLUSTER_SETTINGS);

        long updateTime = currentTime - TimeUnit.MINUTES.toNanos(5) - TimeUnit.SECONDS.toNanos(1);
        for (int i = 0; i < 100; i++) {
            customService.getIndicesMemoryMetrics()
                .put(
                    new Index(randomIdentifier(), randomUUID()),
                    new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", updateTime)
                );
        }

        int count = randomIntBetween(10, 100);
        List<Index> indicesToSkip = randomSubsetOf(count, customService.getIndicesMemoryMetrics().keySet());
        for (var entry : customService.getIndicesMemoryMetrics().entrySet()) {
            if (indicesToSkip.contains(entry.getKey()) == false) {
                entry.getValue().update(randomNonNegativeInt(), randomNonNegativeInt(), "node-0", 0);
            }
        }

        var mockLogAppender = new MockLogAppender();
        try (var ignored = mockLogAppender.capturing(MemoryMetricsService.class)) {
            for (Index index : indicesToSkip) {
                mockLogAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "expected warn log about state index",
                        MemoryMetricsService.class.getName(),
                        Level.WARN,
                        Strings.format(
                            "Memory metrics are stale for index %s=IndexMemoryMetrics{sizeInBytes=0, seqNo=0, metricQuality=MISSING, "
                                + "metricShardNodeId='node-0', updateTimestampNanos='%d'}",
                            index,
                            updateTime
                        )
                    )
                );
            }

            customService.getTotalIndicesMappingSize();
            mockLogAppender.assertAllExpectationsMatched();

            // Second call doesn't result in duplicate logs
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            customService.getTotalIndicesMappingSize();
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testNoStaleMetricsInTotalIndicesMappingSize() throws Exception {
        for (int i = 0; i < randomIntBetween(10, 20); i++) {
            service.getIndicesMemoryMetrics()
                .put(
                    new Index(randomIdentifier(), randomUUID()),
                    new MemoryMetricsService.IndexMemoryMetrics(
                        randomNonNegativeInt(),
                        randomNonNegativeInt(),
                        MetricQuality.EXACT,
                        "node-0",
                        System.nanoTime()
                    )
                );
        }

        var mockLogAppender = new MockLogAppender();
        try (var ignored = mockLogAppender.capturing(MemoryMetricsService.class)) {
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            service.getTotalIndicesMappingSize();
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testDoNotReportNonExactMetricsAsStaleImmediatelyOnStartup() {
        for (int i = 0; i < randomIntBetween(5, 10); i++) {
            service.getIndicesMemoryMetrics()
                .put(
                    new Index(randomIdentifier(), randomUUID()),
                    new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", System.nanoTime())
                );
        }

        var mockLogAppender = new MockLogAppender();
        try (var ignored = mockLogAppender.capturing(MemoryMetricsService.class)) {
            mockLogAppender.addExpectation(
                new MockLogAppender.UnseenEventExpectation("no warnings", MemoryMetricsService.class.getName(), Level.WARN, "*")
            );
            service.getTotalIndicesMappingSize();
            mockLogAppender.assertAllExpectationsMatched();
        }
    }

    public void testDoNotThrowMissedIndicesUpdateExceptionOnOutOfOrderMessages() {
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", 0));

        int amount = randomIntBetween(50, 100);
        List<Integer> seqIds = IntStream.range(1, amount + 1).mapToObj(Integer::valueOf).collect(Collectors.toList());
        Collections.shuffle(seqIds, random());
        for (int i = 0; i < amount; i++) {
            service.updateIndicesMappingSize(
                new HeapMemoryUsage(seqIds.get(i), Map.of(INDEX, new IndexMappingSize(randomIntBetween(0, 100), "node-0")))
            );
        }

        assertThat(service.getIndicesMemoryMetrics().get(INDEX).getMetricQuality(), equalTo(MetricQuality.EXACT));
    }

    public void testThrowMissedIndicesUpdateExceptionOnMissedIndex() {
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", 0));

        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateIndicesMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(
                        randomValueOtherThan(INDEX, () -> new Index(randomIdentifier(), randomUUID())),
                        new IndexMappingSize(randomIntBetween(0, 100), "node-0")
                    )
                )
            );
        });
    }

    public void testThrowMissedIndicesUpdateExceptionOnMissedNode() {
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0", 0));

        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateIndicesMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(INDEX, new IndexMappingSize(randomIntBetween(0, 100), randomIdentifier()))
                )
            );
        });
    }

    public void testSpecificValues() {
        boolean small = randomBoolean();
        long size = small ? between(1, 1000) : randomLongBetween(ByteSizeUnit.GB.toBytes(1), ByteSizeUnit.GB.toBytes(2));

        service.getIndicesMemoryMetrics()
            .put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(size, 0, MetricQuality.EXACT, "node-0", 0));

        MemoryMetrics memoryMetrics = service.getMemoryMetrics();
        assertThat(
            memoryMetrics.nodeMemoryInBytes(),
            equalTo((MemoryMetricsService.INDEX_MEMORY_OVERHEAD + MemoryMetricsService.WORKLOAD_MEMORY_OVERHEAD) * 2)
        );
        assertThat(
            memoryMetrics.totalMemoryInBytes(),
            equalTo(small ? HeapToSystemMemory.dataNode(1) : (size + service.shardMemoryOverhead.getBytes()) * 2)
        );

        // a relatively high starting point, coming from 500MB heap work * 2 (for memory)
        assertThat(memoryMetrics.nodeMemoryInBytes(), lessThan(ByteSizeUnit.MB.toBytes(1200)));
    }

    public void testManyShards() {
        for (int i = 0; i < 300; ++i) {
            service.getIndicesMemoryMetrics()
                .put(
                    new Index(randomAlphaOfLength(10), randomUUID()),
                    new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.EXACT, "node-0", 0)
                );
        }

        MemoryMetrics memoryMetrics = service.getMemoryMetrics();
        assertThat(memoryMetrics.totalMemoryInBytes(), greaterThan(ByteSizeUnit.GB.toBytes(2)));
        assertThat(memoryMetrics.totalMemoryInBytes(), lessThan(ByteSizeUnit.GB.toBytes(4)));
        // * 2 to go from heap to system memory
        assertThat(memoryMetrics.totalMemoryInBytes(), equalTo(300 * service.shardMemoryOverhead.getBytes() * 2));

        // show that one 4GB node is not enough, but 1.5 node would be.
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, greaterThan(ByteSizeUnit.GB.toBytes(4)));
        assertThat(memoryMetrics.totalMemoryInBytes() + memoryMetrics.nodeMemoryInBytes() * 2, lessThan(ByteSizeUnit.GB.toBytes(6)));
    }
}
