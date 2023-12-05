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

import org.elasticsearch.index.Index;
import org.elasticsearch.indices.AutoscalingMissedIndicesUpdateException;
import org.elasticsearch.test.ESTestCase;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.equalTo;

public class MemoryMetricsServiceTests extends ESTestCase {

    private static final Index INDEX = new Index("test-index-001", "e0adaff5-8ac4-4bb8-a8d1-adfde1a064cc");

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
        service = new MemoryMetricsService();
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
                new MemoryMetricsService.IndexMemoryMetrics(nameSuffix, MetricQuality.EXACT)
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
            new MemoryMetricsService.IndexMemoryMetrics(nameSuffix, MetricQuality.MINIMUM)
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
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0"));

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

    public void testDoNotThrowMissedIndicesUpdateExceptionOnOutOfOrderMessages() {
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0"));

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
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0"));

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
        service.getIndicesMemoryMetrics().put(INDEX, new MemoryMetricsService.IndexMemoryMetrics(0, 0, MetricQuality.MISSING, "node-0"));

        expectThrows(AutoscalingMissedIndicesUpdateException.class, () -> {
            service.updateIndicesMappingSize(
                new HeapMemoryUsage(
                    randomIntBetween(0, 100),
                    Map.of(INDEX, new IndexMappingSize(randomIntBetween(0, 100), randomIdentifier()))
                )
            );
        });
    }
}
