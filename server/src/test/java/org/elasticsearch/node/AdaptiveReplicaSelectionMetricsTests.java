/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.node.AdaptiveReplicaSelectionMetrics.AVG_QUEUE_SIZE_METRIC_NAME;
import static org.elasticsearch.node.AdaptiveReplicaSelectionMetrics.AVG_RESPONSE_TIME_NS_METRIC_NAME;
import static org.elasticsearch.node.AdaptiveReplicaSelectionMetrics.AVG_SERVICE_TIME_NS_METRIC_NAME;
import static org.elasticsearch.node.AdaptiveReplicaSelectionMetrics.NODE_ID_ATTRIBUTE;

public class AdaptiveReplicaSelectionMetricsTests extends ESTestCase {

    private TestThreadPool threadPool;
    private ResponseCollectorService collector;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool("adaptive_replica_selection_metrics_tests");
        ClusterService clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadPool,
            null
        );
        collector = new ResponseCollectorService(clusterService);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public void testNoMetricsWhenNoStatsCollected() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new AdaptiveReplicaSelectionMetrics(registry, collector);
        registry.getRecorder().collect();

        assertEmpty(registry, AVG_QUEUE_SIZE_METRIC_NAME);
        assertEmpty(registry, AVG_SERVICE_TIME_NS_METRIC_NAME);
        assertEmpty(registry, AVG_RESPONSE_TIME_NS_METRIC_NAME);
    }

    public void testSingleNodeMetrics() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new AdaptiveReplicaSelectionMetrics(registry, collector);

        collector.addNodeStatistics("node1", 3, 5_000_000L, 1_000_000L);
        registry.getRecorder().collect();

        assertGaugeValues(registry, AVG_QUEUE_SIZE_METRIC_NAME, Map.of("node1", List.of(3L)));
        assertGaugeValues(registry, AVG_SERVICE_TIME_NS_METRIC_NAME, Map.of("node1", List.of(1_000_000L)));
        assertGaugeValues(registry, AVG_RESPONSE_TIME_NS_METRIC_NAME, Map.of("node1", List.of(5_000_000L)));
    }

    public void testMultipleNodesMetrics() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new AdaptiveReplicaSelectionMetrics(registry, collector);

        collector.addNodeStatistics("node1", 2, 4_000_000L, 800_000L);
        collector.addNodeStatistics("node2", 8, 12_000_000L, 3_000_000L);
        registry.getRecorder().collect();

        // Each node appears once with its EWMA values
        List<Measurement> queueMeasurements = getMeasurements(registry, AVG_QUEUE_SIZE_METRIC_NAME);
        assertEquals(2, queueMeasurements.size());

        Map<String, List<Long>> queueByNode = groupByNodeId(queueMeasurements);
        assertEquals(List.of(2L), queueByNode.get("node1"));
        assertEquals(List.of(8L), queueByNode.get("node2"));

        Map<String, List<Long>> serviceByNode = groupByNodeId(getMeasurements(registry, AVG_SERVICE_TIME_NS_METRIC_NAME));
        assertEquals(List.of(800_000L), serviceByNode.get("node1"));
        assertEquals(List.of(3_000_000L), serviceByNode.get("node2"));
    }

    public void testMetricsReflectUpdatedEwmaAfterMultipleObservations() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new AdaptiveReplicaSelectionMetrics(registry, collector);

        collector.addNodeStatistics("node1", 10, 10_000_000L, 2_000_000L);
        collector.addNodeStatistics("node1", 0, 2_000_000L, 500_000L);

        registry.getRecorder().collect();

        // The EWMA smooths toward the second observation; queue size should be between 0 and 10
        List<Measurement> measurements = getMeasurements(registry, AVG_QUEUE_SIZE_METRIC_NAME);
        assertEquals(1, measurements.size());
        long queueSize = measurements.get(0).getLong();
        assertTrue("EWMA queue size should be between 0 and 10, was: " + queueSize, queueSize >= 0 && queueSize < 10);
    }

    public void testMetricsRemovedWhenNodeLeaves() {
        RecordingMeterRegistry registry = new RecordingMeterRegistry();
        new AdaptiveReplicaSelectionMetrics(registry, collector);

        collector.addNodeStatistics("node1", 5, 5_000_000L, 1_000_000L);
        collector.removeNode("node1");

        registry.getRecorder().collect();

        assertEmpty(registry, AVG_QUEUE_SIZE_METRIC_NAME);
        assertEmpty(registry, AVG_SERVICE_TIME_NS_METRIC_NAME);
        assertEmpty(registry, AVG_RESPONSE_TIME_NS_METRIC_NAME);
    }

    private static List<Measurement> getMeasurements(RecordingMeterRegistry registry, String metricName) {
        return registry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, metricName);
    }

    private static Map<String, List<Long>> groupByNodeId(List<Measurement> measurements) {
        return Measurement.groupMeasurementsByAttribute(measurements, attrs -> (String) attrs.get(NODE_ID_ATTRIBUTE), Measurement::getLong);
    }

    private static void assertEmpty(RecordingMeterRegistry registry, String metricName) {
        assertEquals(List.of(), getMeasurements(registry, metricName));
    }

    private static void assertGaugeValues(RecordingMeterRegistry registry, String metricName, Map<String, List<Long>> expected) {
        assertEquals(expected, groupByNodeId(getMeasurements(registry, metricName)));
    }
}
