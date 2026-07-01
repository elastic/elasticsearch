/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.node;

import org.apache.logging.log4j.Level;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.OperationRouting;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;

public class ResponseCollectorServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ResponseCollectorService collector;
    private ThreadPool threadpool;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("response_collector_tests");
        clusterService = new ClusterService(
            Settings.EMPTY,
            new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool,
            null
        );
        collector = new ResponseCollectorService(clusterService, MeterRegistry.NOOP);
    }

    @After
    public void tearDown() throws Exception {
        super.tearDown();
        threadpool.shutdownNow();
    }

    public void testNodeStats() throws Exception {
        collector.addNodeStatistics("node1", 1, 100, 10);
        Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertThat(nodeStats.get("node1").queueSize, equalTo(1));
        assertThat(nodeStats.get("node1").responseTime, equalTo(100.0));
        assertThat(nodeStats.get("node1").serviceTime, equalTo(10.0));
    }

    /*
     * Test that concurrently adding values and removing nodes does not cause exceptions
     */
    public void testConcurrentAddingAndRemoving() throws Exception {
        String[] nodes = new String[] { "a", "b", "c", "d" };

        final CountDownLatch latch = new CountDownLatch(5);

        Runnable f = () -> {
            latch.countDown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                fail("should not be interrupted");
            }
            for (int i = 0; i < randomIntBetween(100, 200); i++) {
                if (randomBoolean()) {
                    collector.removeNode(randomFrom(nodes));
                }
                collector.addNodeStatistics(
                    randomFrom(nodes),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100),
                    randomIntBetween(1, 100)
                );
            }
        };

        Thread t1 = new Thread(f);
        Thread t2 = new Thread(f);
        Thread t3 = new Thread(f);
        Thread t4 = new Thread(f);

        t1.start();
        t2.start();
        t3.start();
        t4.start();
        latch.countDown();
        t1.join();
        t2.join();
        t3.join();
        t4.join();

        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        logger.info("--> got stats: {}", nodeStats);
        for (String nodeId : nodes) {
            if (nodeStats.containsKey(nodeId)) {
                assertThat(nodeStats.get(nodeId).queueSize, greaterThan(0));
                assertThat(nodeStats.get(nodeId).responseTime, greaterThan(0.0));
                assertThat(nodeStats.get(nodeId).serviceTime, greaterThan(0.0));
            }
        }
    }

    public void testNodeRemoval() throws Exception {
        collector.addNodeStatistics("node1", randomIntBetween(1, 100), randomIntBetween(1, 100), randomIntBetween(1, 100));
        collector.addNodeStatistics("node2", randomIntBetween(1, 100), randomIntBetween(1, 100), randomIntBetween(1, 100));

        ClusterState previousState = ClusterState.builder(new ClusterName("cluster"))
            .nodes(
                DiscoveryNodes.builder()
                    .add(DiscoveryNodeUtils.create("node1", new TransportAddress(TransportAddress.META_ADDRESS, 9200)))
                    .add(DiscoveryNodeUtils.create("node2", new TransportAddress(TransportAddress.META_ADDRESS, 9201)))
            )
            .build();
        ClusterState newState = ClusterState.builder(previousState)
            .nodes(DiscoveryNodes.builder(previousState.nodes()).remove("node2"))
            .build();
        ClusterChangedEvent event = new ClusterChangedEvent("test", newState, previousState);

        collector.clusterChanged(event);
        final Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = collector.getAllNodeStatistics();
        assertTrue(nodeStats.containsKey("node1"));
        assertFalse(nodeStats.containsKey("node2"));
    }

    /**
     * Verifies that the {@code es.ars.nodes.probing.current} gauge reports the number of
     * ARS-candidate nodes that have no statistics yet, and drops to zero once the first
     * observation is recorded.
     */
    public void testProbingGaugeCountsStatelessNodes() {
        // ClusterServiceUtils creates a started ClusterService with a single local node
        // that has all roles, making it an ARS candidate. Before any observations have been
        // gathered this coordinator has no statistics for that node (probing state).
        try (ClusterService startedClusterService = ClusterServiceUtils.createClusterService(threadpool)) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            ResponseCollectorService svc = new ResponseCollectorService(startedClusterService, meterRegistry);
            String localNodeId = startedClusterService.localNode().getId();

            // Before any stats: the single ARS-candidate node has no data → probing gauge = 1
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.probing.current"),
                RecordingMeterRegistry.measures(1L)
            );

            // After the first observation: the node now has stats → no longer probing → gauge = 0
            meterRegistry.getRecorder().resetCalls();
            svc.addNodeStatistics(localNodeId, 1, 100, 10);
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.probing.current"),
                RecordingMeterRegistry.measures(0L)
            );
        }
    }

    /**
     * Verifies that the {@code es.ars.nodes.warming.current} gauge correctly tracks the warming
     * lifecycle: it rises to 1 on the first observation and falls back to 0 when the node
     * accumulates {@code warmupSamples} observations and graduates to warm.
     */
    public void testWarmingGaugeTracksObservationThreshold() {
        // Use warmupSamples=3 to keep the test brief while still exercising the full
        // probing → warming → warm lifecycle. A started ClusterService is required so that
        // the probing gauge callback (fired alongside the warming one on every collect) can
        // read a valid cluster state without hitting the "initial cluster state not set yet"
        // assertion inside ClusterApplierService.
        Settings warmupSettings = Settings.builder()
            .put(OperationRouting.ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING.getKey(), 3)
            .build();
        try (ClusterService warmupClusterService = ClusterServiceUtils.createClusterService(threadpool, warmupSettings)) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            ResponseCollectorService svc = new ResponseCollectorService(warmupClusterService, meterRegistry);

            // No stats yet: no nodes in warming state
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.warming.current"),
                RecordingMeterRegistry.measures(0L)
            );

            // First observation (observationCount = 1 < 3): node enters warming
            meterRegistry.getRecorder().resetCalls();
            svc.addNodeStatistics("node1", 1, 100, 10);
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.warming.current"),
                RecordingMeterRegistry.measures(1L)
            );

            // Second observation (observationCount = 2 < 3): still warming
            meterRegistry.getRecorder().resetCalls();
            svc.addNodeStatistics("node1", 1, 100, 10);
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.warming.current"),
                RecordingMeterRegistry.measures(1L)
            );

            // Third observation (observationCount = 3 = warmupSamples): node graduates → warming gauge drops to 0
            meterRegistry.getRecorder().resetCalls();
            svc.addNodeStatistics("node1", 1, 100, 10);
            meterRegistry.getRecorder().collect();
            assertThat(
                meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, "es.ars.nodes.warming.current"),
                RecordingMeterRegistry.measures(0L)
            );
        }
    }

    /**
     * Verifies that INFO messages are emitted at the correct state transitions: when a node
     * receives its first ARS observation (enters warming) and when it accumulates enough
     * observations to graduate to warm.
     */
    public void testNodeTransitionLogs() {
        // Use warmupSamples=3 to keep the test brief
        Settings warmupSettings = Settings.builder()
            .put(OperationRouting.ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING.getKey(), 3)
            .build();
        ClusterService warmupClusterService = new ClusterService(
            warmupSettings,
            new ClusterSettings(warmupSettings, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS),
            threadpool,
            null
        );
        ResponseCollectorService svc = new ResponseCollectorService(warmupClusterService, MeterRegistry.NOOP);

        try (MockLog mockLog = MockLog.capture(ResponseCollectorService.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "warming",
                    ResponseCollectorService.class.getName(),
                    Level.INFO,
                    "Node [node1] entered ARS warming state (first observation recorded)"
                )
            );
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "graduation",
                    ResponseCollectorService.class.getName(),
                    Level.INFO,
                    "Node [node1] graduated from ARS warming to warm (observations=3)"
                )
            );

            svc.addNodeStatistics("node1", 1, 100, 10);  // observationCount=1 → warming log
            svc.addNodeStatistics("node1", 1, 100, 10);  // observationCount=2 → no log
            svc.addNodeStatistics("node1", 1, 100, 10);  // observationCount=3 → graduation log

            mockLog.assertAllExpectationsMatched();
        }
    }

    /**
     * Verifies that {@code es.ars.nodes.probing.time} records a non-negative millisecond
     * value when the first ARS observation is received for a node whose join event was seen
     * via {@link ResponseCollectorService#clusterChanged}.
     */
    public void testProbingDurationHistogram() {
        try (ClusterService startedClusterService = ClusterServiceUtils.createClusterService(threadpool)) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            ResponseCollectorService svc = new ResponseCollectorService(startedClusterService, meterRegistry);

            // Simulate a new data node joining; clusterChanged populates nodeJoinTimeNanos
            var newNode = DiscoveryNodeUtils.create("newNode");
            ClusterState prev = startedClusterService.state();
            ClusterState next = ClusterState.builder(prev).nodes(DiscoveryNodes.builder(prev.nodes()).add(newNode)).build();
            svc.clusterChanged(new ClusterChangedEvent("test", next, prev));

            // First observation for that node ends the probing phase → histogram records
            svc.addNodeStatistics("newNode", 1, 100, 10);

            var measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.ars.nodes.probing.time");
            assertThat(measurements, hasSize(1));
            assertThat(measurements.getFirst().getLong(), greaterThanOrEqualTo(0L));
        }
    }

    /**
     * Verifies that {@code es.ars.nodes.warming.time} records a non-negative millisecond
     * value when a node accumulates {@code warmupSamples} observations and graduates from
     * warming to warm.
     */
    public void testWarmingDurationHistogram() {
        Settings warmupSettings = Settings.builder()
            .put(OperationRouting.ADAPTIVE_REPLICA_SELECTION_WARMUP_SAMPLES_SETTING.getKey(), 3)
            .build();
        try (ClusterService startedClusterService = ClusterServiceUtils.createClusterService(threadpool, warmupSettings)) {
            RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
            ResponseCollectorService svc = new ResponseCollectorService(startedClusterService, meterRegistry);

            // Simulate a new data node joining
            var newNode = DiscoveryNodeUtils.create("newNode");
            ClusterState prev = startedClusterService.state();
            ClusterState next = ClusterState.builder(prev).nodes(DiscoveryNodes.builder(prev.nodes()).add(newNode)).build();
            svc.clusterChanged(new ClusterChangedEvent("test", next, prev));

            // count=1: probing ends, warming starts; count=2: still warming; count=3: graduation
            svc.addNodeStatistics("newNode", 1, 100, 10);
            svc.addNodeStatistics("newNode", 1, 100, 10);
            svc.addNodeStatistics("newNode", 1, 100, 10);

            var measurements = meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_HISTOGRAM, "es.ars.nodes.warming.time");
            assertThat(measurements, hasSize(1));
            assertThat(measurements.getFirst().getLong(), greaterThanOrEqualTo(0L));
        }
    }

    public void testArsFormulaAdjustmentFeatureFlag() {
        // 100ms response time, 10ms service time
        collector.addNodeStatistics("node1", 1, 100 * 1_000_000L, 10 * 1_000_000L);
        double rank = collector.getAllNodeStatistics().get("node1").rank(1);

        if (ResponseCollectorService.ARS_FORMULA_ADJUSTMENT_FEATURE_FLAG.isEnabled()) {
            // With the adjustment enabled, the response time component (rS - muBarSInverse) is dropped,
            // so rank should equal just the queue-based term: qHatS^3 * muBarSInverse
            // qHatS = 1 + 1*1 + 1 = 3, muBarSInverse = 10ms, so rank = 27 * 10 = 270
            assertThat(rank, equalTo(270.0));
        } else {
            // Without the adjustment, rank = (rS - muBarSInverse) + qHatS^3 * muBarSInverse
            // = (100 - 10) + 270 = 360
            assertThat(rank, equalTo(360.0));
        }
    }
}
