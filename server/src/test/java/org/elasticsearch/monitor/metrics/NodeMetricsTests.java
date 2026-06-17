/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.monitor.metrics;

import org.elasticsearch.action.admin.cluster.node.stats.NodeStats;
import org.elasticsearch.action.admin.indices.stats.CommonStatsFlags;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.common.network.HandlingTimeTracker;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.node.NodeService;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportActionStats;
import org.elasticsearch.transport.TransportStats;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NodeMetricsTests extends ESTestCase {

    private static final List<String> TRANSPORT_DATA_COUNTER_NAMES = List.of(
        "es.transport.data_read.rx.size",
        "es.transport.data_read.tx.size",
        "es.transport.data_write.rx.size",
        "es.transport.data_write.tx.size"
    );

    private RecordingMeterRegistry registry;
    private NodeMetrics nodeMetrics;

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        nodeMetrics.close();
    }

    /**
     * Verifies that transport byte metrics for data read and write operations are correctly categorized
     * by operation, aggregated across action name variants (e.g. search phase sub-actions), and that
     * non-data actions (cluster:admin, indices:admin) are excluded.
     */
    public void testTransportDataActionMetrics() {
        // TransportActionStats histogram length: 28 buckets + 1 overflow bucket
        long[] emptyHistogram = new long[29];

        Map<String, TransportActionStats> actionStats = Map.of(
            // Two search phase variants: bracket suffixes stripped, both collapse into "search"
            "indices:data/read/search[phase/query]",
            new TransportActionStats(10, 1000, emptyHistogram, 10, 2000, emptyHistogram),
            "indices:data/read/search[phase/fetch/id]",
            new TransportActionStats(5, 500, emptyHistogram, 5, 800, emptyHistogram),
            // Sub-path action: only the first segment "esql" is used as the operation
            "indices:data/read/esql/resolve_fields",
            new TransportActionStats(3, 300, emptyHistogram, 3, 600, emptyHistogram),
            // Write actions, each producing its own operation bucket
            "indices:data/write/bulk",
            new TransportActionStats(20, 5000, emptyHistogram, 20, 100, emptyHistogram),
            "indices:data/write/bulk_shard_operations",
            new TransportActionStats(15, 4000, emptyHistogram, 15, 80, emptyHistogram),
            // Non-data actions that must not appear in any of the four metrics
            "cluster:admin/settings/update",
            new TransportActionStats(1, 100, emptyHistogram, 1, 50, emptyHistogram),
            "indices:admin/refresh",
            new TransportActionStats(2, 200, emptyHistogram, 2, 100, emptyHistogram)
        );

        TransportStats transportStats = new TransportStats(
            0,
            0,
            0,
            0,
            0,
            0,
            new long[HandlingTimeTracker.BUCKET_COUNT],
            new long[HandlingTimeTracker.BUCKET_COUNT],
            actionStats
        );

        withTransportDataCounters(transportStats);

        Map<String, Long> readRx = measurementsByOperation(registry, "es.transport.data_read.rx.size");
        // search: 1000 (query phase) + 500 (fetch phase) aggregated
        assertEquals(1500L, (long) readRx.get("search"));
        assertEquals(300L, (long) readRx.get("esql"));
        assertNull("cluster:admin actions must be excluded", readRx.get("settings"));
        assertNull("indices:admin actions must be excluded", readRx.get("refresh"));

        Map<String, Long> readTx = measurementsByOperation(registry, "es.transport.data_read.tx.size");
        // search: 2000 (query phase) + 800 (fetch phase) aggregated
        assertEquals(2800L, (long) readTx.get("search"));
        assertEquals(600L, (long) readTx.get("esql"));

        Map<String, Long> writeRx = measurementsByOperation(registry, "es.transport.data_write.rx.size");
        assertEquals(5000L, (long) writeRx.get("bulk"));
        assertEquals(4000L, (long) writeRx.get("bulk_shard_operations"));

        Map<String, Long> writeTx = measurementsByOperation(registry, "es.transport.data_write.tx.size");
        assertEquals(100L, (long) writeTx.get("bulk"));
        assertEquals(80L, (long) writeTx.get("bulk_shard_operations"));
    }

    /**
     * Verifies that all four transport byte counters emit no measurements when NodeStats returns null
     * transport stats, rather than throwing.
     */
    public void testTransportDataActionMetricsWithNullTransport() {
        withTransportDataCounters(null);

        for (String name : TRANSPORT_DATA_COUNTER_NAMES) {
            assertTrue(
                "expected no measurements for " + name + " when transport stats are null",
                registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_COUNTER, name).isEmpty()
            );
        }
    }

    /**
     * Initializes {@link #registry} and {@link #nodeMetrics} backed by the given TransportStats and
     * invokes the four transport data byte counters. Accepts null TransportStats to test the no-transport case.
     *
     * <p>NodeService requires a fully running Elasticsearch node to instantiate; a mock is used here.
     * Only the four transport counters are invoked: collecting all registered metrics would trigger
     * unrelated callbacks that NPE when NodeStats.indices is null (we only populate transport).
     * RecordingAsyncLongCounter extends CallbackRecordingInstrument which implements Runnable;
     * the cast triggers the observer callback and records measurements.
     */
    private void withTransportDataCounters(TransportStats transportStats) {
        NodeStats nodeStats = new NodeStats(
            DiscoveryNodeUtils.create("test-node"),
            0L,
            null,
            null,
            null,
            null,
            null,
            null,
            transportStats,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );

        NodeService nodeService = mock(NodeService.class);
        when(
            nodeService.stats(
                any(CommonStatsFlags.class),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean(),
                anyBoolean()
            )
        ).thenReturn(nodeStats);

        registry = new RecordingMeterRegistry();
        nodeMetrics = new NodeMetrics(registry, nodeService, TimeValue.timeValueSeconds(10));
        nodeMetrics.start();
        for (String name : TRANSPORT_DATA_COUNTER_NAMES) {
            ((Runnable) registry.getLongAsyncCounter(name)).run();
        }
    }

    private static Map<String, Long> measurementsByOperation(RecordingMeterRegistry registry, String metricName) {
        List<Measurement> measurements = registry.getRecorder().getMeasurements(InstrumentType.LONG_ASYNC_COUNTER, metricName);
        return measurements.stream().collect(Collectors.toMap(m -> (String) m.attributes().get("operation"), Measurement::getLong));
    }
}
