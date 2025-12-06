/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation.allocator;

import org.elasticsearch.cluster.routing.allocation.allocator.DesiredBalanceMetrics.AllocationStats;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;

public class DesiredBalanceMetricsTests extends ESTestCase {

    public void testZeroAllMetrics() {
        DesiredBalanceMetrics metrics = new DesiredBalanceMetrics(MeterRegistry.NOOP);
        long unassignedShards = randomNonNegativeLong();
        long totalAllocations = randomNonNegativeLong();
        long undesiredAllocations = randomNonNegativeLong();
        metrics.updateMetrics(new AllocationStats(unassignedShards, totalAllocations, undesiredAllocations), Map.of(), Map.of());
        assertEquals(totalAllocations, metrics.totalAllocations());
        assertEquals(unassignedShards, metrics.unassignedShards());
        assertEquals(undesiredAllocations, metrics.undesiredAllocations());
        metrics.zeroAllMetrics();
        assertEquals(0, metrics.totalAllocations());
        assertEquals(0, metrics.unassignedShards());
        assertEquals(0, metrics.undesiredAllocations());
    }

    public void testMetricsAreOnlyPublishedWhenNodeIsMaster() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        DesiredBalanceMetrics metrics = new DesiredBalanceMetrics(meterRegistry);

        long unassignedShards = randomNonNegativeLong();
        long totalAllocations = randomLongBetween(100, 10000000);
        long undesiredAllocations = randomLongBetween(0, totalAllocations);
        metrics.updateMetrics(new AllocationStats(unassignedShards, totalAllocations, undesiredAllocations), Map.of(), Map.of());

        // Collect when not master
        meterRegistry.getRecorder().collect();
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME),
            empty()
        );
        assertThat(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME),
            empty()
        );
        assertThat(
            meterRegistry.getRecorder().getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME),
            empty()
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_GAUGE, DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME),
            empty()
        );

        // Collect when master
        metrics.setNodeIsMaster(true);
        meterRegistry.getRecorder().collect();
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.UNDESIRED_ALLOCATION_COUNT_METRIC_NAME)
                .getFirst()
                .getLong(),
            equalTo(undesiredAllocations)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.TOTAL_SHARDS_METRIC_NAME)
                .getFirst()
                .getLong(),
            equalTo(totalAllocations)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.LONG_GAUGE, DesiredBalanceMetrics.UNASSIGNED_SHARDS_METRIC_NAME)
                .getFirst()
                .getLong(),
            equalTo(unassignedShards)
        );
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_GAUGE, DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME)
                .getFirst()
                .getDouble(),
            equalTo((double) undesiredAllocations / totalAllocations)
        );
    }

    public void testUndesiredAllocationRatioIsZeroWhenTotalShardsIsZero() {
        RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        DesiredBalanceMetrics metrics = new DesiredBalanceMetrics(meterRegistry);
        long unassignedShards = randomNonNegativeLong();
        metrics.updateMetrics(new AllocationStats(unassignedShards, 0, 0), Map.of(), Map.of());

        metrics.setNodeIsMaster(true);
        meterRegistry.getRecorder().collect();
        assertThat(
            meterRegistry.getRecorder()
                .getMeasurements(InstrumentType.DOUBLE_GAUGE, DesiredBalanceMetrics.UNDESIRED_ALLOCATION_RATIO_METRIC_NAME)
                .getFirst()
                .getDouble(),
            equalTo(0d)
        );
    }
}
