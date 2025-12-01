/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.telemetry.InstrumentType;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.RecordingMeterRegistry;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardWriteLoadDistributionMetricsTests extends ESTestCase {

    public void testShardWriteLoadDistributionMetrics() {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final ShardWriteLoadDistributionMetrics shardWriteLoadDistributionMetrics = new ShardWriteLoadDistributionMetrics(
            meterRegistry,
            0,
            50,
            90,
            100
        );
        final double maxp50 = randomDoubleBetween(0, 10, true);
        final double maxp90 = randomDoubleBetween(maxp50, 30, true);
        final double maxp100 = randomDoubleBetween(maxp90, 50, true);

        final var clusterInfo = ClusterInfo.builder().shardWriteLoads(randomWriteLoads(maxp50, maxp90, maxp100)).build();
        shardWriteLoadDistributionMetrics.onNewInfo(clusterInfo);

        meterRegistry.getRecorder().collect();

        List<Measurement> measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.METRIC_NAME);

        assertEquals(4, measurements.size());
        assertThat(measurementForPercentile(measurements, 50.0), lessThanOrEqualTo(maxp50));
        assertThat(measurementForPercentile(measurements, 90.0), lessThanOrEqualTo(maxp90));
        assertThat(measurementForPercentile(measurements, 100.0), lessThanOrEqualTo(maxp100));
    }

    private double measurementForPercentile(List<Measurement> measurements, double percentile) {
        return measurements.stream()
            .filter(m -> m.attributes().get("percentile").equals(String.valueOf(percentile)))
            .findFirst()
            .orElseThrow()
            .getDouble();
    }

    private Map<ShardId, Double> randomWriteLoads(double p50, double p90, double p100) {
        final Map<ShardId, Double> shardWriteLoads = new HashMap<>();
        final Index index = new Index(randomIdentifier(), randomUUID());
        int shardId = 0;
        for (int i = 0; i < 50; i++) {
            shardWriteLoads.put(new ShardId(index, shardId++), randomDoubleBetween(0, p50, true));
        }
        for (int i = 0; i < 40; i++) {
            shardWriteLoads.put(new ShardId(index, shardId++), randomDoubleBetween(p50, p90, true));
        }
        for (int i = 0; i < 10; i++) {
            shardWriteLoads.put(new ShardId(index, shardId++), randomDoubleBetween(p90, p100, true));
        }
        assert shardWriteLoads.size() == 100;
        return shardWriteLoads;
    }
}
