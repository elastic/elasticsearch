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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ShardWriteLoadDistributionMetricsTests extends ESTestCase {

    public void testShardWriteLoadDistributionMetrics() {
        final RecordingMeterRegistry meterRegistry = new RecordingMeterRegistry();
        final int numberOfSignificantDigits = randomIntBetween(2, 4);
        final ShardWriteLoadDistributionMetrics shardWriteLoadDistributionMetrics = new ShardWriteLoadDistributionMetrics(
            meterRegistry,
            numberOfSignificantDigits,
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

        final var measurements = meterRegistry.getRecorder()
            .getMeasurements(InstrumentType.DOUBLE_GAUGE, ShardWriteLoadDistributionMetrics.METRIC_NAME);

        logger.info("Generated maximums p50={}/p90={}/p100={}", maxp50, maxp90, maxp100);
        assertEquals(4, measurements.size());
        assertThat(measurementForPercentile(measurements, 0.0), greaterThanOrEqualTo(0.0));
        assertRoughlyInRange(numberOfSignificantDigits, measurementForPercentile(measurements, 50.0), 0.0, maxp50);
        assertRoughlyInRange(numberOfSignificantDigits, measurementForPercentile(measurements, 90.0), maxp50, maxp90);
        assertRoughlyInRange(numberOfSignificantDigits, measurementForPercentile(measurements, 100.0), maxp90, maxp100);
    }

    /**
     * HDR histograms are accurate to a number of significant digits, so it's possible the values might be slightly off. This comparison
     * accounts for the configured significant digits to prevent test flakiness.
     */
    private static void assertRoughlyInRange(int numberOfSignificantDigits, double value, double min, double max) {
        final double valueLow = roundDown(value, numberOfSignificantDigits);
        final double valueHigh = roundUp(value, numberOfSignificantDigits);
        final double maxHigh = roundUp(max, numberOfSignificantDigits);
        final double minLow = roundDown(min, numberOfSignificantDigits);

        assertThat(valueHigh, greaterThanOrEqualTo(minLow));
        assertThat(valueLow, lessThanOrEqualTo(maxHigh));
    }

    private static double roundUp(double value, int significantDigits) {
        return BigDecimal.valueOf(value).multiply(BigDecimal.ONE, new MathContext(significantDigits, RoundingMode.CEILING)).doubleValue();
    }

    private static double roundDown(double value, int significantDigits) {
        return BigDecimal.valueOf(value).multiply(BigDecimal.ONE, new MathContext(significantDigits, RoundingMode.FLOOR)).doubleValue();
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
