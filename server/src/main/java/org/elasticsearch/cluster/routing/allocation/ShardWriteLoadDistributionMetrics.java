/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.routing.allocation;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.cluster.ClusterInfo;
import org.elasticsearch.telemetry.metric.DoubleWithAttributes;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Publishes metrics about the distribution of shard write loads in the cluster
 */
public class ShardWriteLoadDistributionMetrics {

    public static final String METRIC_NAME = "es.allocator.shard_write_load.distribution";

    private final DoubleHistogram shardWeightHistogram;
    private final double[] percentiles;
    private final Map<String, Object>[] attributes;
    private final double[] lastValues;

    @SuppressWarnings("unchecked")
    public ShardWriteLoadDistributionMetrics(MeterRegistry meterRegistry, double... percentiles) {
        this.shardWeightHistogram = new DoubleHistogram(4);
        this.percentiles = percentiles;
        this.attributes = (Map<String, Object>[]) Array.newInstance(Map.class, percentiles.length);
        for (int i = 0; i < percentiles.length; i++) {
            attributes[i] = Map.of("percentile", String.valueOf(percentiles[i]));
        }
        this.lastValues = new double[percentiles.length];
        Arrays.fill(lastValues, Double.NaN);
        meterRegistry.registerDoublesGauge(
            METRIC_NAME,
            "Distribution of values for shard write load",
            "write load",
            this::getTrackedPercentiles
        );
    }

    public void onNewInfo(ClusterInfo clusterInfo) {
        shardWeightHistogram.reset();
        clusterInfo.getShardWriteLoads().forEach((shardId, shardWriteLoad) -> shardWeightHistogram.recordValue(shardWriteLoad));
        for (int i = 0; i < percentiles.length; i++) {
            lastValues[i] = shardWeightHistogram.getValueAtPercentile(percentiles[i]);
        }
    }

    private Collection<DoubleWithAttributes> getTrackedPercentiles() {
        final List<DoubleWithAttributes> metricValues = new ArrayList<>();
        for (int i = 0; i < percentiles.length; i++) {
            double lastValue = lastValues[i];
            if (Double.isNaN(lastValue) == false) {
                Map<String, Object> attributes = this.attributes[i];
                metricValues.add(new DoubleWithAttributes(lastValue, attributes));
                lastValues[i] = Double.NaN;
            }
        }
        return metricValues;
    }
}
