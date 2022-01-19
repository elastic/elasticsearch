/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rollup.v2.indexer.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.xpack.core.rollup.job.MetricConfig;
import org.elasticsearch.xpack.rollup.v2.FieldValueFetcher;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricCollector.Max;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricCollector.Min;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricCollector.Sum;
import org.elasticsearch.xpack.rollup.v2.indexer.metrics.MetricCollector.ValueCount;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class MetricField {
    final String name;
    protected final MetricCollector[] collectors;
    private final FieldValueFetcher fetcher;

    public MetricField(String name, MetricCollector[] collectors, FieldValueFetcher fetcher) {
        this.name = name;
        this.collectors = collectors;
        this.fetcher = fetcher;
    }

    public String getName() {
        return name;
    }

    public MetricCollector[] getCollectors() {
        return collectors;
    }

    public LeafMetricField getMetricFieldLeaf(LeafReaderContext context) {
        return fetcher.getMetricFieldLeaf(context, collectors);
    }

    public void resetCollectors() {
        for (MetricCollector metricCollector : collectors) {
            metricCollector.reset();
        }
    }

    public static MetricField buildMetricField(MetricConfig metricConfig, FieldValueFetcher fetcher) {
        final List<String> normalizedMetrics = normalizeMetrics(metricConfig.getMetrics());
        final List<MetricCollector> list = new ArrayList<>();
        MetricCollector[] metricCollectors;
        if (normalizedMetrics.isEmpty() == false) {
            for (String metricName : normalizedMetrics) {
                switch (metricName) {
                    case "min":
                        list.add(new Min());
                        break;
                    case "max":
                        list.add(new Max());
                        break;
                    case "sum":
                        list.add(new Sum());
                        break;
                    case "value_count":
                        list.add(new ValueCount());
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported metric type [" + metricName + "]");
                }
            }

            metricCollectors = list.toArray(new MetricCollector[0]);
        } else {
            metricCollectors = new MetricCollector[0];
        }

        return new NumberMetricField(metricConfig.getField(), metricCollectors, fetcher);
    }

    public static List<String> normalizeMetrics(List<String> metrics) {
        List<String> newMetrics = new ArrayList<>(metrics);
        // avg = sum + value_count
        if (newMetrics.remove(MetricConfig.AVG.getPreferredName())) {
            if (newMetrics.contains(MetricConfig.VALUE_COUNT.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.VALUE_COUNT.getPreferredName());
            }
            if (newMetrics.contains(MetricConfig.SUM.getPreferredName()) == false) {
                newMetrics.add(MetricConfig.SUM.getPreferredName());
            }
        }
        return newMetrics;
    }

    public abstract void collectMetric(StreamInput in) throws IOException;
}
