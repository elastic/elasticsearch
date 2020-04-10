/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.eql.stats;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class encapsulating the metrics collected for EQL
 */
public class Metrics {
    private enum OperationType {
        FAILED, TOTAL;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }
    }
    
    // map that holds total/failed counters for each eql "feature" (join, pipe, sequence...)
    private final Map<FeatureMetric, Map<OperationType, CounterMetric>> featuresMetrics;
    protected static String FPREFIX = "features.";
    
    public Metrics() {
        Map<FeatureMetric, Map<OperationType, CounterMetric>> fMap = new LinkedHashMap<>();
        for (FeatureMetric metric : FeatureMetric.values()) {
            Map<OperationType, CounterMetric> metricsMap = new LinkedHashMap<>(OperationType.values().length);
            for (OperationType type : OperationType.values()) {
                metricsMap.put(type,  new CounterMetric());
            }
            
            fMap.put(metric, Collections.unmodifiableMap(metricsMap));
        }
        featuresMetrics = Collections.unmodifiableMap(fMap);
    }

    /**
     * Increments the "total" counter for a metric
     * This method should be called only once per query.
     */
    public void total(FeatureMetric metric) {
        inc(metric, OperationType.TOTAL);
    }
    
    /**
     * Increments the "failed" counter for a metric
     */
    public void failed(FeatureMetric metric) {
        inc(metric, OperationType.FAILED);
    }

    private void inc(FeatureMetric metric, OperationType op) {
        this.featuresMetrics.get(metric).get(op).inc();
    }

    public Counters stats() {
        Counters counters = new Counters();
        
        // queries metrics
        for (Entry<FeatureMetric, Map<OperationType, CounterMetric>> entry : featuresMetrics.entrySet()) {
            String metricName = entry.getKey().toString();
            
            for (OperationType type : OperationType.values()) {
                long metricCounter = entry.getValue().get(type).count();
                String operationTypeName = type.toString();
                
                counters.inc(FPREFIX + metricName + "." + operationTypeName, metricCounter);
                counters.inc(FPREFIX + "_all." + operationTypeName, metricCounter);
            }
        }
        
        return counters;
    }
}
