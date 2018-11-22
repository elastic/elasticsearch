/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.stats;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class encapsulating the metrics collected for ES SQL
 */
public class Metrics {
    private enum OperationType {
        FAILED, PAGING, TOTAL;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }
    }
    
    // map that holds total/paging/failed counters for each client type (rest, cli, jdbc, odbc...)
    private final Map<QueryMetric, Map<OperationType, CounterMetric>> opsByTypeMetrics = new HashMap<>();
    // map that holds one counter per sql query "feature" (having, limit, order by, group by...) 
    private final Map<FeatureMetric, CounterMetric> featuresMetrics = new HashMap<>();
    // because the hook in the Verifier/Analyzer is being called several times during the query analysis flow
    // we keep a set of flags so that we don't count a certain metric more than once
    private final Map<FeatureMetric, Boolean> featuresCollected = new HashMap<>();
    private String QPREFIX = "queries.";
    private String FPREFIX = "features.";
    
    public Metrics() {
        for (QueryMetric metric : QueryMetric.values()) {
            Map<OperationType, CounterMetric> metricsMap = new HashMap<>(OperationType.values().length);
            for (OperationType type : OperationType.values()) {
                metricsMap.put(type,  new CounterMetric());
            }
            
            opsByTypeMetrics.put(metric, metricsMap);
        }
        for (FeatureMetric featureMetric : FeatureMetric.values()) {
            featuresMetrics.put(featureMetric,  new CounterMetric());
            featuresCollected.put(featureMetric, false);
        }
    }

    /**
     * Increments the "total" counter for a metric
     */
    public void total(QueryMetric metric) {
        // flags initialized here because this is the "entry point" in the query execution
        // when a query comes in, it is counted against a metric client type (rest, cli, jdbc, odbc...) 
        for (FeatureMetric featureMetric : FeatureMetric.values()) {
            featuresCollected.put(featureMetric, false);
        }
        inc(metric, OperationType.TOTAL);
    }
    
    /**
     * Increments the "failed" counter for a metric
     */
    public void failed(QueryMetric metric) {
        inc(metric, OperationType.FAILED);
    }
    
    /**
     * Increments the "paging" counter for a metric
     */
    public void paging(QueryMetric metric) {
        inc(metric, OperationType.PAGING);
    }

    private void inc(QueryMetric metric, OperationType op) {
        this.opsByTypeMetrics.get(metric).get(op).inc();
    }
    
    public void inc(FeatureMetric metric) {
        // count each "feature" metric only once by checking its flag
        if (!featuresCollected.get(metric)) {
            featuresCollected.put(metric, true);
            this.featuresMetrics.get(metric).inc();
        }
    }

    public Counters stats() {
        Counters counters = new Counters();
        for (Entry<QueryMetric, Map<OperationType, CounterMetric>> entry : opsByTypeMetrics.entrySet()) {
            for (OperationType type : OperationType.values()) {
                counters.inc(QPREFIX + entry.getKey().toString() + "." + type.toString(), entry.getValue().get(type).count());
                counters.inc(QPREFIX + "_all." + type.toString(), entry.getValue().get(type).count());
            }
        }
        
        for (Entry<FeatureMetric, CounterMetric> entry : featuresMetrics.entrySet()) {
            counters.inc(FPREFIX + entry.getKey().toString(), entry.getValue().count());
        }
        
        return counters;
    }
}
