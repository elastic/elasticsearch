/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.sql.stats;

import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.xpack.sql.proto.RequestInfo.ODBC_CLIENT_IDS;
import static org.elasticsearch.xpack.sql.stats.QueryMetric.ODBC;

/**
 * Class encapsulating the metrics collected for ES SQL
 */
public class Metrics {
    private enum OperationType {
        FAILED,
        PAGING,
        TOTAL;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }
    }

    // map that holds total/paging/failed counters for each client type (rest, cli, jdbc, odbc...)
    private final Map<QueryMetric, Map<OperationType, CounterMetric>> opsByTypeMetrics;
    // map that holds one counter per sql query "feature" (having, limit, order by, group by...)
    private final Map<FeatureMetric, CounterMetric> featuresMetrics;
    // counter for "translate" requests
    private final CounterMetric translateMetric;
    protected static String QPREFIX = "queries.";
    protected static String FPREFIX = "features.";
    protected static String TRANSLATE_METRIC = "queries.translate.count";

    public Metrics() {
        Map<QueryMetric, Map<OperationType, CounterMetric>> qMap = new LinkedHashMap<>();
        for (QueryMetric metric : QueryMetric.values()) {
            Map<OperationType, CounterMetric> metricsMap = Maps.newLinkedHashMapWithExpectedSize(OperationType.values().length);
            for (OperationType type : OperationType.values()) {
                metricsMap.put(type, new CounterMetric());
            }

            qMap.put(metric, Collections.unmodifiableMap(metricsMap));
        }
        opsByTypeMetrics = Collections.unmodifiableMap(qMap);

        Map<FeatureMetric, CounterMetric> fMap = Maps.newLinkedHashMapWithExpectedSize(FeatureMetric.values().length);
        for (FeatureMetric featureMetric : FeatureMetric.values()) {
            fMap.put(featureMetric, new CounterMetric());
        }
        featuresMetrics = Collections.unmodifiableMap(fMap);

        translateMetric = new CounterMetric();
    }

    /**
     * Increments the "total" counter for a metric
     * This method should be called only once per query.
     */
    public void total(QueryMetric metric) {
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

    /**
     * Increments the "translate" metric
     */
    public void translate() {
        translateMetric.inc();
    }

    private void inc(QueryMetric metric, OperationType op) {
        this.opsByTypeMetrics.get(metric).get(op).inc();
    }

    public void inc(FeatureMetric metric) {
        this.featuresMetrics.get(metric).inc();
    }

    public Counters stats() {
        Counters counters = new Counters();

        // queries metrics
        for (Entry<QueryMetric, Map<OperationType, CounterMetric>> entry : opsByTypeMetrics.entrySet()) {
            String metricName = entry.getKey().toString();

            for (OperationType type : OperationType.values()) {
                long metricCounter = entry.getValue().get(type).count();
                String operationTypeName = type.toString();

                counters.inc(QPREFIX + metricName + "." + operationTypeName, metricCounter);
                counters.inc(QPREFIX + "_all." + operationTypeName, metricCounter);
                // compute the ODBC total metric
                if (ODBC_CLIENT_IDS.contains(metricName)) {
                    counters.inc(QPREFIX + ODBC.toString() + "." + operationTypeName, metricCounter);
                }
            }
        }

        // features metrics
        for (Entry<FeatureMetric, CounterMetric> entry : featuresMetrics.entrySet()) {
            counters.inc(FPREFIX + entry.getKey().toString(), entry.getValue().count());
        }

        // translate operation metric
        counters.inc(TRANSLATE_METRIC, translateMetric.count());

        return counters;
    }
}
