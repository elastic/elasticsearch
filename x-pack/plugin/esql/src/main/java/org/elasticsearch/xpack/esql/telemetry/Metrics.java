/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.Build;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.crossproject.CrossProjectModeDecider;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plan.QuerySettings;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Class encapsulating the metrics collected for ESQL
 */
public class Metrics {

    private enum OperationType {
        FAILED,
        TOTAL;

        @Override
        public String toString() {
            return this.name().toLowerCase(Locale.ROOT);
        }
    }

    protected static final String QUERIES_PREFIX = "queries.";
    protected static final String FEATURES_PREFIX = "features.";
    protected static final String SETTINGS_PREFIX = "settings.";
    protected static final String FUNC_PREFIX = "functions.";
    protected static final String TOOK_PREFIX = "took.";

    // map that holds total/failed counters for each client type (rest, kibana)
    private final Map<QueryMetric, Map<OperationType, CounterMetric>> opsByTypeMetrics;
    // map that holds one counter per esql query "feature" (eval, sort, limit, where....)
    private final Map<FeatureMetric, CounterMetric> featuresMetrics;
    // map that holds one counter per esql query setting (unmapped_fields, time_zone, etc.)
    private final Map<String, CounterMetric> settingsMetrics;
    private final Map<String, CounterMetric> functionMetrics;
    private final TookMetrics tookMetrics = new TookMetrics();

    private final EsqlFunctionRegistry functionRegistry;
    private final Map<Class<?>, String> classToFunctionName;

    /**
     * Creates a Metrics instance for production use.
     * Settings metrics are filtered based on the current build type (snapshot vs release)
     * and deployment mode (serverless vs stateful).
     */
    public Metrics(EsqlFunctionRegistry functionRegistry, Settings settings) {
        this(functionRegistry, Build.current().isSnapshot(), new CrossProjectModeDecider(settings).crossProjectEnabled());
    }

    /**
     * Creates a Metrics instance with explicit environment flags.
     * This constructor is primarily for testing purposes.
     *
     * @param functionRegistry the function registry
     * @param isSnapshot whether this is a snapshot build
     * @param isServerless whether cross-project search is enabled (serverless mode)
     */
    public Metrics(EsqlFunctionRegistry functionRegistry, boolean isSnapshot, boolean isServerless) {
        this.functionRegistry = functionRegistry.snapshotRegistry();
        this.classToFunctionName = initClassToFunctionType();
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

        // Only register settings metrics for settings that are applicable to the current environment
        Map<String, CounterMetric> sMap = Maps.newLinkedHashMapWithExpectedSize(QuerySettings.SETTINGS_BY_NAME.size());
        for (var entry : QuerySettings.SETTINGS_BY_NAME.entrySet()) {
            String settingName = entry.getKey();
            QuerySettings.QuerySettingDef<?> def = entry.getValue();
            // Skip snapshot-only settings in non-snapshot builds
            if (def.snapshotOnly() && isSnapshot == false) {
                continue;
            }
            // Skip serverless-only settings in stateful (non-serverless) deployments
            if (def.serverlessOnly() && isServerless == false) {
                continue;
            }
            sMap.put(settingName, new CounterMetric());
        }
        settingsMetrics = Collections.unmodifiableMap(sMap);

        functionMetrics = initFunctionMetrics();
    }

    private Map<String, CounterMetric> initFunctionMetrics() {
        Map<String, CounterMetric> result = new LinkedHashMap<>();
        for (var entry : classToFunctionName.entrySet()) {
            result.put(entry.getValue(), new CounterMetric());
        }
        return Collections.unmodifiableMap(result);
    }

    private Map<Class<?>, String> initClassToFunctionType() {
        Map<Class<?>, String> tmp = new HashMap<>();
        for (FunctionDefinition func : functionRegistry.listFunctions()) {
            if (tmp.containsKey(func.clazz()) == false) {
                tmp.put(func.clazz(), func.name());
            }
        }
        return Collections.unmodifiableMap(tmp);
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

    private void inc(QueryMetric metric, OperationType op) {
        this.opsByTypeMetrics.get(metric).get(op).inc();
    }

    public void inc(FeatureMetric metric) {
        this.featuresMetrics.get(metric).inc();
    }

    public void incSetting(String settingName) {
        CounterMetric counter = this.settingsMetrics.get(settingName);
        if (counter != null) {
            counter.inc();
        }
    }

    public void incFunctionMetric(Class<?> functionType) {
        String functionName = classToFunctionName.get(functionType);
        if (functionName != null) {
            functionMetrics.get(functionName).inc();
        }
    }

    public void recordTook(long tookMillis) {
        tookMetrics.count(tookMillis);
    }

    public Counters stats() {
        Counters counters = new Counters();

        // queries metrics
        for (Entry<QueryMetric, Map<OperationType, CounterMetric>> entry : opsByTypeMetrics.entrySet()) {
            String metricName = entry.getKey().toString();

            for (OperationType type : OperationType.values()) {
                long metricCounter = entry.getValue().get(type).count();
                String operationTypeName = type.toString();

                counters.inc(QUERIES_PREFIX + metricName + "." + operationTypeName, metricCounter);
                counters.inc(QUERIES_PREFIX + "_all." + operationTypeName, metricCounter);
            }
        }

        // features metrics
        for (Entry<FeatureMetric, CounterMetric> entry : featuresMetrics.entrySet()) {
            counters.inc(FEATURES_PREFIX + entry.getKey().toString(), entry.getValue().count());
        }

        // settings metrics
        for (Entry<String, CounterMetric> entry : settingsMetrics.entrySet()) {
            counters.inc(SETTINGS_PREFIX + entry.getKey(), entry.getValue().count());
        }

        // function metrics
        for (Entry<String, CounterMetric> entry : functionMetrics.entrySet()) {
            counters.inc(FUNC_PREFIX + entry.getKey(), entry.getValue().count());
        }

        tookMetrics.counters(TOOK_PREFIX, counters);

        return counters;
    }
}
