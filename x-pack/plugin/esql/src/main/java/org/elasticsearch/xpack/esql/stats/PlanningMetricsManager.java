/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * This class is responsible for publishing metrics related to ES|QL planning.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/blob/main/modules/apm/METERING.md">METERING</a>
 */
public class PlanningMetricsManager {

    // APM counters
    private final LongCounter featuresCounter;
    private final LongCounter functionsCounter;

    public static String ESQL_PREFIX = "es.esql.";
    public static String FEATURES_PREFIX = "features.";
    public static String FUNCTIONS_PREFIX = "functions.";
    public static final String FEATURE_METRICS = ESQL_PREFIX + FEATURES_PREFIX + "total";
    public static final String FUNCTION_METRICS = ESQL_PREFIX + FUNCTIONS_PREFIX + "total";
    public static final String FEATURE_NAME = "feature_name";

    public PlanningMetricsManager(MeterRegistry meterRegistry) {
        featuresCounter = meterRegistry.registerLongCounter(FEATURE_METRICS, "ESQL features, total usage", "unit");
        functionsCounter = meterRegistry.registerLongCounter(FUNCTION_METRICS, "ESQL functions, total usage", "unit");
    }

    /**
     * Publishes the collected metrics to the meter registry
     */
    public void publish(PlanningMetrics metrics) {
        metrics.commands().entrySet().forEach(x -> incCommand(x.getKey(), x.getValue()));
        metrics.functions().entrySet().forEach(x -> incFunction(x.getKey(), x.getValue()));
    }

    private void incCommand(String name, int count) {
        this.featuresCounter.incrementBy(count, Map.of(FEATURE_NAME, name));
    }

    private void incFunction(String name, int count) {
        this.functionsCounter.incrementBy(count, Map.of(FEATURE_NAME, name));
    }
}
