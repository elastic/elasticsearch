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
    private final LongCounter featuresCounterAll;
    private final LongCounter functionsCounter;
    private final LongCounter functionsCounterAll;

    public static String ESQL_PREFIX = "es.esql.";
    public static String FEATURES_PREFIX = "commands.";
    public static String FUNCTIONS_PREFIX = "functions.";

    /**
     * Number of times a command is used.
     * If the command is used N times in a single query, this counter will be incremented by N
     */
    public static final String FEATURE_METRICS_ALL = ESQL_PREFIX + FEATURES_PREFIX + "usages.total";

    /**
     * Queries that use a command.
     * If a query uses a command N times, this will still be incremented by one only
     */
    public static final String FEATURE_METRICS = ESQL_PREFIX + FEATURES_PREFIX + "queries.total";

    /**
     * Number of times a function is used.
     * If the function is used N times in a single query, this counter will be incremented by N
     */
    public static final String FUNCTION_METRICS_ALL = ESQL_PREFIX + FUNCTIONS_PREFIX + "usages.total";

    /**
     * Queries that use a command.
     * If a query uses a command N times, this will still be incremented by one only
     */
    public static final String FUNCTION_METRICS = ESQL_PREFIX + FUNCTIONS_PREFIX + "queries.total";
    public static final String FEATURE_NAME = "feature_name";

    /**
     * the query was executed successfully or not
     */
    public static final String SUCCESS = "success";

    public PlanningMetricsManager(MeterRegistry meterRegistry) {
        featuresCounter = meterRegistry.registerLongCounter(
            FEATURE_METRICS,
            "ESQL features, total number of queries that use them",
            "unit"
        );
        featuresCounterAll = meterRegistry.registerLongCounter(FEATURE_METRICS_ALL, "ESQL features, total usage", "unit");
        functionsCounter = meterRegistry.registerLongCounter(
            FUNCTION_METRICS,
            "ESQL functions, total number of queries that use them",
            "unit"
        );
        functionsCounterAll = meterRegistry.registerLongCounter(FUNCTION_METRICS_ALL, "ESQL functions, total usage", "unit");
    }

    /**
     * Publishes the collected metrics to the meter registry
     */
    public void publish(PlanningMetrics metrics, boolean success) {
        metrics.commands().entrySet().forEach(x -> incCommand(x.getKey(), x.getValue(), success));
        metrics.functions().entrySet().forEach(x -> incFunction(x.getKey(), x.getValue(), success));
    }

    private void incCommand(String name, int count, boolean success) {
        this.featuresCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.featuresCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }

    private void incFunction(String name, int count, boolean success) {
        this.functionsCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.functionsCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }
}
