/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This class is responsible for collecting and publising metrics related to ES|QL planning.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/blob/main/modules/apm/METERING.md">METERING</a>
 */
public class PlanningMetrics {

    // APM counters
    private final LongCounter featuresCounter;
    private final LongCounter functionsCounter;

    public static String ESQL_PREFIX = "es.esql.";
    public static String FEATURES_PREFIX = "features.";
    public static String FUNCTIONS_PREFIX = "functions.";
    public static final String FEATURE_METRICS = ESQL_PREFIX + FEATURES_PREFIX + "total";
    public static final String FUNCTION_METRICS = ESQL_PREFIX + FUNCTIONS_PREFIX + "total";
    public static final String FEATURE_NAME = "feature_name";

    Map<String, Integer> commands = new HashMap<>();
    Map<String, Integer> functions = new HashMap<>();

    public PlanningMetrics(MeterRegistry meterRegistry) {
        featuresCounter = meterRegistry.registerLongCounter(FEATURE_METRICS, "ESQL features, total usage", "unit");
        functionsCounter = meterRegistry.registerLongCounter(FUNCTION_METRICS, "ESQL functions, total usage", "unit");
    }

    /**
     * Gathers statistics from the execution plan after pre-analysis phase
     * @param plan the pre-analized plan
     */
    public void gatherPreAnalysisMetrics(LogicalPlan plan) {
        plan.forEachDown(p -> add(commands, p.commandName()));
        plan.forEachExpressionDown(UnresolvedFunction.class, p -> add(functions, p.name().toUpperCase(Locale.ROOT)));
    }

    private void add(Map<String, Integer> map, String key) {
        Integer cmd = map.get(key);
        map.put(key, cmd == null ? 1 : cmd + 1);
    }

    /**
     * Publishes the collected metrics to the meter registry
     */
    public void publish() {
        commands.entrySet().forEach(x -> incCommand(x.getKey(), x.getValue()));
        functions.entrySet().forEach(x -> incFunction(x.getKey(), x.getValue()));
    }

    private void incCommand(String name, int count) {
        this.featuresCounter.incrementBy(count, Map.of(FEATURE_NAME, name));
    }

    private void incFunction(String name, int count) {
        this.functionsCounter.incrementBy(count, Map.of(FEATURE_NAME, name));
    }
}
