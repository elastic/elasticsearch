/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.Map;

/**
 * This class is responsible for publishing metrics related to ES|QL planning.
 *
 * @see <a href="https://github.com/elastic/elasticsearch/blob/main/modules/apm/METERING.md">METERING</a>
 */
public class PlanTelemetryManager {

    // APM counters
    private final LongCounter featuresCounter;
    private final LongCounter featuresCounterAll;
    private final LongCounter functionsCounter;
    private final LongCounter functionsCounterAll;
    private final LongCounter settingsCounter;
    private final LongCounter settingsCounterAll;
    private final LongCounter linkedProjectsHistogram;

    /**
     * Number of times a command is used.
     * If the command is used N times in a single query, this counter will be incremented by N
     */
    public static final String FEATURE_METRICS_ALL = "es.esql.commands.usages.total";

    /**
     * Queries that use a command.
     * If a query uses a command N times, this will still be incremented by one only
     */
    public static final String FEATURE_METRICS = "es.esql.commands.queries.total";

    /**
     * Number of times a function is used.
     * If the function is used N times in a single query, this counter will be incremented by N
     */
    public static final String FUNCTION_METRICS_ALL = "es.esql.functions.usages.total";

    /**
     * Queries that use a function.
     * If a query uses a function N times, this will still be incremented by one only
     */
    public static final String FUNCTION_METRICS = "es.esql.functions.queries.total";

    /**
     * Number of times a setting is used.
     * If the setting is used N times in a single query, this counter will be incremented by N
     */
    public static final String SETTING_METRICS_ALL = "es.esql.settings.usages.total";

    /**
     * Queries that use a setting.
     * If a query uses a setting N times, this will still be incremented by one only
     */
    public static final String SETTING_METRICS = "es.esql.settings.queries.total";

    /**
     * Histogram of linked projects per ES|QL query.
     */
    public static final String LINKED_PROJECTS_HISTOGRAM = "es.esql.linked_projects.histogram";

    public static final String FEATURE_NAME = "feature_name";

    /**
     * the query was executed successfully or not
     */
    public static final String SUCCESS = "success";

    public PlanTelemetryManager(MeterRegistry meterRegistry) {
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
        settingsCounter = meterRegistry.registerLongCounter(
            SETTING_METRICS,
            "ESQL settings, total number of queries that use them",
            "unit"
        );
        settingsCounterAll = meterRegistry.registerLongCounter(SETTING_METRICS_ALL, "ESQL settings, total usage", "unit");
        linkedProjectsHistogram = meterRegistry.registerLongCounter(
            LINKED_PROJECTS_HISTOGRAM,
            "Histogram of linked projects per esql query",
            "unit"
        );
    }

    /**
     * Publishes the collected metrics to the meter registry
     */
    public void publish(PlanTelemetry metrics, boolean success) {
        metrics.commands().forEach((key, value) -> incCommand(key, value, success));
        metrics.functions().forEach((key, value) -> incFunction(key, value, success));
        metrics.settings().forEach((key, value) -> incSetting(key, value, success));
        if (metrics.linkedProjectsCount() != null) {
            linkedProjectsHistogram.incrementBy(1, Map.of("es_linked_projects_count", metrics.linkedProjectsCount()));
        }
    }

    private void incCommand(String name, int count, boolean success) {
        this.featuresCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.featuresCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }

    private void incFunction(String name, int count, boolean success) {
        this.functionsCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.functionsCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }

    private void incSetting(String name, int count, boolean success) {
        this.settingsCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.settingsCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }
}
