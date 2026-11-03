/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.MeterRegistry;

import java.util.HashMap;
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
    private final LongCounter resolvedSettingsCounter;
    private final LongCounter viewSettingsCounter;
    private final LongCounter viewSettingsCounterAll;
    private final LongCounter viewResolvedSettingsCounter;
    private final LongCounter linkedProjectsHistogram;
    private final LongCounter queriesCounter;

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
     * Queries in which a setting resolved to a value. This covers every applicable setting, regardless
     * of whether the user set it explicitly (from a {@code SET} or the request body), or left
     * it to its default.
     */
    public static final String RESOLVED_SETTINGS_METRICS = "es.esql.resolved_settings.queries.total";

    /**
     * Number of resolved views whose definition {@code SET}s a setting.
     * If a query resolves N views that set it, this counter will be incremented by N.
     */
    public static final String VIEW_SETTING_METRICS_ALL = "es.esql.view_settings.usages.total";

    /**
     * Queries that resolve at least one view whose definition {@code SET}s a setting.
     */
    public static final String VIEW_SETTING_METRICS = "es.esql.view_settings.queries.total";

    /**
     * Queries that resolve at least one view whose definition {@code SET}s a setting to a value. A setting a definition
     * leaves at its default is not counted. Counted apart from {@link #RESOLVED_SETTINGS_METRICS}, which is the caller's
     * settings.
     */
    public static final String VIEW_RESOLVED_SETTINGS_METRICS = "es.esql.view_resolved_settings.queries.total";

    /**
     * Histogram of linked projects per ES|QL query.
     */
    public static final String LINKED_PROJECTS_HISTOGRAM = "es.esql.linked_projects.histogram";

    /**
     * Queries, counted once each when they finish, with the {@link #CLIENT}, {@link #ENTRY_POINT}, {@link #LEADING_COMMAND} and
     * {@link #QUERY_SUCCESS} attributes, the {@link #DIRECT_CLIENT} attribute for queries without an Elastic product origin, and
     * the {@link #KIBANA_FEATURE} and {@link #KIBANA_INTERACTION} attributes for queries from Kibana.
     */
    public static final String QUERY_METRICS = "es.esql.queries.total";

    /**
     * Attribute of the query counter: who sent the query, one of {@link QueryMetric}
     */
    public static final String CLIENT = "es_esql_client";

    /**
     * Attribute of the query counter: the API the query came in on, one of {@link QueryEntryPoint}
     */
    public static final String ENTRY_POINT = "es_esql_entry_point";

    /**
     * Attribute of the query counter: the query's first source command, one of {@link LeadingCommand}
     */
    public static final String LEADING_COMMAND = "es_esql_leading_command";

    /**
     * Attribute of the query counter: the tool that sent a query without an Elastic product origin, one of {@link DirectClient}.
     * Queries with a product origin don't carry it, since {@link #CLIENT} already names their product.
     */
    public static final String DIRECT_CLIENT = "es_esql_direct_client";

    /**
     * Attribute of the query counter: whether the query succeeded. The older counters' {@link #SUCCESS} name predates the
     * {@code es_*} attribute naming rule, so new counters cannot use it.
     */
    public static final String QUERY_SUCCESS = "es_esql_success";

    /**
     * Attribute of the query counter: the Kibana feature that sent a query from Kibana, one of
     * {@link KibanaExecutionContextClassifier.Feature}. Queries from anywhere else don't carry it.
     */
    public static final String KIBANA_FEATURE = "es_esql_kibana_feature";

    /**
     * Attribute of the query counter: whether a person chose to run a query from Kibana, one of
     * {@link KibanaExecutionContextClassifier.Interaction}. Set together with {@link #KIBANA_FEATURE}.
     */
    public static final String KIBANA_INTERACTION = "es_esql_kibana_interaction";

    public static final String FEATURE_NAME = "feature_name";

    /**
     * the query was executed successfully or not
     */
    public static final String SUCCESS = "success";

    /**
     * Attribute of the setting counters: the value the setting took
     */
    public static final String SETTING_VALUE = "setting_value";

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
        resolvedSettingsCounter = meterRegistry.registerLongCounter(
            RESOLVED_SETTINGS_METRICS,
            "ESQL settings, total number of queries in which a setting resolved to a value",
            "unit"
        );
        viewSettingsCounter = meterRegistry.registerLongCounter(
            VIEW_SETTING_METRICS,
            "ESQL view settings, total number of queries that resolve a view whose definition sets them",
            "unit"
        );
        viewSettingsCounterAll = meterRegistry.registerLongCounter(
            VIEW_SETTING_METRICS_ALL,
            "ESQL view settings, total number of resolved views whose definition sets them",
            "unit"
        );
        viewResolvedSettingsCounter = meterRegistry.registerLongCounter(
            VIEW_RESOLVED_SETTINGS_METRICS,
            "ESQL view settings, total number of queries in which a view body resolved a setting to a value",
            "unit"
        );
        queriesCounter = meterRegistry.registerLongCounter(QUERY_METRICS, "ESQL queries, total number of queries", "unit");
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
        metrics.resolvedSettings().forEach((name, label) -> incResolvedSetting(resolvedSettingsCounter, name, label, success));
        metrics.viewSettings().forEach((key, value) -> incViewSetting(key, value, success));
        metrics.viewResolvedSettings()
            .forEach((name, labels) -> labels.forEach(label -> incResolvedSetting(viewResolvedSettingsCounter, name, label, success)));
        if (metrics.linkedProjectsCount() != null) {
            linkedProjectsHistogram.incrementBy(1, Map.of("es_linked_projects_count", metrics.linkedProjectsCount()));
        }
    }

    /**
     * Counts one finished query under its caller, entry point, direct client and leading command. {@code directClient} is
     * {@code null} for a query with an Elastic product origin, which is not counted under direct clients.
     */
    public void publishQuery(
        QueryCaller caller,
        QueryEntryPoint entryPoint,
        @Nullable DirectClient directClient,
        LeadingCommand leadingCommand,
        boolean success
    ) {
        Map<String, Object> attributes = new HashMap<>();
        attributes.put(CLIENT, caller.client().toString());
        attributes.put(ENTRY_POINT, entryPoint.toString());
        attributes.put(LEADING_COMMAND, leadingCommand.toString());
        attributes.put(QUERY_SUCCESS, success);
        if (directClient != null) {
            attributes.put(DIRECT_CLIENT, directClient.toString());
        }
        if (caller.kibanaFeature() != null) {
            attributes.put(KIBANA_FEATURE, caller.kibanaFeature().toString());
            attributes.put(KIBANA_INTERACTION, caller.kibanaFeature().interaction().toString());
        }
        queriesCounter.incrementBy(1, attributes);
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

    private void incViewSetting(String name, int count, boolean success) {
        this.viewSettingsCounter.incrementBy(1, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
        this.viewSettingsCounterAll.incrementBy(count, Map.ofEntries(Map.entry(FEATURE_NAME, name), Map.entry(SUCCESS, success)));
    }

    private static void incResolvedSetting(LongCounter counter, String name, String label, boolean success) {
        Map<String, Object> attributes = Map.ofEntries(
            Map.entry(FEATURE_NAME, name),
            Map.entry(SETTING_VALUE, label),
            Map.entry(SUCCESS, success)
        );
        counter.incrementBy(1, attributes);
    }
}
