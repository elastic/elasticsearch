/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.action.search.SearchRequestAttributesExtractor;
import org.elasticsearch.action.search.TimeRangeBucket;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.telemetry.metric.LongCounter;
import org.elasticsearch.telemetry.metric.LongHistogram;
import org.elasticsearch.telemetry.metric.MeterRegistry;
import org.elasticsearch.xpack.core.esql.QueryMetricsListener;
import org.elasticsearch.xpack.esql.core.querydsl.QueryDslTimestampBoundsExtractor.TimestampBounds;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Locale;
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
    private final LongHistogram queryDurationHistogram;
    private final LongCounter queryCounter;

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
     * Duration in milliseconds of each successful ES|QL query: its {@code took}, as reported in the response and the query logs.
     * Split by {@link #SOURCE_COMMAND} and {@link #TIME_RANGE_FILTER_FROM}, so latency can be analyzed by how old the data a query
     * asks for is and by whether it is a metrics query. Failed and cancelled queries are left out, as their duration says little
     * about query latency; they are counted by {@link #QUERY_TOTAL}, which must be read alongside this histogram: the slowest
     * queries are the most likely to time out or be cancelled, so the tail of a bucket with many of them is biased low.
     */
    public static final String QUERY_DURATION_HISTOGRAM = "es.esql.query.duration.histogram";

    /**
     * Number of ES|QL queries, by {@link #SOURCE_COMMAND}, {@link #TIME_RANGE_FILTER_FROM} and {@link #OUTCOME}, so the share of
     * queries that failed or were cancelled can be read per bucket of {@link #QUERY_DURATION_HISTOGRAM}.
     */
    public static final String QUERY_TOTAL = "es.esql.query.total";

    /**
     * Explicit millisecond bucket boundaries for {@link #QUERY_DURATION_HISTOGRAM}, covering 1 ms up to 10 minutes.
     * Avoids the default sqrt(2) ladder, which clips above ~131 s and emits 36 buckets per series.
     */
    private static final List<Long> QUERY_DURATION_BUCKET_BOUNDARIES = List.of(
        1L,
        5L,
        10L,
        25L,
        50L,
        100L,
        250L,
        500L,
        1_000L,
        2_500L,
        5_000L,
        10_000L,
        30_000L,
        60_000L,
        120_000L,
        300_000L,
        600_000L
    );

    /**
     * Attribute of {@link #QUERY_DURATION_HISTOGRAM} and {@link #QUERY_TOTAL}: what the query reads, the name of a
     * {@link QuerySourceCommand}.
     */
    public static final String SOURCE_COMMAND = "es_esql_source_command";

    /**
     * Attribute of {@link #QUERY_DURATION_HISTOGRAM} and {@link #QUERY_TOTAL}: the age of the lower bound of the request filter's
     * {@code @timestamp} range when the query started, bucketed by {@link TimeRangeBucket}. Absent if the request filter has no such
     * range. Shared with the search metrics, which bucket a Query DSL search's time range the same way, so one filter covers both.
     */
    public static final String TIME_RANGE_FILTER_FROM = SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FROM_ATTRIBUTE;

    /**
     * Attribute of {@link #QUERY_TOTAL}: how the query ended, the lowercase name of a {@link QueryMetricsListener.Outcome}.
     */
    public static final String OUTCOME = "es_esql_outcome";

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
        linkedProjectsHistogram = meterRegistry.registerLongCounter(
            LINKED_PROJECTS_HISTOGRAM,
            "Histogram of linked projects per esql query",
            "unit"
        );
        queryDurationHistogram = meterRegistry.registerLongHistogram(
            QUERY_DURATION_HISTOGRAM,
            "ESQL query duration on the coordinator for successful queries, by source command and time range filter age",
            "ms",
            QUERY_DURATION_BUCKET_BOUNDARIES
        );
        queryCounter = meterRegistry.registerLongCounter(
            QUERY_TOTAL,
            "ESQL queries on the coordinator, by source command, time range filter age and outcome",
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
     * Records a query, once per query: counts it by its outcome, and records its duration if it succeeded.
     *
     * @param requestedTimeRange the {@code @timestamp} range the request filter asks for, or {@code null} if unknown
     * @param queryStartMillis   when the query started, in milliseconds since the epoch, which the age of the range is relative to
     */
    public void recordQuery(
        PlanTelemetry metrics,
        @Nullable TimestampBounds requestedTimeRange,
        long queryStartMillis,
        QueryMetricsListener.Outcome outcome,
        TimeValue took
    ) {
        Map<String, Object> attributes = requestedTimeRange == null
            ? Map.of(SOURCE_COMMAND, metrics.sourceCommand().name())
            : Map.of(
                SOURCE_COMMAND,
                metrics.sourceCommand().name(),
                TIME_RANGE_FILTER_FROM,
                timeRangeFilterFrom(requestedTimeRange, queryStartMillis)
            );
        queryCounter.incrementBy(1, Maps.copyMapWithAddedEntry(attributes, OUTCOME, outcome.name().toLowerCase(Locale.ROOT)));
        if (outcome == QueryMetricsListener.Outcome.SUCCESS) {
            queryDurationHistogram.record(took.millis(), attributes);
        }
    }

    /**
     * Buckets how long before the query started the requested range starts, as {@link SearchRequestAttributesExtractor} does for
     * Query DSL searches. A range that starts in the future lands in the first bucket.
     */
    private static String timeRangeFilterFrom(TimestampBounds requestedTimeRange, long queryStartMillis) {
        Duration age = Duration.between(requestedTimeRange.start(), Instant.ofEpochMilli(queryStartMillis));
        long ageMillis;
        try {
            ageMillis = age.toMillis();
        } catch (ArithmeticException e) {
            // A date far outside the epoch millisecond range is as old, or as far in the future, as can be.
            ageMillis = age.isNegative() ? Long.MIN_VALUE : Long.MAX_VALUE;
        }
        return TimeRangeBucket.resolve(ageMillis);
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
