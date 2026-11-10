/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.Build;
import org.elasticsearch.common.metrics.CounterMetric;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.xpack.core.watcher.common.stats.Counters;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.function.FunctionDefinition;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.QuerySettings;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Feature;
import org.elasticsearch.xpack.esql.telemetry.KibanaExecutionContextClassifier.Interaction;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLongArray;

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
    protected static final String ENTRY_POINTS_PREFIX = "entry_points.";
    protected static final String LEADING_COMMANDS_PREFIX = "leading_commands.";
    protected static final String DIRECT_CLIENTS_PREFIX = "direct_clients.";
    protected static final String KIBANA_FEATURES_PREFIX = "kibana_features.";
    protected static final String KIBANA_INTERACTIONS_PREFIX = "kibana_interactions.";
    protected static final String QUERY_PATHS_PREFIX = QueryPath.USAGE_KEY + ".";
    protected static final String FEATURES_PREFIX = "features.";
    protected static final String SETTINGS_PREFIX = "settings.";
    protected static final String RESOLVED_SETTINGS_PREFIX = "resolved_settings.";
    protected static final String VIEW_SETTINGS_PREFIX = "view_settings.";
    protected static final String VIEW_RESOLVED_SETTINGS_PREFIX = "view_resolved_settings.";
    protected static final String FUNC_PREFIX = "functions.";
    protected static final String TOOK_PREFIX = "took.";

    private static final int PATH_TOTAL = 0;
    private static final int PATH_FAILED = 1;
    private static final int PATH_COMMANDS = 2;

    // map that holds total/failed counters for each client type (each known Elastic product, other, rest)
    private final Map<QueryMetric, Map<OperationType, CounterMetric>> opsByTypeMetrics;
    private final Map<QueryEntryPoint, Map<OperationType, CounterMetric>> opsByEntryPointMetrics;
    private final Map<LeadingCommand, Map<OperationType, CounterMetric>> opsByLeadingCommandMetrics;
    private final Map<DirectClient, Map<OperationType, CounterMetric>> opsByDirectClientMetrics;
    private final Map<Feature, Map<OperationType, CounterMetric>> opsByKibanaFeatureMetrics;
    private final Map<Interaction, Map<OperationType, CounterMetric>> opsByKibanaInteractionMetrics;
    // Created on a path's first query, so only paths some query took are kept and reported. Each array holds the path's total,
    // its failed count, then one count per FeatureMetric ordinal. Every level is a fixed list, so the number of paths is bounded:
    // (Kibana features + direct clients + the other callers) x leading commands.
    private final Map<QueryPath, AtomicLongArray> queryPathMetrics = new ConcurrentHashMap<>();
    // map that holds one counter per esql query "feature" (eval, sort, limit, where....)
    private final Map<FeatureMetric, CounterMetric> featuresMetrics;
    // map that holds one counter per esql query setting (unmapped_fields, time_zone, etc.)
    private final Map<String, CounterMetric> settingsMetrics;
    // map that holds one counter per esql query setting's resolved value (e.g. unmapped_fields.nullify, approximation.true)
    private final Map<String, Map<String, CounterMetric>> resolvedSettingsMetrics;
    // the same two maps for the settings view definitions apply to their own bodies, counted apart from the caller's
    private final Map<String, CounterMetric> viewSettingsMetrics;
    private final Map<String, Map<String, CounterMetric>> viewResolvedSettingsMetrics;
    private final Map<String, CounterMetric> functionMetrics;
    private final TookMetrics tookMetrics = new TookMetrics();

    private final EsqlFunctionRegistry functionRegistry;
    private final List<QuerySettingDef<?>> applicableQuerySettings;
    private final List<QuerySettingDef<?>> applicableViewSettings;
    private final Map<Class<?>, String> classToFunctionName;

    /**
     * Creates a Metrics instance for production use.
     * Settings metrics are filtered based on the current build type (snapshot vs release)
     * and deployment mode (serverless vs stateful).
     */
    public Metrics(EsqlFunctionRegistry functionRegistry, boolean isServerless) {
        this(functionRegistry, Build.current().isSnapshot(), isServerless);
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
        opsByTypeMetrics = initOpsMetrics(QueryMetric.values());
        opsByEntryPointMetrics = initOpsMetrics(QueryEntryPoint.values());
        opsByLeadingCommandMetrics = initOpsMetrics(LeadingCommand.values());
        opsByDirectClientMetrics = initOpsMetrics(DirectClient.values());
        opsByKibanaFeatureMetrics = initOpsMetrics(Feature.values());
        opsByKibanaInteractionMetrics = initOpsMetrics(Interaction.values());

        Map<FeatureMetric, CounterMetric> fMap = Maps.newLinkedHashMapWithExpectedSize(FeatureMetric.values().length);
        for (FeatureMetric featureMetric : FeatureMetric.values()) {
            fMap.put(featureMetric, new CounterMetric());
        }
        featuresMetrics = Collections.unmodifiableMap(fMap);

        this.applicableQuerySettings = QuerySettings.applicableIn(isSnapshot, isServerless);
        settingsMetrics = initSettingsMetrics(applicableQuerySettings);
        resolvedSettingsMetrics = initResolvedSettingsMetrics(applicableQuerySettings);

        this.applicableViewSettings = ViewQuerySettings.SETTINGS.stream().filter(applicableQuerySettings::contains).toList();
        viewSettingsMetrics = initSettingsMetrics(applicableViewSettings);
        viewResolvedSettingsMetrics = initResolvedSettingsMetrics(applicableViewSettings);

        functionMetrics = initFunctionMetrics();
    }

    private static <E extends Enum<E>> Map<E, Map<OperationType, CounterMetric>> initOpsMetrics(E[] values) {
        Map<E, Map<OperationType, CounterMetric>> map = new LinkedHashMap<>();
        for (E value : values) {
            Map<OperationType, CounterMetric> metricsMap = Maps.newLinkedHashMapWithExpectedSize(OperationType.values().length);
            for (OperationType type : OperationType.values()) {
                metricsMap.put(type, new CounterMetric());
            }
            map.put(value, Collections.unmodifiableMap(metricsMap));
        }
        return Collections.unmodifiableMap(map);
    }

    private static Map<String, CounterMetric> initSettingsMetrics(List<QuerySettingDef<?>> settings) {
        Map<String, CounterMetric> sMap = Maps.newLinkedHashMapWithExpectedSize(settings.size());
        for (var def : settings) {
            sMap.put(def.name(), new CounterMetric());
        }
        return Collections.unmodifiableMap(sMap);
    }

    private static Map<String, Map<String, CounterMetric>> initResolvedSettingsMetrics(List<QuerySettingDef<?>> settings) {
        Map<String, Map<String, CounterMetric>> rsMap = Maps.newLinkedHashMapWithExpectedSize(settings.size());
        for (var def : settings) {
            Map<String, CounterMetric> valueMap = Maps.newLinkedHashMapWithExpectedSize(def.telemetryLabels().size());
            for (String label : def.telemetryLabels()) {
                valueMap.put(label, new CounterMetric());
            }
            rsMap.put(def.name(), Collections.unmodifiableMap(valueMap));
        }
        return Collections.unmodifiableMap(rsMap);
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

    /**
     * Increments the "total" counter of the API the query came in on. Called once per query.
     */
    public void total(QueryEntryPoint entryPoint) {
        opsByEntryPointMetrics.get(entryPoint).get(OperationType.TOTAL).inc();
    }

    public void failed(QueryEntryPoint entryPoint) {
        opsByEntryPointMetrics.get(entryPoint).get(OperationType.FAILED).inc();
    }

    /**
     * Increments the "total" counter of the query's first source command. Called once per query, when it finishes.
     */
    public void total(LeadingCommand leadingCommand) {
        opsByLeadingCommandMetrics.get(leadingCommand).get(OperationType.TOTAL).inc();
    }

    public void failed(LeadingCommand leadingCommand) {
        opsByLeadingCommandMetrics.get(leadingCommand).get(OperationType.FAILED).inc();
    }

    /**
     * Increments the "total" counter of the tool that sent a query without an Elastic product origin. Called once per query.
     */
    public void total(DirectClient directClient) {
        opsByDirectClientMetrics.get(directClient).get(OperationType.TOTAL).inc();
    }

    public void failed(DirectClient directClient) {
        opsByDirectClientMetrics.get(directClient).get(OperationType.FAILED).inc();
    }

    /**
     * Increments the "total" counters of the Kibana feature that sent a query from Kibana, and of that feature's
     * {@link Interaction}. Called once per query.
     */
    public void total(Feature feature) {
        opsByKibanaFeatureMetrics.get(feature).get(OperationType.TOTAL).inc();
        opsByKibanaInteractionMetrics.get(feature.interaction()).get(OperationType.TOTAL).inc();
    }

    public void failed(Feature feature) {
        opsByKibanaFeatureMetrics.get(feature).get(OperationType.FAILED).inc();
        opsByKibanaInteractionMetrics.get(feature.interaction()).get(OperationType.FAILED).inc();
    }

    /**
     * Counts a finished query under its {@link QueryPath}, with each {@link FeatureMetric} it used, as {@code features.*} counts
     * them. Called once per query.
     */
    public void total(QueryPath path, BitSet commands) {
        AtomicLongArray counts = queryPathCounts(path);
        counts.incrementAndGet(PATH_TOTAL);
        for (int i = commands.nextSetBit(0); i >= 0; i = commands.nextSetBit(i + 1)) {
            counts.incrementAndGet(PATH_COMMANDS + i);
        }
    }

    public void failed(QueryPath path) {
        queryPathCounts(path).incrementAndGet(PATH_FAILED);
    }

    private AtomicLongArray queryPathCounts(QueryPath path) {
        return queryPathMetrics.computeIfAbsent(path, p -> new AtomicLongArray(PATH_COMMANDS + FeatureMetric.values().length));
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

    public void incResolvedSettings(ResolvedSettings resolvedSettings) {
        for (QuerySettingDef<?> def : applicableQuerySettings) {
            var counters = resolvedSettingsMetrics.get(def.name());
            assert counters != null;
            var counter = counters.get(def.telemetryLabel(resolvedSettings));
            assert counter != null;
            counter.inc();
        }
    }

    /**
     * Counts, once per query, the settings of the views a query resolved, the way {@link #incSetting} and
     * {@link #incResolvedSettings} count the caller's: each setting some view's definition {@code SET}s explicitly, and
     * each value a view body resolved a setting to, whether its definition {@code SET} it or left it at its default.
     * The caller's own counters never include a view's.
     */
    public void incViewSettings(Collection<ViewQuerySettings> views) {
        Set<CounterMetric> counted = new HashSet<>();
        for (ViewQuerySettings view : views) {
            ResolvedSettings resolved = view.asResolvedSettings();
            for (QuerySettingDef<?> def : applicableViewSettings) {
                if (view.supplied().contains(def.name())) {
                    counted.add(viewSettingsMetrics.get(def.name()));
                }
                var counter = viewResolvedSettingsMetrics.get(def.name()).get(def.telemetryLabel(resolved));
                assert counter != null;
                counted.add(counter);
            }
        }
        counted.forEach(CounterMetric::inc);
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

        opsCounters(counters, ENTRY_POINTS_PREFIX, opsByEntryPointMetrics);
        opsCounters(counters, LEADING_COMMANDS_PREFIX, opsByLeadingCommandMetrics);
        opsCounters(counters, DIRECT_CLIENTS_PREFIX, opsByDirectClientMetrics);
        opsCounters(counters, KIBANA_FEATURES_PREFIX, opsByKibanaFeatureMetrics);
        opsCounters(counters, KIBANA_INTERACTIONS_PREFIX, opsByKibanaInteractionMetrics);
        queryPathCounters(counters);

        // features metrics
        for (Entry<FeatureMetric, CounterMetric> entry : featuresMetrics.entrySet()) {
            counters.inc(FEATURES_PREFIX + entry.getKey().toString(), entry.getValue().count());
        }

        settingsCounters(counters, SETTINGS_PREFIX, settingsMetrics);
        resolvedSettingsCounters(counters, RESOLVED_SETTINGS_PREFIX, resolvedSettingsMetrics);
        settingsCounters(counters, VIEW_SETTINGS_PREFIX, viewSettingsMetrics);
        resolvedSettingsCounters(counters, VIEW_RESOLVED_SETTINGS_PREFIX, viewResolvedSettingsMetrics);

        // function metrics
        for (Entry<String, CounterMetric> entry : functionMetrics.entrySet()) {
            counters.inc(FUNC_PREFIX + entry.getKey(), entry.getValue().count());
        }

        tookMetrics.counters(TOOK_PREFIX, counters);

        return counters;
    }

    private static <E extends Enum<E>> void opsCounters(
        Counters counters,
        String prefix,
        Map<E, Map<OperationType, CounterMetric>> metrics
    ) {
        for (Entry<E, Map<OperationType, CounterMetric>> entry : metrics.entrySet()) {
            for (OperationType type : OperationType.values()) {
                counters.inc(prefix + entry.getKey() + "." + type, entry.getValue().get(type).count());
            }
        }
    }

    /**
     * Adds {@code query_paths.<caller>.<source>.<leading command>.total|failed|commands.<command>} for every count above zero.
     */
    private void queryPathCounters(Counters counters) {
        FeatureMetric[] commands = FeatureMetric.values();
        for (Entry<QueryPath, AtomicLongArray> entry : queryPathMetrics.entrySet()) {
            QueryPath path = entry.getKey();
            AtomicLongArray counts = entry.getValue();
            String prefix = QUERY_PATHS_PREFIX + path.caller() + "." + path.source() + "." + path.leadingCommand() + ".";
            nonZero(counters, prefix + QueryPath.TOTAL, counts.get(PATH_TOTAL));
            nonZero(counters, prefix + QueryPath.FAILED, counts.get(PATH_FAILED));
            for (FeatureMetric command : commands) {
                nonZero(counters, prefix + QueryPath.COMMANDS + "." + command, counts.get(PATH_COMMANDS + command.ordinal()));
            }
        }
    }

    private static void nonZero(Counters counters, String key, long count) {
        if (count > 0) {
            counters.inc(key, count);
        }
    }

    private static void settingsCounters(Counters counters, String prefix, Map<String, CounterMetric> metrics) {
        for (Entry<String, CounterMetric> entry : metrics.entrySet()) {
            counters.inc(prefix + entry.getKey(), entry.getValue().count());
        }
    }

    private static void resolvedSettingsCounters(Counters counters, String prefix, Map<String, Map<String, CounterMetric>> metrics) {
        for (Entry<String, Map<String, CounterMetric>> setting : metrics.entrySet()) {
            for (Entry<String, CounterMetric> value : setting.getValue().entrySet()) {
                counters.inc(prefix + setting.getKey() + "." + value.getKey(), value.getValue().count());
            }
        }
    }
}
