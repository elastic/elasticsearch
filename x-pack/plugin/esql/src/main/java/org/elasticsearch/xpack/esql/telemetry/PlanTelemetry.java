/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.datasources.spi.ExternalSourceMetrics;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.plan.QuerySettingDef;
import org.elasticsearch.xpack.esql.plan.ResolvedSettings;
import org.elasticsearch.xpack.esql.view.ViewQuerySettings;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class is responsible for collecting metrics related to ES|QL planning.
 */
public class PlanTelemetry {
    private final EsqlFunctionRegistry functionRegistry;
    private final List<QuerySettingDef<?>> applicableSettings;
    private final Map<String, Integer> commands = new HashMap<>();
    private final Map<String, Integer> functions = new HashMap<>();
    private final Map<String, Integer> settings = new HashMap<>();
    private final Map<String, String> resolvedSettings = new HashMap<>();
    private final Map<String, Integer> viewSettings = new HashMap<>();
    private final Map<String, Set<String>> viewResolvedSettings = new HashMap<>();
    private Integer linkedProjectsCount = null;
    private final Set<String> externalStorageTypes = new TreeSet<>();
    private final Set<String> externalFormats = new TreeSet<>();

    public PlanTelemetry(EsqlFunctionRegistry functionRegistry, List<QuerySettingDef<?>> applicableSettings) {
        this.functionRegistry = functionRegistry;
        this.applicableSettings = applicableSettings;
    }

    private static void add(Map<String, Integer> map, String key) {
        map.compute(key.toUpperCase(Locale.ROOT), (k, count) -> count == null ? 1 : count + 1);
    }

    public EsqlFunctionRegistry functionRegistry() {
        return functionRegistry;
    }

    public void linkedProjectsCount(int linkedProjectsCount) {
        this.linkedProjectsCount = linkedProjectsCount;
    }

    public Integer linkedProjectsCount() {
        return linkedProjectsCount;
    }

    /**
     * Records one external relation of the analyzed plan by its storage type and format, so the query's labels can be derived
     * from every source it reads.
     */
    public void externalRelation(String storageType, String format) {
        externalStorageTypes.add(storageType);
        externalFormats.add(format);
    }

    /**
     * The storage-type label of the external sources this query read: the one type when they all agree,
     * {@link ExternalSourceMetrics#MIXED} when they differ, and {@code null} when it read none.
     */
    @Nullable
    public String externalStorageType() {
        return collapse(externalStorageTypes);
    }

    /** The format label of the external sources this query read, with the same rule as {@link #externalStorageType()}. */
    @Nullable
    public String externalFormat() {
        return collapse(externalFormats);
    }

    @Nullable
    private static String collapse(Set<String> values) {
        if (values.isEmpty()) {
            return null;
        }
        return values.size() == 1 ? values.iterator().next() : ExternalSourceMetrics.MIXED;
    }

    public void command(TelemetryAware command) {
        Check.notNull(command.telemetryLabel(), "TelemetryAware [{}] has no telemetry label", command);
        add(commands, command.telemetryLabel());
    }

    public void function(String name) {
        var functionName = functionRegistry.resolveAlias(name);
        if (functionRegistry.functionExists(functionName)) {
            // The metrics have been collected initially with their uppercase spelling
            add(functions, functionName);
        }
    }

    public void function(Class<? extends Function> clazz) {
        add(functions, functionRegistry.snapshotRegistry().functionName(clazz));
    }

    public void setting(String name) {
        add(settings, name);
    }

    public void resolvedSettings(ResolvedSettings resolvedSettings) {
        for (QuerySettingDef<?> def : applicableSettings) {
            this.resolvedSettings.put(def.name().toUpperCase(Locale.ROOT), def.telemetryLabel(resolvedSettings));
        }
    }

    /**
     * Records the settings of the views the query resolved, apart from the caller's and the way {@link #setting} and
     * {@link #resolvedSettings} record those: one usage of a setting per view whose definition {@code SET}s it explicitly,
     * and every value a view body resolved a setting to, whether its definition {@code SET} it or left it at its default.
     */
    public void viewSettings(Collection<ViewQuerySettings> views) {
        for (ViewQuerySettings view : views) {
            ResolvedSettings resolved = view.asResolvedSettings();
            for (QuerySettingDef<?> def : ViewQuerySettings.SETTINGS) {
                if (applicableSettings.contains(def) == false) {
                    continue;
                }
                if (view.supplied().contains(def.name())) {
                    add(viewSettings, def.name());
                }
                viewResolvedSettings.computeIfAbsent(def.name().toUpperCase(Locale.ROOT), k -> new TreeSet<>())
                    .add(def.telemetryLabel(resolved));
            }
        }
    }

    public Map<String, Integer> commands() {
        return commands;
    }

    public Map<String, Integer> functions() {
        return functions;
    }

    public Map<String, Integer> settings() {
        return settings;
    }

    public Map<String, String> resolvedSettings() {
        return resolvedSettings;
    }

    /** Number of resolved views whose definition {@code SET}s each setting. */
    public Map<String, Integer> viewSettings() {
        return viewSettings;
    }

    /** Every value resolved view definitions {@code SET} each setting to; a query with several such views may have several. */
    public Map<String, Set<String>> viewResolvedSettings() {
        return viewResolvedSettings;
    }
}
