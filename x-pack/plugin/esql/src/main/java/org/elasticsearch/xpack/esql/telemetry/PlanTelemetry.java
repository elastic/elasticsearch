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

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * This class is responsible for collecting metrics related to ES|QL planning.
 */
public class PlanTelemetry {
    private final EsqlFunctionRegistry functionRegistry;
    private final Map<String, Integer> commands = new HashMap<>();
    private final Map<String, Integer> functions = new HashMap<>();
    private final Map<String, Integer> settings = new HashMap<>();
    private Integer linkedProjectsCount = null;
    private boolean externalSource = false;
    private final Set<String> externalStorageTypes = new TreeSet<>();
    private final Set<String> externalFormats = new TreeSet<>();

    public PlanTelemetry(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
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
     * Marks that the query touched an ES|QL external data source (an {@code ExternalRelation} was present in the
     * analyzed plan). Read at query completion so the coordinator emits external-source-scoped operational metrics
     * only for queries that actually scanned an external source.
     */
    public void externalSource(boolean externalSource) {
        this.externalSource = externalSource;
    }

    /** Whether the analyzed plan touched an ES|QL external data source; gates the external-source query metrics at completion. */
    public boolean externalSource() {
        return externalSource;
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

    public Map<String, Integer> commands() {
        return commands;
    }

    public Map<String, Integer> functions() {
        return functions;
    }

    public Map<String, Integer> settings() {
        return settings;
    }
}
