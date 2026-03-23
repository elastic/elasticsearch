/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.telemetry;

import org.elasticsearch.xpack.esql.capabilities.TelemetryAware;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.core.util.Check;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This class is responsible for collecting metrics related to ES|QL planning.
 */
public class PlanTelemetry {
    private final EsqlFunctionRegistry functionRegistry;
    private final Map<String, Integer> commands = new HashMap<>();
    private final Map<String, Integer> functions = new HashMap<>();
    private final Map<String, Integer> settings = new HashMap<>();
    private Integer linkedProjectsCount = null;

    public PlanTelemetry(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    private static void add(Map<String, Integer> map, String key) {
        map.compute(key.toUpperCase(Locale.ROOT), (k, count) -> count == null ? 1 : count + 1);
    }

    public void linkedProjectsCount(int linkedProjectsCount) {
        this.linkedProjectsCount = linkedProjectsCount;
    }

    public Integer linkedProjectsCount() {
        return linkedProjectsCount;
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
