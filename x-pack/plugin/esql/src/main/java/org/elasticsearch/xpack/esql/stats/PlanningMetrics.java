/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.xpack.esql.capabilities.MetricsAware;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.function.Function;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.common.Strings.format;

/**
 * This class is responsible for collecting metrics related to ES|QL planning.
 */
public class PlanningMetrics {
    private final EsqlFunctionRegistry functionRegistry;
    private final Set<MetricsAware> metricsAwares = new HashSet<>();
    private final Map<String, Integer> commands = new HashMap<>();
    private final Map<String, Integer> functions = new HashMap<>();

    public PlanningMetrics(EsqlFunctionRegistry functionRegistry) {
        this.functionRegistry = functionRegistry;
    }

    private void add(Map<String, Integer> map, String key) {
        map.compute(key, (k, count) -> count == null ? 1 : count + 1);
    }

    public void command(MetricsAware command) {
        if (metricsAwares.add(command)) {
            if (command.metricName() == null) {
                throw new QlIllegalArgumentException(format("MetricsAware [{}] has no metric name", command));
            }
            add(commands, command.metricName());
        }
    }

    public void function(String name) {
        var functionName = functionRegistry.resolveAlias(name);
        if (functionRegistry.functionExists(functionName)) {
            // The metrics have been collected initially with their uppercase spelling
            add(functions, functionName.toUpperCase(Locale.ROOT));
        }
    }

    public void function(Class<? extends Function> clazz) {
        var functionName = functionRegistry.functionName(clazz);
        add(functions, functionName.toUpperCase(Locale.ROOT));
    }

    public Map<String, Integer> commands() {
        return commands;
    }

    public Map<String, Integer> functions() {
        return functions;
    }
}
