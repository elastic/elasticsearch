/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * This class is responsible for collecting metrics related to ES|QL planning.
 */
public class PlanningMetrics {
    private Map<String, Integer> commands = new HashMap<>();
    private Map<String, Integer> functions = new HashMap<>();

    public void gatherPreAnalysisMetrics(LogicalPlan plan) {
        plan.forEachDown(p -> add(commands, p.commandName()));
        plan.forEachExpressionDown(UnresolvedFunction.class, p -> add(functions, p.name().toUpperCase(Locale.ROOT)));
    }

    private void add(Map<String, Integer> map, String key) {
        Integer cmd = map.get(key);
        map.put(key, cmd == null ? 1 : cmd + 1);
    }

    public Map<String, Integer> commands() {
        return commands;
    }

    public Map<String, Integer> functions() {
        return functions;
    }
}
