/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.stats;

import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.BitSet;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class PlanningMetrics {
    Map<String, Integer> commands = new HashMap<>();
    Map<String, Integer> functions = new HashMap<>();

    public final BitSet legacyBitset = new BitSet(FeatureMetric.values().length);

    public void addCommand(LogicalPlan cmd) {
        FeatureMetric.set(cmd, legacyBitset);
        add(commands, cmd.commandName());
    }

    public void addFunction(String name) {
        add(functions, name);
    }

    private void add(Map<String, Integer> map, String item) {
        String key = item.toLowerCase(Locale.ROOT);
        Integer cmd = map.get(key);
        map.put(key, cmd == null ? 1 : cmd + 1);
    }

    public void publish(Metrics metrics) {
        commands.entrySet().forEach(x -> metrics.incCommand(x.getKey(), x.getValue()));
        functions.entrySet().forEach(x -> metrics.incFunction(x.getKey(), x.getValue()));
        publishLegacyMetrics(metrics);
    }

    private void publishLegacyMetrics(Metrics metrics) {
        for (int i = legacyBitset.nextSetBit(0); i >= 0; i = legacyBitset.nextSetBit(i + 1)) {
            metrics.inc(FeatureMetric.values()[i]);
        }
    }

    public void gatherPreAnalysisMetrics(LogicalPlan plan) {
        plan.forEachDown(p -> addCommand(p));
        plan.forEachExpressionDown(UnresolvedFunction.class, p -> addFunction(p.name().toUpperCase(Locale.ROOT)));
    }

}
