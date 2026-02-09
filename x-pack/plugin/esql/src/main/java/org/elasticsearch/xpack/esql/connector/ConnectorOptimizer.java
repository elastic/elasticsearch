/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.connector;

import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Runs connector-provided optimization rules as a separate pass after the main
 * {@code LogicalPlanOptimizer}.
 *
 * <p>This utility:
 * <ol>
 *   <li>Walks the logical plan tree to find all {@link ConnectorPlan} leaf nodes</li>
 *   <li>Collects {@link Connector#optimizationRules()} from each distinct connector</li>
 *   <li>Runs the collected rules as a single batch</li>
 * </ol>
 *
 * <p>The separate pass ensures that connector rules see a fully simplified/normalized
 * tree (constant folding done, boolean logic simplified, redundant operations removed).
 *
 * @see Connector#optimizationRules()
 */
final class ConnectorOptimizer extends RuleExecutor<LogicalPlan> {

    private final List<Rule<?, LogicalPlan>> connectorRules;

    private ConnectorOptimizer(List<Rule<?, LogicalPlan>> connectorRules) {
        this.connectorRules = connectorRules;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected Iterable<Batch<LogicalPlan>> batches() {
        if (connectorRules.isEmpty()) {
            return List.of();
        }
        Rule<?, LogicalPlan>[] rulesArray = connectorRules.toArray(new Rule[0]);
        return List.of(new Batch<>("Connector Optimization", Limiter.ONCE, rulesArray));
    }

    /**
     * Optimize a logical plan by running connector-provided rules.
     *
     * <p>No-op if the plan contains no connector plan nodes or connectors provide no rules.
     *
     * @param plan The plan already processed by the main {@code LogicalPlanOptimizer}
     * @return The optimized plan
     */
    public static LogicalPlan optimize(LogicalPlan plan) {
        List<Rule<?, LogicalPlan>> rules = collectConnectorRules(plan);
        if (rules.isEmpty()) {
            return plan;
        }
        ConnectorOptimizer optimizer = new ConnectorOptimizer(rules);
        return optimizer.execute(plan);
    }

    /**
     * Walk the plan tree and collect optimization rules from all distinct connectors.
     */
    private static List<Rule<?, LogicalPlan>> collectConnectorRules(LogicalPlan plan) {
        Set<Connector> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Rule<?, LogicalPlan>> rules = new ArrayList<>();

        plan.forEachDown(LogicalPlan.class, p -> {
            if (p instanceof ConnectorPlan cp) {
                Connector connector = cp.connector();
                if (seen.add(connector)) {
                    rules.addAll(connector.optimizationRules());
                }
            }
        });

        return rules;
    }
}
