/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Set;

/**
 * Runs data source-provided optimization rules as a separate pass after the main
 * {@code LogicalPlanOptimizer}.
 *
 * <p>This utility:
 * <ol>
 *   <li>Walks the logical plan tree to find all {@link DataSourcePlan} leaf nodes</li>
 *   <li>Collects {@link DataSource#optimizationRules()} from each distinct data source</li>
 *   <li>Runs the collected rules as a single batch</li>
 * </ol>
 *
 * <p>The separate pass ensures that data source rules see a fully simplified/normalized
 * tree (constant folding done, boolean logic simplified, redundant operations removed).
 *
 * @see DataSource#optimizationRules()
 */
final class DataSourceOptimizer extends RuleExecutor<LogicalPlan> {

    private static final Logger logger = LogManager.getLogger(DataSourceOptimizer.class);

    private final List<Rule<?, LogicalPlan>> dataSourceRules;

    private DataSourceOptimizer(List<Rule<?, LogicalPlan>> dataSourceRules) {
        this.dataSourceRules = dataSourceRules;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected Iterable<Batch<LogicalPlan>> batches() {
        if (dataSourceRules.isEmpty()) {
            return List.of();
        }
        Rule<?, LogicalPlan>[] rulesArray = dataSourceRules.toArray(new Rule[0]);
        return List.of(new Batch<>("DataSource Optimization", Limiter.ONCE, rulesArray));
    }

    /**
     * Optimize a logical plan by running data source-provided rules.
     *
     * <p>No-op if the plan contains no data source plan nodes or data sources provide no rules.
     *
     * @param plan The plan already processed by the main {@code LogicalPlanOptimizer}
     * @return The optimized plan
     */
    public static LogicalPlan optimize(LogicalPlan plan) {
        List<Rule<?, LogicalPlan>> rules = collectDataSourceRules(plan);
        if (rules.isEmpty()) {
            return plan;
        }
        logger.debug("Running [{}] data source optimization rules", rules.size());
        logger.trace("DataSource optimization input plan:\n{}", plan);
        DataSourceOptimizer optimizer = new DataSourceOptimizer(rules);
        LogicalPlan optimized = optimizer.execute(plan);
        logger.trace("DataSource optimization output plan:\n{}", optimized);
        return optimized;
    }

    /**
     * Walk the plan tree and collect optimization rules from all distinct data sources.
     */
    private static List<Rule<?, LogicalPlan>> collectDataSourceRules(LogicalPlan plan) {
        Set<DataSource> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        List<Rule<?, LogicalPlan>> rules = new ArrayList<>();

        plan.forEachDown(LogicalPlan.class, p -> {
            if (p instanceof DataSourcePlan cp) {
                DataSource dataSource = cp.dataSource();
                if (seen.add(dataSource)) {
                    List<Rule<?, LogicalPlan>> dataSourceRules = dataSource.optimizationRules();
                    logger.trace("Collected [{}] rules from data source [{}]", dataSourceRules.size(), dataSource.type());
                    rules.addAll(dataSourceRules);
                }
            }
        });

        return rules;
    }
}
