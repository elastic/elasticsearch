/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.spi;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.rule.Rule;
import org.elasticsearch.xpack.esql.rule.RuleExecutor;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Runs data source-provided optimization rules as a separate pass after the main
 * {@code LogicalPlanOptimizer}.
 *
 * <p>Constructed with all registered data sources. Rules are collected once at
 * construction time — the rule set is fixed and does not depend on which plan
 * is being optimized. Each rule is responsible for pattern-matching on the plan
 * nodes it cares about (including verifying data source identity via
 * {@code plan.dataSource() == this}).
 *
 * <p>The separate pass ensures that data source rules see a fully simplified/normalized
 * tree (constant folding done, boolean logic simplified, redundant operations removed).
 *
 * @see DataSource#optimizationRules()
 */
final class DataSourceOptimizer extends RuleExecutor<LogicalPlan> {

    private static final Logger logger = LogManager.getLogger(DataSourceOptimizer.class);

    private final List<Rule<?, LogicalPlan>> dataSourceRules;

    /**
     * Create an optimizer with rules from all registered data sources.
     *
     * @param dataSources All registered data sources
     */
    DataSourceOptimizer(Collection<DataSource> dataSources) {
        this.dataSourceRules = collectDataSourceRules(dataSources);
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
     * Optimize a logical plan by running the fixed set of data source rules.
     *
     * <p>No-op if no data sources provided rules at construction time.
     *
     * @param plan The plan already processed by the main {@code LogicalPlanOptimizer}
     * @return The optimized plan
     */
    LogicalPlan optimize(LogicalPlan plan) {
        if (dataSourceRules.isEmpty()) {
            return plan;
        }
        logger.debug("Running [{}] data source optimization rules", dataSourceRules.size());
        logger.trace("DataSource optimization input plan:\n{}", plan);
        LogicalPlan optimized = execute(plan);
        logger.trace("DataSource optimization output plan:\n{}", optimized);
        return optimized;
    }

    /**
     * Collect optimization rules from all registered data sources.
     */
    private static List<Rule<?, LogicalPlan>> collectDataSourceRules(Collection<DataSource> dataSources) {
        List<Rule<?, LogicalPlan>> rules = new ArrayList<>();
        for (DataSource dataSource : dataSources) {
            List<Rule<?, LogicalPlan>> dataSourceRules = dataSource.optimizationRules();
            if (dataSourceRules.isEmpty() == false) {
                logger.trace("Collected [{}] rules from data source [{}]", dataSourceRules.size(), dataSource.type());
                rules.addAll(dataSourceRules);
            }
        }
        return rules;
    }
}
