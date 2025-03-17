/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.PhysicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.planner.mapper.Mapper;
import org.elasticsearch.xpack.esql.plugin.QueryPragmas;
import org.elasticsearch.xpack.esql.session.Configuration;

import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.configuration;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzer;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.loadMapping;
import static org.hamcrest.Matchers.equalTo;

public class PlanConcurrencyCalculatorTests extends ESTestCase {

    public void testSimpleLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 123
            """, 123);
    }

    public void testImplicitLimit() {
        assertConcurrency("""
            FROM x
            """, 1000);
    }

    public void testStats() {
        assertConcurrency("""
            FROM x
            | STATS COUNT(salary)
            """, null);
    }

    public void testStatsWithLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 123
            | STATS COUNT(salary)
            """, 123);
    }

    public void testStatsWithSortBeforeLimit() {
        assertConcurrency("""
            FROM x
            | SORT salary
            | LIMIT 123
            | STATS COUNT(salary)
            """, null);
    }

    public void testStatsWithSortAfterLimit() {
        assertConcurrency("""
            FROM x
            | SORT salary
            | LIMIT 123
            | STATS COUNT(salary)
            """, null);
    }

    public void testSort() {
        assertConcurrency("""
            FROM x
            | SORT salary
            """, null);
    }

    private void assertConcurrency(String query, Integer expectedConcurrency) {
        assertConcurrency(query, null, expectedConcurrency);
    }

    private void assertConcurrency(String query, Integer concurrencyPragmaValue, Integer expectedConcurrency) {
        Configuration configuration = concurrencyPragmaValue == null
            ? configuration(query)
            : configuration(
                new QueryPragmas(Settings.builder().put("max_concurrent_nodes_per_cluster", concurrencyPragmaValue).build()),
                query
            );

        Analyzer analyzer = analyzer(loadMapping("mapping-basic.json", "test"), TEST_VERIFIER, configuration);
        LogicalPlan logicalPlan = AnalyzerTestUtils.analyze(query, analyzer);
        logicalPlan = new LogicalPlanOptimizer(new LogicalOptimizerContext(configuration, FoldContext.small())).optimize(logicalPlan);

        PhysicalPlan physicalPlan = new Mapper().map(logicalPlan);
        physicalPlan = new PhysicalPlanOptimizer(new PhysicalOptimizerContext(configuration)).optimize(physicalPlan);

        PhysicalPlan dataNodePlan = PlannerUtils.breakPlanBetweenCoordinatorAndDataNode(physicalPlan, configuration).v2();

        Integer actualConcurrency = PlanConcurrencyCalculator.INSTANCE.calculateNodesConcurrency(dataNodePlan, configuration);

        assertThat(actualConcurrency, equalTo(expectedConcurrency));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
