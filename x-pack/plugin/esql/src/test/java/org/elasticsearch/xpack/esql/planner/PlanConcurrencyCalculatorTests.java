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
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.analyzerDefaultMapping;
import static org.hamcrest.Matchers.equalTo;

public class PlanConcurrencyCalculatorTests extends ESTestCase {
    public void testSimpleLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            """, 9);
    }

    public void testLimitZero() {
        assertConcurrency("FROM x | LIMIT 0", null);
    }

    public void testBiggestPragmaOverride() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            """, Integer.MAX_VALUE, Integer.MAX_VALUE);
    }

    public void testSmallestPragmaOverride() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            """, 1, 1);
    }

    public void testPragmaOverrideWithUnsupportedCommands() {
        assertConcurrency("""
            FROM x
            | WHERE salary * 2 > 5
            | LIMIT 512
            """, 1, 1);
    }

    public void testImplicitLimit() {
        assertConcurrency("""
            FROM x
            """, 9);
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
            | LIMIT 512
            | STATS COUNT(salary)
            """, 9);
    }

    public void testSortBeforeLimit() {
        assertConcurrency("""
            FROM x
            | SORT salary
            """, null);
    }

    public void testSortAfterLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            | SORT salary
            """, 9);
    }

    public void testStatsWithSortBeforeLimit() {
        assertConcurrency("""
            FROM x
            | SORT salary
            | LIMIT 512
            | STATS COUNT(salary)
            """, null);
    }

    public void testStatsWithSortAfterLimit() {
        assertConcurrency("""
            FROM x
            | SORT salary
            | LIMIT 512
            | STATS COUNT(salary)
            """, null);
    }

    public void testWhereBeforeLimit() {
        assertConcurrency("""
            FROM x
            | WHERE salary * 2 > 5
            | LIMIT 512
            """, null);
    }

    public void testWhereAfterLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            | WHERE salary * 2 > 5
            """, 9);
    }

    public void testWherePushedToLuceneQueryBeforeLimit() {
        assertConcurrency("""
            FROM x
            | WHERE first_name LIKE "A%"
            | LIMIT 512
            """, null);
    }

    public void testWherePushedToLuceneQueryAfterLimit() {
        assertConcurrency("""
            FROM x
            | LIMIT 512
            | WHERE first_name LIKE "A%"
            """, 9);
    }

    public void testExpand() {
        assertConcurrency("""
            FROM x
            | LIMIT 2048
            | MV_EXPAND salary
            | LIMIT 512
            """, 9);
    }

    public void testEval() {
        assertConcurrency("""
            FROM x
            | EVAL x=salary*2
            | LIMIT 512
            """, 9);
    }

    public void testRename() {
        assertConcurrency("""
            FROM x
            | RENAME salary as x
            | LIMIT 512
            """, 9);
    }

    public void testKeep() {
        assertConcurrency("""
            FROM x
            | KEEP salary
            | LIMIT 512
            """, 9);
    }

    public void testDrop() {
        assertConcurrency("""
            FROM x
            | DROP salary
            | LIMIT 512
            """, 9);
    }

    public void testDissect() {
        assertConcurrency("""
            FROM x
            | DISSECT first_name "%{a} %{b}"
            | LIMIT 512
            """, 9);
    }

    public void testGrok() {
        assertConcurrency("""
            FROM x
            | GROK first_name "%{EMAILADDRESS:email}"
            | LIMIT 512
            """, 9);
    }

    public void testEnrich() {
        assertConcurrency("""
            FROM x
            | ENRICH languages ON first_name
            | LIMIT 512
            """, 9);
    }

    public void testLookup() {
        assertConcurrency("""
            FROM x
            | RENAME salary as language_code
            | LOOKUP JOIN languages_lookup on language_code
            | LIMIT 512
            """, 9);
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

        Analyzer analyzer = analyzer(analyzerDefaultMapping(), TEST_VERIFIER, configuration);
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
