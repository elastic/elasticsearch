/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

/**
 * Tests for {@link LookupLogicalOptimizer}, verifying that logical optimization rules are applied
 * to the lookup node's logical plan before physical planning.
 */
public class LookupLogicalOptimizerTests extends MapperServiceTestCase {

    private Analyzer analyzer;
    private TestPlannerOptimizer plannerOptimizer;

    @Before
    public void init() {
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));

        analyzer = new Analyzer(
            testAnalyzerContext(
                TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(new EsqlFunctionRegistry(), true, true), new XPackLicenseState(() -> 0L))
        );
        plannerOptimizer = new TestPlannerOptimizer(TEST_CFG, analyzer);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    /**
     * Filter referencing a missing field should be folded away (the condition becomes null/false).
     * ReplaceFieldWithConstantOrNull replaces the missing field with null, then LookupPruneFilters
     * marks the ParameterizedQuery as emptyResult instead of collapsing the plan to LocalRelation,
     * preserving the plan structure for the LookupExecutionPlanner.
     * Expects: Project -> Eval -> ParameterizedQuery(emptyResult=true)
     */
    public void testFilterOnMissingFieldFolded() {
        EsqlTestUtils.TestConfigurableSearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );

        LogicalPlan plan = optimizeLookupLogicalPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """, stats);

        Project project = as(plan, Project.class);
        Eval eval = as(project.child(), Eval.class);
        ParameterizedQuery pq = as(eval.child(), ParameterizedQuery.class);
        assertTrue("Expected emptyResult=true on ParameterizedQuery", pq.emptyResult());
    }

    /**
     * Constant field NOT matching the filter value: {@code language_name} is a constant {@code "Spanish"},
     * but the filter is {@code WHERE language_name == "English"}.  The constant replaces the field reference,
     * the filter folds to {@code false}, and LookupPruneFilters marks the ParameterizedQuery as emptyResult.
     * Expects: Project -> ParameterizedQuery(emptyResult=true)
     */
    public void testConstantFieldMismatchFoldsToEmpty() {
        EsqlTestUtils.TestConfigurableSearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().withConstantValue(
            "language_name",
            "Spanish"
        );

        LogicalPlan plan = optimizeLookupLogicalPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """, stats);

        Project project = as(plan, Project.class);
        ParameterizedQuery pq = as(project.child(), ParameterizedQuery.class);
        assertTrue("Expected emptyResult=true on ParameterizedQuery", pq.emptyResult());
    }

    private LogicalPlan optimizeLookupLogicalPlan(String esql, SearchStats searchStats) {
        List<LogicalPlan> plans = optimizeAllLookupLogicalPlans(esql, searchStats);
        assertThat("Expected exactly one LOOKUP JOIN", plans, hasSize(1));
        return plans.getFirst();
    }

    /**
     * Runs the full planning pipeline, finds LookupJoinExec nodes, then builds and logically optimizes
     * each lookup plan. Returns the optimized logical plans in tree traversal order.
     */
    private List<LogicalPlan> optimizeAllLookupLogicalPlans(String esql, SearchStats searchStats) {
        PhysicalPlan dataNodePlan = plannerOptimizer.plan(esql);

        List<LookupJoinExec> joins = findAllLookupJoins(dataNodePlan);
        assertThat("Expected at least one LookupJoinExec in the plan", joins.isEmpty(), is(false));

        List<LogicalPlan> lookupPlans = new ArrayList<>(joins.size());
        for (LookupJoinExec join : joins) {
            lookupPlans.add(buildAndOptimizeLookupLogicalPlan(join, searchStats));
        }
        return lookupPlans;
    }

    private static LogicalPlan buildAndOptimizeLookupLogicalPlan(LookupJoinExec join, SearchStats searchStats) {
        List<MatchConfig> matchFields = new ArrayList<>(join.leftFields().size());
        for (int i = 0; i < join.leftFields().size(); i++) {
            FieldAttribute right = (FieldAttribute) join.rightFields().get(i);
            String fieldName = right.exactAttribute().fieldName().string();
            if (join.isOnJoinExpression()) {
                fieldName = join.leftFields().get(i).name();
            }
            matchFields.add(new MatchConfig(fieldName, i, join.leftFields().get(i).dataType()));
        }

        LogicalPlan logicalPlan = LookupFromIndexService.buildLocalLogicalPlan(
            join.source(),
            matchFields,
            join.joinOnConditions(),
            join.right(),
            join.addedFields().stream().map(f -> (NamedExpression) f).toList()
        );

        var context = new LocalLogicalOptimizerContext(TEST_CFG, FoldContext.small(), searchStats);
        return new LookupLogicalOptimizer(context).localOptimize(logicalPlan);
    }

    private static List<LookupJoinExec> findAllLookupJoins(PhysicalPlan plan) {
        List<LookupJoinExec> joins = new ArrayList<>();
        collectLookupJoins(plan, joins);
        return joins;
    }

    private static void collectLookupJoins(PhysicalPlan plan, List<LookupJoinExec> joins) {
        if (plan instanceof LookupJoinExec join) {
            joins.add(join);
        }
        for (PhysicalPlan child : plan.children()) {
            collectLookupJoins(child, joins);
        }
    }
}
