/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.mapper.MapperServiceTestCase;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.analysis.MutableAnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.Verifier;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FieldAttribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.Literal;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.enrich.AbstractLookupService;
import org.elasticsearch.xpack.esql.enrich.LookupFromIndexService;
import org.elasticsearch.xpack.esql.enrich.MatchConfig;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.Equals;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.ParameterizedQuery;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.EvalExec;
import org.elasticsearch.xpack.esql.plan.physical.FieldExtractExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.LookupJoinExec;
import org.elasticsearch.xpack.esql.plan.physical.ParameterizedQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.ProjectExec;
import org.elasticsearch.xpack.esql.planner.PlannerSettings;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.session.Configuration;
import org.elasticsearch.xpack.esql.stats.SearchStats;
import org.elasticsearch.xpack.esql.telemetry.Metrics;
import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_CFG;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_SEARCH_STATS;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.emptyInferenceResolution;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.Analyzer.ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Tests for {@link LookupPhysicalPlanOptimizer}, verifying that the lookup-node planning pipeline
 * (buildLocalLogicalPlan -> LookupLogicalOptimizer -> LocalMapper -> LookupPhysicalPlanOptimizer) produces correct physical plans.
 */
public class LookupPhysicalPlanOptimizerTests extends MapperServiceTestCase {

    private Analyzer analyzer;
    private TestPlannerOptimizer plannerOptimizer;

    @Before
    public void init() {
        Configuration config = TEST_CFG;
        Map<String, EsField> mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));

        analyzer = new Analyzer(
            testAnalyzerContext(
                config,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
                defaultLookupResolution(),
                new EnrichResolution(),
                emptyInferenceResolution()
            ),
            new Verifier(new Metrics(new EsqlFunctionRegistry(), true, true), new XPackLicenseState(() -> 0L))
        );
        plannerOptimizer = new TestPlannerOptimizer(config, analyzer);
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }

    /**
     * Simple lookup with no filters.
     * Expects: ProjectExec -> FieldExtractExec -> ParameterizedQueryExec(query=null)
     */
    public void testSimpleLookup() {
        PhysicalPlan plan = optimizeLookupPlan("FROM test | LOOKUP JOIN test_lookup ON emp_no");

        ProjectExec project = as(plan, ProjectExec.class);
        FieldExtractExec fieldExtract = as(project.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(fieldExtract.child(), ParameterizedQueryExec.class);

        assertThat(paramQuery.query(), nullValue());
        assertThat(paramQuery.emptyResult(), is(false));
        assertThat(fieldExtract.attributesToExtract().isEmpty(), is(false));
    }

    /**
     * WHERE clause with a pushable right-only filter (equality on a keyword field).
     * The logical optimizer pushes it into the join's right side, and the lookup physical optimizer
     * pushes it down to ParameterizedQueryExec.query().
     */
    public void testPushableRightOnlyFilter() {
        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """);

        ProjectExec project = as(plan, ProjectExec.class);
        FieldExtractExec fieldExtract = as(project.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(fieldExtract.child(), ParameterizedQueryExec.class);

        QueryBuilder query = paramQuery.query();
        assertNotNull("Expected filter to be pushed to ParameterizedQueryExec", query);
        assertThat(query.toString(), containsString("language_name"));
        assertThat(paramQuery.emptyResult(), is(false));
    }

    /**
     * WHERE clause with a non-pushable right-only filter (LENGTH function comparison).
     * The logical optimizer pushes it into the join's right side, but the lookup physical optimizer
     * cannot push it to Lucene, so it stays as FilterExec.
     */
    public void testNonPushableRightOnlyFilter() {
        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE LENGTH(language_name) > 3
            """);

        ProjectExec project = as(plan, ProjectExec.class);
        FilterExec filter = as(project.child(), FilterExec.class);
        FieldExtractExec fieldExtract = as(filter.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(fieldExtract.child(), ParameterizedQueryExec.class);

        assertThat(paramQuery.query(), nullValue());
        assertThat(paramQuery.emptyResult(), is(false));
        assertThat(filter.condition().toString(), containsString("LENGTH"));
    }

    /**
     * WHERE clause with both pushable and non-pushable right-only filters.
     * The pushable part goes to ParameterizedQueryExec.query(), the non-pushable stays as FilterExec.
     */
    public void testMixedPushableAndNonPushableFilters() {
        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English" AND LENGTH(language_name) > 3
            """);

        ProjectExec project = as(plan, ProjectExec.class);
        FilterExec filter = as(project.child(), FilterExec.class);
        FieldExtractExec fieldExtract = as(filter.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(fieldExtract.child(), ParameterizedQueryExec.class);

        assertNotNull("Expected pushable filter on ParameterizedQueryExec", paramQuery.query());
        assertThat(paramQuery.query().toString(), containsString("language_name"));
        assertThat(paramQuery.emptyResult(), is(false));
        assertThat(filter.condition().toString(), containsString("LENGTH"));
    }

    /**
     * ON expression with a pushable right-only filter combined with a non-pushable WHERE clause.
     * The ON equality filter is pushed to ParameterizedQueryExec.query(), while LENGTH stays as FilterExec.
     */
    public void testOnExpressionFilterWithWhereClause() {
        assumeTrue("Requires LOOKUP JOIN on expression", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | LOOKUP JOIN languages_lookup ON languages == language_code AND language_name == "English"
            | WHERE LENGTH(language_name) > 3
            """, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        ProjectExec project = as(plan, ProjectExec.class);
        FieldExtractExec extractForJoinKey = as(project.child(), FieldExtractExec.class);

        // LENGTH is not pushable, stays as FilterExec
        FilterExec filter = as(extractForJoinKey.child(), FilterExec.class);
        assertThat(filter.condition().toString(), containsString("LENGTH"));

        FieldExtractExec extractForFilter = as(filter.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(extractForFilter.child(), ParameterizedQueryExec.class);

        // language_name == "English" (pushable ON right-only filter) is pushed to query
        assertNotNull("Expected pushable ON filter on ParameterizedQueryExec", paramQuery.query());
        assertThat(paramQuery.query().toString(), containsString("language_name"));
        assertThat(paramQuery.emptyResult(), is(false));

        // joinOnConditions is the left-right join key comparison
        assertNotNull("Expected join on conditions", paramQuery.joinOnConditions());
        assertThat(paramQuery.joinOnConditions().toString(), containsString("languages"));
    }

    /**
     * Filter through an EvalExec that aliases a field. The filter references the alias,
     * which should be resolved and pushed to ParameterizedQueryExec.query().
     * This tests the FilterExec -> EvalExec -> ParameterizedQueryExec path in PushFiltersToSource.
     */
    public void testPushFilterThroughEvalExec() {
        Source src = Source.EMPTY;

        FieldAttribute langName = new FieldAttribute(
            src,
            "language_name",
            new EsField("language_name", DataType.KEYWORD, Map.of(), true, EsField.TimeSeriesFieldType.NONE)
        );
        List<MatchConfig> matchFields = List.of(new MatchConfig("language_code", 0, DataType.KEYWORD));
        FieldAttribute docAttr = new FieldAttribute(src, null, null, EsQueryExec.DOC_ID_FIELD.getName(), EsQueryExec.DOC_ID_FIELD);
        List<Attribute> pqOutput = List.of(docAttr, AbstractLookupService.LOOKUP_POSITIONS_FIELD, langName);

        ParameterizedQuery pq = new ParameterizedQuery(src, pqOutput, matchFields, null);
        Alias alias = new Alias(src, "ln", langName);
        Eval eval = new Eval(src, pq, List.of(alias));
        Equals condition = new Equals(src, alias.toAttribute(), Literal.keyword(src, "English"), null);
        LogicalPlan filter = new org.elasticsearch.xpack.esql.plan.logical.Filter(src, eval, condition);
        LogicalPlan project = new Project(
            src,
            filter,
            List.of(AbstractLookupService.LOOKUP_POSITIONS_FIELD, langName, alias.toAttribute())
        );

        PhysicalPlan plan = LookupFromIndexService.createLookupPhysicalPlan(
            project,
            TEST_CFG,
            PlannerSettings.DEFAULTS,
            FoldContext.small(),
            TEST_SEARCH_STATS,
            new EsqlFlags(true)
        );

        // The filter on 'ln' (alias for language_name) should be resolved and pushed to ParameterizedQueryExec.
        ProjectExec resultProject = as(plan, ProjectExec.class);
        EvalExec evalExec = as(resultProject.child(), EvalExec.class);
        FieldExtractExec extract = as(evalExec.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(extract.child(), ParameterizedQueryExec.class);

        assertNotNull("Expected filter pushed through EvalExec to ParameterizedQueryExec", paramQuery.query());
        assertThat(paramQuery.query().toString(), containsString("language_name"));
        assertThat(paramQuery.emptyResult(), is(false));
    }

    /**
     * Filter that becomes always-true due to missing field stats should be pruned during logical optimization.
     * "language_name IS NULL" with language_name missing → "null IS NULL" → true → filter removed.
     * The null Eval from ReplaceFieldWithConstantOrNull should be pruned by the physical optimizer since
     * the field is not needed for extraction.
     */
    public void testFilterOnMissingFieldFoldedToTrue() {
        EsqlTestUtils.TestConfigurableSearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );

        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name IS NULL
            """, stats);

        ProjectExec project = as(plan, ProjectExec.class);
        EvalExec eval = as(project.child(), EvalExec.class);
        ParameterizedQueryExec paramQuery = as(eval.child(), ParameterizedQueryExec.class);
        assertThat("Filter should have been pruned, no query on ParameterizedQueryExec", paramQuery.query(), nullValue());
        assertThat(paramQuery.emptyResult(), is(false));
    }

    /**
     * Filter on a missing field with equality (e.g. {@code language_name == "English"}) folds to {@code null == "English"} → null,
     * which marks the {@link ParameterizedQueryExec} as {@code emptyResult=true} instead of collapsing the plan.
     */
    public void testFilterOnMissingFieldFoldedToEmpty() {
        EsqlTestUtils.TestConfigurableSearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );

        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name == "English"
            """, stats);

        ProjectExec project = as(plan, ProjectExec.class);
        EvalExec eval = as(project.child(), EvalExec.class);
        ParameterizedQueryExec paramQuery = as(eval.child(), ParameterizedQueryExec.class);
        assertThat(paramQuery.emptyResult(), is(true));
    }

    /**
     * When a missing field is dropped from the output, it never appears in the lookup plan's addedFields,
     * so no null Eval is needed. Using expression-based join so that language_code remains as an
     * extractable added field after dropping language_name.
     * Expects: ProjectExec -> FieldExtractExec -> ParameterizedQueryExec
     */
    public void testDropMissingFieldPrunesEval() {
        assumeTrue("Requires LOOKUP JOIN on expression", EsqlCapabilities.Cap.LOOKUP_JOIN_WITH_FULL_TEXT_FUNCTION.isEnabled());

        EsqlTestUtils.TestConfigurableSearchStats stats = new EsqlTestUtils.TestConfigurableSearchStats().exclude(
            EsqlTestUtils.TestConfigurableSearchStats.Config.EXISTS,
            "language_name"
        );

        PhysicalPlan plan = optimizeLookupPlan("""
            FROM test
            | LOOKUP JOIN languages_lookup ON languages == language_code
            | DROP language_name
            """, stats, ESQL_LOOKUP_JOIN_FULL_TEXT_FUNCTION);

        ProjectExec project = as(plan, ProjectExec.class);
        FieldExtractExec fieldExtract = as(project.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery = as(fieldExtract.child(), ParameterizedQueryExec.class);
        assertThat(paramQuery.query(), nullValue());
        assertThat(paramQuery.emptyResult(), is(false));
    }

    /**
     * Two consecutive LOOKUP JOINs: first on test_lookup (by emp_no), then on languages_lookup (by language_code).
     * Each join's right side is independently planned on its respective lookup node.
     */
    public void testTwoLookupJoins() {
        List<PhysicalPlan> plans = optimizeAllLookupPlans("""
            FROM test
            | LOOKUP JOIN test_lookup ON emp_no
            | RENAME languages AS language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """, null, TEST_SEARCH_STATS);

        assertThat(plans, hasSize(2));

        // Outermost join (languages_lookup) is found first in tree traversal
        PhysicalPlan languagesPlan = plans.get(0);
        ProjectExec project0 = as(languagesPlan, ProjectExec.class);
        FieldExtractExec extract0 = as(project0.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery0 = as(extract0.child(), ParameterizedQueryExec.class);
        assertThat(paramQuery0.query(), nullValue());
        assertThat(paramQuery0.emptyResult(), is(false));

        // Inner join (test_lookup) is found second
        PhysicalPlan testPlan = plans.get(1);
        ProjectExec project1 = as(testPlan, ProjectExec.class);
        FieldExtractExec extract1 = as(project1.child(), FieldExtractExec.class);
        ParameterizedQueryExec paramQuery1 = as(extract1.child(), ParameterizedQueryExec.class);
        assertThat(paramQuery1.query(), nullValue());
        assertThat(paramQuery1.emptyResult(), is(false));
    }

    private PhysicalPlan optimizeLookupPlan(String esql) {
        return optimizeLookupPlan(esql, TEST_SEARCH_STATS);
    }

    private PhysicalPlan optimizeLookupPlan(String esql, SearchStats searchStats) {
        List<PhysicalPlan> plans = optimizeAllLookupPlans(esql, null, searchStats);
        assertThat("Expected exactly one LOOKUP JOIN", plans, hasSize(1));
        return plans.getFirst();
    }

    private PhysicalPlan optimizeLookupPlan(String esql, TransportVersion minVersion) {
        return optimizeLookupPlan(esql, TEST_SEARCH_STATS, minVersion);
    }

    private PhysicalPlan optimizeLookupPlan(String esql, SearchStats searchStats, TransportVersion minVersion) {
        List<PhysicalPlan> plans = optimizeAllLookupPlans(esql, minVersion, searchStats);
        assertThat("Expected exactly one LOOKUP JOIN", plans, hasSize(1));
        return plans.getFirst();
    }

    /**
     * Runs the full planning pipeline and returns a lookup-node physical plan for each LookupJoinExec
     * found in the data-node plan. The plans are returned in tree traversal order (outermost join first).
     */
    private List<PhysicalPlan> optimizeAllLookupPlans(String esql, TransportVersion minVersion, SearchStats searchStats) {
        PhysicalPlan dataNodePlan;
        if (minVersion != null) {
            MutableAnalyzerContext mutableContext = (MutableAnalyzerContext) analyzer.context();
            try (
                MutableAnalyzerContext.RestoreTransportVersion restore = mutableContext.setTemporaryTransportVersionOnOrAfter(minVersion)
            ) {
                dataNodePlan = plannerOptimizer.plan(esql);
            }
        } else {
            dataNodePlan = plannerOptimizer.plan(esql);
        }

        List<LookupJoinExec> joins = findAllLookupJoins(dataNodePlan);
        assertThat("Expected at least one LookupJoinExec in the plan", joins.isEmpty(), is(false));

        List<PhysicalPlan> lookupPlans = new ArrayList<>(joins.size());
        for (LookupJoinExec join : joins) {
            lookupPlans.add(buildLookupPlan(join, searchStats));
        }
        return lookupPlans;
    }

    private static PhysicalPlan buildLookupPlan(LookupJoinExec join, SearchStats searchStats) {
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
        return LookupFromIndexService.createLookupPhysicalPlan(
            logicalPlan,
            TEST_CFG,
            PlannerSettings.DEFAULTS,
            FoldContext.small(),
            searchStats,
            new EsqlFlags(true)
        );
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
