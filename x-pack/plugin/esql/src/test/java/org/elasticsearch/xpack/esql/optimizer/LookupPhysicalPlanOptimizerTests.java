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
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

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

    private PhysicalPlan optimizeLookupPlan(String esql, SearchStats searchStats) {
        PhysicalPlan dataNodePlan = plannerOptimizer.plan(esql);

        List<LookupJoinExec> joins = findAllLookupJoins(dataNodePlan);
        assertThat("Expected exactly one LOOKUP JOIN", joins, hasSize(1));
        return buildLookupPlan(joins.getFirst(), searchStats);
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
