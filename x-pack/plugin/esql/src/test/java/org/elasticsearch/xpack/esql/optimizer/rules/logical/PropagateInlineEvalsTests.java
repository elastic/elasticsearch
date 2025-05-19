/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.analysis.AnalyzerContext;
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.IndexResolution;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.EsqlProject;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PropagateInlineEvalsTests extends ESTestCase {

    private static EsqlParser parser;
    private static Map<String, EsField> mapping;
    private static Analyzer analyzer;

    @BeforeClass
    public static void init() {
        parser = new EsqlParser();
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = new EsIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        IndexResolution getIndexResult = IndexResolution.valid(test);
        analyzer = new Analyzer(
            new AnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                getIndexResult,
                defaultLookupResolution(),
                new EnrichResolution(),
                defaultInferenceResolution()
            ),
            TEST_VERIFIER
        );
    }

    /**
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     *
     * Limit[1000[INTEGER],false]
     * \_InlineJoin[LEFT,[y{r}#10],[y{r}#10],[y{r}#10]]
     *   |_Eval[[gender{f}#13 AS y]]
     *   | \_EsqlProject[[emp_no{f}#11, languages{f}#14, gender{f}#13]]
     *   |   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *   \_Aggregate[STANDARD,[y{r}#10],[MAX(languages{f}#14,true[BOOLEAN]) AS max_lang, y{r}#10]]
     *     \_StubRelation[[emp_no{f}#11, languages{f}#14, gender{f}#13, y{r}#10]]
     */
    public void testGroupingAliasingMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINESTATS", EsqlCapabilities.Cap.INLINESTATS_V6.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inlinestats max_lang = MAX(languages) BY y = gender
            """, LogicalPlanOptimizerTests.SubstitutionOnlyOptimizer.INSTANCE);

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        var project = as(leftEval.child(), EsqlProject.class);

        assertThat(Expressions.names(project.projections()), contains("emp_no", "languages", "gender"));

        as(project.child(), EsRelation.class);
        var rightAgg = as(inline.right(), Aggregate.class);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "y"));

        var groupings = rightAgg.groupings();
        var aggs = rightAgg.aggregates();
        var ref = as(groupings.get(0), ReferenceAttribute.class);
        assertThat(aggs.get(1), is(ref));
        assertThat(leftEval.fields(), hasSize(1));
        assertThat(leftEval.fields().get(0).toAttribute(), is(ref)); // the only grouping is passed as eval on the join's left hand side
        assertThat(leftEval.fields().get(0).name(), is("y"));
    }

    /**
     * Expects after running the {@link LogicalPlanOptimizer#substitutions()}:
     * Limit[1000[INTEGER],false]
     * \_InlineJoin[LEFT,[f{r}#18, g{r}#21, first_name_l{r}#9],[f{r}#18, g{r}#21, first_name_l{r}#9],[f{r}#18, g{r}#21, first_name_l{
     * r}#9]]
     *   |_Eval[[LEFT(last_name{f}#27,1[INTEGER]) AS f, gender{f}#25 AS g]]
     *   | \_Eval[[LEFT(first_name{f}#24,1[INTEGER]) AS first_name_l]]
     *   |   \_EsqlProject[[emp_no{f}#23, languages{f}#26, gender{f}#25, last_name{f}#27, first_name{f}#24]]
     *   |     \_EsRelation[test][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     *   \_Aggregate[STANDARD,[f{r}#18, g{r}#21, first_name_l{r}#9],[MAX(languages{f}#26,true[BOOLEAN]) AS max_lang, MIN(languages{f}
     * #26,true[BOOLEAN]) AS min_lang, f{r}#18, g{r}#21, first_name_l{r}#9]]
     *     \_StubRelation[[emp_no{f}#23, languages{f}#26, gender{f}#25, last_name{f}#27, first_name{f}#24, first_name_l{r}#9, f{r}#18, g
     * {r}#21]]
     */
    public void testGroupingAliasingMoved_To_LeftSideOfJoin_WithExpression() {
        assumeTrue("Requires INLINESTATS", EsqlCapabilities.Cap.INLINESTATS_V6.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender, last_name, first_name
            | eval first_name_l = left(first_name, 1)
            | inlinestats max_lang = MAX(languages), min_lang = MIN(languages) BY f = left(last_name, 1), g = gender, first_name_l
            """, LogicalPlanOptimizerTests.SubstitutionOnlyOptimizer.INSTANCE);

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval1 = as(inline.left(), Eval.class);
        var leftEval2 = as(leftEval1.child(), Eval.class);
        var project = as(leftEval2.child(), EsqlProject.class);

        assertThat(Expressions.names(project.projections()), contains("emp_no", "languages", "gender", "last_name", "first_name"));

        as(project.child(), EsRelation.class);
        var rightAgg = as(inline.right(), Aggregate.class);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(
            Expressions.names(stubRelation.expressions()),
            contains("emp_no", "languages", "gender", "last_name", "first_name", "first_name_l", "f", "g")
        );

        var groupings = rightAgg.groupings();
        assertThat(groupings, hasSize(3));
        var aggs = rightAgg.aggregates();
        var ref1 = as(groupings.get(0), ReferenceAttribute.class); // f = left(last_name, 1)
        var ref2 = as(groupings.get(1), ReferenceAttribute.class); // g = gender
        var ref3 = as(groupings.get(2), ReferenceAttribute.class); // first_name_l
        assertThat(aggs.get(2), is(ref1));
        assertThat(aggs.get(3), is(ref2));
        assertThat(leftEval1.fields(), hasSize(2));
        assertThat(leftEval1.fields().get(0).toAttribute(), is(ref1)); // f = left(last_name, 1)
        assertThat(leftEval1.fields().get(0).name(), is("f"));
        assertThat(leftEval1.fields().get(1).toAttribute(), is(ref2)); // g = gender
        assertThat(leftEval1.fields().get(1).name(), is("g"));
        assertThat(leftEval2.fields(), hasSize(1));
        assertThat(leftEval2.fields().get(0).toAttribute(), is(ref3)); // first_name_l is in the second eval (the one the user added)
        assertThat(leftEval2.fields().get(0).name(), is("first_name_l"));
    }

    private LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        return optimizer.optimize(analyzer.analyze(parser.createStatement(query)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
