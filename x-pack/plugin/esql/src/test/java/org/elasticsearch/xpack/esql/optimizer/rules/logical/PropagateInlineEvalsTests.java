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
import org.elasticsearch.xpack.esql.analysis.EnrichResolution;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.type.EsField;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.EsqlFunctionRegistry;
import org.elasticsearch.xpack.esql.index.EsIndex;
import org.elasticsearch.xpack.esql.index.EsIndexGenerator;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.parser.EsqlParser;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.StubRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.junit.BeforeClass;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.TEST_VERIFIER;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.loadMapping;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.testAnalyzerContext;
import static org.elasticsearch.xpack.esql.EsqlTestUtils.withDefaultLimitWarning;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultInferenceResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.defaultLookupResolution;
import static org.elasticsearch.xpack.esql.analysis.AnalyzerTestUtils.indexResolutions;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class PropagateInlineEvalsTests extends ESTestCase {

    private static Map<String, EsField> mapping;
    private static Analyzer analyzer;

    @BeforeClass
    public static void init() {
        mapping = loadMapping("mapping-basic.json");
        EsIndex test = EsIndexGenerator.esIndex("test", mapping, Map.of("test", IndexMode.STANDARD));
        analyzer = new Analyzer(
            testAnalyzerContext(
                EsqlTestUtils.TEST_CFG,
                new EsqlFunctionRegistry(),
                indexResolutions(test),
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
     *   | \_Project[[emp_no{f}#11, languages{f}#14, gender{f}#13]]
     *   |   \_EsRelation[test][_meta_field{f}#17, emp_no{f}#11, first_name{f}#12, ..]
     *   \_Aggregate[STANDARD,[y{r}#10],[MAX(languages{f}#14,true[BOOLEAN]) AS max_lang, y{r}#10]]
     *     \_StubRelation[[emp_no{f}#11, languages{f}#14, gender{f}#13, y{r}#10]]
     */
    public void testGroupingAliasingMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inline stats max_lang = MAX(languages) BY y = gender
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        var project = as(leftEval.child(), Project.class);

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
     *   |   \_Project[[emp_no{f}#23, languages{f}#26, gender{f}#25, last_name{f}#27, first_name{f}#24]]
     *   |     \_EsRelation[test][_meta_field{f}#29, emp_no{f}#23, first_name{f}#24, ..]
     *   \_Aggregate[STANDARD,[f{r}#18, g{r}#21, first_name_l{r}#9],[MAX(languages{f}#26,true[BOOLEAN]) AS max_lang, MIN(languages{f}
     * #26,true[BOOLEAN]) AS min_lang, f{r}#18, g{r}#21, first_name_l{r}#9]]
     *     \_StubRelation[[emp_no{f}#23, languages{f}#26, gender{f}#25, last_name{f}#27, first_name{f}#24, first_name_l{r}#9, f{r}#18, g
     * {r}#21]]
     */
    public void testGroupingAliasingMoved_To_LeftSideOfJoin_WithExpression() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender, last_name, first_name
            | eval first_name_l = left(first_name, 1)
            | inline stats max_lang = MAX(languages), min_lang = MIN(languages) BY f = left(last_name, 1), g = gender, first_name_l
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval1 = as(inline.left(), Eval.class);
        var leftEval2 = as(leftEval1.child(), Eval.class);
        var project = as(leftEval2.child(), Project.class);

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

    public void testInlineStatsAggOnConstantDoesNotRequireStubReplacement() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | inline stats x = min(123)
            | keep x
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        InlineJoin inlineJoin = inlineJoin(plan);
        assertThat(inlineJoin.config().leftFields().isEmpty(), is(true));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof StubRelation), is(false));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof LocalRelation), is(true));
    }

    public void testInlineStatsExpressionOfConstantAggsDoesNotRequireStubReplacement() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | inline stats x = min(123) + max(123)
            | keep x
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        InlineJoin inlineJoin = inlineJoin(plan);
        assertThat(inlineJoin.config().leftFields().isEmpty(), is(true));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof StubRelation), is(false));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof LocalRelation), is(true));
    }

    public void testGroupingByConstantMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inline stats max_lang = MAX(languages) BY c = 1
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        assertThat(leftEval.fields().stream().map(a -> a.name()).toList(), contains("c"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "c"));
    }

    public void testGroupingByConstantAndFieldMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inline stats max_lang = MAX(languages) BY c = 1, y = gender
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        assertThat(leftEval.fields().stream().map(a -> a.name()).toList(), contains("c", "y"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "c", "y"));
    }

    public void testInlineStatsWithConstantAggsAndGroupingAliasingMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inline stats a = min(123), b = max(456), max_lang = MAX(languages) BY y = gender
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        assertThat(leftEval.fields().stream().map(a -> a.name()).toList(), contains("y"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "y"));
    }

    public void testGroupingOnEvalDefinedFieldDoesNotRequirePropagation() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | eval g2 = gender
            | inline stats max_lang = MAX(languages) BY g2
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        assertThat(leftEval.fields().stream().map(a -> a.name()).toList(), contains("g2"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "g2"));
    }

    public void testConstantDefinedOutsideInlineStatsDoesNotTriggerStubReplacement() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | eval c = 123
            | inline stats x = min(c)
            | keep x
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        InlineJoin inlineJoin = inlineJoin(plan);
        assertThat(inlineJoin.config().leftFields().isEmpty(), is(true));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof StubRelation), is(true));
    }

    public void testGroupingAliasingMoved_WhenLeftHasEvalAndAggUsesRenamedField() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | eval langs = languages
            | inline stats max_lang = MAX(langs) BY y = gender
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval1 = as(inline.left(), Eval.class);
        assertThat(leftEval1.fields().stream().map(a -> a.name()).toList(), contains("y"));
        var leftEval2 = as(leftEval1.child(), Eval.class);
        assertThat(leftEval2.fields().stream().map(a -> a.name()).toList(), contains("langs"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "languages", "gender", "langs", "y"));
    }

    public void testAggOnConstantWithGroupingFieldKeepsStubRelation() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no
            | inline stats one = max(1) by emp_no
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        assertThat(inline.config().leftFields().isEmpty(), is(false));
        assertThat(inline.left() instanceof Project, is(true));
        assertThat(inline.right().anyMatch(p -> p instanceof StubRelation), is(true));
    }

    public void testAggOnConstantWithGroupingAliasingMoved_To_LeftSideOfJoin() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, gender
            | inline stats one = max(1) by y = gender
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var limit = as(plan, Limit.class);
        var inline = as(limit.child(), InlineJoin.class);
        var leftEval = as(inline.left(), Eval.class);
        assertThat(leftEval.fields().stream().map(a -> a.name()).toList(), contains("y"));

        Aggregate rightAgg = rightAggregate(inline);
        var stubRelation = as(rightAgg.child(), StubRelation.class);
        assertThat(Expressions.names(stubRelation.expressions()), contains("emp_no", "gender", "y"));
    }

    public void testInlineStatsAvgOnConstantDoesNotRequireStubReplacement() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | inline stats a = avg(123)
            | keep a
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        InlineJoin inlineJoin = inlineJoin(plan);
        assertThat(inlineJoin.config().leftFields().isEmpty(), is(true));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof StubRelation), is(false));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof LocalRelation), is(true));
    }

    public void testInlineStatsAggsOnNullDoesNotRequireStubReplacement() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | inline stats x = min(null) + median(null)
            | keep x
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        InlineJoin inlineJoin = inlineJoin(plan);
        assertThat(inlineJoin.config().leftFields().isEmpty(), is(true));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof StubRelation), is(false));
        assertThat(inlineJoin.right().anyMatch(p -> p instanceof LocalRelation), is(true));
    }

    public void testTwoInlineStatsOneConstantOneGroupedDoesNotBreak() {
        assumeTrue("Requires INLINE STATS", EsqlCapabilities.Cap.INLINE_STATS.isEnabled());
        var plan = plan("""
            from test
            | keep emp_no, languages, gender
            | inline stats x = min(123)
            | inline stats max_lang = max(languages) by y = gender
            | keep emp_no, max_lang, y
            """, new AbstractLogicalPlanOptimizerTests.TestSubstitutionOnlyOptimizer());

        var inlineJoins = new java.util.ArrayList<InlineJoin>();
        plan.forEachDown(InlineJoin.class, inlineJoins::add);
        assertThat(inlineJoins.size(), is(2));

        boolean foundConstantInlineJoin = inlineJoins.stream()
            .anyMatch(ij -> ij.config().leftFields().isEmpty() && ij.right().anyMatch(p -> p instanceof LocalRelation));
        assertThat(foundConstantInlineJoin, is(true));

        boolean foundGroupedInlineJoinWithPropagatedY = inlineJoins.stream().anyMatch(ij -> {
            if (ij.right().anyMatch(p -> p instanceof StubRelation) == false) {
                return false;
            }
            if (ij.config().leftFields().stream().anyMatch(a -> a.name().equals("y")) == false) {
                return false;
            }
            return ij.left().output().stream().anyMatch(a -> a.name().equals("y"));
        });
        assertThat(foundGroupedInlineJoinWithPropagatedY, is(true));
    }

    private static InlineJoin inlineJoin(LogicalPlan plan) {
        var ijHolder = new Holder<InlineJoin>();
        plan.forEachDown(InlineJoin.class, ijHolder::setIfAbsent);
        InlineJoin inlineJoin = ijHolder.get();
        assertNotNull(inlineJoin);
        return inlineJoin;
    }

    private static Aggregate rightAggregate(InlineJoin inlineJoin) {
        var aggHolder = new Holder<Aggregate>();
        inlineJoin.right().forEachDown(Aggregate.class, aggHolder::setIfAbsent);
        Aggregate agg = aggHolder.get();
        assertNotNull(agg);
        return agg;
    }

    private LogicalPlan plan(String query, LogicalPlanOptimizer optimizer) {
        return optimizer.optimize(analyzer.analyze(EsqlParser.INSTANCE.parseQuery(query)));
    }

    @Override
    protected List<String> filteredWarnings() {
        return withDefaultLimitWarning(super.filteredWarnings());
    }
}
