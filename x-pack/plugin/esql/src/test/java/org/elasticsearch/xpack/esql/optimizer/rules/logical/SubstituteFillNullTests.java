/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.Expressions;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.optimizer.AbstractLogicalPlanOptimizerTests;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.FillNull;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.InlineStats;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.join.AbstractSubqueryJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.LookupJoin;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.EsqlTestUtils.as;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;

/**
 * Logical-optimizer coverage for the {@code FILLNULL} command, whose {@link FillNull} node is rewritten away by
 * {@link SubstituteFillNull} into a {@code Project} over an {@code Eval} of {@code Coalesce} aliases, and whose
 * un-fillable targets are surfaced as response-header warnings by {@link WarnUnfillableFillNull}.
 */
public class SubstituteFillNullTests extends AbstractLogicalPlanOptimizerTests {

    public SubstituteFillNullTests(VersionMode versionMode) {
        super(versionMode);
    }

    private static List<Alias> fillAliases(LogicalPlan plan) {
        List<Alias> aliases = new ArrayList<>();
        plan.forEachDown(Eval.class, eval -> {
            for (Alias alias : eval.fields()) {
                if (alias.child() instanceof Coalesce) {
                    aliases.add(alias);
                }
            }
        });
        return aliases;
    }

    private static Attribute filledColumn(Alias fill) {
        return (Attribute) ((Coalesce) fill.child()).children().getFirst();
    }

    public void testFillNullChainedWithFork() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = null
            | EVAL a = a::keyword, b = b::integer
            | FILLNULL "unknown" ON a
            | FILLNULL 0 ON b
            | FORK (WHERE true | LIMIT 300) (WHERE true)
            | LIMIT 300
            | WHERE _fork == "fork1"
            | DROP _fork
            """);
        assertThat(plan, instanceOf(Project.class));
    }

    public void testFillNullChained() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = null
            | EVAL a = a::keyword, b = b::integer
            | FILLNULL "unknown" ON a
            | FILLNULL 0 ON b
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "b"));
    }

    public void testFillNullBetweenEvalsIsCombined() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, x = 1
            | EVAL a = a::integer
            | FILLNULL 0 ON a
            | EVAL sum = a + x
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "x", "sum"));
    }

    public void testFillNullUnfillableTargetedFieldWarns() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d = null
            | EVAL d = d::datetime
            | FILLNULL DEFAULT ON d
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        // A named target gets the same single summary warning as `ON *` - the form of the target list never changes it.
        assertWarnings(
            "Line 3:3: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d]; provide an explicit value"
        );
    }

    public void testFillNullAllFieldsUnfillableColumnsWarnOnce() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d = null, a = null, i = null
            | EVAL d = d::datetime, a = a::ip, i = i::integer
            | FILLNULL DEFAULT ON *
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertWarnings(
            "Line 3:3: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d, a]; provide an explicit value"
        );
    }

    public void testFillNullAllFieldsUnfillableColumnsSummaryIsCapped() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW d1 = null, d2 = null, d3 = null, d4 = null, d5 = null, d6 = null, d7 = null, d8 = null, d9 = null,
                d10 = null, d11 = null
            | EVAL d1 = d1::datetime, d2 = d2::datetime, d3 = d3::datetime, d4 = d4::datetime, d5 = d5::datetime,
                d6 = d6::datetime, d7 = d7::datetime, d8 = d8::datetime, d9 = d9::datetime, d10 = d10::datetime,
                d11 = d11::datetime
            | FILLNULL DEFAULT ON *
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertWarnings(
            "Line 6:3: [FILLNULL] the following fields have no default fill value for their type and were left "
                + "unchanged: [d1, d2, d3, d4, d5, d6, d7, d8, d9, d10]; provide an explicit value; "
                + "only the first 10 of 11 fields are shown"
        );
    }

    public void testFillNullUnderNullnessFiltersIsAccepted() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        var plan = plan("""
            ROW a = null, b = 1
            | EVAL a = a::integer
            | WHERE a IS NULL
            | FILLNULL 0 ON a
            | WHERE a IS NOT NULL
            """);
        assertFalse("FILLNULL must be substituted away", plan.anyMatch(p -> p instanceof FillNull));
        assertThat(Expressions.names(plan.output()), containsInAnyOrder("a", "b"));
    }

    public void testReferencesAboveFollowTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query("""
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | EVAL upper = TO_UPPER(first_name)
            | WHERE first_name != "y"
            """));

        Filter filter = as(as(substituted, Limit.class).child(), Filter.class);
        Eval upper = as(filter.child(), Eval.class);
        Project fillProject = as(upper.child(), Project.class);
        Alias fill = as(fillProject.child(), Eval.class).fields().getFirst();
        Attribute filled = fill.toAttribute();
        Attribute original = filledColumn(fill);

        assertThat(fill.name(), equalTo("first_name"));
        assertNotEquals(original.id(), filled.id());
        assertTrue(fillProject.outputSet().contains(filled));
        assertTrue(upper.fields().getFirst().references().contains(filled));
        assertTrue(filter.condition().references().contains(filled));
        assertFalse(filter.condition().references().contains(original));
        assertTrue(substituted.outputSet().contains(filled));
    }

    public void testChainedFillNullOnTheSameColumnFeedsTheOuterFill() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query("""
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | FILLNULL "y" ON first_name
            | EVAL copy = first_name
            """));

        List<Alias> fills = fillAliases(substituted);
        assertThat(fills, hasSize(2));
        Alias outer = fills.get(0);
        Alias inner = fills.get(1);
        assertEquals(inner.id(), filledColumn(outer).id());
        Eval copy = as(as(substituted, Limit.class).child(), Eval.class);
        assertTrue(copy.fields().getFirst().references().contains(outer.toAttribute()));
        plan("FROM test | KEEP emp_no, first_name | FILLNULL \"x\" ON first_name | FILLNULL \"y\" ON first_name");
    }

    public void testLookupJoinKeyFollowsTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        String query = """
            FROM test
            | EVAL language_code = languages
            | FILLNULL 0 ON language_code
            | LOOKUP JOIN languages_lookup ON language_code
            """;
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Alias fill = fillAliases(substituted).getFirst();
        LookupJoin join = as(as(substituted, Limit.class).child(), LookupJoin.class);
        assertThat(join.config().leftFields(), contains(fill.toAttribute()));
        plan(query);
    }

    public void testInlineStatsWithoutGroupingsPassesTheFilledColumnThrough() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        String query = """
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | INLINE STATS c = COUNT(*)
            | WHERE first_name != "y"
            """;
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Attribute filled = fillAliases(substituted).getFirst().toAttribute();
        InlineStats inlineStats = as(substituted.collect(InlineStats.class).getFirst(), InlineStats.class);
        assertTrue(inlineStats.outputSet().contains(filled));
        Filter filter = substituted.collect(Filter.class).getFirst();
        assertTrue(filter.condition().references().contains(filled));
        plan(query);
    }

    public void testInlineStatsWithoutGroupingsAggregatesTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        String query = """
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | INLINE STATS c = COUNT(first_name)
            | KEEP first_name, c
            """;
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Attribute filled = fillAliases(substituted).getFirst().toAttribute();
        InlineStats inlineStats = as(substituted.collect(InlineStats.class).getFirst(), InlineStats.class);
        assertTrue(inlineStats.aggregate().references().contains(filled));
        assertTrue(substituted.outputSet().contains(filled));
        plan(query);
    }

    public void testFillNullInOneForkBranchLeavesTheOtherBranchAlone() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        String query = """
            FROM test
            | KEEP emp_no, first_name
            | FORK (FILLNULL "x" ON first_name | WHERE first_name == "x") (WHERE first_name IS NULL)
            """;
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Fork fork = as(substituted.collect(Fork.class).getFirst(), Fork.class);
        Alias fill = fillAliases(fork.children().get(0)).getFirst();
        assertTrue(fork.children().get(0).outputSet().contains(fill.toAttribute()));
        assertThat(fillAliases(fork.children().get(1)), hasSize(0));
        Filter otherBranchFilter = fork.children().get(1).collect(Filter.class).getFirst();
        assertFalse(otherBranchFilter.condition().references().contains(fill.toAttribute()));
        plan(query);
    }

    public void testFillNullBeforeAForkIsExpandedInEachBranch() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        String query = """
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | FORK (WHERE first_name == "x") (WHERE emp_no > 1)
            """;
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Fork fork = as(substituted.collect(Fork.class).getFirst(), Fork.class);
        Alias first = fillAliases(fork.children().get(0)).getFirst();
        Alias second = fillAliases(fork.children().get(1)).getFirst();
        assertNotEquals(first.id(), second.id());
        assertTrue(fork.children().get(0).outputSet().contains(first.toAttribute()));
        assertTrue(fork.children().get(1).outputSet().contains(second.toAttribute()));
        assertFalse(plan(query).anyMatch(FillNull.class::isInstance));
    }

    public void testFullTextOnAFilledColumnRunsOnTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        LogicalPlan plan = plan("FROM test | FILLNULL \"\" ON first_name | WHERE MATCH(first_name, \"Georgi\") | KEEP emp_no, first_name");
        Filter filter = plan.collect(Filter.class).getFirst();
        Alias fill = fillAliases(plan).getFirst();
        assertTrue(filter.condition().references().contains(fill.toAttribute()));
    }

    public void testFillNullInASubqueryBranch() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assumeTrue("Requires subqueries", EsqlCapabilities.Cap.SUBQUERY_IN_FROM_COMMAND.isEnabled());
        LogicalPlan plan = planSubquery("""
            FROM test, (FROM test | KEEP emp_no, first_name | FILLNULL "x" ON first_name)
            | WHERE first_name == "x"
            | KEEP emp_no, first_name
            """);
        assertFalse(plan.anyMatch(FillNull.class::isInstance));
        assertThat(Expressions.names(plan.output()), contains("emp_no", "first_name"));
    }

    public void testFillNullInAnInSubquery() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assumeTrue("Requires IN subqueries", EsqlCapabilities.Cap.WHERE_IN_SUBQUERY_WITHOUT_VIEW.isEnabled());
        for (String query : List.of(
            "FROM test | WHERE emp_no IN (FROM test | FILLNULL 0 ON emp_no | KEEP emp_no)",
            "FROM test | WHERE emp_no NOT IN (FROM test | FILLNULL 0 ON emp_no | KEEP emp_no)",
            "FROM test | EVAL found = emp_no IN (FROM test | FILLNULL 0 ON emp_no | KEEP emp_no)"
        )) {
            LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));
            AbstractSubqueryJoin join = as(substituted.collect(AbstractSubqueryJoin.class).getFirst(), AbstractSubqueryJoin.class);
            Alias fill = fillAliases(join.right()).getFirst();
            assertThat(query, join.config().rightFields(), contains(fill.toAttribute()));
            assertFalse(query, plan(query).anyMatch(FillNull.class::isInstance));
        }
    }

    public void testEvalRedefiningTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assertColumnRedefinedAboveTheFill("""
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | EVAL first_name = TO_UPPER(first_name)
            | WHERE first_name != "y"
            """, "first_name", true);
    }

    public void testStatsRedefiningTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assertColumnRedefinedAboveTheFill("""
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | STATS first_name = MAX(first_name) BY emp_no
            | WHERE first_name != "y"
            """, "first_name", true);
    }

    public void testLookupJoinShadowingTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assertColumnRedefinedAboveTheFill("""
            FROM test
            | EVAL language_code = languages, language_name = first_name
            | FILLNULL "x" ON language_name
            | LOOKUP JOIN languages_lookup ON language_code
            | WHERE language_name != "y"
            """, "language_name", false);
    }

    public void testEnrichShadowingTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assertColumnRedefinedAboveTheFill("""
            FROM test
            | EVAL language_name = first_name
            | FILLNULL "x" ON language_name
            | ENRICH languages_idx ON first_name WITH language_name
            | WHERE language_name != "y"
            """, "language_name", false);
    }

    public void testDropThenEvalRedefiningTheFilledColumn() {
        assumeTrue("FILLNULL is dev-gated", EsqlCapabilities.Cap.FILLNULL.isEnabled());
        assertColumnRedefinedAboveTheFill("""
            FROM test
            | KEEP emp_no, first_name
            | FILLNULL "x" ON first_name
            | DROP first_name
            | EVAL first_name = "z"
            | WHERE first_name != "y"
            """, "first_name", false);
    }

    private void assertColumnRedefinedAboveTheFill(String query, String column, boolean redefinitionReadsTheFill) {
        LogicalPlan substituted = new SubstituteFillNull().apply(defaultAnalyzer().query(query));

        Alias fill = fillAliases(substituted).getFirst();
        Filter filter = as(as(substituted, Limit.class).child(), Filter.class);
        Attribute redefined = filter.child().output().stream().filter(a -> a.name().equals(column)).findFirst().orElseThrow();
        assertNotEquals(fill.id(), redefined.id());
        assertNotEquals(filledColumn(fill).id(), redefined.id());
        assertEquals(redefinitionReadsTheFill, filter.child().references().contains(fill.toAttribute()));
        assertTrue(filter.condition().references().contains(redefined));
        assertFalse(filter.condition().references().contains(fill.toAttribute()));
        assertTrue(substituted.outputSet().contains(redefined));
        assertFalse(plan(query).anyMatch(FillNull.class::isInstance));
    }
}
