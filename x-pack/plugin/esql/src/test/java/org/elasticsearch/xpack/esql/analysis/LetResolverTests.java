/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.EsqlTestUtils;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.action.EsqlCapabilities;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;
import org.elasticsearch.xpack.esql.view.ViewResolver;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;
import static org.hamcrest.Matchers.startsWith;

/**
 * Unit tests for {@link LetResolver}.
 */
public class LetResolverTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Parses the full statement (LET prefix included) and runs the resolver on it, exactly as EsqlSession does. */
    private static LogicalPlan parseAndResolveLet(String query) {
        assumeTrue("requires NAMED_SUBQUERY_LET capability", EsqlCapabilities.Cap.NAMED_SUBQUERY_LET.isEnabled());
        var statement = EsqlTestUtils.TEST_PARSER.createStatement(query);
        return LetResolver.resolve(statement.plan(), statement.letBindings(), ViewResolver.DEFAULT_MAX_PLAN_NODES_COUNT);
    }

    /** Creates a bare UnresolvedRelation for the given pattern string. */
    private static UnresolvedRelation relation(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, Collections.emptyList(), IndexMode.STANDARD, null);
    }

    /** Builds {@code FROM <source>} followed by {@code length} chained LIMIT commands. */
    private static String limitChain(String source, int length) {
        return "FROM " + source + " | LIMIT 10".repeat(length);
    }

    private static UnresolvedRelation assertUnresolvedRelation(LogicalPlan plan, String indexPattern) {
        assertThat(plan, instanceOf(UnresolvedRelation.class));
        UnresolvedRelation relation = (UnresolvedRelation) plan;
        assertThat(relation.indexPattern().indexPattern(), is(indexPattern));
        return relation;
    }

    /** Asserts that {@code plan} is a Limit directly over a relation with the given pattern, i.e. {@code FROM x | LIMIT n}. */
    private static void assertLimitOver(LogicalPlan plan, String indexPattern) {
        assertThat(plan, instanceOf(Limit.class));
        assertUnresolvedRelation(((Limit) plan).child(), indexPattern);
    }

    // -----------------------------------------------------------------------
    // No-op when bindings list is empty
    // -----------------------------------------------------------------------

    public void testEmptyBindingsReturnsSamePlan() {
        LogicalPlan plan = relation("some_index");
        LogicalPlan result = LetResolver.resolve(plan, List.of(), ViewResolver.DEFAULT_MAX_PLAN_NODES_COUNT);
        assertThat(result, sameInstance(plan));
    }

    // -----------------------------------------------------------------------
    // Binding body substituted directly — no wrapping
    // -----------------------------------------------------------------------

    public void testBindingBodySubstitutedDirectly() {
        LogicalPlan result = parseAndResolveLet("LET top3 = (FROM base_index | LIMIT 3); FROM top3");
        assertLimitOver(result, "base_index");
    }

    // -----------------------------------------------------------------------
    // LET binding shadows an ES index with the same name
    // -----------------------------------------------------------------------

    public void testLetBindingShadowsIndexWithSameName() {
        // The LET binding named "languages" takes precedence over the ES index "languages".
        // FROM languages in the main query resolves to the binding body, not the index.
        LogicalPlan result = parseAndResolveLet("LET languages = (FROM employees | LIMIT 5); FROM languages");
        assertLimitOver(result, "employees");
    }

    // -----------------------------------------------------------------------
    // Unmatched name → left as UnresolvedRelation
    // -----------------------------------------------------------------------

    public void testUnmatchedNameLeftAsUnresolvedRelation() {
        LogicalPlan result = parseAndResolveLet("LET known = (FROM some_index); FROM unknown_index");
        // "unknown_index" ≠ "known", should not be substituted
        assertUnresolvedRelation(result, "unknown_index");
    }

    // -----------------------------------------------------------------------
    // Sequential (chained) scoping
    // -----------------------------------------------------------------------

    public void testChainedBindingsResolveLeftToRight() {
        // "a" in b's body resolves to a's body
        LogicalPlan result = parseAndResolveLet("LET a = (FROM base | LIMIT 5); LET b = (FROM a | LIMIT 3); FROM b");
        // "b" resolves to bBody with "a" substituted: Limit(Limit(base))
        assertThat(result, instanceOf(Limit.class));
        assertLimitOver(((Limit) result).child(), "base");
    }

    // -----------------------------------------------------------------------
    // Forward references / cycle detection
    // -----------------------------------------------------------------------

    public void testLetResolutionSimpleCycle() {
        // body references its own binding name
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM a | LIMIT 1); FROM a")
        );
    }

    public void testCycleDetectedThroughMixedPattern() {
        // "a,real_index" contains the binding name "a" as a token — cycle.
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM a, real_index | LIMIT 1); FROM a")
        );
    }

    public void testForwardReferenceDetectedThroughMixedPattern() {
        // "b,real_index" in a's body contains the forward-reference "b".
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM b, real_index | LIMIT 1); LET b = (FROM base); FROM a")
        );
    }

    public void testForwardReferenceIsRejected() {
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM b | LIMIT 1); LET b = (FROM a | LIMIT 1); FROM a")
        );
    }

    public void testForwardReferenceInSubquery() {
        // substitute replaces InSubquery(x, UR("a")) and checkForCycles recurses into the subquery
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM base | WHERE x IN (FROM a) | LIMIT 1); FROM a")
        );
    }

    public void testForwardReferenceMultiColumnInSubquery() {
        // Same cycle as above but through a multi-column IN subquery.
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> parseAndResolveLet("LET a = (FROM base | WHERE (x, y) IN (FROM a) | LIMIT 1); FROM a")
        );
    }

    // -----------------------------------------------------------------------
    // Mixed FROM: binding name alongside real index patterns
    // -----------------------------------------------------------------------

    public void testMixedFromBindingAndRealIndex() {
        // The UnresolvedRelation "top3,real_index" should be split: top3 → binding body,
        // real_index → new UnresolvedRelation. The result is a UnionAll of the two.
        LogicalPlan result = parseAndResolveLet("LET top3 = (FROM logs | LIMIT 3); FROM top3, real_index");

        assertThat(result, instanceOf(UnionAll.class));
        List<LogicalPlan> children = ((UnionAll) result).children();
        assertThat(children, hasSize(2));
        assertLimitOver(children.get(0), "logs");
        assertUnresolvedRelation(children.get(1), "real_index");
    }

    public void testMixedFromTwoRealOneBinding() {
        // binding in the middle
        LogicalPlan result = parseAndResolveLet("LET top3 = (FROM logs | LIMIT 3); FROM real1, top3, real2");

        // all non-binding patterns are grouped in one trailing relation → UnionAll of 2 parts:
        // bindingBody, UR("real1,real2")
        assertThat(result, instanceOf(UnionAll.class));
        List<LogicalPlan> children = ((UnionAll) result).children();
        assertThat(children, hasSize(2));
        assertLimitOver(children.get(0), "logs");
        assertUnresolvedRelation(children.get(1), "real1,real2");
    }

    public void testMixedFromBindingsInterleavedWithRealIndices() {
        LogicalPlan result = parseAndResolveLet("LET x = (FROM logs | LIMIT 1); LET y = (FROM metrics | LIMIT 1); FROM a, x, b, y");

        assertThat(result, instanceOf(UnionAll.class));
        List<LogicalPlan> children = ((UnionAll) result).children();
        assertThat(children, hasSize(3));
        assertLimitOver(children.get(0), "logs");
        assertLimitOver(children.get(1), "metrics");
        assertUnresolvedRelation(children.get(2), "a,b");
    }

    public void testMixedInSubqueryBindingAndRealIndex() {
        // The InSubquery's subquery plan is UR("real_index_2,top3"); LetResolver should split it.
        LogicalPlan result = parseAndResolveLet(
            "LET top3 = (FROM logs | LIMIT 3); FROM real_index | WHERE field IN (FROM real_index_2, top3)"
        );

        Filter resultFilter = (Filter) result;
        InSubquery inSub = (InSubquery) resultFilter.condition();
        assertThat(inSub.subquery(), instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) inSub.subquery();
        assertThat(union.children(), hasSize(2));
        assertLimitOver(union.children().get(0), "logs");
        assertUnresolvedRelation(union.children().get(1), "real_index_2");
    }

    // -----------------------------------------------------------------------
    // Plan size limit protections
    // -----------------------------------------------------------------------

    private static final String MAX_PLAN_NODES_ERROR =
        "ES|QL query exceeded the maximum of [500] plan nodes allowed after expanding views, subqueries and FORKs.";

    public void testPlanSizeWithinLimit() {
        LogicalPlan result = parseAndResolveLet("LET b = (" + limitChain("src", 300) + "); FROM b");
        assertThat(result, instanceOf(Limit.class));
    }

    public void testBindingExceedingPlanSizeIsRejected() {
        expectThrows(
            VerificationException.class,
            startsWith(MAX_PLAN_NODES_ERROR),
            () -> parseAndResolveLet("LET b = (" + limitChain("src", 500) + "); FROM b")
        );
    }

    public void testChainedBindingsAccumulatePlanSize() {
        expectThrows(
            VerificationException.class,
            startsWith(MAX_PLAN_NODES_ERROR),
            () -> parseAndResolveLet("LET a = (" + limitChain("src", 250) + "); LET b = (" + limitChain("a", 250) + "); FROM b")
        );
    }

    public void testMainQueryExceedingPlanSizeIsRejected() {
        expectThrows(
            VerificationException.class,
            startsWith(MAX_PLAN_NODES_ERROR),
            () -> parseAndResolveLet("LET b = (FROM a); " + limitChain("b", 500))
        );
    }

    public void testPlanSizeIncludesSubqueryPlans() {
        expectThrows(
            VerificationException.class,
            startsWith(MAX_PLAN_NODES_ERROR),
            () -> parseAndResolveLet("LET b = (" + limitChain("src", 250) + "); FROM b, (FROM b) | WHERE x > 0")
        );
    }

    public void testPlanSizeIncludesInSubqueryPlans() {
        expectThrows(
            VerificationException.class,
            startsWith(MAX_PLAN_NODES_ERROR),
            () -> parseAndResolveLet(
                "LET b = (" + limitChain("src", 205) + "); " + limitChain("main", 100) + " | WHERE x IN (FROM b) | WHERE y IN (FROM b)"
            )
        );
    }

    public void testSharedBindingExpansionIsRejectedWithoutExpanding() {
        // 2^50 leaves if expanded; sizes are tracked arithmetically, so this must be rejected promptly.
        StringBuilder query = new StringBuilder("LET b0 = (FROM src); ");
        for (int i = 1; i < 50; i++) {
            String previous = "b" + (i - 1);
            query.append("LET b").append(i).append(" = (FROM ").append(previous).append(", ").append(previous).append("); ");
        }
        query.append("FROM b49");
        expectThrows(VerificationException.class, startsWith(MAX_PLAN_NODES_ERROR), () -> parseAndResolveLet(query.toString()));
    }

    // -----------------------------------------------------------------------
    // Substitution into multiple positions
    // -----------------------------------------------------------------------

    public void testSubstitutionIntoMultiplePositions() {
        // "snap" is referenced in three distinct positions: a union branch, a nested subquery branch and an IN subquery.
        LogicalPlan result = parseAndResolveLet(
            "LET snap = (FROM src | LIMIT 10); FROM snap, (FROM snap | LIMIT 10) | WHERE x IN (FROM snap)"
        );

        // Every occurrence of "snap" is replaced by the body.
        assertThat(result, instanceOf(Filter.class));
        Filter filter = (Filter) result;
        assertThat(filter.child(), instanceOf(UnionAll.class));
        List<LogicalPlan> branches = ((UnionAll) filter.child()).children();
        assertThat(branches, hasSize(2));
        assertLimitOver(branches.get(0), "src");
        List<UnresolvedRelation> nested = branches.get(1).collect(UnresolvedRelation.class);
        assertThat(nested, hasSize(1));
        assertThat(nested.get(0).indexPattern().indexPattern(), is("src"));
        assertThat(filter.condition(), instanceOf(InSubquery.class));
        assertLimitOver(((InSubquery) filter.condition()).subquery(), "src");
    }

    // -----------------------------------------------------------------------
    // TS source: a binding cannot replace a time-series relation
    // -----------------------------------------------------------------------

    public void testBindingAsTsSourceIsRejected() {
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in TS command"),
            () -> parseAndResolveLet("LET b = (FROM idx | LIMIT 10); TS b")
        );
    }

    public void testBindingInMixedTsSourceIsRejected() {
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in TS command"),
            () -> parseAndResolveLet("LET b = (FROM idx | LIMIT 10); TS tsdb_index, b")
        );
    }

    public void testTsSourceNotMatchingAnyBindingIsLeftAlone() {
        LogicalPlan result = parseAndResolveLet("LET b = (FROM idx | LIMIT 10); TS tsdb_index");
        assertThat(assertUnresolvedRelation(result, "tsdb_index").indexMode(), is(IndexMode.TIME_SERIES));
    }

    // -----------------------------------------------------------------------
    // Source commands other than FROM inside a binding
    // -----------------------------------------------------------------------

    public void testBindingWithRowSource() {
        LogicalPlan result = parseAndResolveLet("LET r = (ROW a = 1, b = 2 | WHERE a > 0 | LIMIT 1); FROM r | KEEP a");
        assertThat(result, instanceOf(Project.class));
        LogicalPlan limit = ((Project) result).child();
        assertThat(limit, instanceOf(Limit.class));
        assertThat(limit.collect(Row.class), hasSize(1));
    }

    public void testBindingWithTsSource() {
        LogicalPlan result = parseAndResolveLet("LET t = (TS idx | LIMIT 1); FROM t");
        assertThat(result, instanceOf(Limit.class));
        var relations = result.collect(UnresolvedRelation.class);
        assertThat(relations, hasSize(1));
        assertThat(relations.get(0).indexMode(), is(IndexMode.TIME_SERIES));
    }

    // -----------------------------------------------------------------------
    // LOOKUP JOIN right-hand side
    // -----------------------------------------------------------------------

    public void testBindingAsLookupJoinRightSideIsRejected() {
        // Without the check this reached the physical planner and failed with a 500 "unsupported logical plan node [Join]".
        assumeTrue("requires JOIN_LOOKUP_V12 capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in LOOKUP JOIN"),
            () -> parseAndResolveLet("LET x = (FROM sample_data METADATA _index); FROM sample_data | LOOKUP JOIN x ON @timestamp")
        );
    }

    public void testLookupJoinOnNonBindingNameIsLeftAlone() {
        assumeTrue("requires JOIN_LOOKUP_V12 capability", EsqlCapabilities.Cap.JOIN_LOOKUP_V12.isEnabled());
        LogicalPlan result = parseAndResolveLet("LET x = (FROM sample_data); FROM x | LOOKUP JOIN lookup_idx ON @timestamp");
        var relations = result.collect(UnresolvedRelation.class);
        assertThat(relations, hasSize(2));
        assertThat(relations.get(1).indexMode(), is(IndexMode.LOOKUP));
        assertThat(relations.get(1).indexPattern().indexPattern(), is("lookup_idx"));
    }

    // -----------------------------------------------------------------------
    // Current behavior with wildcard and exclusion patterns
    // -----------------------------------------------------------------------

    /**
     * Captures CURRENT behavior: a wildcard pattern such as {@code x*} is matched against binding names by exact string, so it
     * never matches the binding {@code x}. The relation is left for field-caps, which resolves it against indices.
     */
    public void testCurrentBehaviorWildcardPatternDoesNotMatchBinding() {
        LogicalPlan result = parseAndResolveLet("LET x = (FROM sample_data METADATA _index); FROM x*");
        assertUnresolvedRelation(result, "x*");
    }

    /**
     * Captures CURRENT behavior: the exclusion {@code -x} is not recognized as the binding {@code x}, so it is
     * passed through untouched to index resolution, where it is a no-op because no index matches {@code x}.
     */
    public void testCurrentBehaviorExclusionPatternDoesNotMatchBinding() {
        LogicalPlan result = parseAndResolveLet("LET x = (FROM sample_data METADATA _index); FROM sample_data, -x");
        assertUnresolvedRelation(result, "sample_data,-x");
    }
}
