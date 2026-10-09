/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.UnresolvedAttribute;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InSubquery;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.MultiColumnInSubquery;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.Collections;
import java.util.List;

import static java.util.List.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.sameInstance;

/**
 * Unit tests for {@link LetResolver}.
 */
public class LetResolverTests extends ESTestCase {

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /** Creates a bare UnresolvedRelation for the given pattern string. */
    private static UnresolvedRelation relation(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, Collections.emptyList(), IndexMode.STANDARD, null);
    }

    /** Creates a time-series (TS command) UnresolvedRelation for the given pattern string. */
    private static UnresolvedRelation tsRelation(String pattern) {
        return new UnresolvedRelation(EMPTY, new IndexPattern(EMPTY, pattern), false, Collections.emptyList(), IndexMode.TIME_SERIES, null);
    }

    /** Wraps a plan in a trivial Limit to produce a non-trivial subplan. */
    private static LogicalPlan withLimit(LogicalPlan child) {
        return new Limit(
            EMPTY,
            new org.elasticsearch.xpack.esql.core.expression.Literal(EMPTY, 10, org.elasticsearch.xpack.esql.core.type.DataType.INTEGER),
            child
        );
    }

    /** Creates a single LetBinding. */
    private static LetBinding binding(String name, LogicalPlan body) {
        return new LetBinding(EMPTY, name, body);
    }

    // -----------------------------------------------------------------------
    // No-op when bindings list is empty
    // -----------------------------------------------------------------------

    public void testEmptyBindingsReturnsSamePlan() {
        LogicalPlan plan = relation("some_index");
        LogicalPlan result = LetResolver.resolve(plan, List.of());
        assertThat(result, sameInstance(plan));
    }

    // -----------------------------------------------------------------------
    // Binding body substituted directly — no wrapping
    // -----------------------------------------------------------------------

    public void testBindingBodySubstitutedDirectly() {
        LogicalPlan body = withLimit(relation("base_index"));
        LetBinding b = binding("top3", body);

        LogicalPlan main = relation("top3");
        LogicalPlan result = LetResolver.resolve(main, List.of(b));

        assertThat(result, sameInstance(body));
    }

    // -----------------------------------------------------------------------
    // LET binding shadows an ES index with the same name
    // -----------------------------------------------------------------------

    public void testLetBindingShadowsIndexWithSameName() {
        // LET languages = (FROM employees | LIMIT 5);
        // FROM languages
        // The LET binding named "languages" takes precedence over the ES index "languages".
        // FROM languages in the main query resolves to the binding body, not the index.
        LogicalPlan body = withLimit(relation("employees"));
        LetBinding languages = binding("languages", body);
        LogicalPlan result = LetResolver.resolve(relation("languages"), List.of(languages));
        assertThat(result, sameInstance(body));
    }

    // -----------------------------------------------------------------------
    // Unmatched name → left as UnresolvedRelation
    // -----------------------------------------------------------------------

    public void testUnmatchedNameLeftAsUnresolvedRelation() {
        LetBinding b = binding("known", relation("some_index"));
        LogicalPlan main = relation("unknown_index");
        LogicalPlan result = LetResolver.resolve(main, List.of(b));

        // "unknown_index" ≠ "known", should not be substituted
        assertThat(result, instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) result).indexPattern().indexPattern(), is("unknown_index"));
    }

    // -----------------------------------------------------------------------
    // Sequential (chained) scoping
    // -----------------------------------------------------------------------

    public void testChainedBindingsResolveLeftToRight() {
        // LET a = (FROM base | LIMIT 5);
        // LET b = (FROM a | LIMIT 3); -- "a" in b's body resolves to aBody
        // FROM b
        LogicalPlan aBody = withLimit(relation("base"));
        LetBinding a = binding("a", aBody);

        LogicalPlan bBody = withLimit(relation("a"));
        LetBinding b = binding("b", bBody);

        LogicalPlan result = LetResolver.resolve(relation("b"), List.of(a, b));

        // "b" resolves to bBody with "a" substituted: Limit(aBody)
        assertThat(result, instanceOf(Limit.class));
        assertThat(((Limit) result).child(), sameInstance(aBody));
    }

    // -----------------------------------------------------------------------
    // Cycle detection
    // -----------------------------------------------------------------------

    public void testLetResolutionSimpleCycle() {
        // LET a = (FROM a | LIMIT 1); FROM a — body references its own binding name
        LetBinding a = binding("a", withLimit(relation("a")));
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a))
        );
    }

    public void testCycleDetectedThroughMixedPattern() {
        // LET a = (FROM a,real_index | LIMIT 1); FROM a
        // "a,real_index" contains the binding name "a" as a token — cycle.
        LetBinding a = binding("a", withLimit(relation("a,real_index")));
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [a] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a))
        );
    }

    public void testForwardReferenceDetectedThroughMixedPattern() {
        // LET a = (FROM b,real_index | LIMIT 1);
        // LET b = (FROM base);
        // FROM a
        // "b,real_index" in a's body contains the forward-reference "b".
        LetBinding a = binding("a", withLimit(relation("b,real_index")));
        LetBinding b = binding("b", relation("base"));
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a, b))
        );
    }

    public void testLetResolutionComplexCycle() {
        // LET a = (FROM b | LIMIT 1);
        // LET b = (FROM a | LIMIT 1);
        // FROM a
        LetBinding a = binding("a", withLimit(relation("b")));
        LetBinding b = binding("b", withLimit(relation("a")));
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a, b))
        );
    }

    public void testLetResolutionCycleInInSubquery() {
        // LET a = (FROM b | LIMIT 1);
        // LET b = (FROM base | WHERE x IN (FROM a) | LIMIT 1);
        // FROM a
        // substitute replaces InSubquery(x, UR("a")) → InSubquery(x, Limit(UR("b"),1))
        // checkForCycles recurses into the subquery and finds UR("b") ∈ resolved → cycle
        LetBinding a = binding("a", withLimit(relation("b")));
        Expression value = new UnresolvedAttribute(EMPTY, "x");
        LetBinding b = binding("b", withLimit(new Filter(EMPTY, relation("base"), new InSubquery(EMPTY, value, relation("a")))));
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a, b))
        );
    }

    public void testLetResolutionCycleInMultiColumnInSubquery() {
        // LET a = (FROM b | LIMIT 1);
        // LET b = (FROM base | WHERE (x, y) IN (FROM a) | LIMIT 1);
        // FROM a
        // Same cycle as above but through a multi-column IN subquery.
        LetBinding a = binding("a", withLimit(relation("b")));
        List<Expression> values = of(new UnresolvedAttribute(EMPTY, "x"), new UnresolvedAttribute(EMPTY, "y"));
        LetBinding b = binding(
            "b",
            withLimit(new Filter(EMPTY, relation("base"), new MultiColumnInSubquery(EMPTY, values, relation("a"))))
        );
        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a, b))
        );
    }

    // -----------------------------------------------------------------------
    // Forward references (binding A referencing binding B declared later)
    // -----------------------------------------------------------------------

    public void testForwardReferenceIsRejected() {
        // LET a = (FROM b | LIMIT 1); -- a references b, declared later (forward reference)
        // LET b = (FROM real_index);
        // FROM a
        //
        // Sequential scoping: a is evaluated against the empty map (b is not yet declared),
        // so UR("b") survives unresolved in a's body. The main substitution replaces UR("a")
        // with Limit(UR("b")) and does NOT descend into the replacement (transformDownSkipBranch).
        // checkForForwardReferences then finds UR("b") ∈ resolved and rejects the forward reference.
        LetBinding a = binding("a", withLimit(relation("b")));
        LetBinding b = binding("b", relation("real_index"));

        expectThrows(
            VerificationException.class,
            containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"),
            () -> LetResolver.resolve(relation("a"), List.of(a, b))
        );
    }

    // -----------------------------------------------------------------------
    // Mixed FROM: binding name alongside real index patterns
    // -----------------------------------------------------------------------

    public void testMixedFromBindingAndRealIndex() {
        // LET top3 = (FROM logs | LIMIT 3);
        // FROM top3, real_index
        // The UnresolvedRelation "top3,real_index" should be split: top3 → binding body,
        // real_index → new UnresolvedRelation. The result is a UnionAll of the two.
        LogicalPlan body = withLimit(relation("logs"));
        LetBinding top3 = binding("top3", body);

        LogicalPlan main = relation("top3,real_index");
        LogicalPlan result = LetResolver.resolve(main, List.of(top3));

        assertThat(result, instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) result;
        assertThat(union.children(), hasSize(2));
        assertThat(union.children().get(0), sameInstance(body));
        assertThat(union.children().get(1), instanceOf(UnresolvedRelation.class));
        assertThat(((UnresolvedRelation) union.children().get(1)).indexPattern().indexPattern(), is("real_index"));
    }

    public void testMixedFromTwoRealOneBinding() {
        // FROM real1, top3, real2 — binding in the middle
        LogicalPlan body = withLimit(relation("logs"));
        LetBinding top3 = binding("top3", body);

        LogicalPlan main = relation("real1,top3,real2");
        LogicalPlan result = LetResolver.resolve(main, List.of(top3));

        // real1 is grouped before top3, real2 after → UnionAll of 3 parts:
        // UR("real1"), bindingBody, UR("real2")
        assertThat(result, instanceOf(UnionAll.class));
        List<LogicalPlan> children = ((UnionAll) result).children();
        assertThat(children, hasSize(3));
        assertThat(((UnresolvedRelation) children.get(0)).indexPattern().indexPattern(), is("real1"));
        assertThat(children.get(1), sameInstance(body));
        assertThat(((UnresolvedRelation) children.get(2)).indexPattern().indexPattern(), is("real2"));
    }

    public void testMixedInSubqueryBindingAndRealIndex() {
        // LET top3 = (FROM logs | LIMIT 3);
        // FROM real_index | WHERE field IN (FROM real_index_2, top3)
        // The InSubquery's subquery plan is UR("real_index_2,top3"); LetResolver should split it.
        LogicalPlan body = withLimit(relation("logs"));
        LetBinding top3 = binding("top3", body);

        Expression value = new UnresolvedAttribute(EMPTY, "field");
        LogicalPlan inSubqueryPlan = relation("real_index_2,top3");
        Filter filter = new Filter(EMPTY, relation("real_index"), new InSubquery(EMPTY, value, inSubqueryPlan));

        LogicalPlan result = LetResolver.resolve(filter, List.of(top3));

        Filter resultFilter = (Filter) result;
        InSubquery inSub = (InSubquery) resultFilter.condition();
        assertThat(inSub.subquery(), instanceOf(UnionAll.class));
        UnionAll union = (UnionAll) inSub.subquery();
        assertThat(union.children(), hasSize(2));
        assertThat(((UnresolvedRelation) union.children().get(0)).indexPattern().indexPattern(), is("real_index_2"));
        assertThat(union.children().get(1), sameInstance(body));
    }

    // -----------------------------------------------------------------------
    // Substitution into multiple positions
    // -----------------------------------------------------------------------

    public void testSubstitutionIntoMultiplePositions() {
        // "snap" referenced twice in the plan: Limit(Limit(snap))
        LogicalPlan body = withLimit(relation("src"));
        LetBinding b = binding("snap", body);

        LogicalPlan outer = withLimit(withLimit(relation("snap")));
        LogicalPlan result = LetResolver.resolve(outer, List.of(b));

        // Both occurrences of "snap" are replaced by body directly.
        assertThat(result, instanceOf(Limit.class));
        assertThat(((Limit) result).child(), instanceOf(Limit.class));
        assertThat(((Limit) ((Limit) result).child()).child(), sameInstance(body));
    }

    // -----------------------------------------------------------------------
    // TS source: a binding cannot replace a time-series relation
    // -----------------------------------------------------------------------

    public void testBindingAsTsSourceIsRejected() {
        // LET b = (FROM idx | LIMIT 10); TS b
        LetBinding b = binding("b", withLimit(relation("idx")));
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in TS command"),
            () -> LetResolver.resolve(tsRelation("b"), List.of(b))
        );
    }

    public void testBindingInMixedTsSourceIsRejected() {
        // LET b = (FROM idx | LIMIT 10); TS tsdb_index, b
        LetBinding b = binding("b", withLimit(relation("idx")));
        expectThrows(
            ParsingException.class,
            containsString("Subqueries are not supported in TS command"),
            () -> LetResolver.resolve(tsRelation("tsdb_index,b"), List.of(b))
        );
    }

    public void testTsSourceNotMatchingAnyBindingIsLeftAlone() {
        LetBinding b = binding("b", withLimit(relation("idx")));
        LogicalPlan main = tsRelation("tsdb_index");
        assertThat(LetResolver.resolve(main, List.of(b)), sameInstance(main));
    }
}
