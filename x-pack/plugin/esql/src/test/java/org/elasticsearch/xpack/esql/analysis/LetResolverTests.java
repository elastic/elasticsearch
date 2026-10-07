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
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.Collections;
import java.util.List;

import static java.util.List.of;
import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
import static org.hamcrest.Matchers.containsString;
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
        // LET a = (FROM base | LIMIT 5),
        // b = (FROM a | LIMIT 3); -- "a" in b's body resolves to aBody
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
        var e = expectThrows(VerificationException.class, () -> LetResolver.resolve(relation("a"), List.of(a)));
        assertThat(e.getMessage(), containsString("Circular reference detected in LET bindings"));
    }

    public void testLetResolutionComplexCycle() {
        // LET a = (FROM b | LIMIT 1),
        // b = (FROM a | LIMIT 1);
        // FROM a
        LetBinding a = binding("a", withLimit(relation("b")));
        LetBinding b = binding("b", withLimit(relation("a")));
        var e = expectThrows(VerificationException.class, () -> LetResolver.resolve(relation("a"), List.of(a, b)));
        assertThat(e.getMessage(), containsString("Circular reference detected in LET bindings"));
    }

    public void testLetResolutionCycleInInSubquery() {
        // LET a = (FROM b | LIMIT 1),
        // b = (FROM base | WHERE x IN a | LIMIT 1);
        // FROM a
        // substitute replaces InSubquery(x, UR("a")) → InSubquery(x, Limit(UR("b"),1))
        // checkForCycles recurses into the subquery and finds UR("b") ∈ resolved → cycle
        LetBinding a = binding("a", withLimit(relation("b")));
        Expression value = new UnresolvedAttribute(EMPTY, "x");
        LetBinding b = binding("b", withLimit(new Filter(EMPTY, relation("base"), new InSubquery(EMPTY, value, relation("a")))));
        var e = expectThrows(VerificationException.class, () -> LetResolver.resolve(relation("a"), List.of(a, b)));
        assertThat(e.getMessage(), containsString("Circular reference detected in LET bindings"));
    }

    public void testLetResolutionCycleInMultiColumnInSubquery() {
        // LET a = (FROM b | LIMIT 1),
        // b = (FROM base | WHERE (x, y) IN a | LIMIT 1);
        // FROM a
        // Same cycle as above but through a multi-column IN subquery.
        LetBinding a = binding("a", withLimit(relation("b")));
        List<Expression> values = of(new UnresolvedAttribute(EMPTY, "x"), new UnresolvedAttribute(EMPTY, "y"));
        LetBinding b = binding(
            "b",
            withLimit(new Filter(EMPTY, relation("base"), new MultiColumnInSubquery(EMPTY, values, relation("a"))))
        );
        var e = expectThrows(VerificationException.class, () -> LetResolver.resolve(relation("a"), List.of(a, b)));
        assertThat(e.getMessage(), containsString("Circular reference detected in LET bindings"));
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

        var e = expectThrows(VerificationException.class, () -> LetResolver.resolve(relation("a"), List.of(a, b)));
        assertThat(e.getMessage(), containsString("Forward reference in LET bindings: [b] cannot be referenced before its declaration"));
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
}
