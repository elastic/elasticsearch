/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis;

import org.elasticsearch.index.IndexMode;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.plan.IndexPattern;
import org.elasticsearch.xpack.esql.plan.LetBinding;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnresolvedRelation;

import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xpack.esql.core.tree.Source.EMPTY;
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
