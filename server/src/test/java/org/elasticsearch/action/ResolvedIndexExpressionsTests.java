/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.test.ESTestCase;

import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.sameInstance;

public class ResolvedIndexExpressionsTests extends ESTestCase {

    public void testExcludeRemovesIndicesFromPriorAdd() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("logs-*", Set.of("logs-1", "logs-2", "logs-internal"), SUCCESS, Set.of());
        b.excludeFromLocalExpressions(Set.of("logs-internal"));
        assertThat(b.build().getLocalIndicesList(), containsInAnyOrder("logs-1", "logs-2"));
    }

    public void testExcludeMatchingOriginalDropsEntry() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("drop-me", Set.of("drop-me"), SUCCESS, Set.of());
        b.addExpressions("keep-me", Set.of("keep-me"), SUCCESS, Set.of());
        b.excludeFromLocalExpressions(Set.of("drop-me"));
        var result = b.build();
        assertThat(result.expressions(), hasSize(1));
        assertThat(result.expressions().get(0).original(), equalTo("keep-me"));
    }

    public void testExcludeDoesNotAffectSubsequentAdd() {
        var b = ResolvedIndexExpressions.builder();
        b.excludeFromLocalExpressions(Set.of("x"));
        b.addExpressions("a", Set.of("x", "y"), SUCCESS, Set.of());
        assertThat(b.build().getLocalIndicesList(), containsInAnyOrder("x", "y"));
    }

    public void testMultipleExcludesApplyToSameAdd() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("w", Set.of("a", "b", "c"), SUCCESS, Set.of());
        b.excludeFromLocalExpressions(Set.of("a"));
        b.excludeFromLocalExpressions(Set.of("b"));
        assertThat(b.build().getLocalIndicesList(), containsInAnyOrder("c"));
    }

    public void testExcludeAppliesToAllPriorAdds() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("e1", Set.of("shared", "a"), SUCCESS, Set.of());
        b.addExpressions("e2", Set.of("shared", "b"), SUCCESS, Set.of());
        b.excludeFromLocalExpressions(Set.of("shared"));
        assertThat(b.build().getLocalIndicesList(), containsInAnyOrder("a", "b"));
    }

    public void testDisjointExcludeReusesObject() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("e", Set.of("a"), SUCCESS, Set.of());
        b.excludeFromLocalExpressions(Set.of("zzz"));
        var result = b.build();
        assertThat(result.getLocalIndicesList(), containsInAnyOrder("a"));
        assertThat(result.expressions(), hasSize(1));
    }

    public void testEmptyExcludeIsNoOp() {
        var expr = new ResolvedIndexExpression("e", new ResolvedIndexExpression.LocalExpressions(Set.of("a"), SUCCESS, null), Set.of());
        var b = ResolvedIndexExpressions.builder();
        b.addExpression(expr);
        b.excludeFromLocalExpressions(Set.of()); // no-op
        assertThat(b.build().expressions().get(0), sameInstance(expr));
    }

    public void testBuildIsRepeatable() {
        var b = ResolvedIndexExpressions.builder();
        b.addExpressions("e", Set.of("a"), SUCCESS, Set.of());
        var first = b.build();
        b.excludeFromLocalExpressions(Set.of("a"));
        var second = b.build();
        assertThat(first.getLocalIndicesList(), containsInAnyOrder("a"));
        assertThat(second.getLocalIndicesList(), empty());
    }
}
