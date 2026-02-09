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

import java.util.HashSet;
import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasSize;

public class ResolvedIndexExpressionsBuilderTests extends ESTestCase {

    private static final Set<String> NO_REMOTE = Set.of();

    public void testBuildProducesImmutableExpressionsList() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("expr", Set.of("index-1"), SUCCESS, NO_REMOTE);
        var result = builder.build();

        expectThrows(UnsupportedOperationException.class, () -> result.expressions().add(null));
    }

    public void testBuildProducesImmutableLocalIndices() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("expr", new HashSet<>(Set.of("index-1", "index-2")), SUCCESS, NO_REMOTE);
        var result = builder.build();

        Set<String> indices = result.expressions().getFirst().localExpressions().indices();
        expectThrows(UnsupportedOperationException.class, () -> indices.add("index-3"));
    }

    public void testBuildProducesImmutableRemoteExpressions() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("expr", Set.of(), SUCCESS, new HashSet<>(Set.of("remote:index-1")));
        var result = builder.build();

        Set<String> remote = result.expressions().getFirst().remoteExpressions();
        expectThrows(UnsupportedOperationException.class, () -> remote.add("remote:index-2"));
    }

    public void testExcludeFromLocalExpressionsFiltersIndices() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("expr", new HashSet<>(Set.of("index-1", "index-2", "index-3")), SUCCESS, NO_REMOTE);
        builder.excludeFromLocalExpressions(Set.of("index-2"));
        var result = builder.build();

        assertThat(result.expressions(), hasSize(1));
        assertThat(result.expressions().getFirst().localExpressions().indices(), containsInAnyOrder("index-1", "index-3"));
    }

    public void testExcludeFromLocalExpressionsRemovesMatchingOriginal() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("to-remove", new HashSet<>(Set.of("index-1")), SUCCESS, NO_REMOTE);
        builder.addExpressions("to-keep", new HashSet<>(Set.of("index-2")), SUCCESS, NO_REMOTE);
        builder.excludeFromLocalExpressions(Set.of("to-remove"));
        var result = builder.build();

        assertThat(result.expressions(), hasSize(1));
        assertEquals("to-keep", result.expressions().getFirst().original());
    }

    public void testExcludeFromLocalExpressionsSkipsEmptyIndices() {
        var builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("expr", Set.of(), SUCCESS, NO_REMOTE);
        builder.excludeFromLocalExpressions(Set.of("anything"));
        var result = builder.build();

        assertThat(result.expressions(), hasSize(1));
        assertThat(result.expressions().getFirst().localExpressions().indices(), empty());
    }

    public void testExcludeDoesNotMutateOriginalExpression() {
        var builder = ResolvedIndexExpressions.builder();
        var originalIndices = new HashSet<>(Set.of("index-1", "index-2", "index-3"));
        builder.addExpressions("expr", originalIndices, SUCCESS, NO_REMOTE);

        // Capture the LocalExpressions instance before exclusion
        var beforeExclusion = builder.build().expressions().getFirst().localExpressions().indices();
        assertThat(beforeExclusion, containsInAnyOrder("index-1", "index-2", "index-3"));

        // Now rebuild and exclude -- the previously built result must be unaffected
        builder.addExpressions("expr", originalIndices, SUCCESS, NO_REMOTE);
        builder.excludeFromLocalExpressions(Set.of("index-2"));
        assertThat(beforeExclusion, containsInAnyOrder("index-1", "index-2", "index-3"));
    }
}
