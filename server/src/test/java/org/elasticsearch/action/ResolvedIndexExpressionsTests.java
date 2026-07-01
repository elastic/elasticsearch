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
import java.util.List;
import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalExpressions.NONE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;

public class ResolvedIndexExpressionsTests extends ESTestCase {

    public void testOriginOnlyExclusionMatchingOriginalPreservesRemoteExpressions() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("shared-index", new HashSet<>(Set.of("shared-index")), SUCCESS, Set.of("remote:shared-index"));

        builder.excludeFromExpressions(Set.of("shared-index"), true);

        List<ResolvedIndexExpression> expressions = builder.build().expressions();
        assertThat(expressions, hasSize(1));
        assertThat(expressions.get(0).original(), equalTo("shared-index"));
        assertThat(expressions.get(0).localExpressions(), equalTo(NONE));
        assertThat(expressions.get(0).remoteExpressions(), contains("remote:shared-index"));
    }

    public void testFlatExclusionMatchingOriginalRemovesEntryEvenWhenRemoteExpressionsPresent() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("shared-index", new HashSet<>(Set.of("shared-index")), SUCCESS, Set.of("remote:shared-index"));

        builder.excludeFromExpressions(Set.of("shared-index"), false);

        assertThat(builder.build().expressions(), empty());
    }

    public void testExclusionMatchingOriginalRemovesLocalOnlyExpression() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("shared-index", new HashSet<>(Set.of("shared-index")), SUCCESS, Set.of());

        builder.excludeFromExpressions(Set.of("shared-index"), randomBoolean());

        assertThat(builder.build().expressions(), empty());
    }

    public void testExclusionNotMatchingOriginalStillRemovesFromLocalIndices() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("index-*", new HashSet<>(Set.of("index-1", "shared-index")), SUCCESS, Set.of("remote:index-*"));

        builder.excludeFromExpressions(Set.of("shared-index"), randomBoolean());

        List<ResolvedIndexExpression> expressions = builder.build().expressions();
        assertThat(expressions, hasSize(1));
        assertThat(expressions.get(0).original(), equalTo("index-*"));
        assertThat(expressions.get(0).localExpressions().indices(), contains("index-1"));
        assertThat(expressions.get(0).remoteExpressions(), contains("remote:index-*"));
    }

    public void testExclusionMatchingOriginalPreservesNotVisibleEntry() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("missing-index", new HashSet<>(Set.of("missing-index")), CONCRETE_RESOURCE_NOT_VISIBLE, Set.of());

        builder.excludeFromExpressions(Set.of("missing-index"), randomBoolean());

        List<ResolvedIndexExpression> expressions = builder.build().expressions();
        assertThat(expressions, hasSize(1));
        assertThat(expressions.get(0).original(), equalTo("missing-index"));
        assertThat(expressions.get(0).localExpressions().localIndexResolutionResult(), equalTo(CONCRETE_RESOURCE_NOT_VISIBLE));
        assertThat(expressions.get(0).localExpressions().indices(), contains("missing-index"));
        assertThat(expressions.get(0).remoteExpressions(), empty());
    }

    public void testExclusionMatchingOriginalPreservesNotVisibleEntryWithRemoteExpressions() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions(
            "missing-index",
            new HashSet<>(Set.of("missing-index")),
            CONCRETE_RESOURCE_NOT_VISIBLE,
            Set.of("remote:missing-index")
        );

        builder.excludeFromExpressions(Set.of("missing-index"), randomBoolean());

        List<ResolvedIndexExpression> expressions = builder.build().expressions();
        assertThat(expressions, hasSize(1));
        assertThat(expressions.get(0).original(), equalTo("missing-index"));
        assertThat(expressions.get(0).localExpressions().localIndexResolutionResult(), equalTo(CONCRETE_RESOURCE_NOT_VISIBLE));
        assertThat(expressions.get(0).localExpressions().indices(), contains("missing-index"));
        assertThat(expressions.get(0).remoteExpressions(), contains("remote:missing-index"));
    }

    public void testExclusionMatchingOriginalPreservesUnauthorizedEntry() {
        ResolvedIndexExpressions.Builder builder = ResolvedIndexExpressions.builder();
        builder.addExpressions("forbidden-index", new HashSet<>(Set.of("forbidden-index")), CONCRETE_RESOURCE_UNAUTHORIZED, Set.of());

        builder.excludeFromExpressions(Set.of("forbidden-index"), randomBoolean());

        List<ResolvedIndexExpression> expressions = builder.build().expressions();
        assertThat(expressions, hasSize(1));
        assertThat(expressions.get(0).original(), equalTo("forbidden-index"));
        assertThat(expressions.get(0).localExpressions().localIndexResolutionResult(), equalTo(CONCRETE_RESOURCE_UNAUTHORIZED));
        assertThat(expressions.get(0).localExpressions().indices(), contains("forbidden-index"));
    }
}
