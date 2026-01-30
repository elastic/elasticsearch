/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.NONE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.elasticsearch.search.crossproject.CrossProjectIndexResolutionValidator.indicesOptionsForCrossProjectFanout;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class ResolvedIndicesTests extends ESTestCase {
    private static final Set<String> NO_REMOTE_EXPRESSIONS = Set.of();

    /**
     * Given project-1 resolved to foo indices foo-1, foo-2, foo-3
     * When we create ResolvedIndices
     * Then project-1 maps to foo
     */
    public void testResolveWithIndexExpressions() {
        var remoteExpressions = Map.of(
            "project-1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "foo",
                        new ResolvedIndexExpression.LocalExpressions(Set.of("foo-1", "foo-2", "foo-3"), SUCCESS, null),
                        NO_REMOTE_EXPRESSIONS
                    )
                )
            )
        );

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(not(anEmptyMap())));

        assertThat(resolvedIndices.getRemoteClusterIndices(), hasKey("project-1"));
        assertThat(Arrays.stream(resolvedIndices.getRemoteClusterIndices().get("project-1").indices()).toList(), containsInAnyOrder("foo"));
    }

    /**
     * Given project-1 resolved logs* to no indices
     * When we create ResolvedIndices
     * Then we do not include project-1 in the results
     */
    public void testResolveWithIndexExpressionsWithEmptyWildcard() {
        var remoteExpressions = Map.of(
            "project-1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs*",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), SUCCESS, null),
                        NO_REMOTE_EXPRESSIONS
                    )
                )
            )
        );

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(anEmptyMap()));
    }

    /**
     * Given project-1 resolved logs to no indices
     * When we create ResolvedIndices
     * Then we do not include project-1 in the results
     */
    public void testResolveWithIndexExpressionsWithNoMatch() {
        var localExpressionNotFoundReason = randomFrom(CONCRETE_RESOURCE_NOT_VISIBLE, CONCRETE_RESOURCE_UNAUTHORIZED, NONE);
        var remoteExpressions = Map.of(
            "project-1",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        "logs",
                        new ResolvedIndexExpression.LocalExpressions(Set.of(), localExpressionNotFoundReason, null),
                        NO_REMOTE_EXPRESSIONS
                    )
                )
            )
        );

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(anEmptyMap()));
    }

    /**
     * Given project-1 resolved foo to foo-1, bar to no indices, and blah to no indices
     * And project-2 resolved foo to no indices, bar to bar-1, and blah to no indices
     * And project-3 resolved foo to no indices, bar to no indices, and blah to no indices
     * When we create ResolvedIndices
     * Then project-1 maps to foo
     * And project-2 maps to bar
     * And we do not include project-3 in the results
     */
    public void testResolveWithIndexExpressionsWithMultipleProjects() {
        var remoteExpressions = Map.ofEntries(
            Map.entry(
                "project-1",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "foo",
                            new ResolvedIndexExpression.LocalExpressions(Set.of("foo-1"), SUCCESS, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "bar",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "blah",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        )
                    )
                )
            ),
            Map.entry(
                "project-2",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "foo",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "bar",
                            new ResolvedIndexExpression.LocalExpressions(Set.of("bar-1"), SUCCESS, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "blah",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        )
                    )
                )
            ),
            Map.entry(
                "project-3",
                new ResolvedIndexExpressions(
                    List.of(
                        new ResolvedIndexExpression(
                            "foo",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "bar",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        ),
                        new ResolvedIndexExpression(
                            "blah",
                            new ResolvedIndexExpression.LocalExpressions(Set.of(), NONE, null),
                            NO_REMOTE_EXPRESSIONS
                        )
                    )
                )
            )
        );

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(not(anEmptyMap())));

        assertThat(resolvedIndices.getRemoteClusterIndices(), hasKey("project-1"));
        assertThat(resolvedIndices.getRemoteClusterIndices(), hasKey("project-2"));
        assertThat(resolvedIndices.getRemoteClusterIndices(), not(hasKey("project-3")));
        assertThat(Arrays.stream(resolvedIndices.getRemoteClusterIndices().get("project-1").indices()).toList(), containsInAnyOrder("foo"));
        assertThat(Arrays.stream(resolvedIndices.getRemoteClusterIndices().get("project-2").indices()).toList(), containsInAnyOrder("bar"));
    }

    private static ResolvedIndices resolveWithIndexExpressions(Map<String, ResolvedIndexExpressions> remoteExpressions) {
        var cpsIndicesOptions = indicesOptionsForCrossProjectFanout(IndicesOptions.DEFAULT);
        return ResolvedIndices.resolveWithIndexExpressions(
            new OriginalIndices(new String[] { "some-local-index" }, cpsIndicesOptions),
            Map.of(mock(), mock()),
            remoteExpressions,
            cpsIndicesOptions
        );
    }
}
