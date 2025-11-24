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
import java.util.stream.Collectors;

import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.NONE;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

public class ResolvedIndicesTests extends ESTestCase {

    public void testResolveWithIndexExpressions() {
        var remoteExpressions = resolvedIndexExpression("logs", SUCCESS, "logs-1", "logs-2", "logs-3");

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(not(anEmptyMap())));
        assertThat(remoteIndices(resolvedIndices), containsInAnyOrder("logs"));
    }

    public void testResolveWithIndexExpressionsWithEmptyWildcard() {
        var remoteExpressions = resolvedIndexExpression("logs*", SUCCESS);

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(anEmptyMap()));
    }

    public void testResolveWithIndexExpressionsWithNoMatch() {
        var remoteExpressions = resolvedIndexExpression(
            "logs",
            randomFrom(CONCRETE_RESOURCE_NOT_VISIBLE, CONCRETE_RESOURCE_UNAUTHORIZED, NONE)
        );

        var resolvedIndices = resolveWithIndexExpressions(remoteExpressions);

        assertThat(resolvedIndices.getRemoteClusterIndices(), is(anEmptyMap()));
    }

    private static Map<String, ResolvedIndexExpressions> resolvedIndexExpression(
        String original,
        ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult,
        String... indices
    ) {
        return Map.of(
            "remote-project",
            new ResolvedIndexExpressions(
                List.of(
                    new ResolvedIndexExpression(
                        original,
                        new ResolvedIndexExpression.LocalExpressions(
                            indices == null ? Set.of() : Arrays.stream(indices).collect(Collectors.toSet()),
                            resolutionResult,
                            null
                        ),
                        Set.of()
                    )
                )
            )
        );
    }

    private static ResolvedIndices resolveWithIndexExpressions(Map<String, ResolvedIndexExpressions> remoteExpressions) {
        return ResolvedIndices.resolveWithIndexExpressions(
            new OriginalIndices(new String[] { "some-local-index" }, IndicesOptions.DEFAULT),
            Map.of(mock(), mock()),
            remoteExpressions,
            IndicesOptions.DEFAULT
        );
    }

    private static List<String> remoteIndices(ResolvedIndices resolvedIndices) {
        var remoteClusterIndices = resolvedIndices.getRemoteClusterIndices();
        assertThat(remoteClusterIndices, hasKey("remote-project"));
        var remoteProjectIndices = remoteClusterIndices.get("remote-project");
        return Arrays.stream(remoteProjectIndices.indices()).toList();
    }
}
