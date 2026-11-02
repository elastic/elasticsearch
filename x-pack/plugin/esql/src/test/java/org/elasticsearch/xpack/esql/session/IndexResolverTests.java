/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult;
import org.elasticsearch.action.ResolvedIndexExpressions;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.VerificationException;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;

public class IndexResolverTests extends ESTestCase {

    /**
     * The original indices of a derived pattern of data streams are the requested expressions that field caps resolved to at least one
     * index, per cluster; requested data streams that do not exist (here {@code exemplars-k8s} locally and {@code exemplars-cpu} on the
     * remote) are absent.
     */
    public void testResolvedExpressions() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder()
            .withResolvedLocally(
                resolved(
                    resolvedTo("exemplars-cpu", ".ds-exemplars-cpu-2026.09.01-000001", ".ds-exemplars-cpu-2026.09.08-000002"),
                    notFound("exemplars-k8s")
                )
            )
            .withResolvedRemotely(
                Map.of(
                    "remote",
                    resolved(
                        notFound("exemplars-cpu"),
                        resolvedTo("exemplars-generic.otel-default", ".ds-exemplars-generic.otel-default-2026.09.08-000001")
                    )
                )
            )
            .build();

        Map<String, List<String>> originalIndices = IndexResolver.RESOLVED_EXPRESSIONS.apply(
            "exemplars-cpu,exemplars-k8s,remote:exemplars-cpu,remote:exemplars-generic.otel-default",
            response
        );

        assertEquals(Map.of("", List.of("exemplars-cpu"), "remote", List.of("exemplars-generic.otel-default")), originalIndices);
    }

    public void testResolvedExpressionsWithoutMatches() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder()
            .withResolvedLocally(resolved(notFound("exemplars-cpu")))
            .build();
        assertEquals(Map.of(), IndexResolver.RESOLVED_EXPRESSIONS.apply("exemplars-cpu", response));
    }

    /**
     * Field caps leaves the resolution information out when a cluster involved is too old to provide it; the derived pattern cannot be
     * resolved then.
     */
    public void testResolvedExpressionsRequireResolutionInformation() {
        FieldCapabilitiesResponse response = FieldCapabilitiesResponse.builder().build();
        VerificationException e = expectThrows(
            VerificationException.class,
            () -> IndexResolver.RESOLVED_EXPRESSIONS.apply("exemplars-cpu,old:exemplars-cpu", response)
        );
        assertThat(
            e.getMessage(),
            containsString(
                "When querying exemplars, all nodes in all involved clusters must be at least at version 9.3.0; "
                    + "cannot resolve which of [exemplars-cpu,old:exemplars-cpu] exist otherwise"
            )
        );
    }

    private static ResolvedIndexExpressions resolved(ResolvedIndexExpression... expressions) {
        return new ResolvedIndexExpressions(List.of(expressions), null);
    }

    private static ResolvedIndexExpression resolvedTo(String expression, String... indices) {
        return new ResolvedIndexExpression(
            expression,
            new ResolvedIndexExpression.LocalExpressions(Set.of(indices), LocalIndexResolutionResult.SUCCESS),
            Set.of()
        );
    }

    private static ResolvedIndexExpression notFound(String expression) {
        return new ResolvedIndexExpression(
            expression,
            new ResolvedIndexExpression.LocalExpressions(Set.of(), LocalIndexResolutionResult.CONCRETE_RESOURCE_NOT_VISIBLE),
            Set.of()
        );
    }
}
