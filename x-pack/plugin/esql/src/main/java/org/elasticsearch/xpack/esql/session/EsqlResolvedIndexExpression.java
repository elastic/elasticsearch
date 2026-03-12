/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ResolvedIndexExpression;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.transport.RemoteClusterAware;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;
import static org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS;
import static org.elasticsearch.transport.RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;

public record EsqlResolvedIndexExpression(Set<String> expression, Set<String> resolved) {

    private static final EsqlResolvedIndexExpression EMPTY = new EsqlResolvedIndexExpression(Set.of(), Set.of());

    public static Map<String, EsqlResolvedIndexExpression> from(FieldCapabilitiesResponse response) {
        return Stream.concat(
            Stream.of(Map.entry(LOCAL_CLUSTER_GROUP_KEY, response.getResolvedLocally())),
            response.getResolvedRemotely().entrySet().stream()
        )
            .map(
                entry -> Map.entry(
                    entry.getKey(),
                    entry.getValue()
                        .expressions()
                        .stream()
                        .filter(e -> e.localExpressions().indices().isEmpty() == false)
                        .filter(e -> e.localExpressions().localIndexResolutionResult() == SUCCESS)
                        .map(e -> stripClusterAliasForOriginProject(entry.getKey(), e))
                        .reduce(EMPTY, EsqlResolvedIndexExpression::merge)
                )
            )
            .filter(entry -> entry.getValue().expression().isEmpty() == false)
            .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private static EsqlResolvedIndexExpression merge(EsqlResolvedIndexExpression a, EsqlResolvedIndexExpression b) {
        return new EsqlResolvedIndexExpression(Sets.union(a.expression(), b.expression()), Sets.union(a.resolved(), b.resolved()));
    }

    private static EsqlResolvedIndexExpression stripClusterAliasForOriginProject(String origin, ResolvedIndexExpression e) {
        return Objects.equals(origin, LOCAL_CLUSTER_GROUP_KEY) && RemoteClusterAware.isRemoteIndexName(e.original())
            ? new EsqlResolvedIndexExpression(Set.of(RemoteClusterAware.parseLocalIndexName(e.original())), e.localExpressions().indices())
            : new EsqlResolvedIndexExpression(Set.of(e.original()), e.localExpressions().indices());
    }
}
