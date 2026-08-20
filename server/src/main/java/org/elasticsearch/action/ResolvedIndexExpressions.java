/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndexExpression.LocalExpressions;
import org.elasticsearch.action.ResolvedIndexExpression.LocalIndexResolutionResult;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A collection of {@link ResolvedIndexExpression}.
 *
 * @param authorizationFailureTemplate not null iff any entry in {@code expressions} has a resolution result of
 *                                     {@link LocalIndexResolutionResult#CONCRETE_RESOURCE_UNAUTHORIZED}.
 *                                     When resolving cross-project index expressions, if an index expression is unauthorized, the context
 *                                     required for an appropriate error message, such as the local role and granting privileges, are only
 *                                     available on the fulfilling cluster. The fulfilling cluster returns the template as the querying
 *                                     cluster may need this for error handling.
 */
public record ResolvedIndexExpressions(List<ResolvedIndexExpression> expressions, @Nullable String authorizationFailureTemplate)
    implements
        Writeable {

    public static final TransportVersion RESOLVED_INDEX_EXPRESSIONS = TransportVersion.fromName("resolved_index_expressions");
    public static final TransportVersion RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE = TransportVersion.fromName(
        "resolved_index_expressions_auth_template"
    );

    public ResolvedIndexExpressions(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(ResolvedIndexExpression::new), in);
    }

    private ResolvedIndexExpressions(List<ResolvedIndexExpression> expressions, StreamInput in) throws IOException {
        this(
            expressions,
            in.getTransportVersion().supports(RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE)
                ? in.readOptionalString()
                : extractErrorTemplateFromLegacyExceptions(expressions)
        );
    }

    private static @Nullable String extractErrorTemplateFromLegacyExceptions(List<ResolvedIndexExpression> expressions) {
        String template = null;
        for (var expression : expressions) {
            var local = expression.localExpressions();
            if (local.localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED
                && local.legacyException() != null) {
                if (template == null) {
                    template = local.legacyException().getMessage();
                } else {
                    assert Objects.equals(template, local.legacyException().getMessage());
                }
            }
        }
        return template;
    }

    public List<String> getLocalIndicesList() {
        return expressions.stream().flatMap(e -> e.localExpressions().indices().stream()).toList();
    }

    public List<String> getRemoteIndicesList() {
        return expressions.stream().flatMap(e -> e.remoteExpressions().stream()).toList();
    }

    public boolean localIndicesEmptyOrMissing() {
        return expressions.stream().noneMatch(e -> {
            var local = e.localExpressions();
            return local.localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.SUCCESS
                && local.indices().isEmpty() == false;
        });
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getTransportVersion().supports(RESOLVED_INDEX_EXPRESSIONS_AUTH_TEMPLATE)) {
            out.writeCollection(expressions);
            out.writeOptionalString(authorizationFailureTemplate);
        } else {
            var ex = authorizationFailureTemplate != null
                ? new ElasticsearchSecurityException(authorizationFailureTemplate, RestStatus.FORBIDDEN)
                : null;
            out.writeCollection(expressions, (o, expression) -> {
                if (expression.localExpressions()
                    .localIndexResolutionResult() == ResolvedIndexExpression.LocalIndexResolutionResult.CONCRETE_RESOURCE_UNAUTHORIZED) {
                    expression.writeToLegacy(o, ex);
                } else {
                    expression.writeToLegacy(o, null);
                }
            });
        }
    }

    public static final class Builder {
        private final List<ResolvedIndexExpression> expressions = new ArrayList<>();

        /**
         * Add a new resolved expression.
         * @param original         the original expression that was resolved -- may be blank for "access all" cases
         * @param localExpressions is a HashSet as an optimization -- the set needs to be mutable, and we want to avoid copying it.
         *                         May be empty.
         */
        public void addExpressions(
            String original,
            HashSet<String> localExpressions,
            ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult,
            Set<String> remoteExpressions
        ) {
            Objects.requireNonNull(original);
            Objects.requireNonNull(localExpressions);
            Objects.requireNonNull(resolutionResult);
            Objects.requireNonNull(remoteExpressions);
            expressions.add(
                new ResolvedIndexExpression(original, new LocalExpressions(localExpressions, resolutionResult), remoteExpressions)
            );
        }

        public void addRemoteExpressions(String original, Set<String> remoteExpressions) {
            Objects.requireNonNull(original);
            Objects.requireNonNull(remoteExpressions);
            expressions.add(new ResolvedIndexExpression(original, LocalExpressions.NONE, remoteExpressions));
        }

        public void setAllLocalExpressionsToNone() {
            for (int i = 0; i < expressions.size(); i++) {
                ResolvedIndexExpression current = expressions.get(i);
                if (current.localExpressions() != LocalExpressions.NONE) {
                    expressions.set(
                        i,
                        new ResolvedIndexExpression(
                            current.original(),
                            current.remoteExpressions().isEmpty()
                                ? new LocalExpressions(Set.of(), LocalIndexResolutionResult.SUCCESS)
                                : LocalExpressions.NONE,
                            current.remoteExpressions()
                        )
                    );
                }
            }
        }

        /**
         * Exclude the given expressions from the local expressions of all prior added {@link ResolvedIndexExpression}.
         * When {@code originOnlyExclusion} is {@code true}, a matching entry's local side is cleared but any remote
         * expressions are preserved; otherwise the matching entry is dropped entirely.
         * <p>
         * Only entries whose {@link LocalIndexResolutionResult} is {@link LocalIndexResolutionResult#SUCCESS} are
         * removed or cleared on an exact-original match. Entries that already represent a resolution failure
         * (e.g. {@link LocalIndexResolutionResult#CONCRETE_RESOURCE_NOT_VISIBLE} or
         * {@link LocalIndexResolutionResult#CONCRETE_RESOURCE_UNAUTHORIZED}) are preserved so that downstream
         * validation can still surface the original 404 or 403 — an exclusion like {@code -foo} must not silently
         * suppress an error from a preceding failed inclusion of {@code foo}.
         */
        public void excludeFromExpressions(Set<String> expressionsToExclude, boolean originOnlyExclusion) {
            Objects.requireNonNull(expressionsToExclude);
            if (expressionsToExclude.isEmpty() == false) {
                final var iter = expressions.listIterator();
                while (iter.hasNext()) {
                    final ResolvedIndexExpression current = iter.next();
                    if (expressionsToExclude.contains(current.original())) {
                        if (current.localExpressions().localIndexResolutionResult() != LocalIndexResolutionResult.SUCCESS) {
                            continue;
                        }
                        if (originOnlyExclusion && false == current.remoteExpressions().isEmpty()) {
                            iter.set(new ResolvedIndexExpression(current.original(), LocalExpressions.NONE, current.remoteExpressions()));
                        } else {
                            iter.remove();
                        }
                        continue;
                    }
                    final Set<String> localExpressions = current.localExpressions().indices();
                    if (localExpressions.isEmpty()) {
                        continue;
                    }
                    localExpressions.removeAll(expressionsToExclude);
                }
            }
        }

        public ResolvedIndexExpressions build() {
            // TODO make all sets on `expressions` immutable
            return new ResolvedIndexExpressions(expressions, (String) null);
        }
    }
}
