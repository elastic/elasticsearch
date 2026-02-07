/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ResolvedIndexExpression.LocalExpressions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * A collection of {@link ResolvedIndexExpression}.
 */
public record ResolvedIndexExpressions(List<ResolvedIndexExpression> expressions) implements Writeable {
    public static final TransportVersion RESOLVED_INDEX_EXPRESSIONS = TransportVersion.fromName("resolved_index_expressions");

    public ResolvedIndexExpressions(StreamInput in) throws IOException {
        this(in.readCollectionAsImmutableList(ResolvedIndexExpression::new));
    }

    public List<String> getLocalIndicesList() {
        return expressions.stream().flatMap(e -> e.localExpressions().indices().stream()).toList();
    }

    public List<String> getRemoteIndicesList() {
        return expressions.stream().flatMap(e -> e.remoteExpressions().stream()).toList();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeCollection(expressions);
    }

    public static final class Builder {
        private final List<Op> ops = new ArrayList<>();

        sealed interface Op {
            record Add(ResolvedIndexExpression expression) implements Op {}

            record Exclude(Set<String> expressions) implements Op {}
        }

        /**
         * Add a new resolved expression.
         * @param original         the original expression that was resolved -- may be blank for "access all" cases
         */
        public void addExpressions(
            String original,
            Set<String> localExpressions,
            ResolvedIndexExpression.LocalIndexResolutionResult resolutionResult,
            Set<String> remoteExpressions
        ) {
            Objects.requireNonNull(original);
            Objects.requireNonNull(localExpressions);
            Objects.requireNonNull(resolutionResult);
            Objects.requireNonNull(remoteExpressions);
            ops.add(
                new Op.Add(
                    new ResolvedIndexExpression(original, new LocalExpressions(localExpressions, resolutionResult, null), remoteExpressions)
                )
            );
        }

        /**
         * Add a new resolved expression.
         * @param expression       the expression you want to add.
         */
        public void addExpression(ResolvedIndexExpression expression) {
            ops.add(new Op.Add(expression));
        }

        public void addRemoteExpressions(String original, Set<String> remoteExpressions) {
            Objects.requireNonNull(original);
            Objects.requireNonNull(remoteExpressions);
            ops.add(new Op.Add(new ResolvedIndexExpression(original, LocalExpressions.NONE, remoteExpressions)));
        }

        /**
         * Exclude the given expressions from the local expressions of all prior added {@link ResolvedIndexExpression}.
         */
        public void excludeFromLocalExpressions(Set<String> expressionsToExclude) {
            Objects.requireNonNull(expressionsToExclude);
            if (expressionsToExclude.isEmpty() == false) {
                ops.add(new Op.Exclude(expressionsToExclude));
            }
        }

        public ResolvedIndexExpressions build() {
            List<ResolvedIndexExpression> expressions = new ArrayList<>();
            outer: for (int i = 0; i < ops.size(); i++) {
                if (ops.get(i) instanceof Op.Add(ResolvedIndexExpression e)) {
                    Set<String> indices = null; // lazily initialized to avoid unnecessary copying if there are no excludes
                    for (int j = i + 1; j < ops.size(); j++) {
                        if (ops.get(j) instanceof Op.Exclude(Set<String> excludes)) {
                            if (excludes.contains(e.original())) {
                                continue outer;
                            }
                            if (indices == null) {
                                indices = new HashSet<>(e.localExpressions().indices());
                            }
                            indices.removeAll(excludes);
                        }
                    }
                    expressions.add(
                        indices == null
                            ? e
                            : new ResolvedIndexExpression(
                                e.original(),
                                new LocalExpressions(
                                    indices,
                                    e.localExpressions().localIndexResolutionResult(),
                                    e.localExpressions().exception()
                                ),
                                e.remoteExpressions()
                            )
                    );
                }
            }
            return new ResolvedIndexExpressions(expressions);
        }
    }
}
