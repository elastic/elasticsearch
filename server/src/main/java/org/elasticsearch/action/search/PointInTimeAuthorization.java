/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Binds internal shared-PIT authorization to the source membership that Search must apply. Expressions are
 * resolved by Security under the current caller; membership is captured as private bytes so later builder
 * mutations cannot widen the source. This coordinator-local carrier is deliberately not a wire API.
 */
final class PointInTimeAuthorization {
    private final OriginalIndices indices;
    @Nullable
    private final OriginalIndices scope;
    @Nullable
    private final byte[] membership;

    private PointInTimeAuthorization(OriginalIndices indices, @Nullable OriginalIndices scope, @Nullable byte[] membership) {
        this.indices = copy(indices);
        this.scope = scope == null ? null : copy(scope);
        this.membership = membership;
    }

    static PointInTimeAuthorization snapshot(OriginalIndices indices) {
        return new PointInTimeAuthorization(indices, null, null);
    }

    static PointInTimeAuthorization source(OriginalIndices indices, OriginalIndices scope, QueryBuilder membership) {
        Objects.requireNonNull(scope, "PIT authorization scope must not be null");
        Objects.requireNonNull(membership, "PIT source membership must not be null");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(membership);
            return new PointInTimeAuthorization(indices, scope, BytesReference.toBytes(out.bytes()).clone());
        } catch (IOException e) {
            throw new UncheckedIOException("could not capture PIT source membership", e);
        }
    }

    PointInTimeAuthorization withIndices(OriginalIndices resolved) {
        return new PointInTimeAuthorization(resolved, scope, membership);
    }

    PointInTimeAuthorization withScope(OriginalIndices resolved) {
        if (scope == null) {
            throw new IllegalStateException("PIT scope resolution requires a constructed source authorization");
        }
        return new PointInTimeAuthorization(indices, resolved, membership);
    }

    OriginalIndices indices() {
        return copy(indices);
    }

    @Nullable
    OriginalIndices scope() {
        return scope == null ? null : copy(scope);
    }

    @Nullable
    QueryBuilder membership(NamedWriteableRegistry registry) throws IOException {
        if (membership == null) {
            return null;
        }
        try (var in = new NamedWriteableAwareStreamInput(new BytesArray(membership).streamInput(), registry)) {
            return in.readNamedWriteable(QueryBuilder.class);
        }
    }

    private static OriginalIndices copy(OriginalIndices original) {
        Objects.requireNonNull(original, "PIT authorization indices must not be null");
        String[] expressions = Objects.requireNonNull(original.indices(), "PIT authorization expressions must not be null");
        if (expressions.length == 0) {
            throw new IllegalArgumentException("PIT authorization expressions must not be empty");
        }
        for (String expression : expressions) {
            Objects.requireNonNull(expression, "PIT authorization expression must not be null");
        }
        return new OriginalIndices(
            expressions.clone(),
            Objects.requireNonNull(original.indicesOptions(), "PIT authorization indices options must not be null")
        );
    }

    @Override
    public boolean equals(Object other) {
        return other instanceof PointInTimeAuthorization that
            && indices.equals(that.indices)
            && Objects.equals(scope, that.scope)
            && Arrays.equals(membership, that.membership);
    }

    @Override
    public String toString() {
        return "PointInTimeAuthorization[indices=" + indices + ", scope=" + scope + ", sourceMembership=" + (membership != null) + "]";
    }

    @Override
    public int hashCode() {
        return Objects.hash(indices, scope, Arrays.hashCode(membership));
    }
}
