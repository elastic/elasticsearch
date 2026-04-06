/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.index.query.QueryRewriteAsyncAction;

import java.util.Objects;

import static org.elasticsearch.common.Strings.format;
import static org.elasticsearch.search.vectors.KnnVectorQueryBuilder.QUERY_VECTOR_BUILDER_FIELD;

public final class QueryVectorBuilderAsyncAction extends QueryRewriteAsyncAction<float[], QueryVectorBuilderAsyncAction> {
    private final QueryVectorBuilder queryVectorBuilder;

    public QueryVectorBuilderAsyncAction(QueryVectorBuilder queryVectorBuilder) {
        this.queryVectorBuilder = Objects.requireNonNull(queryVectorBuilder);
    }

    @Override
    protected void execute(Client client, ActionListener<float[]> listener) {
        queryVectorBuilder.buildVector(client, listener.delegateFailureAndWrap((l, v) -> {
            if (v == null) {
                throw new IllegalArgumentException(
                    format(
                        "[%s] with name [%s] returned null query_vector",
                        QUERY_VECTOR_BUILDER_FIELD.getPreferredName(),
                        queryVectorBuilder.getWriteableName()
                    )
                );
            }
            l.onResponse(v);
        }));
    }

    @Override
    public int doHashCode() {
        return Objects.hash(queryVectorBuilder);
    }

    @Override
    public boolean doEquals(QueryVectorBuilderAsyncAction other) {
        return Objects.equals(queryVectorBuilder, other.queryVectorBuilder);
    }
}
