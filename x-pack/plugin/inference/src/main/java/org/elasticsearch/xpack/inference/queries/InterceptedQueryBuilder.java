/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.apache.lucene.search.Query;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public abstract class InterceptedQueryBuilder<T extends AbstractQueryBuilder<T>> extends AbstractQueryBuilder<InterceptedQueryBuilder<T>> {
    private T originalQuery;
    private EmbeddingsProvider embeddingsProvider;

    protected InterceptedQueryBuilder(T originalQuery) {
        Objects.requireNonNull(originalQuery, "original query must not be null");
        this.originalQuery = originalQuery;
        this.embeddingsProvider = null;
    }

    protected InterceptedQueryBuilder(StreamInput in) throws IOException {
        super(in);
        this.originalQuery = in.readNamedWriteable(originalQueryClass());
        this.embeddingsProvider = in.readOptionalNamedWriteable(EmbeddingsProvider.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeNamedWriteable(originalQuery);
        out.writeOptionalNamedWriteable(embeddingsProvider);
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(getName(), originalQuery);
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new UnsupportedOperationException("Query should be rewritten to a different type");
    }

    @Override
    protected boolean doEquals(InterceptedQueryBuilder<T> other) {
        return Objects.equals(originalQuery, other.originalQuery) && Objects.equals(embeddingsProvider, other.embeddingsProvider);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(originalQuery, embeddingsProvider);
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.CCS_COMPATIBLE_QUERY_INTERCEPTORS;
    }

    protected abstract Class<T> originalQueryClass();
}
