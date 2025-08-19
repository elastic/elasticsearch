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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.index.query.AbstractQueryBuilder;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;

public abstract class InterceptedQueryBuilder extends AbstractQueryBuilder<InterceptedQueryBuilder> {
    private EmbeddingsProvider embeddingsProvider;

    public InterceptedQueryBuilder() {
        this.embeddingsProvider = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        // TODO: Implement
    }

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        // TODO: Implement
    }

    @Override
    protected Query doToQuery(SearchExecutionContext context) throws IOException {
        throw new UnsupportedOperationException("Query should be rewritten to a different type");
    }

    @Override
    protected boolean doEquals(InterceptedQueryBuilder other) {
        // TODO: Implement
        return false;
    }

    @Override
    protected int doHashCode() {
        // TODO: Implement
        return 0;
    }

    @Override
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.CCS_COMPATIBLE_QUERY_INTERCEPTORS;
    }
}
