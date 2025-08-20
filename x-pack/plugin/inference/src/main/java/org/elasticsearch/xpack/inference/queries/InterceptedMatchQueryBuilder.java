/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference.queries;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.io.IOException;

public class InterceptedMatchQueryBuilder extends InterceptedQueryBuilder<MatchQueryBuilder> {
    public static final String NAME = "intercepted_match";

    public InterceptedMatchQueryBuilder(MatchQueryBuilder originalQuery) {
        super(originalQuery);
    }

    public InterceptedMatchQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    private InterceptedMatchQueryBuilder(InterceptedMatchQueryBuilder other, EmbeddingsProvider embeddingsProvider) {
        super(other, embeddingsProvider);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    protected Class<MatchQueryBuilder> originalQueryClass() {
        return MatchQueryBuilder.class;
    }

    @Override
    protected String getFieldName() {
        return originalQuery.fieldName();
    }

    @Override
    protected String getQuery() {
        return (String) originalQuery.value();
    }

    @Override
    protected QueryBuilder copy(EmbeddingsProvider embeddingsProvider) {
        return new InterceptedMatchQueryBuilder(this, embeddingsProvider);
    }
}
