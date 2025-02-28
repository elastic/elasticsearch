/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.rank.hybrid;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.retriever.RetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

public class WrappedStandardRetrieverBuilder extends RetrieverBuilder {
    private final StandardRetrieverBuilder wrappedRetriever;
    private final QueryBuilder queryBuilder;
    private final String field;

    public WrappedStandardRetrieverBuilder(String field) {
        Objects.requireNonNull(field);
        this.field = field;
        this.queryBuilder = new BoolQueryBuilder();
        this.wrappedRetriever = new StandardRetrieverBuilder(this.queryBuilder);
    }

    @Override
    public QueryBuilder topDocsQuery() {
        return wrappedRetriever.topDocsQuery();
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        wrappedRetriever.extractToSearchSourceBuilder(searchSourceBuilder, compoundUsed);
    }

    @Override
    public String getName() {
        return wrappedRetriever.getName();
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        wrappedRetriever.doToXContent(builder, params);
    }

    @Override
    protected boolean doEquals(Object o) {
        WrappedStandardRetrieverBuilder that = (WrappedStandardRetrieverBuilder) o;
        return field.equals(that.field) && wrappedRetriever.equals(that.wrappedRetriever);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(field, wrappedRetriever);
    }
}
