/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.retriever;

import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * A wrapper that can be used to modify the behaviour of an existing {@link RetrieverBuilder}.
 */
public abstract class RetrieverBuilderWrapper<T extends RetrieverBuilder> extends RetrieverBuilder {
    protected final RetrieverBuilder in;

    protected RetrieverBuilderWrapper(RetrieverBuilder in) {
        this.in = in;
    }

    protected abstract T clone(RetrieverBuilder sub);

    @Override
    public RetrieverBuilder rewrite(QueryRewriteContext ctx) throws IOException {
        var inRewrite = in.rewrite(ctx);
        if (inRewrite != in) {
            return clone(inRewrite);
        }
        return this;
    }

    @Override
    public QueryBuilder topDocsQuery() {
        return in.topDocsQuery();
    }

    @Override
    public RetrieverBuilder minScore(Float minScore) {
        return in.minScore(minScore);
    }

    @Override
    public List<QueryBuilder> getPreFilterQueryBuilders() {
        return in.preFilterQueryBuilders;
    }

    @Override
    public ActionRequestValidationException validate(
        SearchSourceBuilder source,
        ActionRequestValidationException validationException,
        boolean isScroll,
        boolean allowPartialSearchResults
    ) {
        return in.validate(source, validationException, isScroll, allowPartialSearchResults);
    }

    @Override
    public RetrieverBuilder retrieverName(String retrieverName) {
        return in.retrieverName(retrieverName);
    }

    @Override
    public void setRankDocs(RankDoc[] rankDocs) {
        in.setRankDocs(rankDocs);
    }

    @Override
    public RankDoc[] getRankDocs() {
        return in.getRankDocs();
    }

    @Override
    public boolean isCompound() {
        return in.isCompound();
    }

    @Override
    public QueryBuilder explainQuery() {
        return in.explainQuery();
    }

    @Override
    public Float minScore() {
        return in.minScore();
    }

    @Override
    public boolean isFragment() {
        return in.isFragment();
    }

    @Override
    public String toString() {
        return in.toString();
    }

    @Override
    public String retrieverName() {
        return in.retrieverName();
    }

    @Override
    public void extractToSearchSourceBuilder(SearchSourceBuilder searchSourceBuilder, boolean compoundUsed) {
        in.extractToSearchSourceBuilder(searchSourceBuilder, compoundUsed);
    }

    @Override
    public String getName() {
        return in.getName();
    }

    @Override
    protected void doToXContent(XContentBuilder builder, Params params) throws IOException {
        in.doToXContent(builder, params);
    }

    @Override
    protected boolean doEquals(Object o) {
        // Handle the edge case where we need to unwrap the incoming retriever
        if (o instanceof RetrieverBuilderWrapper<?> wrapper) {
            return in.doEquals(wrapper.in);
        } else {
            return in.doEquals(o);
        }
    }

    @Override
    protected int doHashCode() {
        return in.doHashCode();
    }
}
