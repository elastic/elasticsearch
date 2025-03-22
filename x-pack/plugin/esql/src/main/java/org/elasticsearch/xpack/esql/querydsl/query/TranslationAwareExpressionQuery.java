/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.querydsl.query;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

/**
 * Expressions that store their own {@link QueryBuilder} and implement
 * {@link TranslationAware} can use {@link TranslationAwareExpressionQuery}
 * to wrap their {@link QueryBuilder}, instead of using the other existing {@link Query} implementations.
 */
public class TranslationAwareExpressionQuery extends Query {
    private final QueryBuilder queryBuilder;

    public TranslationAwareExpressionQuery(Source source, QueryBuilder queryBuilder) {
        super(source);
        this.queryBuilder = queryBuilder;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return queryBuilder;
    }

    @Override
    protected String innerToString() {
        return queryBuilder.toString();
    }

    @Override
    public boolean scorable() {
        // All Full Text Functions are translated to queries using this method
        return true;
    }
}
