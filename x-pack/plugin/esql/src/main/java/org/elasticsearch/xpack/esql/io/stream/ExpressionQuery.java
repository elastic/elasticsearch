/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.io.stream;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.querydsl.query.Query;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.Objects;

/**
 * Implements an Expression query, which matches documents based on a given expression.
 */
public class ExpressionQuery extends Query {

    private final String targetFieldName;
    private final Expression expression;

    public ExpressionQuery(Source source, String targetFieldName, Expression expression) {
        super(source);
        this.targetFieldName = targetFieldName;
        this.expression = expression;
    }

    public String field() {
        return targetFieldName;
    }

    @Override
    protected QueryBuilder asBuilder() {
        return new ExpressionQueryBuilder(targetFieldName, expression);
    }

    @Override
    public int hashCode() {
        return Objects.hash(targetFieldName, expression);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        ExpressionQuery other = (ExpressionQuery) obj;
        return Objects.equals(targetFieldName, other.targetFieldName) && Objects.equals(expression, other.expression);
    }

    @Override
    protected String innerToString() {
        return "ExpressionQuery{" + "field='" + targetFieldName + '\'' + '}';
    }

    @Override
    public boolean containsPlan() {
        return true;
    }
}
