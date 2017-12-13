/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import java.util.Objects;

import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MatchQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.FullTextPredicate.Operator;
import org.elasticsearch.xpack.sql.tree.Location;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;

public class MatchQuery extends LeafQuery {

    private final String name;
    private final Object text;
    private final Operator operator;
    private final MatchQueryPredicate predicate;

    public MatchQuery(Location location, String name, Object text) {
        this(location, name, text, Operator.AND, null);
    }

    public MatchQuery(Location location, String name, Object text, MatchQueryPredicate predicate) {
        this(location, name, text, null, predicate);
    }

    private MatchQuery(Location location, String name, Object text, Operator operator, MatchQueryPredicate predicate) {
        super(location);
        this.name = name;
        this.text = text;
        this.predicate = predicate;
        this.operator = operator != null ? operator : predicate.operator();
    }

    @Override
    public QueryBuilder asBuilder() {
        MatchQueryBuilder queryBuilder = matchQuery(name, text);
        if (operator != null) {
            queryBuilder.operator(operator.toEs());
        }
        if (predicate != null) {
            queryBuilder.analyzer(predicate.analyzer());
        }
        return queryBuilder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(text, name, operator, predicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        MatchQuery other = (MatchQuery) obj;
        return Objects.equals(text, other.text)
                && Objects.equals(name, other.name)
                && Objects.equals(operator, other.operator)
                && Objects.equals(predicate, other.predicate);
    }
}
