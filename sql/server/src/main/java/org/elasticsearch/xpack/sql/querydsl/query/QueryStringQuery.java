/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import java.util.Map;
import java.util.Objects;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryStringQueryBuilder;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.StringQueryPredicate;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.FullTextPredicate.Operator;
import org.elasticsearch.xpack.sql.tree.Location;

import static java.util.Collections.singletonMap;

import static org.elasticsearch.index.query.QueryBuilders.queryStringQuery;

public class QueryStringQuery extends LeafQuery {

    private final String query;
    private final Map<String, Float> fields;
    private Operator operator;
    private StringQueryPredicate predicate;

    // dedicated constructor for QueryTranslator
    public QueryStringQuery(Location location, String query, String fieldName) {
        this(location, query, singletonMap(fieldName, Float.valueOf(1.0f)), Operator.AND, null);
    }

    public QueryStringQuery(Location location, String query, Map<String, Float> fields, StringQueryPredicate predicate) {
        this(location, query, fields, null, predicate);
    }

    private QueryStringQuery(Location location, String query, Map<String, Float> fields, Operator operator, StringQueryPredicate predicate) {
        super(location);
        this.query = query;
        this.fields = fields;
        this.predicate = predicate;
        this.operator = operator != null ? operator : predicate.defaultOperator();
    }

    @Override
    public QueryBuilder asBuilder() {
        QueryStringQueryBuilder queryBuilder = queryStringQuery(query);
        queryBuilder.fields(fields);
        if (operator != null) {
            queryBuilder.defaultOperator(operator.toEs());
        }
        if (predicate != null) {
            queryBuilder.analyzer(predicate.analyzer());
        }
        return queryBuilder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, fields, operator, predicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        QueryStringQuery other = (QueryStringQuery) obj;
        return Objects.equals(query, other.query)
                && Objects.equals(fields, other.fields)
                && Objects.equals(operator, other.operator)
                && Objects.equals(predicate, other.predicate);
    }
}
