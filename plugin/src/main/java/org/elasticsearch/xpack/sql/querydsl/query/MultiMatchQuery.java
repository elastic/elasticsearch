/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.querydsl.query;

import java.util.Map;
import java.util.Objects;

import org.elasticsearch.index.query.MultiMatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.sql.expression.predicate.fulltext.MultiMatchQueryPredicate;
import org.elasticsearch.xpack.sql.tree.Location;

import static org.elasticsearch.index.query.QueryBuilders.multiMatchQuery;

public class MultiMatchQuery extends LeafQuery {

    private final String query;
    private final Map<String, Float> fields;
    private final MultiMatchQueryPredicate predicate;

    public MultiMatchQuery(Location location, String query, Map<String, Float> fields, MultiMatchQueryPredicate predicate) {
        super(location);
        this.query = query;
        this.fields = fields;
        this.predicate = predicate;
    }

    @Override
    public QueryBuilder asBuilder() {
        MultiMatchQueryBuilder queryBuilder = multiMatchQuery(query);
        queryBuilder.fields(fields);
        queryBuilder.analyzer(predicate.analyzer());
        if (predicate.operator() != null) {
            queryBuilder.operator(predicate.operator().toEs());
        }
        return queryBuilder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(query, fields, predicate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        
        MultiMatchQuery other = (MultiMatchQuery) obj;
        return Objects.equals(query, other.query)
                && Objects.equals(fields, other.fields)
                && Objects.equals(predicate, other.predicate);
    }
}
