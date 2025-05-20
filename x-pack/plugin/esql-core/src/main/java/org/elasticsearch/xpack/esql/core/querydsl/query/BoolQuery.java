/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.querydsl.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.xpack.esql.core.QlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * Query representing boolean AND or boolean OR.
 */
public class BoolQuery extends Query {
    /**
     * {@code true} for boolean {@code AND}, {@code false} for boolean {@code OR}.
     */
    private final boolean isAnd;
    private final List<Query> queries;

    public BoolQuery(Source source, boolean isAnd, Query left, Query right) {
        this(source, isAnd, Arrays.asList(left, right));
    }

    public BoolQuery(Source source, boolean isAnd, List<Query> queries) {
        super(source);
        if (CollectionUtils.isEmpty(queries) || queries.size() < 2) {
            throw new QlIllegalArgumentException("At least two queries required by bool query");
        }
        this.isAnd = isAnd;
        this.queries = queries;
    }

    @Override
    protected QueryBuilder asBuilder() {
        BoolQueryBuilder boolQuery = boolQuery();
        for (Query query : queries) {
            QueryBuilder queryBuilder = query.toQueryBuilder();
            if (isAnd) {
                boolQuery.must(queryBuilder);
            } else {
                boolQuery.should(queryBuilder);
            }
        }
        return boolQuery;
    }

    public boolean isAnd() {
        return isAnd;
    }

    public List<Query> queries() {
        return queries;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), isAnd, queries);
    }

    @Override
    public boolean equals(Object obj) {
        if (false == super.equals(obj)) {
            return false;
        }
        BoolQuery other = (BoolQuery) obj;
        return isAnd == other.isAnd && queries.equals(other.queries);
    }

    @Override
    protected String innerToString() {
        StringJoiner sb = new StringJoiner(isAnd ? " AND " : " OR ");
        for (Query query : queries) {
            sb.add(query.toString());
        }
        return sb.toString();
    }

    @Override
    public Query negate(Source source) {
        List<Query> negated = queries.stream().map(q -> q.negate(q.source())).toList();
        if (negated.stream().allMatch(q -> q instanceof NotQuery)) {
            return new NotQuery(source, this);
        }
        return new BoolQuery(source, isAnd == false, negated);
    }

    @Override
    public boolean scorable() {
        return true;
    }
}
