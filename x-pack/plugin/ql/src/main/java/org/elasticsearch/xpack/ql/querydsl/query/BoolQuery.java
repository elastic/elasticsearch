/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ql.querydsl.query;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.sort.NestedSortBuilder;
import org.elasticsearch.xpack.ql.QlIllegalArgumentException;
import org.elasticsearch.xpack.ql.tree.Source;
import org.elasticsearch.xpack.ql.util.CollectionUtils;

import java.util.ArrayList;
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
    public boolean containsNestedField(String path, String field) {
        for (Query query : queries) {
            if (query.containsNestedField(path, field)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Query addNestedField(String path, String field, String format, boolean hasDocValues) {
        boolean unchanged = true;
        List<Query> rewritten = new ArrayList<>(queries.size());
        for (Query query : queries) {
            var rewrittenQuery = query.addNestedField(path, field, format, hasDocValues);
            unchanged &= rewrittenQuery == query;
            rewritten.add(rewrittenQuery);
        }
        return unchanged ? this : new BoolQuery(source(), isAnd, rewritten);
    }

    @Override
    public void enrichNestedSort(NestedSortBuilder sort) {
        for (Query query : queries) {
            query.enrichNestedSort(sort);
        }
    }

    @Override
    public QueryBuilder asBuilder() {
        BoolQueryBuilder boolQuery = boolQuery();
        for (Query query : queries) {
            if (isAnd) {
                boolQuery.must(query.asBuilder());
            } else {
                boolQuery.should(query.asBuilder());
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
}
