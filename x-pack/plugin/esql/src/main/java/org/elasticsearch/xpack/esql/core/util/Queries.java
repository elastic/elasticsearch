/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * Utilities for Elasticsearch queries.
 */
public class Queries {

    public enum Clause {
        FILTER(BoolQueryBuilder::filter),
        MUST(BoolQueryBuilder::must);

        final Function<BoolQueryBuilder, List<QueryBuilder>> innerQueries;

        Clause(Function<BoolQueryBuilder, List<QueryBuilder>> innerQueries) {
            this.innerQueries = innerQueries;
        }
    }

    /**
     * Combines the given queries as clauses of a new bool query, or returns the only one, ignoring nulls and the queries that
     * another one already requires, so combining a combination with one of its queries again changes nothing.
     * Lucene flattens nested bool queries on the shard when that keeps their meaning.
     */
    public static QueryBuilder combine(Clause clause, List<QueryBuilder> queries) {
        List<QueryBuilder> clauses = new ArrayList<>();
        for (QueryBuilder query : queries) {
            if (query != null && clauses.stream().noneMatch(c -> requires(clause, c, query))) {
                clauses.add(query);
            }
        }
        return switch (clauses.size()) {
            case 0 -> null;
            case 1 -> clauses.getFirst();
            default -> {
                BoolQueryBuilder bool = boolQuery();
                clause.innerQueries.apply(bool).addAll(clauses);
                yield bool;
            }
        };
    }

    /** Whether {@code query} is {@code required} or holds it as a clause of this kind, at any depth. */
    private static boolean requires(Clause clause, QueryBuilder query, QueryBuilder required) {
        return query.equals(required)
            || query instanceof BoolQueryBuilder bool
                && clause.innerQueries.apply(bool).stream().anyMatch(q -> requires(clause, q, required));
    }
}
