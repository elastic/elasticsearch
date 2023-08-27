/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ql.util;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;

import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.index.query.QueryBuilders.boolQuery;

/**
 * Utilities for Elasticsearch queries.
 */
public class Queries {

    public enum Clause {
        FILTER(BoolQueryBuilder::filter),
        MUST(BoolQueryBuilder::must),
        MUST_NOT(BoolQueryBuilder::mustNot),
        SHOULD(BoolQueryBuilder::should);

        final Function<BoolQueryBuilder, List<QueryBuilder>> operation;

        Clause(Function<BoolQueryBuilder, List<QueryBuilder>> operation) {
            this.operation = operation;
        }
    }

    /**
     * Combines the given queries while attempting to NOT create a new bool query and avoid
     * unnecessary nested queries.
     * The method tries to detect and reuses existing bool queries instead of simply combining
     * them to avoid creating unnecessary nested queries.
     * If a bool query is detected and its {@link BoolQueryBuilder#minimumShouldMatch()} is set,
     * said query will be added using the given clause instead of being merged onto the existing
     * ones.
     */
    public static QueryBuilder combine(Clause clause, List<QueryBuilder> queries) {
        QueryBuilder firstQuery = null;
        BoolQueryBuilder bool = null;

        for (QueryBuilder query : queries) {
            if (query == null) {
                continue;
            }
            if (firstQuery == null) {
                firstQuery = query;
            }
            // at least two entries, start copying
            else {
                // lazy init the root bool
                if (bool == null) {
                    bool = combine(clause, boolQuery(), firstQuery);
                }
                // keep adding queries to it
                bool = combine(clause, bool, query);
            }
        }

        return bool == null ? firstQuery : bool;
    }

    private static BoolQueryBuilder combine(Clause clause, BoolQueryBuilder bool, QueryBuilder query) {
        if (query instanceof BoolQueryBuilder boolQuery && hasDefaultMinMatch(boolQuery)) {
            bool.filter().addAll(boolQuery.filter());
            bool.should().addAll(boolQuery.should());
            bool.must().addAll(boolQuery.must());
            bool.mustNot().addAll(boolQuery.mustNot());
        } else {
            var list = clause.operation.apply(bool);
            if (list.contains(query) == false) {
                list.add(query);
            }
        }
        return bool;
    }

    private static boolean hasDefaultMinMatch(BoolQueryBuilder boolQuery) {
        var minMatch = boolQuery.minimumShouldMatch();
        return minMatch == null || "0".equals(minMatch);
    }

}
