/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.core.util;

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

        final Function<BoolQueryBuilder, List<QueryBuilder>> innerQueries;

        Clause(Function<BoolQueryBuilder, List<QueryBuilder>> innerQueries) {
            this.innerQueries = innerQueries;
        }
    }

    /**
     * Combines the given queries while attempting to NOT create a new bool query and avoid
     * unnecessary nested queries.
     * The method tries to detect if the first query is a bool query - if that is the case it will
     * reuse that for adding the rest of the clauses, unless that would change what the first query matches
     * (see {@link #canAddTo}).
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
                if (firstQuery instanceof BoolQueryBuilder bqb && canAddTo(clause, bqb)) {
                    bool = bqb.shallowCopy();
                }
            }
            // at least two entries, start copying
            else if (query.equals(firstQuery) == false) {
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

    /**
     * Whether adding a {@code clause} to {@code bool} leaves what {@code bool} matches as it is. A bool with {@code should} clauses and
     * no {@code must} or {@code filter} clause matches at least one of them. Adding a {@code must} or {@code filter} clause makes them
     * optional, since a bool that has one matches without any {@code should} clause, unless {@code minimum_should_match} is set. The
     * {@code OR} that the {@code should} clauses stand for would then stop filtering anything.
     */
    private static boolean canAddTo(Clause clause, BoolQueryBuilder bool) {
        return switch (clause) {
            case FILTER, MUST -> bool.should().isEmpty()
                || bool.must().isEmpty() == false
                || bool.filter().isEmpty() == false
                || bool.minimumShouldMatch() != null;
            case MUST_NOT, SHOULD -> true;
        };
    }

    private static BoolQueryBuilder combine(Clause clause, BoolQueryBuilder bool, QueryBuilder query) {
        var list = clause.innerQueries.apply(bool);
        if (list.contains(query) == false) {
            list.add(query);
        }
        return bool;
    }
}
