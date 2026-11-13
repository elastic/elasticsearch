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
import java.util.Objects;
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
     * Combines the given queries as clauses of a new bool query, or returns the only one, ignoring nulls and duplicates.
     * Lucene flattens nested bool queries on the shard when that keeps their meaning.
     */
    public static QueryBuilder combine(Clause clause, List<QueryBuilder> queries) {
        List<QueryBuilder> distinct = queries.stream().filter(Objects::nonNull).distinct().toList();
        if (distinct.size() <= 1) {
            return distinct.isEmpty() ? null : distinct.getFirst();
        }
        BoolQueryBuilder bool = boolQuery();
        clause.innerQueries.apply(bool).addAll(distinct);
        return bool;
    }
}
