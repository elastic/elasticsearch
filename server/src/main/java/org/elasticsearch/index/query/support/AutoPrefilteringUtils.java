/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.DisMaxQueryBuilder;
import org.elasticsearch.index.query.InterceptedQueryBuilderWrapper;
import org.elasticsearch.index.query.NestedQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public final class AutoPrefilteringUtils {

    private AutoPrefilteringUtils() {}

    /**
     * Prunes any query branch on queries whose type is included in the prunedTypes set.
     * Note that queries may not preserve their scoring parameters as this assumes the returned
     * query is going to be used for filtering only.
     *
     * @param query the root query to start pruning from
     * @param prunedTypes the types of queries to prune
     * @return an {@link Optional} containing the pruned query, or {@link Optional#empty()} if the query was pruned to nothing.
     */
    public static Optional<QueryBuilder> pruneQuery(QueryBuilder query, Set<Class<? extends QueryBuilder>> prunedTypes) {
        if (prunedTypes.contains(query.getClass())) {
            // Matched a pruned query type, prune away!
            return Optional.empty();
        }

        if (query instanceof InterceptedQueryBuilderWrapper interceptedQuery) {
            Optional<QueryBuilder> pruned = pruneQuery(interceptedQuery.query(), prunedTypes);
            return pruned.map(q -> q == interceptedQuery.query() ? interceptedQuery : new InterceptedQueryBuilderWrapper(q));
        }
        if (query instanceof NestedQueryBuilder nestedQuery) {
            Optional<QueryBuilder> pruned = pruneQuery(nestedQuery.query(), prunedTypes);
            return pruned.map(
                q -> q == nestedQuery.query() ? nestedQuery : new NestedQueryBuilder(nestedQuery.path(), q, nestedQuery.scoreMode())
            );
        }
        if (query instanceof BoolQueryBuilder boolQuery) {
            return pruneBooleanQueryClausesThatAreIneligibleForPrefiltering(boolQuery, prunedTypes);
        }
        if (query instanceof BoostingQueryBuilder boostingQuery) {
            // We only need the positive query here as the negative query is used for scoring only - we are filtering.
            return pruneQuery(boostingQuery.positiveQuery(), prunedTypes);
        }
        if (query instanceof ConstantScoreQueryBuilder constantScoreQuery) {
            Optional<QueryBuilder> pruned = pruneQuery(constantScoreQuery.innerQuery(), prunedTypes);
            return pruned.map(q -> q == constantScoreQuery.innerQuery() ? constantScoreQuery : new ConstantScoreQueryBuilder(q));
        }
        if (query instanceof FunctionScoreQueryBuilder functionScoreQuery) {
            // We could remove the function score entirely as it should not be helpful for filtering,
            // but leaving it in for now to preserve the original query.
            Optional<QueryBuilder> pruned = pruneQuery(functionScoreQuery.query(), prunedTypes);
            return pruned.map(
                q -> q == functionScoreQuery.query()
                    ? functionScoreQuery
                    : new FunctionScoreQueryBuilder(q, functionScoreQuery.filterFunctionBuilders())
            );
        }
        if (query instanceof DisMaxQueryBuilder disMaxQuery) {
            DisMaxQueryBuilder builder = new DisMaxQueryBuilder();
            for (QueryBuilder innerQuery : disMaxQuery.innerQueries()) {
                Optional<QueryBuilder> pruned = pruneQuery(innerQuery, prunedTypes);
                pruned.ifPresent(builder::add);
            }
            // No need to preserve tiebreaks for filtering.
            return builder.innerQueries().isEmpty() ? Optional.empty() : Optional.of(builder);
        }

        return Optional.of(query);
    }

    private static Optional<QueryBuilder> pruneBooleanQueryClausesThatAreIneligibleForPrefiltering(
        BoolQueryBuilder boolQuery,
        Set<Class<? extends QueryBuilder>> prunedTypes
    ) {
        BoolQueryBuilder builder = new BoolQueryBuilder();
        pruneQueries(boolQuery.must(), prunedTypes).forEach(builder::must);
        pruneQueries(boolQuery.should(), prunedTypes).forEach(builder::should);
        pruneQueries(boolQuery.filter(), prunedTypes).forEach(builder::filter);
        pruneQueries(boolQuery.mustNot(), prunedTypes).forEach(builder::mustNot);
        builder.minimumShouldMatch(boolQuery.minimumShouldMatch());
        // No need to preserve scoring parameters for filtering.
        return builder.hasClauses() ? Optional.of(builder) : Optional.empty();
    }

    private static List<QueryBuilder> pruneQueries(List<QueryBuilder> queries, Set<Class<? extends QueryBuilder>> prunedTypes) {
        return queries.stream().map(q -> pruneQuery(q, prunedTypes)).filter(Optional::isPresent).map(Optional::get).toList();
    }
}
