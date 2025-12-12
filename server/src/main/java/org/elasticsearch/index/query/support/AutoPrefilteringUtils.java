/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.query.support;

import org.elasticsearch.common.lucene.search.Queries;
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

        return switch (query) {
            case BoolQueryBuilder boolQuery -> pruneBoolQuery(boolQuery, prunedTypes);
            // We only need the positive query here as the negative query is used for scoring only - we are filtering.
            case BoostingQueryBuilder boostingQuery -> pruneQuery(boostingQuery.positiveQuery(), prunedTypes);
            case ConstantScoreQueryBuilder constantScoreQuery -> {
                Optional<QueryBuilder> pruned = pruneQuery(constantScoreQuery.innerQuery(), prunedTypes);
                yield pruned.map(q -> q == constantScoreQuery.innerQuery() ? constantScoreQuery : new ConstantScoreQueryBuilder(q));
            }
            case DisMaxQueryBuilder disMaxQuery -> pruneDisMaxQuery(disMaxQuery, prunedTypes);
            case FunctionScoreQueryBuilder functionScoreQuery -> pruneFunctionScoreQuery(functionScoreQuery, prunedTypes);
            case InterceptedQueryBuilderWrapper interceptedQuery -> {
                Optional<QueryBuilder> pruned = pruneQuery(interceptedQuery.query(), prunedTypes);
                yield pruned.map(q -> q == interceptedQuery.query() ? interceptedQuery : new InterceptedQueryBuilderWrapper(q));
            }
            case NestedQueryBuilder nestedQuery -> {
                Optional<QueryBuilder> pruned = pruneQuery(nestedQuery.query(), prunedTypes);
                yield pruned.map(
                    q -> q == nestedQuery.query() ? nestedQuery : new NestedQueryBuilder(nestedQuery.path(), q, nestedQuery.scoreMode())
                );
            }
            default -> Optional.of(query);
        };
    }

    private static Optional<QueryBuilder> pruneBoolQuery(BoolQueryBuilder boolQuery, Set<Class<? extends QueryBuilder>> prunedTypes) {
        BoolQueryBuilder prunedBool = new BoolQueryBuilder();
        pruneQueries(boolQuery.must(), prunedTypes).forEach(prunedBool::must);
        pruneQueries(boolQuery.should(), prunedTypes).forEach(prunedBool::should);
        pruneQueries(boolQuery.filter(), prunedTypes).forEach(prunedBool::filter);
        pruneQueries(boolQuery.mustNot(), prunedTypes).forEach(prunedBool::mustNot);
        adjustMinimumShouldMatchForPrunedShouldClauses(boolQuery, prunedBool);

        if (prunedBool.equals(boolQuery)) {
            return Optional.of(boolQuery);
        }

        // No need to preserve scoring parameters for filtering.
        return prunedBool.hasClauses() ? Optional.of(prunedBool) : Optional.empty();
    }

    private static void adjustMinimumShouldMatchForPrunedShouldClauses(BoolQueryBuilder originalBool, BoolQueryBuilder prunedBool) {
        if (originalBool.minimumShouldMatch() == null) {
            return;
        }
        if (prunedBool.should().size() == originalBool.should().size()) {
            prunedBool.minimumShouldMatch(originalBool.minimumShouldMatch());
        } else {
            int originalMsm = Queries.calculateMinShouldMatch(originalBool.should().size(), originalBool.minimumShouldMatch());
            int numPrunedClauses = originalBool.should().size() - prunedBool.should().size();
            // We need to adjust the minimum should match to account for the pruned clauses.
            // We considered the following approaches:
            // 1. strict approach: set to min(remaining_should_clauses, original_msm)
            // 2. lenient approach: if msm is set and at least one should clause is pruned, prune all should clauses.
            // 3. middle ground approach: set to max(0, original_msm - remaining_should_clauses)
            // Let us imagine a query with 5 should clauses. 2 get pruned. msm is 3. 1 remaining clause matches.
            // Approach 1 would make the entire bool query to not match as we would retain msm of 3 but only 1 clause would match.
            // We do not know whether the pruned clauses would match or not. Thus, this approach seems too restrictive.
            // Approach 2 would mean we prune all should clauses and the query would match,
            // even if none of the remaining should clauses match.
            // Approach 3 would mean we adjust the msm to 3 - 2 = 1. This would mean that the query would match if at least one
            // of the remaining clauses matches.
            // We opt for the lenient approach. It is as if we assume the pruned clauses matched. Seems to be the best compromise.
            int prunedMsm = Math.max(0, originalMsm - numPrunedClauses);
            prunedBool.minimumShouldMatch(prunedMsm);
        }
    }

    private static Optional<QueryBuilder> pruneDisMaxQuery(DisMaxQueryBuilder disMaxQuery, Set<Class<? extends QueryBuilder>> prunedTypes) {
        DisMaxQueryBuilder builder = new DisMaxQueryBuilder();
        for (QueryBuilder innerQuery : disMaxQuery.innerQueries()) {
            Optional<QueryBuilder> pruned = pruneQuery(innerQuery, prunedTypes);
            pruned.ifPresent(builder::add);
        }
        if (builder.innerQueries().equals(disMaxQuery.innerQueries())) {
            return Optional.of(disMaxQuery);
        }
        // No need to preserve tiebreaks for filtering.
        return builder.innerQueries().isEmpty() ? Optional.empty() : Optional.of(builder);
    }

    private static Optional<QueryBuilder> pruneFunctionScoreQuery(
        FunctionScoreQueryBuilder functionScoreQuery,
        Set<Class<? extends QueryBuilder>> prunedTypes
    ) {
        // We could remove the function score entirely as it should not be helpful for filtering,
        // but leaving it in for now to preserve the original query.
        Optional<QueryBuilder> pruned = pruneQuery(functionScoreQuery.query(), prunedTypes);
        return pruned.map(
            q -> q == functionScoreQuery.query()
                ? functionScoreQuery
                : new FunctionScoreQueryBuilder(q, functionScoreQuery.filterFunctionBuilders())
        );
    }

    private static List<QueryBuilder> pruneQueries(List<QueryBuilder> queries, Set<Class<? extends QueryBuilder>> prunedTypes) {
        return queries.stream().map(q -> pruneQuery(q, prunedTypes)).filter(Optional::isPresent).map(Optional::get).toList();
    }
}
