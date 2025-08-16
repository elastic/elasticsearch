/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.enrich;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.compute.operator.lookup.LookupEnrichQueryGenerator;
import org.elasticsearch.compute.operator.lookup.QueryList;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.physical.EsQueryExec;
import org.elasticsearch.xpack.esql.plan.physical.FilterExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;
import org.elasticsearch.xpack.esql.stats.SearchContextStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * A {@link LookupEnrichQueryGenerator} that combines multiple {@link QueryList}s into a single query.
 * Each query in the resulting query will be a conjunction of all queries from the input lists at the same position.
 * In the future we can extend this to support more complex expressions, such as disjunctions or negations.
 */
public class ExpressionQueryList implements LookupEnrichQueryGenerator, PostJoinFilterable {
    private final List<QueryList> queryLists;
    private final List<Query> preJoinFilters = new ArrayList<>();
    private FilterExec postJoinFilter;
    private final SearchExecutionContext context;

    public ExpressionQueryList(
        List<QueryList> queryLists,
        SearchExecutionContext context,
        PhysicalPlan rightPreJoinPlan,
        ClusterService clusterService
    ) {
        if (queryLists.size() < 2 && rightPreJoinPlan == null) {
            throw new IllegalArgumentException("ExpressionQueryList must have at least two QueryLists");
        }
        this.queryLists = queryLists;
        this.context = context;
        buildPrePostJoinFilter(rightPreJoinPlan, clusterService);
    }

    private void buildPrePostJoinFilter(PhysicalPlan rightPreJoinPlan, ClusterService clusterService) {
        // we support a FilterExec as the pre-join filter
        // if the filter Exec is not translatable to a QueryBuilder, we will apply it after the join
        if (rightPreJoinPlan instanceof FilterExec filterExec) {
            try {
                LucenePushdownPredicates lucenePushdownPredicates = LucenePushdownPredicates.from(
                    SearchContextStats.from(List.of(context)),
                    new EsqlFlags(clusterService.getClusterSettings())
                );
                // If the pre-join filter is a FilterExec, we can convert it to a QueryBuilder
                // try to convert it to a QueryBuilder, if not possible apply it after the join
                if (filterExec.condition() instanceof TranslationAware translationAware
                    && TranslationAware.Translatable.YES.equals(translationAware.translatable(lucenePushdownPredicates))) {
                    preJoinFilters.add(
                        translationAware.asQuery(lucenePushdownPredicates, TRANSLATOR_HANDLER).toQueryBuilder().toQuery(context)
                    );
                } else {
                    // if the filter is not translatable, we will apply it after the join
                    postJoinFilter = filterExec;
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to translate pre-join filter: " + filterExec, e);
            }

        } else if (rightPreJoinPlan instanceof EsQueryExec esQueryExec) {
            try {
                // check the EsQueryExec for a pre-join filter
                if (esQueryExec.query() != null) {
                    preJoinFilters.add(esQueryExec.query().toQuery(context));
                }
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to translate pre-join filter: " + esQueryExec, e);
            }
        } else if (rightPreJoinPlan != null) {
            throw new IllegalArgumentException("Unsupported pre-join filter type: " + rightPreJoinPlan.getClass().getName());
        }
    }

    @Override
    public Query getQuery(int position) throws IOException {
        BooleanQuery.Builder builder = new BooleanQuery.Builder();
        for (QueryList queryList : queryLists) {
            Query q = queryList.getQuery(position);
            if (q == null) {
                // if any of the matchFields are null, it means there is no match for this position
                // A AND NULL is always NULL, so we can skip this position
                return null;
            }
            builder.add(q, BooleanClause.Occur.FILTER);
        }
        // also attach the pre-join filter if it exists
        for (Query preJoinFilter : preJoinFilters) {
            builder.add(preJoinFilter, BooleanClause.Occur.FILTER);
        }
        return builder.build();
    }

    @Override
    public int getPositionCount() {
        int positionCount = queryLists.get(0).getPositionCount();
        for (QueryList queryList : queryLists) {
            if (queryList.getPositionCount() != positionCount) {
                throw new IllegalArgumentException(
                    "All QueryLists must have the same position count, expected: "
                        + positionCount
                        + ", but got: "
                        + queryList.getPositionCount()
                );
            }
        }
        return positionCount;
    }

    @Override
    public FilterExec getPostJoinFilter() {
        return postJoinFilter;
    }
}
