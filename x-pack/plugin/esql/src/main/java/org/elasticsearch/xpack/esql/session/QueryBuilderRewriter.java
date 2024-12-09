/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Match;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;


// TODO: needs a better name than QueryBuilderRewriter???
public class QueryBuilderRewriter {
    private final SearchService searchService;
    private final ClusterService clusterService;

    public QueryBuilderRewriter(SearchService searchService, ClusterService clusterService) {
        this.searchService = searchService;
        this.clusterService = clusterService;
    }

    public void rewriteQueryBuilders(
        LogicalPlan plan,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        Map<Index, IndexMetadata> indexMetadata = new HashMap<>();
        Set<Match> functions = fullTextFunctions(plan);

        Set<String> indexNames = indexNames(plan);
        if (indexNames == null || indexNames.size() == 0 || functions.size() == 0) {
            callback.accept(plan, listener);
            return;
        }

        for (String indexName : indexNames(plan)) {
            Index index = new Index(indexName, clusterService.state().metadata().index(indexName).getIndexUUID());
            indexMetadata.put(index, clusterService.state().metadata().index(indexName));
        }

        /*
            We might need to refactor ResolvedIndices into a base class - and for the search DSL we can subclass it because
            it does more than what `QueryRewriteContext` needs.
            Here I am passing an empty map for the first argument and null for the second.
            That's because I know QueryRewriteContext does not actually need them.
         */
        Map<String, OriginalIndices> originalIndicesMap = new HashMap<>();
        ResolvedIndices resolvedIndices = new ResolvedIndices(originalIndicesMap, null, indexMetadata);

        QueryRewriteContext ctx = searchService.getRewriteContext(() -> System.currentTimeMillis(), resolvedIndices, null);

        // this should be for all FullTextFunctions - not just Match
        ConcurrentMap<Match, QueryBuilder> results = new ConcurrentHashMap<>();

        GroupedActionListener<QueryBuilder> actionListener = new GroupedActionListener<>(
            functions.size(),
            new ActionListener<Collection<QueryBuilder>>() {
                @Override
                public void onResponse(Collection<QueryBuilder> ignored) {
                    try {
                        LogicalPlan newPlan = updatedPlan(plan, results);
                        callback.accept(newPlan, listener);
                    } catch (Exception e) {
                        onFailure(e);
                    }
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            }
        );

        for (Match fullTextFunction : functions) {
            QueryBuilder queryBuilder = fullTextFunction.asQuery().asBuilder();

            // We might want to have special handling for onFailure because the error message we return back
            // should also include for which function the failure happened.
            Rewriteable.rewriteAndFetch(queryBuilder, ctx, actionListener.delegateFailureAndWrap((next, nextQueryBuilder) -> {
                // store nextQueryBuilder
                results.put(fullTextFunction, nextQueryBuilder);
                next.onResponse(nextQueryBuilder);
            }));
        }

    }

    public LogicalPlan updatedPlan(LogicalPlan plan, ConcurrentMap<Match, QueryBuilder> newQueryBuilders) {
        LogicalPlan newPlan = plan.transformExpressionsDown(Match.class, m -> {
            if (newQueryBuilders.keySet().contains(m)) {
                return m.newWithQueryBuilder(newQueryBuilders.get(m));
            }
            return m;
        });
        newPlan.setAnalyzed();
        return newPlan;
    }

    public Set<Match> fullTextFunctions(LogicalPlan plan) {
        Set<Match> functions = new HashSet<>();
        plan.forEachExpressionDown(Match.class, match -> functions.add(match));
        return functions;
    }

    public Set<String> indexNames(LogicalPlan analyzedPlan) {
        AtomicReference<Set<String>> indexNames = new AtomicReference<>();
        analyzedPlan.forEachDown(EsRelation.class, esRelation -> { indexNames.set(esRelation.index().concreteIndices()); });

        return indexNames.get();
    }
}
