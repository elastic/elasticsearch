/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.action.support.GroupedActionListener;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.BiConsumer;

public class QueryBuilderResolver {
    private final SearchService searchService;
    private final ClusterService clusterService;
    private final TransportService transportService;
    private final IndexNameExpressionResolver indexNameExpressionResolver;

    public QueryBuilderResolver(
        SearchService searchService,
        ClusterService clusterService,
        TransportService transportService,
        IndexNameExpressionResolver indexNameExpressionResolver
    ) {
        this.searchService = searchService;
        this.clusterService = clusterService;
        this.transportService = transportService;
        this.indexNameExpressionResolver = indexNameExpressionResolver;
    }

    public void resolveQueryBuilders(
        LogicalPlan plan,
        ActionListener<Result> listener,
        BiConsumer<LogicalPlan, ActionListener<Result>> callback
    ) {
        Set<FullTextFunction> unresolved = fullTextFunctions(plan);
        Set<String> indexNames = indexNames(plan);

        if (indexNames == null || indexNames.isEmpty() || unresolved.isEmpty()) {
            callback.accept(plan, listener);
            return;
        }
        ConcurrentMap<FullTextFunction, QueryBuilder> results = new ConcurrentHashMap<>();

        // We are using a GroupedActionListener here so that we update the plan only after al the QueryBuilders have been resolved.
        // This way we avoid making concurrent updates to the plan.
        GroupedActionListener<QueryBuilder> groupedActionListener = new GroupedActionListener<>(
            unresolved.size(), // how many QueryBuilders need to be collected before onResponse is called
            new ActionListener<Collection<QueryBuilder>>() {
                @Override
                public void onResponse(Collection<QueryBuilder> ignored) {
                    // This is called only after the groupedActionListener has received all the QueryBuilders
                    try {
                        LogicalPlan newPlan = planWithResolvedQueryBuilders(plan, results);
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

        QueryRewriteContext ctx = queryRewriteContext(indexNames);

        for (FullTextFunction fullTextFunction : unresolved) {
            // First we get the initial query builder
            QueryBuilder queryBuilder = fullTextFunction.asQuery(PlannerUtils.TRANSLATOR_HANDLER).asBuilder();
            // Then the rewrite will produce a new query builder
            Rewriteable.rewriteAndFetch(queryBuilder, ctx, groupedActionListener.delegateFailureAndWrap((next, resolvedQueryBuilder) -> {
                results.put(fullTextFunction, resolvedQueryBuilder);
                next.onResponse(resolvedQueryBuilder);
            }));
        }
    }

    private Set<FullTextFunction> fullTextFunctions(LogicalPlan plan) {
        Set<FullTextFunction> functions = new HashSet<>();
        plan.forEachExpressionDown(FullTextFunction.class, func -> functions.add(func));
        return functions;
    }

    public Set<String> indexNames(LogicalPlan plan) {
        Holder<Set<String>> indexNames = new Holder<>();

        plan.forEachDown(EsRelation.class, esRelation -> { indexNames.set(esRelation.index().concreteIndices()); });

        return indexNames.get();
    }

    public LogicalPlan planWithResolvedQueryBuilders(LogicalPlan plan, ConcurrentMap<FullTextFunction, QueryBuilder> newQueryBuilders) {
        LogicalPlan newPlan = plan.transformExpressionsDown(FullTextFunction.class, m -> {
            if (newQueryBuilders.keySet().contains(m)) {
                return m.replaceQueryBuilder(newQueryBuilders.get(m));
            }
            return m;
        });
        newPlan.setAnalyzed();
        newPlan.setOptimized();
        return newPlan;
    }

    private QueryRewriteContext queryRewriteContext(Set<String> indexNames) {
        ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndexNamesAndOptions(
            indexNames.toArray(String[]::new),
            IndexResolver.FIELD_CAPS_INDICES_OPTIONS,
            clusterService.state(),
            indexNameExpressionResolver,
            transportService.getRemoteClusterService(),
            System.currentTimeMillis()
        );

        return searchService.getRewriteContext(() -> System.currentTimeMillis(), resolvedIndices, null);
    }
}
