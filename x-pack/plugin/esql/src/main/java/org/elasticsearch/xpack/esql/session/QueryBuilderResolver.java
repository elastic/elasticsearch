/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.session;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
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

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

import static org.elasticsearch.xpack.esql.planner.TranslatorHandler.TRANSLATOR_HANDLER;

/**
 * Some {@link FullTextFunction} implementations such as {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match}
 * will be translated to a {@link QueryBuilder} that require a rewrite phase on the coordinator.
 * {@link QueryBuilderResolver#resolveQueryBuilders(LogicalPlan, ActionListener, BiConsumer)} will rewrite the plan by replacing
 * {@link FullTextFunction} expression with new ones that hold rewritten {@link QueryBuilder}s.
 */
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
        if (plan.optimized() == false) {
            listener.onFailure(new IllegalStateException("Expected optimized plan before query builder rewrite."));
            return;
        }

        Set<FullTextFunction> unresolved = fullTextFunctions(plan);
        Set<String> indexNames = indexNames(plan);

        if (indexNames == null || indexNames.isEmpty() || unresolved.isEmpty()) {
            callback.accept(plan, listener);
            return;
        }
        QueryRewriteContext ctx = queryRewriteContext(indexNames);
        FullTextFunctionsRewritable rewritable = new FullTextFunctionsRewritable(unresolved);
        Rewriteable.rewriteAndFetch(rewritable, ctx, new ActionListener<FullTextFunctionsRewritable>() {
            @Override
            public void onResponse(FullTextFunctionsRewritable fullTextFunctionsRewritable) {
                try {
                    LogicalPlan newPlan = planWithResolvedQueryBuilders(plan, fullTextFunctionsRewritable.results());
                    callback.accept(newPlan, listener);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    private Set<FullTextFunction> fullTextFunctions(LogicalPlan plan) {
        Set<FullTextFunction> functions = new HashSet<>();
        plan.forEachExpressionDown(FullTextFunction.class, func -> functions.add(func));
        return functions;
    }

    public Set<String> indexNames(LogicalPlan plan) {
        Holder<Set<String>> indexNames = new Holder<>();
        plan.forEachDown(EsRelation.class, esRelation -> indexNames.set(esRelation.concreteIndices()));
        return indexNames.get();
    }

    public LogicalPlan planWithResolvedQueryBuilders(LogicalPlan plan, Map<FullTextFunction, QueryBuilder> newQueryBuilders) {
        LogicalPlan newPlan = plan.transformExpressionsDown(FullTextFunction.class, m -> {
            if (newQueryBuilders.keySet().contains(m)) {
                return m.replaceQueryBuilder(newQueryBuilders.get(m));
            }
            return m;
        });
        // The given plan was already analyzed and optimized, so we set the resulted plan to optimized as well.
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

    private class FullTextFunctionsRewritable implements Rewriteable<FullTextFunctionsRewritable> {

        private final Map<FullTextFunction, QueryBuilder> queryBuilderMap;

        FullTextFunctionsRewritable(Map<FullTextFunction, QueryBuilder> queryBuilderMap) {
            this.queryBuilderMap = queryBuilderMap;
        }

        FullTextFunctionsRewritable(Set<FullTextFunction> functions) {
            this.queryBuilderMap = new HashMap<>();

            for (FullTextFunction func : functions) {
                queryBuilderMap.put(func, TRANSLATOR_HANDLER.asQuery(func).asBuilder());
            }
        }

        @Override
        public FullTextFunctionsRewritable rewrite(QueryRewriteContext ctx) throws IOException {
            Map<FullTextFunction, QueryBuilder> results = new HashMap<>();

            boolean hasChanged = false;
            for (var entry : queryBuilderMap.entrySet()) {
                var initial = entry.getValue();
                var rewritten = initial.rewrite(ctx);
                hasChanged |= rewritten != initial;

                results.put(entry.getKey(), rewritten);
            }

            return hasChanged ? new FullTextFunctionsRewritable(results) : this;
        }

        public Map<FullTextFunction, QueryBuilder> results() {
            return queryBuilderMap;
        }
    }
}
