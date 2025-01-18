/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.mapper.preprocessor;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.xpack.esql.expression.function.fulltext.FullTextFunction;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Some {@link FullTextFunction} implementations such as {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match}
 * will be translated to a {@link QueryBuilder} that require a rewrite phase on the coordinator.
 * {@link FullTextFunctionMapperPreprocessor#preprocess(LogicalPlan, TransportActionServices, ActionListener)} will rewrite the plan by
 * replacing {@link FullTextFunction} expression with new ones that hold rewritten {@link QueryBuilder}s.
 */
public class FullTextFunctionMapperPreprocessor implements MappingPreProcessor {

    @Override
    public void preprocess(LogicalPlan plan, TransportActionServices services, ActionListener<LogicalPlan> listener) {
        Set<FullTextFunction> unresolved = fullTextFunctions(plan);
        Set<String> indexNames = indexNames(plan);

        if (indexNames == null || indexNames.isEmpty() || unresolved.isEmpty()) {
            listener.onResponse(plan);
            return;
        }
        QueryRewriteContext ctx = queryRewriteContext(services, indexNames);
        FullTextFunctionsRewritable rewritable = new FullTextFunctionsRewritable(unresolved);
        Rewriteable.rewriteAndFetch(rewritable, ctx, new ActionListener<>() {
            @Override
            public void onResponse(FullTextFunctionsRewritable fullTextFunctionsRewritable) {
                try {
                    LogicalPlan newPlan = planWithResolvedQueryBuilders(plan, fullTextFunctionsRewritable.results());
                    listener.onResponse(newPlan);
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

    public LogicalPlan planWithResolvedQueryBuilders(LogicalPlan plan, Map<FullTextFunction, QueryBuilder> newQueryBuilders) {
        LogicalPlan newPlan = plan.transformExpressionsDown(FullTextFunction.class, m -> {
            if (newQueryBuilders.containsKey(m)) {
                return m.replaceQueryBuilder(newQueryBuilders.get(m));
            }
            return m;
        });
        // The given plan was already analyzed and optimized, so we set the resulted plan to optimized as well.
        newPlan.setOptimized();
        return newPlan;
    }

    private static QueryRewriteContext queryRewriteContext(TransportActionServices services, Set<String> indexNames) {
        ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndexNamesAndOptions(
            indexNames.toArray(String[]::new),
            IndexResolver.FIELD_CAPS_INDICES_OPTIONS,
            services.clusterService().state(),
            services.indexNameExpressionResolver(),
            services.transportService().getRemoteClusterService(),
            System.currentTimeMillis()
        );

        return services.searchService().getRewriteContext(System::currentTimeMillis, resolvedIndices, null);
    }

    private static Set<FullTextFunction> fullTextFunctions(LogicalPlan plan) {
        Set<FullTextFunction> functions = new HashSet<>();
        plan.forEachExpressionDown(FullTextFunction.class, functions::add);
        return functions;
    }

    public Set<String> indexNames(LogicalPlan plan) {
        Set<String> indexNames = new HashSet<>();
        plan.forEachDown(EsRelation.class, esRelation -> indexNames.addAll(esRelation.index().concreteIndices()));
        return indexNames;
    }

    private static class FullTextFunctionsRewritable
        implements
            Rewriteable<FullTextFunctionMapperPreprocessor.FullTextFunctionsRewritable> {

        private final Map<FullTextFunction, QueryBuilder> queryBuilderMap;

        FullTextFunctionsRewritable(Map<FullTextFunction, QueryBuilder> queryBuilderMap) {
            this.queryBuilderMap = queryBuilderMap;
        }

        FullTextFunctionsRewritable(Set<FullTextFunction> functions) {
            this.queryBuilderMap = new HashMap<>();

            for (FullTextFunction func : functions) {
                queryBuilderMap.put(func, func.asQuery(PlannerUtils.TRANSLATOR_HANDLER).asBuilder());
            }
        }

        @Override
        public FullTextFunctionMapperPreprocessor.FullTextFunctionsRewritable rewrite(QueryRewriteContext ctx) throws IOException {
            Map<FullTextFunction, QueryBuilder> results = new HashMap<>();

            boolean hasChanged = false;
            for (var entry : queryBuilderMap.entrySet()) {
                var initial = entry.getValue();
                var rewritten = initial.rewrite(ctx);
                hasChanged |= rewritten != initial;

                results.put(entry.getKey(), rewritten);
            }

            return hasChanged ? new FullTextFunctionMapperPreprocessor.FullTextFunctionsRewritable(results) : this;
        }

        public Map<FullTextFunction, QueryBuilder> results() {
            return queryBuilderMap;
        }
    }
}
