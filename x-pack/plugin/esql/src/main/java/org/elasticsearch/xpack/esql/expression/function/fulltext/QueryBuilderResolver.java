/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.planner.TranslatorHandler;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.IndexResolver;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Some {@link FullTextFunction} implementations such as {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match}
 * will be translated to a {@link QueryBuilder} that require a rewrite phase on the coordinator.
 * {@link QueryBuilderResolver#resolveQueryBuilders(LogicalPlan, TransportActionServices, ActionListener)} will rewrite the plan by
 * replacing {@link FullTextFunction} expression with new ones that hold rewritten {@link QueryBuilder}s.
 */
public final class QueryBuilderResolver {

    private QueryBuilderResolver() {}

    public static void resolveQueryBuilders(LogicalPlan plan, TransportActionServices services, ActionListener<LogicalPlan> listener) {
        var hasFullTextFunctions = plan.anyMatch(p -> {
            Holder<Boolean> hasFullTextFunction = new Holder<>(false);
            p.forEachExpression(FullTextFunction.class, unused -> hasFullTextFunction.set(true));
            return hasFullTextFunction.get();
        });
        if (hasFullTextFunctions) {
            Rewriteable.rewriteAndFetch(
                new FullTextFunctionsRewritable(plan),
                queryRewriteContext(services, indexNames(plan)),
                listener.delegateFailureAndWrap((l, r) -> l.onResponse(r.plan))
            );
        } else {
            listener.onResponse(plan);
        }
    }

    private static QueryRewriteContext queryRewriteContext(TransportActionServices services, Set<String> indexNames) {
        ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndexNamesAndOptions(
            indexNames.toArray(String[]::new),
            IndexResolver.FIELD_CAPS_INDICES_OPTIONS,
            services.clusterService().state().metadata().getProject(),
            services.indexNameExpressionResolver(),
            services.transportService().getRemoteClusterService(),
            System.currentTimeMillis()
        );

        return services.searchService().getRewriteContext(System::currentTimeMillis, resolvedIndices, null);
    }

    private static Set<String> indexNames(LogicalPlan plan) {
        Set<String> indexNames = new HashSet<>();
        plan.forEachDown(EsRelation.class, esRelation -> indexNames.addAll(esRelation.concreteIndices()));
        return indexNames;
    }

    private record FullTextFunctionsRewritable(LogicalPlan plan) implements Rewriteable<QueryBuilderResolver.FullTextFunctionsRewritable> {
        @Override
        public FullTextFunctionsRewritable rewrite(QueryRewriteContext ctx) throws IOException {
            Holder<IOException> exceptionHolder = new Holder<>();
            Holder<Boolean> updated = new Holder<>(false);
            LogicalPlan newPlan = plan.transformExpressionsDown(FullTextFunction.class, f -> {
                QueryBuilder builder = f.queryBuilder(), initial = builder;
                builder = builder == null
                    ? f.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER).toQueryBuilder()
                    : builder;
                try {
                    builder = builder.rewrite(ctx);
                } catch (IOException e) {
                    exceptionHolder.setIfAbsent(e);
                }
                var rewritten = builder != initial;
                updated.set(updated.get() || rewritten);
                return rewritten ? f.replaceQueryBuilder(builder) : f;
            });
            if (exceptionHolder.get() != null) {
                throw exceptionHolder.get();
            }
            return updated.get() ? new FullTextFunctionsRewritable(newPlan) : this;
        }
    }
}
