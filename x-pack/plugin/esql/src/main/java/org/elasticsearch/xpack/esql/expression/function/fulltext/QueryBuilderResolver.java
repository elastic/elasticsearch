/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ResolvedIndices;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.xpack.esql.capabilities.RewriteableAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
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
 * Some {@link RewriteableAware} implementations such as {@link org.elasticsearch.xpack.esql.expression.function.fulltext.Match}
 * will be translated to a {@link QueryBuilder} that require a rewrite phase on the coordinator.
 * {@link QueryBuilderResolver#resolveQueryBuilders(LogicalPlan, TransportActionServices, ActionListener)} will rewrite the plan by
 * replacing {@link RewriteableAware} expression with new ones that hold rewritten {@link QueryBuilder}s.
 */
public final class QueryBuilderResolver {

    private QueryBuilderResolver() {}

    public static void resolveQueryBuilders(LogicalPlan plan, TransportActionServices services, ActionListener<LogicalPlan> listener) {
        var hasRewriteableAwareFunctions = plan.anyMatch(p -> {
            Holder<Boolean> hasRewriteable = new Holder<>(false);
            p.forEachExpression(expr -> {
                if (expr instanceof RewriteableAware) {
                    hasRewriteable.set(true);
                }
            });
            return hasRewriteable.get();
        });
        if (hasRewriteableAwareFunctions) {
            Rewriteable.rewriteAndFetch(
                new FunctionsRewriteable(plan),
                queryRewriteContext(services, indexNames(plan)),
                services.transportService().getThreadPool().executor(ThreadPool.Names.SEARCH_COORDINATION),
                listener.delegateFailureAndWrap((l, r) -> l.onResponse(r.plan))
            );
        } else {
            listener.onResponse(plan);
        }
    }

    private static QueryRewriteContext queryRewriteContext(TransportActionServices services, Set<String> indexNames) {
        ClusterState clusterState = services.clusterService().state();
        ResolvedIndices resolvedIndices = ResolvedIndices.resolveWithIndexNamesAndOptions(
            indexNames.toArray(String[]::new),
            IndexResolver.DEFAULT_OPTIONS,
            services.projectResolver().getProjectMetadata(clusterState),
            services.indexNameExpressionResolver(),
            services.transportService().getRemoteClusterService(),
            System.currentTimeMillis()
        );

        // Set the cluster alias to the local cluster and CCS minimize round-trips to false since ES|QL does not perform a remote cluster
        // coordinator node rewrite
        return services.searchService()
            .getRewriteContext(
                System::currentTimeMillis,
                clusterState.getMinTransportVersion(),
                RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY,
                resolvedIndices,
                null,
                false
            );
    }

    private static Set<String> indexNames(LogicalPlan plan) {
        Set<String> indexNames = new HashSet<>();
        plan.forEachDown(EsRelation.class, esRelation -> indexNames.addAll(esRelation.concreteQualifiedIndices()));
        return indexNames;
    }

    private record FunctionsRewriteable(LogicalPlan plan) implements Rewriteable<FunctionsRewriteable> {
        @Override
        public FunctionsRewriteable rewrite(QueryRewriteContext ctx) throws IOException {
            Holder<IOException> exceptionHolder = new Holder<>();
            Holder<Boolean> updated = new Holder<>(false);
            LogicalPlan newPlan = plan.transformExpressionsDown(Expression.class, expr -> {
                Expression finalExpression = expr;
                if (expr instanceof RewriteableAware rewriteableAware) {
                    QueryBuilder builder = rewriteableAware.queryBuilder(), initial = builder;
                    builder = builder == null
                        ? rewriteableAware.asQuery(LucenePushdownPredicates.DEFAULT, TranslatorHandler.TRANSLATOR_HANDLER).toQueryBuilder()
                        : builder;
                    try {
                        builder = builder.rewrite(ctx);
                    } catch (IOException e) {
                        exceptionHolder.setIfAbsent(e);
                    }
                    var rewritten = builder != initial;
                    updated.set(updated.get() || rewritten);
                    finalExpression = rewritten ? rewriteableAware.replaceQueryBuilder(builder) : finalExpression;
                }
                return finalExpression;
            });
            if (exceptionHolder.get() != null) {
                throw exceptionHolder.get();
            }
            return updated.get() ? new FunctionsRewriteable(newPlan) : this;
        }
    }
}
