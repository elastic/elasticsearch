/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryBuilderResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.load.LoadResult;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import java.util.List;
import java.util.ArrayList;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.action.EsqlAsyncGetResultAction;
import org.elasticsearch.xpack.esql.action.EsqlQueryResponse;
import org.elasticsearch.xpack.core.async.GetAsyncResultRequest;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.util.concurrent.Executor;

/**
 * The class is responsible for invoking any premapping steps that need to be applied to the logical plan,
 * before this is being mapped to a physical one.
 */
public class PreMapper {

    private final TransportActionServices services;
    private final Executor searchExecutor;

    public PreMapper(TransportActionServices services) {
        this.services = services;
        this.searchExecutor = services.transportService().getThreadPool().executor(ThreadPool.Names.SEARCH);
    }

    /**
     * Invokes any premapping steps that need to be applied to the logical plan, before this is being mapped to a physical one.
     */
    public void preMapper(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        queryRewrite(plan, listener.delegateFailureAndWrap((l, p) -> {
            if (p.anyMatch(lp -> lp instanceof LoadResult) == false) {
                p.setOptimized();
                l.onResponse(p);
                return;
            }

            // Fetch result and replace all LoadResult nodes with a LocalRelation
            // There should be only one as it's a source command, but handle generically
            // Find the first LoadResult to get the id
            final LoadResult[] holder = new LoadResult[1];
            p.forEachUp(lp -> { if (holder[0] == null && lp instanceof LoadResult lr) { holder[0] = lr; } });
            final LoadResult load = holder[0];
            if (load == null) {
                p.setOptimized();
                l.onResponse(p);
                return;
            }
            GetAsyncResultRequest get = new GetAsyncResultRequest(load.asyncId());
            services.client().execute(EsqlAsyncGetResultAction.INSTANCE, get, l.delegateFailureAndWrap((l2, resp) -> {
                EsqlQueryResponse r = resp;
                LogicalPlan replaced;
                if (r.isRunning()) {
                    replaced = p.transformUp(LoadResult.class, lr -> new LocalRelation(lr.source(), List.of(), LocalSupplier.of(new Block[0])));
                } else {
                    List<Attribute> attrs = r.columns().stream()
                        .map(c -> (Attribute) new ReferenceAttribute(Source.EMPTY, null, c.name(), c.type()))
                        .toList();
                    List<List<Object>> rows = new ArrayList<>();
                    for (Iterable<Object> row : r.rows()) {
                        List<Object> list = new ArrayList<>();
                        for (Object v : row) { list.add(v); }
                        rows.add(list);
                    }
                    Block[] blocks = BlockUtils.fromList(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, rows);
                    replaced = p.transformUp(LoadResult.class, lr -> new LocalRelation(lr.source(), attrs, LocalSupplier.of(blocks)));
                }
                replaced.setOptimized();
                // Ensure we respond on the SEARCH thread pool to satisfy EsqlSession assertions
                searchExecutor.execute(() -> l2.onResponse(replaced));
            }));
        }));
    }

    private void queryRewrite(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        // see https://github.com/elastic/elasticsearch/issues/133312
        // ThreadedActionListener might be removed if above issue is resolved
        SubscribableListener.<LogicalPlan>newForked(l -> QueryBuilderResolver.resolveQueryBuilders(plan, services, l))
            .addListener(listener, searchExecutor, null);
    }
}
