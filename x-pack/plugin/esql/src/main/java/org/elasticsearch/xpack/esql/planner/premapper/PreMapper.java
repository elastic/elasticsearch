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
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Versioned;

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
    public void preMapper(Versioned<LogicalPlan> plan, ActionListener<LogicalPlan> listener) {
        queryRewrite(plan.inner(), listener.delegateFailureAndWrap((l, p) -> {
            p.setOptimized();
            l.onResponse(p);
        }));
    }

    private void queryRewrite(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        // see https://github.com/elastic/elasticsearch/issues/133312
        // ThreadedActionListener might be removed if above issue is resolved
        SubscribableListener.<LogicalPlan>newForked(l -> QueryBuilderResolver.resolveQueryBuilders(plan, services, l))
            .addListener(listener, searchExecutor, null);
    }
}
