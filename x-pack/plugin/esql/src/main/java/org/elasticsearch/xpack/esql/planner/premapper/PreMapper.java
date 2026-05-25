/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.planner.premapper;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.xpack.esql.expression.function.fulltext.QueryBuilderResolver;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plugin.TransportActionServices;
import org.elasticsearch.xpack.esql.session.Versioned;

/**
 * The class is responsible for invoking any premapping steps that need to be applied to the logical plan,
 * before this is being mapped to a physical one.
 */
public class PreMapper {

    private final TransportActionServices services;

    public PreMapper(TransportActionServices services) {
        this.services = services;
    }

    /**
     * Invokes any premapping steps that need to be applied to the logical plan, before this is being mapped to a physical one.
     * <p>
     * The plan is passed as a {@link Versioned} so that the minimum transport version of the cluster is available here, even
     * though the current query rewrites do not need it. Should the pre-mapper's query rewrites ever be refactored in a way
     * that is not backwards compatible, the minimum version can be used to gate the new behavior. See {@link Versioned} for
     * the broader rationale.
     */
    public void preMapper(Versioned<LogicalPlan> plan, ActionListener<LogicalPlan> listener) {
        queryRewrite(plan.inner(), listener.delegateFailureAndWrap((l, p) -> {
            p.setOptimized();
            l.onResponse(p);
        }));
    }

    private void queryRewrite(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        QueryBuilderResolver.resolveQueryBuilders(plan, services, listener);
    }
}
