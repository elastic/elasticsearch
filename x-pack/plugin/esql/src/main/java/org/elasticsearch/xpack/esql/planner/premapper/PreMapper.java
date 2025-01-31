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
     */
    public void preMapper(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        queryRewrite(plan, listener.delegateFailureAndWrap((l, p) -> {
            p.setOptimized();
            l.onResponse(p);
        }));
    }

    private void queryRewrite(LogicalPlan plan, ActionListener<LogicalPlan> listener) {
        QueryBuilderResolver.resolveQueryBuilders(plan, services, listener);
    }
}
