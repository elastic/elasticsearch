/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pushes down any Filter that appears immediately after a FORK if at least one of the FORK branches does not have a pipeline breaker.
 * This increases the chances that any pipeline breaker such as Limit or TopN that appears after FORK will be pushed down into FORK.
 */
public class PushDownFiltersIntoFork extends OptimizerRules.OptimizerRule<Filter> {
    public PushDownFiltersIntoFork() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Filter filter) {
        if (filter.child() instanceof Fork == false || filter.child() instanceof UnionAll) {
            return filter;
        }
        Fork fork = (Fork) filter.child();
        // if none of the FORK branches benefits from pushing down a pipeline breaker, we can do an early return
        if (fork.children().stream().anyMatch(PushDownUtils::shouldPushDownPipelineBreakerIntoForkBranch) == false) {
            return filter;
        }

        // we are pushing the filter to all FORK branches - this should be safe as the filter is not a pipeline breaker
        List<LogicalPlan> newForkChildren = new ArrayList<>();
        for (LogicalPlan forkChild : fork.children()) {
            LogicalPlan newForkChild = pushDownFilterIntoForkBranch(fork, forkChild, filter);
            newForkChildren.add(newForkChild);
        }

        return fork.replaceChildren(newForkChildren);
    }

    private static LogicalPlan pushDownFilterIntoForkBranch(Fork fork, LogicalPlan forkChild, Filter filter) {
        Map<Expression, Expression> outputMap = PushDownUtils.outputMap(fork, forkChild);

        Expression newFilterExpression = filter.condition().transformDown(exp -> {
            if (outputMap.containsKey(exp)) {
                return outputMap.get(exp);
            }
            return exp;
        });

        return new Filter(filter.source(), forkChild, newFilterExpression);
    }
}
