/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.analysis.rules;

import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.expression.function.UnresolvedFunction;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MergePlan;
import org.elasticsearch.xpack.esql.plan.logical.NestedExpand;
import org.elasticsearch.xpack.esql.plan.logical.Subquery;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.List;

/**
 * {@code NESTED_EXPAND} and {@code NESTED_ANY} read the nested documents of the rows of an index. The rows a {@code FORK} or a
 * subquery merge come from several branches, each reading its own index, so the merged rows have no nested documents to read.
 * Both act on one row at a time, so applying them to each branch gives the rows they give the merged rows. This moves such a
 * command, when it directly follows the merge, into every branch.
 */
public final class DistributeNestedOverBranches extends Rule<LogicalPlan, LogicalPlan> {
    @Override
    public LogicalPlan apply(LogicalPlan plan) {
        return plan.transformUp(UnaryPlan.class, unary -> {
            if (unary.child() instanceof MergePlan merge && readsNested(unary)) {
                List<LogicalPlan> branches = merge.children().stream().map(branch -> into(unary, branch)).toList();
                return merge.replaceSubPlans(branches);
            }
            return unary;
        });
    }

    private static boolean readsNested(UnaryPlan plan) {
        if (plan instanceof NestedExpand expand) {
            return expand.bound() == false;
        }
        return (plan instanceof Filter || plan instanceof Eval)
            && plan.expressions().stream().anyMatch(DistributeNestedOverBranches::callsNestedAny);
    }

    private static boolean callsNestedAny(Expression expression) {
        return expression.anyMatch(e -> e instanceof UnresolvedFunction function && function.name().equalsIgnoreCase("nested_any"));
    }

    private static LogicalPlan into(UnaryPlan command, LogicalPlan branch) {
        if (branch instanceof Subquery subquery) {
            return subquery.replaceChild(command.replaceChild(subquery.child()));
        }
        return command.replaceChild(branch);
    }
}
