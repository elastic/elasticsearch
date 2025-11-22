/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.Sample;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInAggregate;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInEsRelation;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInEval;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInInlineJoinRight;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInProject;

/**
 * This is used to prune unused columns and expressions in each branch of a Fork.
 * The output for each fork branch has already been pruned in {@code PruneColumns#pruneColumnsInFork}, so here we only need to
 * remove unused columns and expressions in the sub-plans of each branch, similarly to independently running {@code PruneColumns}.
 */
public final class PruneColumnsInForkBranches extends OptimizerRules.OptimizerRule<Fork> {

    @Override
    protected LogicalPlan rule(Fork plan) {
        // only do this for fork
        if (plan instanceof UnionAll) {
            return plan;
        }
        boolean changed = false;
        List<LogicalPlan> newChildren = new ArrayList<>();
        for (var child : plan.children()) {
            var newChild = pruneSubPlan(child, AttributeSet.forkBuilder().addAll(plan.output()));
            newChildren.add(newChild);
            if (false == newChild.equals(child)) {
                changed = true;
            }
        }
        if (changed) {
            return new Fork(plan.source(), newChildren, plan.output());
        } else {
            return plan;
        }
    }

    private static LogicalPlan pruneSubPlan(LogicalPlan plan, AttributeSet.Builder outputNames) {
        return plan instanceof Fork ? plan : plan.transformDown(p -> {
            if (p instanceof Limit || p instanceof Sample) {
                return p;
            }

            var recheck = new Holder<Boolean>();
            do {
                // we operate using the names of the fields, rather than comparing the attributes directly,
                // as attributes may have been recreated during the transformations of fork branches.
                recheck.set(false);
                p = switch (p) {
                    case Aggregate agg -> pruneColumnsInAggregate(agg, outputNames, false);
                    case InlineJoin inj -> pruneColumnsInInlineJoinRight(inj, outputNames, recheck);
                    case Eval eval -> pruneColumnsInEval(eval, outputNames, recheck);
                    case Project project -> pruneColumnsInProject(project, outputNames);
                    case EsRelation esr -> pruneColumnsInEsRelation(esr, outputNames);
                    default -> p;
                };
            } while (recheck.get());
            outputNames.addAll(p.references());
            return p;
        });
    }
}
