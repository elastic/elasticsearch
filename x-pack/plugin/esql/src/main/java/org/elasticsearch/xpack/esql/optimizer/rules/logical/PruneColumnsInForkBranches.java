/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
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
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInAggregate;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInEsRelation;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInEval;
import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PruneColumns.pruneColumnsInInlineJoinRight;

/**
 * This is used to prune unused columns and expressions in each branch of a Fork.
 * The output for each fork branch has already been pruned in {@code PruneColumns#pruneColumnsInFork}, so here we only need to
 * remove unused columns and expressions in the sub-plans of each branch, similarly to independently running {@code PruneColumns}.
 */
public final class PruneColumnsInForkBranches extends Rule<LogicalPlan, LogicalPlan> {

    @Override
    public LogicalPlan apply(LogicalPlan plan) {

        // collect used attributes from the plan above fork
        var used = AttributeSet.forkBuilder();
        var forkFound = new Holder<>(false);

        // traverse down to the fork node
        return plan.transformDown(p -> {
            // if fork is not found yet, keep collecting used attributes from everything above.
            // Once fork is found, just return the rest of the plan as is, as any pruning/transformation will have
            // taken place in pruneSubPlan for each of the fork branches.
            if (false == (p instanceof Fork) || forkFound.get()) {
                if (false == forkFound.get()) {
                    used.addAll(p.references());
                }
                return p;
            }

            // only do this for fork
            if (p instanceof UnionAll) {
                return p;
            }

            used.addAll(p.output());
            forkFound.set(true);
            var forkOutputNames = p.output().stream().map(NamedExpression::name).collect(Collectors.toSet());
            boolean changed = false;
            List<LogicalPlan> newChildren = new ArrayList<>();
            for (var child : p.children()) {
                var clonedUsed = AttributeSet.forkBuilder().addAll(used);
                var newChild = pruneSubPlan(child, clonedUsed, forkOutputNames);
                newChildren.add(newChild);
                if (false == newChild.equals(child)) {
                    changed = true;
                }
            }
            if (changed) {
                return new Fork(p.source(), newChildren, p.output());
            } else {
                return p;
            }
        });
    }

    private static LogicalPlan pruneSubPlan(LogicalPlan plan, AttributeSet.Builder usedAttrs, Set<String> forkOutput) {
        if (plan instanceof LocalRelation localRelation) {
            var outputAttrs = localRelation.output().stream().filter(usedAttrs::contains).collect(Collectors.toList());
            return new LocalRelation(localRelation.source(), outputAttrs, localRelation.supplier());
        }

        var projectHolder = new Holder<>(false);
        return plan.transformDown(p -> {
            if (p instanceof Limit || p instanceof Sample) {
                return p;
            }

            var recheck = new Holder<Boolean>();
            do {
                // we operate using the names of the fields, rather than comparing the attributes directly,
                // as attributes may have been recreated during the transformations of fork branches.
                recheck.set(false);
                p = switch (p) {
                    case Aggregate agg -> pruneColumnsInAggregate(agg, usedAttrs, false);
                    case InlineJoin inj -> pruneColumnsInInlineJoinRight(inj, usedAttrs, recheck);
                    case Eval eval -> pruneColumnsInEval(eval, usedAttrs, recheck);
                    case Project project -> {
                        // process only the direct Project after Fork, but skip any subsequent instances
                        if (projectHolder.get()) {
                            yield p;
                        } else {
                            projectHolder.set(true);
                            var prunedAttrs = project.projections().stream().filter(x -> forkOutput.contains(x.name())).toList();
                            yield new Project(project.source(), project.child(), prunedAttrs);
                        }
                    }
                    case EsRelation esr -> pruneColumnsInEsRelation(esr, usedAttrs);
                    default -> p;
                };
            } while (recheck.get());
            usedAttrs.addAll(p.references());
            return p;
        });
    }
}
