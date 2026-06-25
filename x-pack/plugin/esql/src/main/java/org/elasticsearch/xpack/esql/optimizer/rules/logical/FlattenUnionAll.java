/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;

import java.util.ArrayList;
import java.util.List;

/**
 * Flattens a {@link UnionAll} that (directly or through an alignment wrapper) contains another {@link UnionAll}, by
 * lifting the inner branches into the outer union. Such nesting arises after {@link InlineView} folds a multi-source
 * view (whose body is itself a {@code UnionAll} of its sources) into a parent {@code FROM} that already unions other
 * sources. A {@code UNION ALL} of a {@code UNION ALL} is semantically a single flat {@code UNION ALL}, so lifting
 * preserves results while satisfying the post-optimization "no nested UnionAll" invariant
 * ({@code UnionAll#checkNestedUnionAlls}).
 * <p>
 * The nested inner {@code UnionAll} is not always a direct child of the outer one. Every {@code UnionAll} branch is
 * wrapped in an alignment {@code Project} (and an optional {@code Eval}) that maps the branch output to the union output
 * (see {@code PushDownFilterAndLimitIntoUnionAll}). When the inner {@code UnionAll} is the body of such a branch, the
 * outer branch looks like {@code Project [-> Eval] -> innerUnionAll}. A {@code Project}/{@code Eval} distributes over
 * union branches, so {@code UnionAll[..., wrapper(innerUnionAll[b1, b2, ...])]} is rewritten to
 * {@code UnionAll[..., wrapper(b1), wrapper(b2), ...]}: the alignment wrapper is re-pointed onto each inner branch.
 * Because union branches share output names but differ in ids, the wrapper's attribute references are remapped from the
 * inner {@code UnionAll}'s output to each inner branch's output by name. The resulting doubled
 * {@code Project(...Project...)} stacks are collapsed by {@code CombineProjections} (same operator batch, runs to
 * fixpoint).
 * <p>
 * Only {@code Project}/{@code Eval} alignment wrappers are traversed. A user-written nested subquery keeps an explicit
 * {@link org.elasticsearch.xpack.esql.plan.logical.Subquery} boundary above its inner {@code UnionAll}
 * ({@code Project -> Subquery -> innerUnionAll}); the walk stops at that boundary and leaves the nesting in place, so the
 * post-optimization verifier still rejects it ("Nested subqueries are not supported"). The only nesting this rule
 * dissolves is view-folding nesting, where no {@code Subquery} boundary intervenes.
 * <p>
 * Runs in the operator-optimization batch right after {@link InlineView}.
 */
public final class FlattenUnionAll extends OptimizerRules.OptimizerRule<UnionAll> {

    @Override
    protected LogicalPlan rule(UnionAll unionAll) {
        boolean changed = false;
        List<LogicalPlan> flattened = new ArrayList<>(unionAll.children().size());
        for (LogicalPlan child : unionAll.children()) {
            if (child instanceof UnionAll inner) {
                // Direct nesting (no alignment wrapper, no Subquery boundary): lift the inner branches verbatim.
                flattened.addAll(inner.children());
                changed = true;
            } else {
                // Wrapped nesting: a Project (and optional Eval) aligning an inner UnionAll's output to the outer one.
                List<LogicalPlan> lifted = liftWrappedInnerUnion(child);
                if (lifted == null) {
                    flattened.add(child);
                } else {
                    flattened.addAll(lifted);
                    changed = true;
                }
            }
        }
        if (changed == false) {
            return unionAll;
        }
        return unionAll.replaceSubPlansAndOutput(flattened, unionAll.output());
    }

    /**
     * If {@code branch} is an alignment wrapper ({@link Project}, optionally over an {@link Eval}) bottoming out directly
     * in a nested {@link UnionAll}, distribute that wrapper over each inner branch and return the lifted branches. Returns
     * {@code null} when the wrapper does not bottom out in an inner {@code UnionAll}, or when any node other than a
     * {@code Project}/{@code Eval} intervenes — in particular a
     * {@link org.elasticsearch.xpack.esql.plan.logical.Subquery}, which marks user-written nesting that must be left for
     * the post-optimization verifier to reject.
     */
    private static List<LogicalPlan> liftWrappedInnerUnion(LogicalPlan branch) {
        // Walk down the alignment wrapper, collecting the chain from the outer-most node to the inner UnionAll.
        List<UnaryPlan> wrapperChain = new ArrayList<>();
        LogicalPlan current = branch;
        while (current instanceof Project || current instanceof Eval) {
            UnaryPlan unary = (UnaryPlan) current;
            if (unary.child() instanceof UnionAll innerUnion) {
                List<LogicalPlan> innerBranches = innerUnion.children();
                List<LogicalPlan> lifted = new ArrayList<>(innerBranches.size());
                for (LogicalPlan innerBranch : innerBranches) {
                    lifted.add(repointWrapper(wrapperChain, unary, innerUnion.output(), innerBranch));
                }
                return lifted;
            }
            wrapperChain.add(unary);
            current = unary.child();
        }
        return null;
    }

    /**
     * Rebuild the alignment wrapper on top of a single inner branch. {@code innermostWrapper} is the {@link UnaryPlan}
     * that sat directly on the inner {@link UnionAll}; its references point at the inner union's output and are remapped
     * by name to {@code innerBranch}'s output, since the branches of a union share output names but differ in ids.
     */
    private static LogicalPlan repointWrapper(
        List<UnaryPlan> wrapperChain,
        UnaryPlan innermostWrapper,
        List<Attribute> innerUnionOutput,
        LogicalPlan innerBranch
    ) {
        // Rebuild bottom-up: the innermost wrapper sits on the inner branch, then each enclosing wrapper on top.
        LogicalPlan rebuilt = remapByName(innermostWrapper.replaceChild(innerBranch), innerUnionOutput, innerBranch.output());
        for (int i = wrapperChain.size() - 1; i >= 0; i--) {
            // Re-attach the outer wrappers as-is; their references are to attributes produced below them, which are
            // preserved by the rebuild (CombineProjections later collapses the doubled projects).
            rebuilt = wrapperChain.get(i).replaceChild(rebuilt);
        }
        return rebuilt;
    }

    /**
     * Replace, by name, every reference to an inner-{@code UnionAll}-output attribute with the matching inner-branch
     * output attribute. Reuses the by-name remap shape from
     * {@code PushDownFilterAndLimitIntoUnionAll#resolveUnionAllOutputByName}: union branches carry the same names but
     * different ids, so attributes must be matched by name, not equality.
     */
    private static LogicalPlan remapByName(LogicalPlan plan, List<Attribute> fromOutput, List<Attribute> toOutput) {
        return plan.transformExpressionsOnly(Attribute.class, attr -> {
            for (Attribute from : fromOutput) {
                if (from.name().equals(attr.name()) && from.id().equals(attr.id())) {
                    for (NamedExpression to : toOutput) {
                        if (to.name().equals(attr.name())) {
                            return to.toAttribute();
                        }
                    }
                }
            }
            return attr;
        });
    }
}
