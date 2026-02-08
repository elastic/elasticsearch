/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.util.Holder;
import org.elasticsearch.xpack.esql.expression.function.fulltext.Score;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.CompoundOutputEval;
import org.elasticsearch.xpack.esql.plan.logical.Enrich;
import org.elasticsearch.xpack.esql.plan.logical.Eval;
import org.elasticsearch.xpack.esql.plan.logical.Fork;
import org.elasticsearch.xpack.esql.plan.logical.Limit;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.MvExpand;
import org.elasticsearch.xpack.esql.plan.logical.Project;
import org.elasticsearch.xpack.esql.plan.logical.RegexExtract;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnionAll;
import org.elasticsearch.xpack.esql.plan.logical.inference.InferencePlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

public final class PushDownAndCombineLimits extends OptimizerRules.ParameterizedOptimizerRule<Limit, LogicalOptimizerContext>
    implements
        OptimizerRules.LocalAware<Limit> {

    private final boolean local;

    public PushDownAndCombineLimits() {
        this(false);
    }

    private PushDownAndCombineLimits(boolean local) {
        super(OptimizerRules.TransformDirection.DOWN);
        this.local = local;
    }

    @Override
    public LogicalPlan rule(Limit limit, LogicalOptimizerContext ctx) {
        if (limit.child() instanceof Limit childLimit) {
            return combineLimits(limit, childLimit, ctx.foldCtx());
        } else if (limit.child() instanceof UnaryPlan unary) {
            if (unary instanceof Eval
                || unary instanceof Project
                || unary instanceof RegexExtract
                || unary instanceof CompoundOutputEval<?>
                || unary instanceof InferencePlan<?>) {
                if (false == local && unary instanceof Eval && evalAliasNeedsData((Eval) unary)) {
                    // do not push down the limit through an eval that needs data (e.g. a score function) during initial planning
                    return limit;
                } else {
                    return unary.replaceChild(limit.replaceChild(unary.child()));
                }
            } else if (unary instanceof MvExpand) {
                // MV_EXPAND can increase the number of rows, so we cannot just push the limit down
                // (we also have to preserve the LIMIT afterwards)
                // To avoid repeating this infinitely, we have to set duplicated = true.
                return duplicateLimitAsFirstGrandchild(limit, false);
            } else if (unary instanceof Enrich enrich) {
                if (enrich.mode() == Enrich.Mode.REMOTE) {
                    return duplicateLimitAsFirstGrandchild(limit, true);
                } else {
                    // We can push past local enrich because it does not increase the number of rows
                    return enrich.replaceChild(limit.replaceChild(enrich.child()));
                }
            }
            // check if there's a 'visible' descendant limit lower than the current one
            // and if so, align the current limit since it adds no value
            // this applies for cases such as | limit 1 | sort field | limit 10
            else {
                Limit descendantLimit = descendantLimit(unary);
                if (descendantLimit != null) {
                    var l1 = (int) limit.limit().fold(ctx.foldCtx());
                    var l2 = (int) descendantLimit.limit().fold(ctx.foldCtx());
                    if (l2 <= l1) {
                        return limit.withLimit(descendantLimit.limit());
                    }
                }
            }
        } else if (limit.child() instanceof Join join && join.config().type() == JoinTypes.LEFT && join instanceof InlineJoin == false) {
            // Left joins increase the number of rows if any join key has multiple matches from the right hand side.
            // Therefore, we cannot simply push down the limit - but we can add another limit before the join.
            // The InlineJoin is currently excluded, as its right-hand side uses as data source a StubRelation that points to the entire
            // left-hand side, so adding a limit in there would lead to the right-hand side work on incomplete data.
            // To avoid repeating this infinitely, we have to set duplicated = true.
            // We use withLocal = false because if we have a remote join it will be forced into the fragment by the mapper anyway,
            // And the verifier checks that there are no non-synthetic limits before the join.
            // TODO: However, this means that the non-remote join will be always forced on the coordinator. We may want to revisit this.
            return duplicateLimitAsFirstGrandchild(limit, false);
        } else if (limit.child() instanceof Fork fork) {
            return maybePushDownLimitToFork(limit, fork, ctx);
        }
        return limit;
    }

    private static LogicalPlan maybePushDownLimitToFork(Limit limit, Fork fork, LogicalOptimizerContext ctx) {
        // TODO: there's no reason why UnionAll should not benefit from this optimization
        if (fork instanceof UnionAll) {
            return limit;
        }

        List<LogicalPlan> newForkChildren = new ArrayList<>();
        boolean changed = false;

        for (LogicalPlan forkChild : fork.children()) {
            LogicalPlan newForkChild = maybePushDownLimitToForkBranch(limit, forkChild, ctx);
            changed = changed || newForkChild != forkChild;
            newForkChildren.add(newForkChild);
        }

        return changed ? limit.replaceChild(fork.replaceChildren(newForkChildren)) : limit;
    }

    private static LogicalPlan maybePushDownLimitToForkBranch(Limit limit, LogicalPlan forkBranch, LogicalOptimizerContext ctx) {
        if (forkBranch instanceof UnaryPlan == false) {
            return forkBranch;
        }

        Limit descendantLimit = descendantLimit((UnaryPlan) forkBranch);
        if (descendantLimit == null) {
            return forkBranch;
        }
        var descendantLimitValue = (int) descendantLimit.limit().fold(ctx.foldCtx());
        var limitValue = (int) limit.limit().fold(ctx.foldCtx());

        // We push down a limit to a Fork branch when the Fork branch contains a limit with a higher value
        return descendantLimitValue > limitValue ? new Limit(forkBranch.source(), limit.limit(), forkBranch) : forkBranch;
    }

    private static Limit combineLimits(Limit upper, Limit lower, FoldContext ctx) {
        // Keep the smallest limit
        var upperLimitValue = (int) upper.limit().fold(ctx);
        var lowerLimitValue = (int) lower.limit().fold(ctx);
        /*
         * We always want to select the smaller of the limits, but with the local flag it gets a bit tricky.
         * If one of the limits is smaller, that's what we will choose. But if the limits are exactly equal,
         * then we can choose the local limit because this may enable some queries that would otherwise be prohibited
         * due to pipeline-breaking nature of non-local limits in combination with remote Enrich.
         * However this may not be true if we have more situations where local limits are generated which do not have
         * guarantees that local limit produced by remote enrich pushing provides.
         * See also: https://github.com/elastic/elasticsearch/pull/139399#pullrequestreview-3573026118
         */
        if (lowerLimitValue < upperLimitValue) {
            return lower;
        } else if (lowerLimitValue == upperLimitValue) {
            // If any of them is local, we want the local limit
            return lower.local() ? lower : lower.withLocal(upper.local());
        } else {
            return new Limit(upper.source(), upper.limit(), lower.child(), upper.duplicated(), upper.local());
        }
    }

    /**
     * Determines if the provided {@link Alias} expression depends on document data by traversing its expression tree.
     * Returns {@code true} if any child expression requires access to document-specific values, such as the {@link Score} function.
     * This is used to prevent pushing down limits past operations that need to evaluate expressions using document data.
     */
    private boolean evalAliasNeedsData(Eval eval) {
        Holder<Boolean> hasScore = new Holder<>(false);
        eval.forEachExpression(expr -> {
            if (expr instanceof Score) {
                hasScore.set(true);
            }
        });
        return hasScore.get();
    }

    /**
     * Checks the existence of another 'visible' Limit, that exists behind an operation that doesn't produce output more data than
     * its input (that is not a relation/source nor aggregation).
     * P.S. Typically an aggregation produces less data than the input.
     */
    private static Limit descendantLimit(UnaryPlan unary) {
        UnaryPlan plan = unary;
        while (plan instanceof Aggregate == false) {
            if (plan instanceof Limit limit) {
                return limit;
            } else if (plan instanceof MvExpand) {
                // the limit that applies to mv_expand shouldn't be changed
                // ie "| limit 1 | mv_expand x | limit 20" where we want that last "limit" to apply on expand results
                return null;
            }
            if (plan.child() instanceof UnaryPlan unaryPlan) {
                plan = unaryPlan;
            } else {
                break;
            }
        }
        return null;
    }

    /**
     * Duplicate the limit past its child if it wasn't duplicated yet. The duplicate is placed on top of its leftmost grandchild.
     * Idempotent. (Sets {@link Limit#duplicated()} to {@code true} on the limit that remains at the top.)
     */
    private static Limit duplicateLimitAsFirstGrandchild(Limit limit, boolean withLocal) {
        if (limit.duplicated()) {
            return limit;
        }

        List<LogicalPlan> grandChildren = limit.child().children();
        LogicalPlan firstGrandChild = grandChildren.getFirst();
        // Use the local limit under the original node, so it won't break the pipeline
        LogicalPlan newFirstGrandChild = (withLocal ? limit.withLocal(withLocal) : limit).replaceChild(firstGrandChild);

        List<LogicalPlan> newGrandChildren = new ArrayList<>();
        newGrandChildren.add(newFirstGrandChild);
        for (int i = 1; i < grandChildren.size(); i++) {
            newGrandChildren.add(grandChildren.get(i));
        }

        LogicalPlan newChild = limit.child().replaceChildren(newGrandChildren);
        return limit.replaceChild(newChild).withDuplicated(true);
    }

    @Override
    public Rule<Limit, LogicalPlan> local() {
        return new PushDownAndCombineLimits(true);
    }
}
