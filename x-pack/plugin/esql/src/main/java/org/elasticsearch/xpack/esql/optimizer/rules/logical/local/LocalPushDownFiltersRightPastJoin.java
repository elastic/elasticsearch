/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.xpack.esql.capabilities.TranslationAware;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.util.CollectionUtils;
import org.elasticsearch.xpack.esql.expression.predicate.Predicates;
import org.elasticsearch.xpack.esql.optimizer.LocalLogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.OptimizerRules;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineFilters;
import org.elasticsearch.xpack.esql.optimizer.rules.physical.local.LucenePushdownPredicates;
import org.elasticsearch.xpack.esql.plan.logical.Filter;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.InlineJoin;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plugin.EsqlFlags;

import java.util.List;

import static org.elasticsearch.xpack.esql.optimizer.rules.logical.PushDownAndCombineFilters.scopeFilter;

public final class LocalPushDownFiltersRightPastJoin extends OptimizerRules.ParameterizedOptimizerRule<
    Filter,
    LocalLogicalOptimizerContext> {

    public LocalPushDownFiltersRightPastJoin() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Filter filter, LocalLogicalOptimizerContext context) {
        LogicalPlan child = filter.child();
        // TODO: see note in PushDownAndCombineFilters about InlineJoin
        return child instanceof Join join && child instanceof InlineJoin == false ? pushDownRightPastJoin(filter, join, context) : filter;
    }

    private static LogicalPlan pushDownRightPastJoin(Filter filter, Join join, LocalLogicalOptimizerContext context) {
        LogicalPlan plan = filter;
        // pushdown only through LEFT joins
        // TODO: generalize this for other join types
        if (join.config().type() == JoinTypes.LEFT) {
            LogicalPlan left = join.left();
            LogicalPlan right = join.right();

            PushDownAndCombineFilters.ScopedFilter scoped = scopeFilter(Predicates.splitAnd(filter.condition()), left, right);
            // Only push down right if the filter can be merged; it would otherwise generate extra work, as the filter's kept before the
            // join too. This is needed since the join produces `null`s for the fields corresponding to the rows that the pushed condition
            // filters out.
            if (areRightFiltersPushable(scoped.rightFilters(), context)) {
                var combinedRightFilters = Predicates.combineAnd(scoped.rightFilters());
                // avoid re-injecting the same filter if the rule applied already before.
                if (right instanceof Filter == false || ((Filter) right).condition().semanticEquals(combinedRightFilters) == false) {
                    // push the right scoped filter down to the right child
                    right = new Filter(right.source(), right, combinedRightFilters);
                    // update the join with the new right child
                    join = (Join) join.replaceRight(right);
                }
            }
            // keep the remaining filters in place, otherwise return the new join;
            Expression remainingFilter = Predicates.combineAnd(CollectionUtils.combine(scoped.commonFilters(), scoped.rightFilters()));
            plan = remainingFilter != null ? filter.with(join, remainingFilter) : join;
        }
        // ignore the rest of the join
        return plan;
    }

    private static boolean areRightFiltersPushable(List<Expression> filters, LocalLogicalOptimizerContext ctx) {
        if (filters.isEmpty()) {
            return false;
        }
        // TODO: the flag isn't relevant for the pushdown decision?
        LucenePushdownPredicates pushdownPredicates = LucenePushdownPredicates.from(ctx.searchStats(), new EsqlFlags(true));
        for (Expression filter : filters) {
            // the rigth filters will remain on top of the join, so any not-NO value is acceptable for "is it pushable?"
            if (TranslationAware.translatable(filter, pushdownPredicates) == TranslationAware.Translatable.NO) {
                return false;
            }
        }
        return true;
    }
}
