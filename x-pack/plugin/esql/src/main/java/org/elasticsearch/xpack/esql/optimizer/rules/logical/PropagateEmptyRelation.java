/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.local.LocalPropagateEmptyRelation;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.join.Join;
import org.elasticsearch.xpack.esql.plan.logical.join.JoinTypes;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;
import org.elasticsearch.xpack.esql.rule.Rule;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("removal")
public class PropagateEmptyRelation extends OptimizerRules.ParameterizedOptimizerRule<LogicalPlan, LogicalOptimizerContext>
    implements
        OptimizerRules.LocalAware<LogicalPlan> {
    public PropagateEmptyRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(LogicalPlan plan, LogicalOptimizerContext ctx) {
        if (plan instanceof UnaryPlan unary && unary.child() instanceof LocalRelation local && local.hasEmptySupplier()) {
            // only care about non-grouped aggs might return something (count)
            if (plan instanceof Aggregate agg && agg.groupings().isEmpty()) {
                List<Block> emptyBlocks = aggsFromEmpty(ctx.foldCtx(), agg.aggregates());
                return new LocalRelation(
                    plan.source(),
                    plan.output(),
                    LocalSupplier.of(emptyBlocks.isEmpty() ? new Page(0) : new Page(emptyBlocks.toArray(Block[]::new)))
                );
            }
            return PruneEmptyPlans.skipPlan(unary);
        }
        if (plan instanceof Join join && join.left() instanceof LocalRelation lr && lr.hasEmptySupplier()) {
            var type = join.config().type();
            if (type == JoinTypes.LEFT || type == JoinTypes.INNER || type == JoinTypes.CROSS) {
                return new LocalRelation(join.source(), join.output(), EmptyLocalSupplier.EMPTY);
            }
        }
        return plan;
    }

    private List<Block> aggsFromEmpty(FoldContext foldCtx, List<? extends NamedExpression> aggs) {
        List<Block> blocks = new ArrayList<>();
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        int i = 0;
        for (var agg : aggs) {
            // there needs to be an alias
            if (Alias.unwrap(agg) instanceof AggregateFunction aggFunc) {
                aggOutput(foldCtx, agg, aggFunc, blockFactory, blocks);
            } else {
                throw new EsqlIllegalArgumentException("Did not expect a non-aliased aggregation {}", agg);
            }
        }
        return blocks;
    }

    /**
     * The folded aggregation output - this variant is for the coordinator/final.
     */
    protected void aggOutput(
        FoldContext foldCtx,
        NamedExpression agg,
        AggregateFunction aggFunc,
        BlockFactory blockFactory,
        List<Block> blocks
    ) {
        // look for count(literal) with literal != null
        Object value;
        if (aggFunc instanceof Count count && (count.foldable() == false || count.fold(foldCtx) != null)) {
            value = switch (aggFunc.dataType()) {
                case LONG -> 0L;     // Count
                case DOUBLE -> 0.0;  // CountApproximate
                default -> throw new EsqlIllegalArgumentException("Unexpected COUNT return type [{}]", aggFunc.dataType());
            };
        } else {
            value = null;
        }
        var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(aggFunc.dataType()), 1);
        wrapper.accept(value);
        blocks.add(wrapper.builder().build());
    }

    @Override
    public Rule<LogicalPlan, LogicalPlan> local() {
        return new LocalPropagateEmptyRelation();
    }
}
