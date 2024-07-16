/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.EsqlIllegalArgumentException;
import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.LogicalPlanOptimizer;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.UnaryPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

@SuppressWarnings("removal")
public class PropagateEmptyRelation extends OptimizerRules.OptimizerRule<UnaryPlan> {

    @Override
    protected LogicalPlan rule(UnaryPlan plan) {
        LogicalPlan p = plan;
        if (plan.child() instanceof LocalRelation local && local.supplier() == LocalSupplier.EMPTY) {
            // only care about non-grouped aggs might return something (count)
            if (plan instanceof Aggregate agg && agg.groupings().isEmpty()) {
                List<Block> emptyBlocks = aggsFromEmpty(agg.aggregates());
                p = LogicalPlanOptimizer.skipPlan(plan, LocalSupplier.of(emptyBlocks.toArray(Block[]::new)));
            } else {
                p = LogicalPlanOptimizer.skipPlan(plan);
            }
        }
        return p;
    }

    private List<Block> aggsFromEmpty(List<? extends NamedExpression> aggs) {
        List<Block> blocks = new ArrayList<>();
        var blockFactory = PlannerUtils.NON_BREAKING_BLOCK_FACTORY;
        int i = 0;
        for (var agg : aggs) {
            // there needs to be an alias
            if (Alias.unwrap(agg) instanceof AggregateFunction aggFunc) {
                aggOutput(agg, aggFunc, blockFactory, blocks);
            } else {
                throw new EsqlIllegalArgumentException("Did not expect a non-aliased aggregation {}", agg);
            }
        }
        return blocks;
    }

    /**
     * The folded aggregation output - this variant is for the coordinator/final.
     */
    protected void aggOutput(NamedExpression agg, AggregateFunction aggFunc, BlockFactory blockFactory, List<Block> blocks) {
        // look for count(literal) with literal != null
        Object value = aggFunc instanceof Count count && (count.foldable() == false || count.fold() != null) ? 0L : null;
        var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(aggFunc.dataType()), 1);
        wrapper.accept(value);
        blocks.add(wrapper.builder().build());
    }
}
