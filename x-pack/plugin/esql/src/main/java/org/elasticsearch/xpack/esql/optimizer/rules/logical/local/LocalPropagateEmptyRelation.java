/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical.local;

import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.FoldContext;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.aggregate.AggregateFunction;
import org.elasticsearch.xpack.esql.expression.function.aggregate.Count;
import org.elasticsearch.xpack.esql.optimizer.rules.logical.PropagateEmptyRelation;
import org.elasticsearch.xpack.esql.planner.AbstractPhysicalOperationProviders;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.List;

/**
 * Local aggregation can only produce intermediate state that get wired into the global agg.
 */
public class LocalPropagateEmptyRelation extends PropagateEmptyRelation {
    /**
     * Local variant of the aggregation that returns the intermediate value.
     */
    @Override
    protected void aggOutput(
        FoldContext foldCtx,
        NamedExpression agg,
        AggregateFunction aggFunc,
        BlockFactory blockFactory,
        List<Block> blocks
    ) {
        List<Attribute> output = AbstractPhysicalOperationProviders.intermediateAttributes(List.of(agg), List.of());
        for (Attribute o : output) {
            DataType dataType = o.dataType();
            // boolean right now is used for the internal #seen so always return true
            var value = dataType == DataType.BOOLEAN ? true
                // look for count(literal) with literal != null
                : aggFunc instanceof Count count && (count.foldable() == false || count.fold(foldCtx) != null) ? 0L
                // otherwise nullify
                : null;
            var wrapper = BlockUtils.wrapperFor(blockFactory, PlannerUtils.toElementType(dataType), 1);
            wrapper.accept(value);
            blocks.add(wrapper.builder().build());
        }
    }
}
