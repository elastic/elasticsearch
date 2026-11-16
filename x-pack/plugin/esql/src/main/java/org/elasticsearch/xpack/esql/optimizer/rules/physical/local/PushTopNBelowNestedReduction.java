/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.physical.local;

import org.elasticsearch.xpack.esql.core.expression.Alias;
import org.elasticsearch.xpack.esql.core.expression.AttributeSet;
import org.elasticsearch.xpack.esql.optimizer.PhysicalOptimizerRules;
import org.elasticsearch.xpack.esql.plan.physical.NestedExpandExec;
import org.elasticsearch.xpack.esql.plan.physical.NestedReduceExec;
import org.elasticsearch.xpack.esql.plan.physical.PhysicalPlan;
import org.elasticsearch.xpack.esql.plan.physical.TopNExec;

/**
 * Sorts before reducing on the data node, so NESTED reduces only the rows the TOP N keeps, and {@link PushTopNToSource} can
 * hand the sort to Lucene. This holds when no sort key is a reduction result: a reduction keeps every row and its other columns.
 * NESTED_FLATTEN keeps them too, so the same holds when no sort key is a flattened column.
 */
public class PushTopNBelowNestedReduction extends PhysicalOptimizerRules.OptimizerRule<TopNExec> {
    @Override
    protected PhysicalPlan rule(TopNExec topN) {
        if (topN.child() instanceof NestedReduceExec reduce) {
            AttributeSet reduced = AttributeSet.of(reduce.fields().stream().map(Alias::toAttribute).toList());
            if (topN.order().stream().noneMatch(order -> order.references().stream().anyMatch(reduced::contains))) {
                return reduce.replaceChild(topN.replaceChild(reduce.child()));
            }
        }
        if (topN.child() instanceof NestedExpandExec flatten && flatten.flatten()) {
            AttributeSet flattened = AttributeSet.of(flatten.expanded());
            if (topN.order().stream().noneMatch(order -> order.references().stream().anyMatch(flattened::contains))) {
                return flatten.replaceChild(topN.replaceChild(flatten.child()));
            }
        }
        return topN;
    }
}
