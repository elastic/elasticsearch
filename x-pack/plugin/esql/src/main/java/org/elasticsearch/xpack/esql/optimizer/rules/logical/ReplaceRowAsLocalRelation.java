/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.BlockUtils;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.AttributeMap;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.List;

public final class ReplaceRowAsLocalRelation extends OptimizerRules.ParameterizedOptimizerRule<Row, LogicalOptimizerContext> {
    public ReplaceRowAsLocalRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Row row, LogicalOptimizerContext context) {
        var fields = row.fields();

        // fold all fields (including shadowed ones) keyed by attribute identity (NameId).
        // ReferenceAttributes are resolved against already-folded values instead of calling fold() directly.
        AttributeMap.Builder<Object> builder = AttributeMap.builder(fields.size());
        AttributeMap<Object> folded = builder.build();
        for (var f : fields) {
            Expression child = f.child();
            if (child instanceof ReferenceAttribute ref) {
                builder.put(f.toAttribute(), folded.get(ref));
            } else {
                builder.put(f.toAttribute(), child.fold(context.foldCtx()));
            }
        }

        // collect values aligned with deduplicated output
        var output = row.output();
        List<Object> values = new ArrayList<>(output.size());
        for (Attribute attr : output) {
            values.add(folded.get(attr));
        }

        var blocks = BlockUtils.fromListRow(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values);
        return new LocalRelation(row.source(), output, LocalSupplier.of(blocks.length == 0 ? new Page(0) : new Page(blocks)));
    }
}
