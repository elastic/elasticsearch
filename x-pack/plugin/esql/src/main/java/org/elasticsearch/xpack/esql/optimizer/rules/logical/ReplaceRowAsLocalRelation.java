/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.BlockUtils;
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
        List<Object> values = new ArrayList<>(fields.size());
        fields.forEach(f -> values.add(f.child().fold(context.foldCtx())));
        var blocks = BlockUtils.fromListRow(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values);
        return new LocalRelation(row.source(), row.output(), LocalSupplier.of(blocks));
    }
}
