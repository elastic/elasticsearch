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
import org.elasticsearch.xpack.esql.core.expression.NameId;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.Row;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;
import org.elasticsearch.xpack.esql.planner.PlannerUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class ReplaceRowAsLocalRelation extends OptimizerRules.ParameterizedOptimizerRule<Row, LogicalOptimizerContext> {
    public ReplaceRowAsLocalRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(Row row, LogicalOptimizerContext context) {
        var fields = row.fields();

        // Fold all fields (including shadowed ones) and index by NameId
        Map<NameId, Object> foldedById = HashMap.newHashMap(fields.size());
        for (var f : fields) {
            foldedById.put(f.id(), f.child().fold(context.foldCtx()));
        }

        // Collect values aligned with deduplicated output
        var output = row.output();
        List<Object> values = new ArrayList<>(output.size());
        for (Attribute attr : output) {
            values.add(foldedById.get(attr.id()));
        }

        var blocks = BlockUtils.fromListRow(PlannerUtils.NON_BREAKING_BLOCK_FACTORY, values);
        return new LocalRelation(row.source(), output, LocalSupplier.of(blocks.length == 0 ? new Page(0) : new Page(blocks)));
    }
}
