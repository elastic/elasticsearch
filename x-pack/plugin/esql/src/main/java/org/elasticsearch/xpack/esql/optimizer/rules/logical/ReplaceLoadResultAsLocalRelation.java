/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.optimizer.LogicalOptimizerContext;
import org.elasticsearch.xpack.esql.plan.logical.LoadResult;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

public final class ReplaceLoadResultAsLocalRelation extends OptimizerRules.ParameterizedOptimizerRule<LoadResult, LogicalOptimizerContext> {
    public ReplaceLoadResultAsLocalRelation() {
        super(OptimizerRules.TransformDirection.DOWN);
    }

    @Override
    protected LogicalPlan rule(LoadResult loadResult, LogicalOptimizerContext context) {
        // Return an empty page with no rows, as requested
        return new LocalRelation(loadResult.source(), loadResult.output(), LocalSupplier.of(new Page(0)));
    }
}

