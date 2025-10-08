/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.compute.data.Page;
import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

import java.util.List;

/**
 * STATS with no aggregates and no groupings can be replaced with a single, empty row.
 * This can happen due to expression pruning at optimization time, after all them are dropped,
 * eg.
 *
 * STATS a = count(*) by b | drop a, b
 */
public final class PruneEmptyAggregates extends OptimizerRules.OptimizerRule<Aggregate> {
    @Override
    protected LogicalPlan rule(Aggregate agg) {
        if (agg.aggregates().isEmpty() && agg.groupings().isEmpty()) {
            return new LocalRelation(agg.source(), List.of(), LocalSupplier.of(new Page(1)));
        }
        return agg;
    }

}
