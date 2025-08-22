/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.Aggregate;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.EmptyLocalSupplier;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;

import java.util.List;

public final class PruneEmptyAggregates extends OptimizerRules.OptimizerRule<Aggregate> {
    @Override
    protected LogicalPlan rule(Aggregate agg) {
        if (agg.aggregates().isEmpty() && agg.groupings().isEmpty()) {
            // TODO this is wrong, it should return -one- row with -no- columns, but I can't represent it as an array of blocks...
            // Needs some refactoring to LocalSupplier
            return new LocalRelation(agg.source(), List.of(), EmptyLocalSupplier.EMPTY);
        }
        return agg;
    }

}
