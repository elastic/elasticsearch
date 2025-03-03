/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.optimizer.rules.logical;

import org.elasticsearch.xpack.esql.plan.logical.EsRelation;
import org.elasticsearch.xpack.esql.plan.logical.LogicalPlan;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalRelation;
import org.elasticsearch.xpack.esql.plan.logical.local.LocalSupplier;

public final class SkipQueryOnEmptyMappings extends OptimizerRules.OptimizerRule<EsRelation> {

    @Override
    protected LogicalPlan rule(EsRelation plan) {
        return plan.concreteIndices().isEmpty() ? new LocalRelation(plan.source(), plan.output(), LocalSupplier.EMPTY) : plan;
    }
}
